-- ============================================================================
-- Stall-safe pruning of the LakeTS CDF shadow (app-owned extension). OFF by default.
--
-- lakets_cdf._shadow_<industry>_sensor_readings exists only to feed Lakehouse Sync
-- (wal2delta) into the UC history table, but LakeTS never removes rows from it:
-- 6.6 GB / 28 M rows after one day at 500 Hz (docs/evidence/2026-11-21).
--
-- SAFETY RULE: a shadow row may be deleted only once the sync has committed its
-- insert to Delta. Shadow rows carry no LSN, so every prune call records a mark:
-- the WAL insert position, the snapshot xmin (oldest transaction still running) and
-- the highest shadow id visible at that moment. A row whose inserting transaction
-- (its xmin) precedes the mark's xmin had finished before the mark, so its commit
-- record is below mark_lsn; once wal2delta's committed_lsn >= mark_lsn the row is
-- durable in Delta. In-flight or later transactions are never touched, whatever
-- their ids. Fails closed: nothing is deleted unless the shadow is STREAMING
-- (lakets._cdf_committed_lsn() returns NULL otherwise), so a stalled sync (frozen
-- committed_lsn) can never be overtaken.
--
-- RECOVERY RISK (why pruning needs an explicit acknowledgement): after pruning the
-- shadow holds only the last p_keep (~2 h). If Lakehouse Sync ever RE-SNAPSHOTS the
-- shadow and that re-creates (replaces) the UC history table instead of appending
-- to it, all older cold history is lost for good. Whether a resnapshot appends or
-- replaces is an open question for the Lakebase team (12_lakehouse_sync_escalation.md).
-- prune_cdf_shadow() therefore refuses until lakets._cdf_prune_acknowledgements
-- holds a row for the shadow (written by the app only with
-- LAKETS_SHADOW_PRUNE_ACK_HISTORY_RISK=true, see services/lakets/retention.py).
--
-- COST: the shadow is REPLICA IDENTITY FULL, so every pruned row writes a full-row
-- delete record to WAL (about as much WAL as its insert) and the sync appends a
-- `delete` change row to the history table (Delta history is append-only: the
-- delete is logged, not applied), roughly doubling the cold tier's row count. The
-- app and Genie read only `_pg_change_type IN ('insert', 'update_postimage')`; the
-- health check does too (CDC_ROW_FILTER in the notebook's queries cell).
--
-- PERFORMANCE: rows are found by an id range through the shadow's existing unique
-- index on id (no extra index, so no extra write cost at 2,000 rows/s): the bound
-- is the max id recorded with a mark that is both behind the committed position
-- and older than p_keep. Each call deletes one bounded batch so the caller commits
-- between batches (no giant transaction for logical decoding).
-- ============================================================================

-- Marks and acknowledgements live in schema lakets without REPLICA IDENTITY FULL,
-- so wal2delta skips them.
CREATE TABLE IF NOT EXISTS lakets._cdf_prune_marks (
    id           BIGSERIAL PRIMARY KEY,
    shadow_table TEXT        NOT NULL,
    taken_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    mark_lsn     PG_LSN      NOT NULL,
    snap_xmin    XID         NOT NULL,
    max_id       BIGINT
);
ALTER TABLE lakets._cdf_prune_marks ADD COLUMN IF NOT EXISTS max_id BIGINT;
CREATE INDEX IF NOT EXISTS idx_cdf_prune_marks_shadow
    ON lakets._cdf_prune_marks (shadow_table, id DESC);

CREATE TABLE IF NOT EXISTS lakets._cdf_prune_acknowledgements (
    shadow_table    TEXT PRIMARY KEY,
    acknowledged_by TEXT        NOT NULL,
    acknowledged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    statement       TEXT        NOT NULL
);

-- Delete one batch of durable shadow rows inserted more than p_keep ago. Returns a
-- summary; "deleted" = 0 means nothing (more) is safe to prune right now.
DROP FUNCTION IF EXISTS lakets.prune_cdf_shadow(TEXT, INTERVAL, INT);
CREATE FUNCTION lakets.prune_cdf_shadow(
    p_shadow_name TEXT,
    p_keep        INTERVAL DEFAULT '2 hours',
    p_batch       INT      DEFAULT 50000
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_rel       REGCLASS;
    v_committed PG_LSN;
    v_mark      RECORD;
    v_deleted   BIGINT := 0;
BEGIN
    IF p_batch IS NULL OR p_batch <= 0 THEN
        RAISE EXCEPTION 'p_batch must be positive';
    END IF;
    v_rel := to_regclass(format('lakets_cdf.%I', p_shadow_name));
    IF v_rel IS NULL THEN
        RAISE EXCEPTION 'lakets_cdf.% does not exist', p_shadow_name;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM lakets._chronotable_registry
                   WHERE shadow_table_name = p_shadow_name AND sync_enabled) THEN
        RAISE EXCEPTION '% is not the shadow of a synced ChronoTable', p_shadow_name;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_attribute
                   WHERE attrelid = v_rel AND attname = 'id' AND NOT attisdropped) THEN
        RAISE EXCEPTION 'lakets_cdf.% has no id column to prune by', p_shadow_name;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM lakets._cdf_prune_acknowledgements
                   WHERE shadow_table = p_shadow_name) THEN
        RETURN jsonb_build_object('status', 'refused', 'deleted', 0,
            'reason', 'history-loss risk not acknowledged (a resnapshot that replaces the '
                      'UC history table would lose history older than the shadow keeps)');
    END IF;

    -- Always leave a mark so a later call can prune what is durable by then.
    EXECUTE format(
        'INSERT INTO lakets._cdf_prune_marks (shadow_table, mark_lsn, snap_xmin, max_id) '
        'SELECT $1, pg_current_wal_insert_lsn(), '
        '       ((pg_snapshot_xmin(pg_current_snapshot())::text::bigint %% 4294967296)::text)::xid, '
        '       (SELECT max(id) FROM lakets_cdf.%I)', p_shadow_name)
    USING p_shadow_name;

    v_committed := lakets._cdf_committed_lsn(p_shadow_name);
    IF v_committed IS NULL THEN
        RETURN jsonb_build_object('status', 'skipped', 'reason', 'shadow not STREAMING',
                                  'deleted', 0);
    END IF;

    -- Newest mark that the sync has committed past AND that is older than p_keep.
    SELECT id, mark_lsn, snap_xmin, max_id, taken_at INTO v_mark
    FROM lakets._cdf_prune_marks
    WHERE shadow_table = p_shadow_name AND mark_lsn <= v_committed
      AND taken_at <= now() - p_keep AND max_id IS NOT NULL
    ORDER BY id DESC
    LIMIT 1;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('status', 'skipped', 'deleted', 0,
            'reason', 'no mark both behind the committed position and older than p_keep',
            'committed_lsn', v_committed::text);
    END IF;

    -- id <= max_id: an index range from the oldest rows; age(xmin) > age(mark.snap_xmin)
    -- <=> the inserting transaction precedes the mark's oldest running one
    -- (wraparound-safe; frozen rows count as old). In-flight stragglers stay.
    EXECUTE format(
        'WITH doomed AS ('
        '  SELECT ctid FROM lakets_cdf.%1$I'
        '  WHERE id <= $1 AND age(xmin) > age($2)'
        '  ORDER BY id'
        '  LIMIT $3)'
        ' DELETE FROM lakets_cdf.%1$I s USING doomed d WHERE s.ctid = d.ctid',
        p_shadow_name)
    USING v_mark.max_id, v_mark.snap_xmin, p_batch;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    -- Marks older than the one just used can never be needed again.
    DELETE FROM lakets._cdf_prune_marks
    WHERE shadow_table = p_shadow_name AND id < v_mark.id;

    RETURN jsonb_build_object(
        'status', 'ok', 'deleted', v_deleted,
        'committed_lsn', v_committed::text, 'mark_lsn', v_mark.mark_lsn::text,
        'mark_max_id', v_mark.max_id, 'mark_taken_at', v_mark.taken_at);
END;
$$;

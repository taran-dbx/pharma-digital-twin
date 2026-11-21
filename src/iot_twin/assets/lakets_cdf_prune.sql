-- ============================================================================
-- Stall-safe pruning of the LakeTS CDF shadow (app-owned extension).
--
-- lakets_cdf._shadow_<industry>_sensor_readings exists only to feed Lakehouse Sync
-- (wal2delta) into the UC history table, but LakeTS never removes rows from it:
-- 6.6 GB / 28 M rows after one day at 500 Hz (docs/evidence/2026-11-21).
--
-- SAFETY RULE: a shadow row may be deleted only once the sync has committed its
-- insert to Delta. Shadow rows carry no LSN, so every prune call records a mark
-- (WAL position + the snapshot xmin = oldest transaction still running). A row
-- whose inserting transaction (its xmin) precedes the xmin of a mark M had
-- finished before M was taken, so its commit record is below M.mark_lsn; once
-- wal2delta's committed_lsn >= M.mark_lsn the row is durable in Delta. In-flight
-- or later transactions are never touched, whatever their ids or event times.
-- Fails closed: nothing is deleted unless the shadow is STREAMING
-- (lakets._cdf_committed_lsn() returns NULL otherwise), so a stalled sync
-- (committed_lsn frozen) can never be overtaken.
--
-- Deletes reach the history table as `delete` change rows; the app, Genie and
-- the health check read only `_pg_change_type IN ('insert', 'update_postimage')`.
-- Each call deletes at most one bounded batch so the caller commits between
-- batches (no giant transaction for logical decoding).
-- ============================================================================

-- Marks live in schema lakets without REPLICA IDENTITY FULL, so wal2delta skips them.
CREATE TABLE IF NOT EXISTS lakets._cdf_prune_marks (
    id           BIGSERIAL PRIMARY KEY,
    shadow_table TEXT        NOT NULL,
    taken_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    mark_lsn     PG_LSN      NOT NULL,
    snap_xmin    XID         NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cdf_prune_marks_shadow
    ON lakets._cdf_prune_marks (shadow_table, id DESC);

-- Delete one batch of durable shadow rows older than p_keep. Returns a summary;
-- "deleted" = 0 means nothing (more) is safe to prune right now.
CREATE OR REPLACE FUNCTION lakets.prune_cdf_shadow(
    p_shadow_name TEXT,
    p_keep        INTERVAL DEFAULT '2 hours',
    p_batch       INT      DEFAULT 50000
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_time_col  TEXT;
    v_committed PG_LSN;
    v_mark      RECORD;
    v_deleted   BIGINT := 0;
BEGIN
    IF p_batch IS NULL OR p_batch <= 0 THEN
        RAISE EXCEPTION 'p_batch must be positive';
    END IF;
    IF to_regclass(format('lakets_cdf.%I', p_shadow_name)) IS NULL THEN
        RAISE EXCEPTION 'lakets_cdf.% does not exist', p_shadow_name;
    END IF;
    SELECT time_column INTO v_time_col
    FROM lakets._chronotable_registry
    WHERE shadow_table_name = p_shadow_name AND sync_enabled;
    IF v_time_col IS NULL THEN
        RAISE EXCEPTION '% is not the shadow of a synced ChronoTable', p_shadow_name;
    END IF;

    -- Always leave a mark so a later call can prune what is durable by then.
    INSERT INTO lakets._cdf_prune_marks (shadow_table, mark_lsn, snap_xmin)
    VALUES (p_shadow_name, pg_current_wal_lsn(),
            -- xid8 -> 32-bit xid (the type of a row's xmin), wraparound-safe for age().
            ((pg_snapshot_xmin(pg_current_snapshot())::text::bigint % 4294967296)::text)::xid);

    v_committed := lakets._cdf_committed_lsn(p_shadow_name);
    IF v_committed IS NULL THEN
        RETURN jsonb_build_object('status', 'skipped', 'reason', 'shadow not STREAMING',
                                  'deleted', 0);
    END IF;

    -- Newest mark the sync has already committed past.
    SELECT id, mark_lsn, snap_xmin, taken_at INTO v_mark
    FROM lakets._cdf_prune_marks
    WHERE shadow_table = p_shadow_name AND mark_lsn <= v_committed
    ORDER BY id DESC
    LIMIT 1;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('status', 'skipped', 'deleted', 0,
            'reason', 'no mark behind the committed position yet',
            'committed_lsn', v_committed::text);
    END IF;

    -- age(xmin) > age(mark.snap_xmin) <=> the row's inserting transaction precedes
    -- the mark's oldest running transaction (wraparound-safe; frozen rows count as old).
    EXECUTE format(
        'WITH doomed AS ('
        '  SELECT ctid FROM lakets_cdf.%1$I'
        '  WHERE age(xmin) > age($1) AND %2$I < now() - $2'
        '  LIMIT $3)'
        ' DELETE FROM lakets_cdf.%1$I s USING doomed d WHERE s.ctid = d.ctid',
        p_shadow_name, v_time_col)
    USING v_mark.snap_xmin, p_keep, p_batch;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    -- Marks older than the one just used can never be needed again.
    DELETE FROM lakets._cdf_prune_marks
    WHERE shadow_table = p_shadow_name AND id < v_mark.id;

    RETURN jsonb_build_object(
        'status', 'ok', 'deleted', v_deleted,
        'committed_lsn', v_committed::text, 'mark_lsn', v_mark.mark_lsn::text,
        'mark_taken_at', v_mark.taken_at);
END;
$$;

-- ============================================================================
-- Set-based INSERT triggers for LakeTS ChronoTables (app-owned extension).
--
-- LakeTS 0.1.0 installs two FOR EACH ROW triggers on a ChronoTable:
--   trg_lakets_lvc   AFTER INSERT                    -> upsert into the last-value cache
--   trg_lakets_sync  AFTER INSERT OR UPDATE OR DELETE -> copy into the lakets_cdf shadow
-- Each runs PL/pgSQL with registry lookups and dynamic SQL once per inserted row.
-- Measured on a branch of the live pipeline database (2026-11-21, docs/evidence/
-- 2026-11-21): ~0.7 ms of server time per row, so one writer tops out near
-- 1,300 rows/s and a 100-row flush takes ~74 ms. The same work done once per
-- statement from a transition table takes ~9 ms per 100 rows (~7.5x) and writes
-- ~22 % less WAL per row.
--
-- This file replaces only the INSERT path. UPDATE/DELETE keep the per-row
-- LakeTS sync trigger (it must find the old row in the shadow). Every statement
-- is idempotent; lakets.use_statement_insert_triggers() swaps the triggers of
-- an existing ChronoTable in one transaction, so no insert is ever copied twice
-- or skipped. Inserts must go through the partitioned parent (as every writer
-- here does): statement triggers on the parent do not fire for direct writes
-- into a partition.
-- ============================================================================

-- Shadow copy for inserted rows: one INSERT ... SELECT per statement.
CREATE OR REPLACE FUNCTION lakets._sync_insert_stmt_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_shadow TEXT;
BEGIN
    SELECT shadow_table_name INTO v_shadow
    FROM lakets._chronotable_registry
    WHERE schema_name = TG_TABLE_SCHEMA
      AND table_name = TG_TABLE_NAME
      AND sync_enabled = TRUE;

    IF v_shadow IS NOT NULL THEN
        -- The shadow mirrors the source columns in attribute order
        -- (lakets._build_shadow_table), exactly what SELECT ($1).* produced.
        EXECUTE format('INSERT INTO lakets_cdf.%I SELECT * FROM _lakets_new_rows', v_shadow);
    END IF;
    RETURN NULL;
END;
$$;

-- Last-value cache for inserted rows: one upsert per key per statement, taking
-- the newest row of each key by the ChronoTable's time column.
CREATE OR REPLACE FUNCTION lakets._lvc_insert_stmt_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_cache     TEXT;
    v_keys      TEXT[];
    v_vals      TEXT[];
    v_time_col  TEXT;
    v_key_list  TEXT;
    v_cols      TEXT;
    v_set       TEXT;
BEGIN
    SELECT lr.cache_table_name, lr.key_columns, lr.value_columns, hr.time_column
    INTO v_cache, v_keys, v_vals, v_time_col
    FROM lakets._lvc_registry lr
    JOIN lakets._chronotable_registry hr ON lr.chronotable_id = hr.id
    WHERE hr.schema_name = TG_TABLE_SCHEMA
      AND hr.table_name = TG_TABLE_NAME;

    IF v_cache IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT string_agg(format('%I', c), ', ') INTO v_key_list FROM unnest(v_keys) AS c;
    SELECT string_agg(format('%I', c), ', ') INTO v_cols FROM unnest(v_keys || v_vals) AS c;
    SELECT string_agg(format('%1$I = EXCLUDED.%1$I', c), ', ') INTO v_set FROM unnest(v_vals) AS c;

    -- DISTINCT ON keeps one row per key, so ON CONFLICT never touches a row twice.
    EXECUTE format(
        'INSERT INTO %1$I.%2$I (%3$s, last_updated) '
        'SELECT DISTINCT ON (%4$s) %3$s, now() FROM _lakets_new_rows '
        'ORDER BY %4$s, %5$I DESC '
        'ON CONFLICT (%4$s) DO UPDATE SET %6$s, last_updated = now()',
        TG_TABLE_SCHEMA, v_cache, v_cols, v_key_list, v_time_col, v_set
    );
    RETURN NULL;
END;
$$;

-- Swap an existing ChronoTable from per-row INSERT triggers to the statement
-- triggers above. Idempotent; returns what it found and changed.
CREATE OR REPLACE FUNCTION lakets.use_statement_insert_triggers(
    p_table_name  TEXT,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_id        INT;
    v_sync      BOOLEAN;
    v_lvc       BOOLEAN;
    v_relid     OID;
BEGIN
    SELECT id, sync_enabled INTO v_id, v_sync
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION '%.% is not a registered ChronoTable', p_schema_name, p_table_name;
    END IF;
    v_relid := format('%I.%I', p_schema_name, p_table_name)::regclass;
    v_lvc := EXISTS (SELECT 1 FROM lakets._lvc_registry WHERE chronotable_id = v_id);

    -- Writers wait (bounded) instead of racing the swap: no insert can run
    -- between dropping a per-row trigger and creating its replacement.
    SET LOCAL lock_timeout = '15s';
    EXECUTE format('LOCK TABLE %I.%I IN SHARE ROW EXCLUSIVE MODE', p_schema_name, p_table_name);

    IF COALESCE(v_sync, FALSE) THEN
        EXECUTE format(
            'CREATE OR REPLACE TRIGGER trg_lakets_sync '
            'AFTER UPDATE OR DELETE ON %I.%I '
            'FOR EACH ROW EXECUTE FUNCTION lakets._sync_trigger_fn()',
            p_schema_name, p_table_name);
        EXECUTE format(
            'CREATE OR REPLACE TRIGGER trg_lakets_sync_insert '
            'AFTER INSERT ON %I.%I REFERENCING NEW TABLE AS _lakets_new_rows '
            'FOR EACH STATEMENT EXECUTE FUNCTION lakets._sync_insert_stmt_fn()',
            p_schema_name, p_table_name);
    END IF;

    IF v_lvc THEN
        EXECUTE format('DROP TRIGGER IF EXISTS trg_lakets_lvc ON %I.%I',
            p_schema_name, p_table_name);
        EXECUTE format(
            'CREATE OR REPLACE TRIGGER trg_lakets_lvc_insert '
            'AFTER INSERT ON %I.%I REFERENCING NEW TABLE AS _lakets_new_rows '
            'FOR EACH STATEMENT EXECUTE FUNCTION lakets._lvc_insert_stmt_fn()',
            p_schema_name, p_table_name);
    END IF;

    RETURN jsonb_build_object(
        'table', format('%s.%s', p_schema_name, p_table_name),
        'sync', COALESCE(v_sync, FALSE),
        'lvc', v_lvc,
        'row_insert_triggers', (
            SELECT COALESCE(jsonb_agg(tgname ORDER BY tgname), '[]'::jsonb)
            FROM pg_trigger
            WHERE tgrelid = v_relid AND NOT tgisinternal
              AND (tgtype & 1) = 1      -- FOR EACH ROW
              AND (tgtype & 4) = 4      -- INSERT
        )
    );
END;
$$;

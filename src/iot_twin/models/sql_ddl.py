"""SQL DDL constants for Lakebase provisioning.

``SENSOR_READINGS_DDL`` is **generated from the SSOT field-map** in
``models/sensor_schema.py`` so the sensor-readings table can never drift from
the Kafka parse schema, Delta DDL, INSERT list and zerobus record.  It keeps
the ``{table_name}`` placeholder so existing ``.format(table_name=...)`` call
sites keep working (the generated DDL contains no other braces).
"""

from __future__ import annotations

from iot_twin.models.sensor_schema import pg_chronotable_ddl

# LakeTS — probe installed version.  Returns NULL/empty if the toolkit is absent.
LAKETS_VERSION_CHECK_SQL = "SELECT version FROM lakets._version ORDER BY installed_at DESC LIMIT 1"

# Derived from the SSOT — now includes min_value/max_value/nominal_value/metadata
# (previously dropped → silent data loss).
SENSOR_READINGS_DDL = pg_chronotable_ddl("{table_name}")

MATVIEW_1M_DDL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_1m AS
SELECT
    date_trunc('minute', event_time) AS bucket,
    sensor_name, sensor_type, industry,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    COUNT(*) AS sample_count,
    BOOL_OR(fault_active) AS any_fault
FROM sensor_readings
GROUP BY bucket, sensor_name, sensor_type, industry
"""

MATVIEW_5M_DDL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_5m AS
SELECT
    date_trunc('hour', event_time)
      + INTERVAL '5 min' * FLOOR(EXTRACT(MINUTE FROM event_time) / 5) AS bucket,
    sensor_type, industry,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    COUNT(*) AS sample_count,
    BOOL_OR(fault_active) AS any_fault
FROM sensor_readings
GROUP BY bucket, sensor_type, industry
"""

MATVIEW_INDEX_DDL = [
    "CREATE UNIQUE INDEX IF NOT EXISTS sensor_1m_pk ON sensor_1m (bucket, sensor_name, sensor_type, industry)",
    "CREATE UNIQUE INDEX IF NOT EXISTS sensor_5m_pk ON sensor_5m (bucket, sensor_type, industry)",
]

SOURCE_BINDINGS_DDL = """
CREATE TABLE IF NOT EXISTS source_bindings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id UUID NOT NULL,
    source_type TEXT NOT NULL,
    source_key TEXT NOT NULL,
    partition_count INT,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_by TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    CONSTRAINT fk_pipeline FOREIGN KEY (pipeline_id)
        REFERENCES pipelines(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_source
    ON source_bindings (source_type, source_key)
    WHERE status = 'active';
"""

# Lakebase role constants
LAKEBASE_ROLE_NAME = "iot_app_user"
LAKEBASE_ROLE_ID = "iot-app-user"

/**
 * Final deploy artifacts, scoped to the selected pipeline modes. Kafka modes
 * render their Spark consumer notebooks; Zerobus renders its serverless SDK
 * producer. Mixed configurations use tabs rather than stacking long previews.
 */
import { useEffect, useState } from "react";
import { FileCode2 } from "lucide-react";

import type { PipelineMode } from "@/lib/industries-api";
import { cn } from "@/lib/utils";
import { MODE_LABELS } from "./types";

interface NotebookArtifact {
  tab: string;
  filename: string;
  runtime: string;
  source: string;
}

const NOTEBOOKS: Record<PipelineMode, NotebookArtifact> = {
  realtime: {
    tab: "Real-time consumer",
    filename: "iot_pharmaceutical_realtime_consumer.py",
    runtime: "Spark job cluster",
    source: `# Databricks notebook source
# Generated from the SSOT field-map — do not edit by hand.
from pyspark.sql import functions as F

INDUSTRY = "pharmaceutical"
TOPIC = f"iot.{INDUSTRY}.realtime"
BOOTSTRAP = dbutils.widgets.get("bootstrap_servers")

raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .load())

(parse_sensor_records(raw).writeStream
    .trigger(processingTime="5 minutes")
    .foreachBatch(write_lakebase_chronotable)
    .option("checkpointLocation", CHECKPOINT)
    .start())`,
  },
  microbatch: {
    tab: "Micro-batch consumer",
    filename: "iot_pharmaceutical_microbatch_consumer.py",
    runtime: "Spark job cluster",
    source: `# Databricks notebook source
# Generated from the SSOT field-map — do not edit by hand.
from pyspark.sql import functions as F

INDUSTRY = "pharmaceutical"
TOPIC = f"iot.{INDUSTRY}.microbatch"
BOOTSTRAP = dbutils.widgets.get("bootstrap_servers")

records = read_kafka_sensor_records(TOPIC, BOOTSTRAP)
(records.writeStream
    .trigger(processingTime="15 seconds")
    .toTable(f"<UC_CATALOG>.<UC_SCHEMA>.{INDUSTRY}_microbatch"))`,
  },
  zerobus: {
    tab: "Zerobus producer",
    filename: "iot_pharmaceutical_zerobus_producer.py",
    runtime: "Serverless Python · no cluster",
    source: `# Databricks notebook source
# Serverless Zerobus producer generated from the SSOT field-map.
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk

INDUSTRY = "pharmaceutical"
TARGET = f"<UC_CATALOG>.<UC_SCHEMA>.{INDUSTRY}_zerobus"

client_id = "<DATABRICKS_CLIENT_ID>"
client_secret = dbutils.secrets.get(
    catalog="<UC_CATALOG>", schema="<UC_SCHEMA>", key="app_sp_oauth"
)

sdk = ZerobusSdk(ZEROBUS_SERVER_ENDPOINT, DATABRICKS_HOST)
stream = sdk.create_stream(
    client_id,
    client_secret,
    TableProperties(TARGET),
    StreamConfigurationOptions(record_type=RecordType.JSON),
)

for record in sensor_records():
    stream.ingest_record(record).wait_for_ack()
stream.close()`,
  },
};

export function StepNotebook({ enabledModes }: { enabledModes: PipelineMode[] }) {
  const [activeMode, setActiveMode] = useState<PipelineMode>(enabledModes[0] ?? "realtime");

  useEffect(() => {
    if (!enabledModes.includes(activeMode)) {
      setActiveMode(enabledModes[0] ?? "realtime");
    }
  }, [activeMode, enabledModes]);

  const artifact = NOTEBOOKS[activeMode];

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <FileCode2 className="h-4 w-4 text-primary" />
        <h3 className="font-display text-sm font-semibold">Deploy artifacts</h3>
        <span className="ml-auto text-xs text-muted-foreground">{artifact.runtime}</span>
      </div>

      {enabledModes.length > 1 && (
        <div
          role="tablist"
          aria-label="Pipeline mode notebooks"
          className="inline-flex rounded-lg border border-border/50 bg-surface-high p-1"
        >
          {enabledModes.map((mode) => (
            <button
              key={mode}
              type="button"
              role="tab"
              aria-selected={activeMode === mode}
              onClick={() => setActiveMode(mode)}
              className={cn(
                "rounded-md px-3 py-1.5 text-xs font-medium transition-colors",
                activeMode === mode
                  ? "bg-background text-foreground shadow-sm"
                  : "text-muted-foreground hover:text-foreground",
              )}
            >
              {NOTEBOOKS[mode].tab}
            </button>
          ))}
        </div>
      )}

      {enabledModes.length === 1 && (
        <div className="inline-flex items-center gap-2 rounded-full border border-primary/25 bg-primary/[0.05] px-3 py-1 text-xs">
          <span className="font-medium">{artifact.tab}</span>
          <span className="text-muted-foreground">· {MODE_LABELS[activeMode]}</span>
        </div>
      )}

      <div className="overflow-hidden rounded-lg border border-border/50 bg-surface-highest">
        <div className="flex items-center gap-1.5 border-b border-border/50 px-4 py-2">
          <span className="h-2.5 w-2.5 rounded-full bg-destructive/60" />
          <span className="h-2.5 w-2.5 rounded-full bg-warning/60" />
          <span className="h-2.5 w-2.5 rounded-full bg-success/60" />
          <span className="ml-2 font-mono text-[11px] text-muted-foreground">
            {artifact.filename}
          </span>
        </div>
        <pre className="overflow-x-auto p-4 font-mono text-[11.5px] leading-relaxed text-foreground">
          <code>{artifact.source}</code>
        </pre>
      </div>

      <div className="rounded-lg border border-border/50 bg-surface-high p-4 text-sm">
        <p className="font-medium">Generated for the selected modes only</p>
        <p className="mt-1 text-xs text-muted-foreground">
          Deploy unlocks after each selected mode&apos;s validation lane passes and its test data is
          purged. Existing healthy modes are not rebuilt when a new mode is added.
        </p>
      </div>
    </div>
  );
}

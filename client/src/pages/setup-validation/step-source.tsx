/**
 * Step 2 — Source config (spec §1.4 / §1.5).
 * Kafka / Kinesis segmented control; only `bootstrap_servers` is editable.
 * Region / RF / MSK ARN / service credential / partitions are locked,
 * auto-derived chips. A pre-flight IAM / UC note precedes validation.
 */
import { Radio, Waves, ShieldCheck } from "lucide-react";
import { Eyebrow, LockedChip, OptionCard } from "./components";
import { SOURCE_DEFAULTS, type SourceType } from "./types";

interface StepProps {
  sourceType: SourceType;
  onSourceTypeChange: (s: SourceType) => void;
  bootstrap: string;
  onBootstrapChange: (v: string) => void;
  partitions: number;
}

export function StepSource({
  sourceType,
  onSourceTypeChange,
  bootstrap,
  onBootstrapChange,
  partitions,
}: StepProps) {
  return (
    <div className="space-y-6">
      <div className="glass space-y-3 rounded-lg p-5">
        <Eyebrow>Source Type</Eyebrow>
        <div className="grid gap-3 sm:grid-cols-2">
          <OptionCard
            title="Apache Kafka"
            sub="Confluent / MSK"
            desc="Kafka / MSK source. You provide only bootstrap_servers — everything else is derived in code."
            icon={Radio}
            selected={sourceType === "kafka"}
            onSelect={() => onSourceTypeChange("kafka")}
          />
          <OptionCard
            title="Amazon Kinesis"
            sub="Coming soon"
            desc="Kinesis Data Streams source. Deferred — Kafka is the only selectable source today."
            icon={Waves}
            selected={false}
            disabled
            badge="Soon"
            onSelect={() => {}}
          />
        </div>
        <p className="text-xs text-muted-foreground">
          Kinesis has a different config set — coming soon.
        </p>
      </div>

      <div className="glass space-y-2 rounded-lg p-5">
        <Eyebrow>Bootstrap Servers</Eyebrow>
        <input
          type="text"
          value={bootstrap}
          onChange={(e) => onBootstrapChange(e.target.value)}
          placeholder="b-1.example.kafka.us-east-1.amazonaws.com:9098"
          className="w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
        />
        <p className="text-xs text-muted-foreground">
          The only field you provide. Everything below is derived in code from the broker hostname
          and the MSK API.
        </p>
      </div>

      <div className="glass space-y-3 rounded-lg p-5">
        <Eyebrow>Auto-derived (locked)</Eyebrow>
        <div className="grid gap-2.5 sm:grid-cols-2">
          <LockedChip label="Region" value={SOURCE_DEFAULTS.region} />
          <LockedChip label="Replication Factor" value={`RF ${SOURCE_DEFAULTS.replicationFactor} · min(3, ${SOURCE_DEFAULTS.brokerCount} brokers)`} />
          <LockedChip label="MSK Cluster ARN" value={SOURCE_DEFAULTS.mskArn} />
          <LockedChip label="UC Service Credential" value={SOURCE_DEFAULTS.serviceCredential} />
          <LockedChip label="Partitions" value={`${partitions} (drives worker sizing)`} />
          <LockedChip label="Security Protocol" value="SASL_SSL / IAM" />
        </div>
      </div>

      <div className="flex items-start gap-3 rounded-lg border border-info/40 bg-info/[0.08] p-4">
        <ShieldCheck className="mt-0.5 h-4 w-4 shrink-0 text-info" />
        <div className="space-y-1 text-sm">
          <p className="font-medium">Pre-flight checks (run before validation)</p>
          <ul className="list-disc space-y-0.5 pl-4 text-xs text-muted-foreground">
            <li>
              IAM role behind the service credential has MSK perms —{" "}
              <span className="font-mono">kafka:ListClusters/DescribeCluster</span>,{" "}
              <span className="font-mono">kafka-cluster:*</span> (incl. topic create).
            </li>
            <li>
              Running identity has UC <span className="font-mono">ACCESS</span> on the service
              credential.
            </li>
            <li>Per-mode write-identity grants exist (streaming SP · deploying user · zerobus SP).</li>
          </ul>
        </div>
      </div>
    </div>
  );
}

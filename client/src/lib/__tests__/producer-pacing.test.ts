import { describe, expect, it } from "vitest";

import { generateDataProducerNotebook } from "@/lib/data-producer-notebook";
import { PRODUCER_PACING_PYTHON } from "@/lib/producer-pacing";

// Name of the UC object that maps to the MSK IAM role (a placeholder, not a secret).
const MSK_ROLE_BINDING = "example-credential";

// Same connection shape industry-deploy-config.ts passes for a realtime deploy.
function realtimeProducer(rateHz: number): string {
  return generateDataProducerNotebook({
    wheelPath: "/Volumes/example/lib/iot_data_simulator-0.1.0-py3-none-any.whl",
    industry: "manufacturing",
    updateRateHz: rateHz,
    sensorConfig: [],
    source: "kafka",
    connection: {
      kafka: {
        bootstrap_servers: "b-1.example-cluster.abc123.c1.kafka.us-east-1.amazonaws.com:9198",
        topic: "example.manufacturing.realtime",
        consumer_group: "example-manufacturing",
        security_protocol: "SASL_SSL",
        service_credential: MSK_ROLE_BINDING,
        num_partitions: 3,
      },
    },
  });
}

describe("realtime producer pacing [C9208]", () => {
  it("embeds the wall-clock pacer and drives the generator with it", () => {
    const notebook = realtimeProducer(2000);
    expect(notebook).toContain(PRODUCER_PACING_PYTHON);
    expect(notebook).toContain("TARGET_RECORDS_PER_SEC = 2000");
    expect(notebook).toContain("pacer = RecordRatePacer(TARGET_RECORDS_PER_SEC, sensor_count)");
    expect(notebook).toContain("for _ in range(pacer.next_batch()):");
    expect(notebook).not.toContain("sim.run()");
    expect(notebook).not.toContain("${");
  });

  it("pins every sensor to one partition taken from the broker metadata", () => {
    const notebook = realtimeProducer(2000);
    expect(notebook).toContain("PARTITION_IDS = sorted(producer.partitions_for(TOPIC) or [])");
    expect(notebook).toContain("partition=SENSOR_PARTITION.get(record.sensor_name)");
    expect(notebook).toContain('key=record.sensor_name.encode("utf-8")');
  });
});

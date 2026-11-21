# Producer rate and Kafka partition assignment (code change; live proof pending the redeploy go)

## Why a 2000/s target sent ~1,780 rows/s

The realtime producer called `Simulator.run()` from the bundled `iot_data_simulator` wheel. Its
tick loop sleeps `interval - (generate + enqueue time)` after every tick. The Kafka work runs in
the sink's drain coroutine on the same event loop (`on_records`: JSON encoding plus
`producer.send` for the 19 records of a tick), and `asyncio.sleep` overshoots. Neither is
counted. At 2000/s the tick interval is 19 / 2000 = 9.5 ms, and the measured rate (~1,780/s,
both days; Spark `input_rps` 1,779-1,782) means each tick really took ~10.7 ms, i.e. ~1.2 ms
of uncounted time per tick.

It is not Kafka:
- `acks=1` and `linger_ms=0` do not throttle `send()`;
- the heartbeat's `ack_rps` matched the send rate;
- the 2400 target delivered 2,114-2,150 rows/s, the same ~11 % shortfall.

It is not partition skew either: skew affects the consumer, not the producer.

**Fix** (`client/src/lib/producer-pacing.ts`, wired into `data-producer-notebook.ts`):
`RecordRatePacer` schedules tick *n* at `start + n / tick_hz` on the wall clock. `next_batch()`
sleeps until the next tick is due and returns how many ticks are due, so a slow send is caught
up instead of lowering the rate. After a stall longer than 1 s the schedule restarts, so a pause
never turns into a burst. The producer drives the simulator's generator directly with it.

Unit tests (`tests/test_producer_pacing.py`) charge the measured costs: 1.1 ms of send per tick
and 0.2 ms of oversleep.
- The old sleep-per-tick loop reproduces ~1,780/s.
- The pacer stays within 0.5 % of 500, 2000 and 2400/s, never runs ahead of the schedule, and
  drops a 5 s outage instead of bursting it.

`vitest` checks the generated notebook wiring, and all its cells compile.

## Partition assignment

With 3 partitions, Kafka's murmur2 hash of the 19 sensor names put 9 sensors (47.4 % of rows) on
partition 0 (`03_partition_skew.txt`). The producer now sends every sensor to ONE fixed partition,
assigned round-robin over the sorted sensor names (`assign_partitions`). The message key stays
`sensor_name`.

| partitions | before (hash) | after (round-robin) |
|---|---|---|
| 3 | 9 / 6 / 4 sensors, busiest 47.4 % | 7 / 6 / 6, busiest 36.8 % |
| 6 | 4/4/3/5/2/1, busiest 26.3 % | 4/3/3/3/3/3, busiest 21.1 % |

**Ordering is preserved:** a sensor still lives on exactly one partition, so its rows stay in
order, which is what the consumer, the LVC and LakeTS's per-chunk bookkeeping assume. The
mapping is deterministic, so every producer restart picks the same partitions. Partition ids
come from the broker (`producer.partitions_for(TOPIC)`), with a fallback to `NUM_PARTITIONS`.
An unknown sensor falls back to the key hash. When a redeploy changes a sensor's partition,
only rows still in flight could interleave; the producer stops before a redeploy and the
backlog is ~0 s, so this is not observable.

## Plan: 3 -> 6 partitions on `pharma-twin.manufacturing.realtime` (NOT executed)

Not needed for throughput: after the trigger fix one Postgres writer handles ~8,000 rows/s,
~10x the busiest partition's ~740 rows/s (7 of 19 sensors) at 2,000 rows/s with the new assignment. It would cut
per-writer load further and gives RTM more parallelism if the rate grows. On the go:

1. **Permission check (read-only first).** `describe_topics` via the MSK-IAM admin client. The
   IAM role must allow `kafka-cluster:AlterTopic` on this topic only. Deletion is denied by
   policy and is not needed.
2. **Increase the count only.** `KafkaAdminClient.create_partitions({"pharma-twin.manufacturing.realtime": NewPartitions(total_count=6)})`.
   No other topic, no new topic.
3. **Consumer slots.** RTM needs one slot per partition: `compute_workers(6) = 2` workers of 4
   cores. Update the deployment's `partition_count` to 6 so the next deploy sizes the cluster.
4. **Redeploy through the app** (stop -> deploy -> start). The producer reads the new partition
   ids from the broker, and the RTM Kafka source discovers new partitions.
5. **Measure.** Per-partition rows/s (consumer metrics), lag, P99; then re-run the health check.

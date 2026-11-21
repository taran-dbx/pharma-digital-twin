// Python embedded in the generated realtime Kafka producer notebook
// (data-producer-notebook.ts). Kept in its own module so the Python tests can
// extract it and run it against a fake clock (tests/test_producer_pacing.py).
//
// Why: the simulator's own loop (Simulator.run) sleeps `interval - tick_time`
// after every tick. The Kafka send work runs in the sink's drain coroutine on
// the same event loop and asyncio.sleep overshoots, so neither is counted: at a
// 2000 rec/s target (19 sensors -> 9.5 ms per tick) each tick really took
// ~10.7 ms and the producer sent ~1,780 rec/s (docs/evidence/2026-11-21).
// RecordRatePacer schedules tick n at start + n / tick_hz on the wall clock, so
// time spent sending is absorbed instead of accumulating as drift.
//
// Partitioning: hashing 19 sensor names onto 3 partitions put 9 sensors (47 % of
// rows) on one partition. assign_partitions() gives each sensor ONE partition,
// round-robin over the sorted names, so every series stays in order (one
// partition per sensor, as before) and the load is within one sensor per
// partition (7/6/6 on 3 partitions, at most 4 of 19 on 6).

export const PRODUCER_PACING_PYTHON = `
class RecordRatePacer:
    """Wall-clock pacing for a target records/sec, one tick = records_per_tick records.

    Tick n is due at start + n / tick_hz. next_batch() sleeps until the next tick is
    due and returns how many ticks are due (>= 1): a slow send is caught up on the
    following ticks instead of lowering the rate. After a stall longer than
    max_catchup_s the schedule restarts from now, so a pause never turns into a
    burst of more than max_catchup_s worth of records.
    """

    def __init__(self, records_per_sec, records_per_tick, clock=None, sleep=None,
                 max_catchup_s=1.0):
        import time as _pacer_time

        if records_per_sec <= 0 or records_per_tick <= 0:
            raise ValueError("records_per_sec and records_per_tick must be positive")
        self.tick_hz = float(records_per_sec) / float(records_per_tick)
        self.interval = 1.0 / self.tick_hz
        self.clock = clock or _pacer_time.monotonic
        self.sleep = sleep or _pacer_time.sleep
        self.max_catchup_ticks = max(1, int(max_catchup_s * self.tick_hz))
        self.start = self.clock()
        self.emitted = 0
        self.resets = 0

    def next_batch(self):
        due_at = self.start + self.emitted * self.interval
        now = self.clock()
        if due_at > now:
            self.sleep(due_at - now)
            now = self.clock()
        due = int((now - self.start) / self.interval) + 1 - self.emitted
        if due > self.max_catchup_ticks:
            # Too far behind (stall, GC, network): restart the schedule from now.
            self.resets += 1
            self.start = now
            self.emitted = 0
            due = 1
        due = max(due, 1)
        self.emitted += due
        return due

    def achieved_rate(self, records_per_tick):
        elapsed = self.clock() - self.start
        return self.emitted * records_per_tick / elapsed if elapsed > 0 else 0.0


def assign_partitions(sensor_names, num_partitions):
    """Map every sensor to exactly one partition, round-robin over the sorted names."""
    if num_partitions <= 0:
        raise ValueError("num_partitions must be positive")
    return {name: index % num_partitions for index, name in enumerate(sorted(set(sensor_names)))}
`.trim();

"""Producer rate control and partition assignment [pipeline-throughput].

The Python lives in ``client/src/lib/producer-pacing.ts`` and is embedded in the generated
realtime Kafka producer notebook. These tests extract it and run it against a simulated clock
that charges the real per-tick costs (send work + sleep overshoot) measured on 2026-11-21.
"""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import pytest

CLIENT_LIB = Path(__file__).resolve().parent.parent / "client" / "src" / "lib"
PACING_TS = CLIENT_LIB / "producer-pacing.ts"
PRODUCER_TS = CLIENT_LIB / "data-producer-notebook.ts"

pytestmark = pytest.mark.skipif(not PACING_TS.exists(), reason="client/ not present")

# 19 manufacturing sensors seen live on 2026-11-21 (docs/evidence/2026-11-21/03_partition_skew.txt).
SENSORS = [
    "assy_line_1_motor_current", "assy_line_1_position", "assy_line_1_speed",
    "cnc_1_coolant_flow", "cnc_1_coolant_temp", "cnc_1_spindle_power", "cnc_1_spindle_speed",
    "cnc_1_spindle_temp", "cnc_1_vibration", "compressor_1_flow", "compressor_1_motor_temp",
    "compressor_1_pressure", "hvac_ahu_1_return_temp", "hvac_ahu_1_supply_temp",
    "hvac_zone_1_humidity", "hvac_zone_1_temp", "press_1_cycle_count",
    "press_1_hydraulic_oil_temp", "press_1_pressure",
]  # fmt: skip
SEND_COST_S = 0.0011  # per tick of 19 records: json + producer.send (unaccounted by Simulator.run)
OVERSLEEP_S = 0.0002


def _pacing_namespace() -> dict:
    text = PACING_TS.read_text()
    match = re.search(r"export const PRODUCER_PACING_PYTHON = `(?P<py>.*?)`\.trim\(\);", text, re.S)
    assert match is not None, "PRODUCER_PACING_PYTHON not found"
    namespace: dict = {}
    exec(compile(match.group("py"), str(PACING_TS), "exec"), namespace)  # noqa: S102 - our own template
    return namespace


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += max(seconds, 0.0) + OVERSLEEP_S


def _run_pacer(seconds: float, target: float = 2000.0, stall_at: float | None = None) -> tuple:
    ns = _pacing_namespace()
    fake = FakeClock()
    pacer = ns["RecordRatePacer"](target, len(SENSORS), clock=fake.clock, sleep=fake.sleep)
    records, batches, stalled = 0, [], False
    while fake.now < seconds:
        due = pacer.next_batch()
        batches.append(due)
        for _ in range(due):
            records += len(SENSORS)
            fake.now += SEND_COST_S
        if stall_at is not None and not stalled and fake.now >= stall_at:
            fake.now += 5.0  # e.g. a broker hiccup
            stalled = True
    return records / fake.now, pacer, batches


def test_naive_sleep_per_tick_reproduces_the_shortfall_c9201_regression() -> None:
    """[C9201][regression] Simulator.run()'s sleep-per-tick loop yields ~89 % of the target."""
    fake, interval, records = FakeClock(), len(SENSORS) / 2000.0, 0
    while fake.now < 60:
        start = fake.now
        records += len(SENSORS)  # generate + enqueue (counted)
        fake.now += 0.00005
        fake.sleep(max(0.0, interval - (fake.now - start)))  # the loop's own sleep
        fake.now += SEND_COST_S  # drain coroutine send time (not counted by the loop)
    rate = records / fake.now
    assert 1_700 < rate < 1_850, rate  # live: ~1,780 rec/s at the 2000 target


@pytest.mark.parametrize("target", [500.0, 2000.0, 2400.0])
def test_pacer_meets_the_target_despite_send_cost_c9202_smoke(target: float) -> None:
    """[C9202][smoke] Wall-clock scheduling absorbs send time: within 0.5 % of the target."""
    rate, pacer, _ = _run_pacer(120.0, target=target)
    assert rate == pytest.approx(target, rel=0.005)
    assert pacer.resets == 0


def test_pacer_never_runs_ahead_of_the_schedule_c9203_regression() -> None:
    """[C9203][regression] Records emitted by time t never exceed target * t (plus one tick)."""
    ns = _pacing_namespace()
    fake = FakeClock()
    pacer = ns["RecordRatePacer"](2000.0, len(SENSORS), clock=fake.clock, sleep=fake.sleep)
    emitted = 0
    for _ in range(5_000):
        emitted += pacer.next_batch() * len(SENSORS)
        assert emitted <= 2000.0 * fake.now + 2 * len(SENSORS)


def test_slow_tick_is_caught_up_but_long_stall_resets_c9204_regression() -> None:
    """[C9204][regression] Short delays are caught up; a 5 s stall is not replayed as a burst."""
    rate, pacer, batches = _run_pacer(30.0, stall_at=10.0)
    assert pacer.resets == 1
    assert max(batches) <= pacer.max_catchup_ticks
    # The 5 s outage is lost (not burst), the rest of the run is on target.
    assert rate == pytest.approx(2000.0 * 25.0 / 30.0, rel=0.02)


def test_pacer_rejects_non_positive_rates_c9205_smoke() -> None:
    """[C9205][smoke] A zero target is a configuration error, not a busy loop."""
    ns = _pacing_namespace()
    with pytest.raises(ValueError):
        ns["RecordRatePacer"](0, 19)
    with pytest.raises(ValueError):
        ns["assign_partitions"](SENSORS, 0)


@pytest.mark.parametrize(("partitions", "max_share"), [(3, 7 / 19), (6, 4 / 19)])
def test_partition_assignment_is_balanced_and_stable_c9206_smoke(
    partitions: int, max_share: float
) -> None:
    """[C9206][smoke] Each sensor has one partition; loads differ by at most one sensor."""
    ns = _pacing_namespace()
    mapping = ns["assign_partitions"](SENSORS, partitions)
    assert set(mapping) == set(SENSORS)
    counts = Counter(mapping.values())
    assert set(counts) == set(range(partitions))
    assert max(counts.values()) - min(counts.values()) <= 1
    assert max(counts.values()) / len(SENSORS) == pytest.approx(max_share)
    # Deterministic and order-independent: every producer restart picks the same partitions.
    assert ns["assign_partitions"](list(reversed(SENSORS)), partitions) == mapping


def test_deployed_producer_uses_the_pacer_and_fixed_partitions_c9207_regression() -> None:
    """[C9207][regression] The IAM producer cell no longer paces with Simulator.run()."""
    text = PRODUCER_TS.read_text()
    match = re.search(
        r"if \(hasCred\).*?mainCell = `(?P<cell>.*?)`\.trim\(\);\s*} else if \(hasInlineCreds\)",
        text,
        re.S,
    )
    assert match is not None
    cell = match.group("cell")
    assert "${PRODUCER_PACING_PYTHON}" in cell
    assert "pacer = RecordRatePacer(TARGET_RECORDS_PER_SEC, sensor_count)" in cell
    assert "for _ in range(pacer.next_batch()):" in cell
    assert "on_records(_generator.tick())" in cell
    assert "sim.run()" not in cell
    assert "CallbackSink(" not in cell
    assert "partition=SENSOR_PARTITION.get(record.sensor_name)" in cell
    assert 'key=record.sensor_name.encode("utf-8")' in cell
    assert "producer.partitions_for(TOPIC)" in cell

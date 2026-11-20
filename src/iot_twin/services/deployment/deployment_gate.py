"""Validation hard-gate for per-mode pipeline deploy (spec §1.12).

The admin cannot deploy a mode until **every** required validation step for
that mode is ``passed`` (and its test data purged).  This module is a thin,
per-mode facade over the single source of truth — the pure validation state
machine (``services/validation/validation_state_machine.py``) — so the deploy gate and the
``/validate/can-deploy`` route never disagree.

Historically this module read a *flat* ``validation_state`` with keys like
``mb_write_test`` / ``rt_*`` / ``zb_*`` while the state machine persists a
*nested* ``{"steps": {"microbatch:write_test": {"status": "passed"}}, ...}``
shape — so a fully-green setup looked entirely "outstanding" and every deploy
raised unless ``gate_override`` was forced.  We now delegate to the state
machine's per-mode helpers (nested keys, per-mode lanes; zerobus = its lane
only, no common Kafka steps per §1.10).
"""

from __future__ import annotations

from iot_twin.services.validation import validation_state_machine as sm

# Re-exported for callers/tests that reference the mode catalogue.  ``COMMON_STEPS``
# and the per-mode required-key lists are derived from the state machine so there
# is exactly one definition of "what a mode requires".
COMMON_STEPS: tuple[str, ...] = sm.COMMON_STEPS
VALID_MODES: tuple[str, ...] = tuple(sm.MODE_STEPS.keys())
REQUIRED_STEPS: dict[str, tuple[str, ...]] = {
    mode: tuple(sm.mode_required_keys(mode)) for mode in VALID_MODES
}


class DeployGateError(RuntimeError):
    """Raised when a deploy is attempted before validation passes."""

    def __init__(self, mode: str, outstanding: list[str]) -> None:
        self.mode = mode
        self.outstanding = outstanding
        super().__init__(
            f"Deploy gate for mode '{mode}' not satisfied — "
            f"{len(outstanding)} step(s) outstanding: {', '.join(outstanding)}"
        )


def outstanding_steps(validation_state: dict | None, mode: str) -> list[str]:
    """Return the required (nested) steps for ``mode`` that are not yet ``passed``."""
    if mode not in VALID_MODES:
        raise ValueError(f"unknown mode: {mode!r}")
    return sm.mode_outstanding_steps(validation_state or {}, mode)


def gate_passed(validation_state: dict | None, mode: str) -> bool:
    """True when every required validation step for ``mode`` is ``passed``."""
    if mode not in VALID_MODES:
        raise ValueError(f"unknown mode: {mode!r}")
    return sm.is_mode_gate_open(validation_state or {}, mode)


def gate_status(validation_state: dict | None, mode: str) -> dict:
    """Return the per-mode deploy-gate verdict (can_deploy / data_purged / outstanding)."""
    if mode not in VALID_MODES:
        raise ValueError(f"unknown mode: {mode!r}")
    return sm.mode_gate_status(validation_state or {}, mode)


def assert_gate_passed(validation_state: dict | None, mode: str) -> None:
    """Raise :class:`DeployGateError` unless the mode's validation gate passed."""
    outstanding = outstanding_steps(validation_state, mode)
    if outstanding:
        raise DeployGateError(mode, outstanding)

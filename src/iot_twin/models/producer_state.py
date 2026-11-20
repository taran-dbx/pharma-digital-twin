"""Per-pipeline data producer state helpers."""

from __future__ import annotations

from typing import Any


def new_producer_state() -> dict[str, Any]:
    """Return a fresh, idle producer state dict."""
    return {
        "running": False,
        "source_type": None,
        "source_label": None,
        "destination": None,
        "run_id": None,
        "state": "IDLE",
        "error": None,
    }


def source_label(source_type: str) -> str:
    """Human-readable label for a streaming source type."""
    labels = {
        "kafka": "Kafka",
        "kinesis": "Kinesis",
        "event_hub": "Event Hub",
        "rabbitmq": "RabbitMQ",
        "pubsub": "Pub/Sub",
    }
    return labels.get(source_type, source_type)

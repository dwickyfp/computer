"""
Lightweight in-process runtime metrics for compute health/debugging.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass
class MetricState:
    count: int = 0
    total: float = 0.0
    last: float = 0.0
    maximum: float = 0.0
    unit: str = "count"
    labels: dict[str, str] = field(default_factory=dict)


_lock = threading.Lock()
_metrics: dict[str, MetricState] = {}


def observe(name: str, value: float, *, unit: str = "ms", **labels: str) -> None:
    with _lock:
        state = _metrics.get(name)
        if state is None:
            state = MetricState(unit=unit, labels={k: str(v) for k, v in labels.items()})
            _metrics[name] = state
        state.count += 1
        state.total += float(value)
        state.last = float(value)
        state.maximum = max(state.maximum, float(value))


def increment_counter(
    name: str, amount: int = 1, *, unit: str = "count", **labels: str
) -> None:
    with _lock:
        state = _metrics.get(name)
        if state is None:
            state = MetricState(unit=unit, labels={k: str(v) for k, v in labels.items()})
            _metrics[name] = state
        state.count += int(amount)
        state.total += float(amount)
        state.last = float(amount)
        state.maximum = max(state.maximum, float(amount))


def set_gauge(name: str, value: float, *, unit: str = "count", **labels: str) -> None:
    with _lock:
        _metrics[name] = MetricState(
            count=1,
            total=float(value),
            last=float(value),
            maximum=float(value),
            unit=unit,
            labels={k: str(v) for k, v in labels.items()},
        )


def snapshot() -> dict[str, dict]:
    with _lock:
        result: dict[str, dict] = {}
        for name, state in _metrics.items():
            avg = state.total / state.count if state.count else 0.0
            result[name] = {
                "count": state.count,
                "last": state.last,
                "avg": avg,
                "max": state.maximum,
                "unit": state.unit,
                "labels": dict(state.labels),
            }
        return result


def reset() -> None:
    with _lock:
        _metrics.clear()

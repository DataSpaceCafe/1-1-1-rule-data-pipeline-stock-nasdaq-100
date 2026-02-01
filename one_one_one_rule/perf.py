from __future__ import annotations

import logging
import sys
import time
from functools import wraps

try:
    import resource
except Exception:  # pragma: no cover - platform-dependent
    resource = None


def _rss_mb() -> float:
    if resource is None:
        return float("nan")
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return usage / (1024 * 1024)
    return usage / 1024


def log_perf(func):
    """Log wall time, CPU time, and RSS memory for a function call."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_wall = time.perf_counter()
        start_cpu = time.process_time()
        start_rss = _rss_mb()
        try:
            return func(*args, **kwargs)
        finally:
            end_wall = time.perf_counter()
            end_cpu = time.process_time()
            end_rss = _rss_mb()

            wall_ms = (end_wall - start_wall) * 1000.0
            cpu_ms = (end_cpu - start_cpu) * 1000.0
            delta_rss = end_rss - start_rss

            logger = logging.getLogger("pipeline")
            logger.info(
                "perf %s wall_ms=%.2f cpu_ms=%.2f rss_mb=%.2f delta_rss_mb=%.2f",
                f"{func.__module__}.{func.__name__}",
                wall_ms,
                cpu_ms,
                end_rss,
                delta_rss,
            )

    return wrapper

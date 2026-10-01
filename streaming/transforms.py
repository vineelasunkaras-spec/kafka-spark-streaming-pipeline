"""Pure validation rules (unit-testable) + Spark expressions built from them."""

RULES = {
    "temperature_c": (-50.0, 200.0),
    "pressure_kpa": (50.0, 200.0),
}
REQUIRED = ["event_id", "device_id", "event_ts"]


def is_valid(event: dict) -> bool:
    if any(not event.get(f) for f in REQUIRED):
        return False
    for col, (lo, hi) in RULES.items():
        v = event.get(col)
        if v is None or not (lo <= v <= hi):
            return False
    return True


def validity_expr() -> str:
    """SQL expression equivalent of `is_valid` for Spark."""
    parts = [f"{f} IS NOT NULL AND {f} != ''" for f in REQUIRED]
    parts += [f"{c} BETWEEN {lo} AND {hi}" for c, (lo, hi) in RULES.items()]
    return " AND ".join(parts)

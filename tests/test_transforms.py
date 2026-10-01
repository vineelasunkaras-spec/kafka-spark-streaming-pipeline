from producer.event_producer import make_event
from streaming.transforms import is_valid, validity_expr


def test_generated_events_have_required_fields():
    e = make_event("dev-0001")
    assert e["device_id"] == "dev-0001"
    assert {"event_id", "event_ts", "site"} <= e.keys()


def test_out_of_range_is_invalid():
    e = make_event()
    e["temperature_c"] = 999
    assert not is_valid(e)


def test_missing_field_is_invalid():
    e = make_event()
    e["event_id"] = ""
    assert not is_valid(e)


def test_valid_event():
    e = make_event()
    e["temperature_c"], e["pressure_kpa"] = 70.0, 100.0
    assert is_valid(e)


def test_expr_mentions_all_rules():
    expr = validity_expr()
    assert "temperature_c BETWEEN" in expr and "device_id IS NOT NULL" in expr

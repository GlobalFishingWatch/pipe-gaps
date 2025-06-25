import pytest
from datetime import datetime, timezone, timedelta
from pipe_gaps.core import GapDetector, GapDetectionError


@pytest.fixture
def example_messages():
    # Example AIS messages sorted by timestamp
    base_time = datetime(2024, 1, 1, 0, tzinfo=timezone.utc).timestamp()
    return [
        {
            "ssvid": "12345",
            "timestamp": base_time,
            "lat": 37.7749,
            "lon": -122.4194,
            "receiver_type": "terrestrial",
        },
        {
            "ssvid": "12345",
            "timestamp": base_time + 3600,  # 1 hour later
            "lat": 37.7750,
            "lon": -122.4195,
            "receiver_type": "terrestrial",
        },
        {
            "ssvid": "12345",
            "timestamp": base_time + 7200,  # 2 hours later
            "lat": 37.7751,
            "lon": -122.4196,
            "receiver_type": "satellite",
        },
        {
            "ssvid": "12345",
            "timestamp": base_time + 43200,  # 12 hours later
            "lat": 37.7800,
            "lon": -122.4200,
            "receiver_type": "dynamic",
        },
    ]


def test_mandatory_keys():
    keys = GapDetector.mandatory_keys()
    assert set(keys) == {"ssvid", "timestamp", "lat", "lon", "receiver_type"}


def test_receiver_type_keys():
    keys = GapDetector.receiver_type_keys()
    assert set(keys) == {"terrestrial", "satellite", "dynamic"}


def test_generate_gap_id_consistency(example_messages):
    gap_id1 = GapDetector.generate_gap_id(example_messages[0])
    gap_id2 = GapDetector.generate_gap_id(example_messages[0])
    assert gap_id1 == gap_id2


def test_detect_no_gaps_if_threshold_not_met(example_messages):
    # threshold 24h means no gaps between 1h or 2h messages
    detector = GapDetector(threshold=24)
    gaps = detector.detect(example_messages)
    assert gaps == []


def test_detect_gaps_with_default_threshold(example_messages):
    detector = GapDetector()
    gaps = detector.detect(example_messages)
    # There should be one gap between 2h and 12h messages (gap = 10h)
    # Default threshold = 12h so 10h gap should NOT be detected
    # But 12h gap between message[2] (2h) and message[3] (12h) is exactly 10h? Let's calculate:
    # message[2].timestamp = base + 7200s (2h)
    # message[3].timestamp = base + 43200s (12h)
    # gap = 43200-7200 = 36000s = 10h < 12h threshold, so no gap
    # So no gaps detected
    assert gaps == []


def test_detect_gaps_with_lower_threshold(example_messages):
    detector = GapDetector(threshold=9, show_progress=True)
    gaps = detector.detect(example_messages)
    # Now gap of 10h > 9h threshold, so 1 gap expected
    assert len(gaps) == 1
    gap = gaps[0]
    assert gap["ssvid"] == "12345"
    assert gap["is_closed"] is True
    # Duration hours should be close to 10
    assert pytest.approx(gap["duration_h"], 0.1) == 10
    # Distance should be a positive float
    assert isinstance(gap["distance_m"], float)
    assert gap["distance_m"] > 0


def test_detect_sorted_input_order_independence(example_messages):
    # Shuffle messages to test sorting inside detect
    shuffled = example_messages[::-1]
    detector = GapDetector(threshold=0.5)
    gaps_sorted = detector.detect(example_messages)
    gaps_shuffled = detector.detect(shuffled)
    assert gaps_sorted == gaps_shuffled


def test_detect_with_start_time_skips_earlier_gaps(example_messages):
    # 0.5h threshold to detect all gaps
    detector = GapDetector(threshold=0.5, normalize_output=True)
    start_time = datetime.fromtimestamp(example_messages[2]["timestamp"], tz=timezone.utc)
    gaps = detector.detect(example_messages, start_time=start_time)
    # Only gaps starting from message[2] forward considered
    assert len(gaps) == 1
    for gap in gaps:
        assert gap["start_timestamp"] >= example_messages[2]["timestamp"]


def test_detect_raises_gap_detection_error_on_missing_key(example_messages):
    # Remove a mandatory key
    faulty_messages = [dict(m) for m in example_messages]
    del faulty_messages[0]["lat"]

    detector = GapDetector(threshold=0.5)
    with pytest.raises(GapDetectionError) as excinfo:
        detector.detect(faulty_messages)
    assert "Missing key" in str(excinfo.value)


def test_eval_open_gap_true_and_false():
    detector = GapDetector(threshold=12)
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp()

    message = {
        "ssvid": "999",
        "timestamp": base_time,
        "lat": 0,
        "lon": 0,
        "receiver_type": "terrestrial",
    }

    # Open gap when gap to end of day > threshold
    # Here timestamp at noon, 12h threshold, next day midnight is 12h ahead exactly
    assert detector.eval_open_gap(message) is False

    # Move timestamp back 1 hour to trigger open gap (gap > 12h)
    message["timestamp"] = base_time - (1 * 3600)
    assert detector.eval_open_gap(message) is True


def test_create_gap_closed_and_open(example_messages):
    detector = GapDetector(normalize_output=True)

    off_msg = example_messages[0]
    on_msg = example_messages[1]

    gap_closed = detector.create_gap(off_msg, on_msg)
    assert gap_closed["is_closed"] is True
    assert gap_closed["gap_id"] is not None
    assert gap_closed["ssvid"] == off_msg["ssvid"]
    assert gap_closed["start_timestamp"] == off_msg["timestamp"]
    assert gap_closed["end_timestamp"] == on_msg["timestamp"]

    # Create an open gap (on_m is None)
    gap_open = detector.create_gap(off_msg)
    assert gap_open["is_closed"] is False
    assert gap_open["end_timestamp"] is None


def test_off_message_from_gap_returns_off_message(example_messages):
    detector = GapDetector(normalize_output=True)

    off_msg = example_messages[0]
    on_msg = example_messages[1]
    gap = detector.create_gap(off_msg, on_msg, gap_id="testgap123")

    off_message = detector.off_message_from_gap(gap)
    # Off message should have keys without "start_" prefix and ssvid restored
    for key in off_msg:
        assert key in off_message

    assert off_message["ssvid"] == gap["ssvid"]


def test_gap_distance_meters_and_duration_and_speed(example_messages):
    detector = GapDetector()

    off_msg = example_messages[0]
    on_msg = example_messages[1]

    distance = detector._gap_distance_meters(off_msg, on_msg)
    duration = detector._gap_duration_seconds(off_msg, on_msg)
    speed = detector._gap_implied_speed_knots(distance, duration / 3600)

    assert distance > 0
    assert duration == on_msg["timestamp"] - off_msg["timestamp"]
    # Speed should be positive or None (if distance or duration is zero)
    assert speed is None or speed >= 0


def test_count_messages_before_gap(example_messages):
    detector = GapDetector()
    # Pick a timestamp in middle of example messages
    off_msg = example_messages[2]

    previous_msgs = detector._previous_positions(example_messages, off_msg)
    count = detector._count_messages_before_gap(previous_msgs)
    # Counts must be integers >= 0
    for key in detector.receiver_type_keys():
        assert count[key] >= 0
    assert count[detector.KEY_TOTAL] == sum(count[k] for k in detector.receiver_type_keys())


def test_normalize_off_on_messages(example_messages):
    detector = GapDetector(normalize_output=True)
    off_msg = example_messages[0]
    on_msg = example_messages[1]

    normalized = detector._normalize_off_on_messages(off_msg, on_msg)
    # Keys should start with "start_" or "end_"
    assert all(k.startswith("start_") or k.startswith("end_") for k in normalized.keys())
    # All off_msg keys present with prefix start_
    for k in off_msg.keys():
        assert f"start_{k}" in normalized
    for k in on_msg.keys():
        assert f"end_{k}" in normalized


# Optional: test internal sort method actually sorts
def test_sort_messages_sorts_unsorted():
    detector = GapDetector()
    messages = [
        {"timestamp": 3, "ssvid": "a", "lat": 0, "lon": 0, "receiver_type": "terrestrial"},
        {"timestamp": 1, "ssvid": "a", "lat": 0, "lon": 0, "receiver_type": "terrestrial"},
        {"timestamp": 2, "ssvid": "a", "lat": 0, "lon": 0, "receiver_type": "terrestrial"},
    ]
    detector._sort_messages(messages)
    timestamps = [m["timestamp"] for m in messages]
    assert timestamps == [1, 2, 3]


def test_min_gap_length_returns_threshold_as_timedelta():
    detector = GapDetector(threshold=5)  # 5 hours
    assert isinstance(detector.min_gap_length, timedelta)
    assert detector.min_gap_length == timedelta(hours=5)


def test_detect_returns_empty_when_start_time_after_all_messages(example_messages):
    # start_time far in the future
    start_time = datetime.utcfromtimestamp(2_000_000_000)  # ~2033

    detector = GapDetector(threshold=1)
    gaps = detector.detect(example_messages, start_time=start_time)

    assert gaps == []


def test_create_gap_uses_base_gap(example_messages):
    detector = GapDetector(threshold=1)

    off_m, on_m = example_messages[0], example_messages[1]

    base_gap = {
        "custom_field": "preserved"
    }

    gap = detector.create_gap(off_m, on_m, base_gap=base_gap)

    assert gap["custom_field"] == "preserved"
    assert "gap_id" in gap
    assert "distance_m" in gap
    assert "is_closed" in gap


def test_gap_distance_meters_handles_value_error(example_messages):
    detector = GapDetector()

    off_m = dict(example_messages[0])
    on_m = dict(example_messages[1])

    # Introduce invalid coordinate to cause geodesic() to raise ValueError
    off_m[detector.KEY_LAT] = None  # or float('nan')
    on_m[detector.KEY_LON] = None

    distance = detector._gap_distance_meters(off_m, on_m)

    assert distance is None


def test_gap_implied_speed_knots_handles_exceptions():
    detector = GapDetector()

    # Case 1: gap_distance_m is None (TypeError when dividing None)
    result_none_distance = detector._gap_implied_speed_knots(None, 1.0)
    assert result_none_distance is None

    # Case 2: gap_duration_h is zero (ZeroDivisionError)
    result_zero_duration = detector._gap_implied_speed_knots(1000.0, 0.0)
    assert result_zero_duration is None

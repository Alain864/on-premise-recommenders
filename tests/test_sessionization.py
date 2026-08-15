from datetime import datetime, timedelta

from onprem_recommenders.ranking import is_session_break


def test_session_gap():
    start = datetime(2026, 1, 1, 12, 0, 0)
    assert is_session_break(None, start) is True
    assert is_session_break(start, start + timedelta(minutes=30), gap_minutes=30) is False
    assert is_session_break(start, start + timedelta(minutes=31), gap_minutes=30) is True

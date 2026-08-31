from nowcoder_crawler.discovery.center import CenterStopTracker


def test_stops_after_three_consecutive_stale_pages() -> None:
    tracker = CenterStopTracker(3)
    assert tracker.observe(0) is False
    assert tracker.observe(0) is False
    assert tracker.observe(1) is False
    assert tracker.observe(0) is False
    assert tracker.observe(0) is False
    assert tracker.observe(0) is True

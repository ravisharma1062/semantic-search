from app.ingestion.offsets import OffsetTracker

TP = ("topic", 0)
OTHER = ("topic", 1)


def _tracker(*offsets: int, tp: tuple[str, int] = TP) -> OffsetTracker:
    tracker = OffsetTracker()
    for offset in offsets:
        tracker.register(tp, offset)
    return tracker


def test_nothing_to_commit_before_anything_finished() -> None:
    assert _tracker(0, 1).committable() == {}


def test_in_order_completion_moves_the_commit_up() -> None:
    tracker = _tracker(0, 1, 2)
    tracker.complete(TP, 0)
    assert tracker.committable() == {TP: 1}
    tracker.complete(TP, 1)
    assert tracker.committable() == {TP: 2}


def test_out_of_order_completion_waits_for_the_slow_message() -> None:
    tracker = _tracker(0, 1, 2)
    tracker.complete(TP, 2)
    tracker.complete(TP, 1)
    assert tracker.committable() == {}  # offset 0 is still running
    tracker.complete(TP, 0)
    assert tracker.committable() == {TP: 3}


def test_gaps_in_offsets_are_fine() -> None:
    tracker = _tracker(5, 7)  # offset 6 does not exist, for example after compaction
    tracker.complete(TP, 5)
    tracker.complete(TP, 7)
    assert tracker.committable() == {TP: 8}


def test_mark_committed_clears_only_what_was_committed() -> None:
    tracker = _tracker(0, 1)
    tracker.complete(TP, 0)
    offsets = tracker.committable()
    tracker.complete(TP, 1)  # moved on while the commit was running
    tracker.mark_committed(offsets)
    assert tracker.committable() == {TP: 2}


def test_failed_commit_is_offered_again() -> None:
    tracker = _tracker(0)
    tracker.complete(TP, 0)
    assert tracker.committable() == {TP: 1}
    assert tracker.committable() == {TP: 1}  # mark_committed was not called


def test_partitions_are_independent() -> None:
    tracker = _tracker(0, tp=TP)
    tracker.register(OTHER, 0)
    tracker.complete(OTHER, 0)
    assert tracker.committable() == {OTHER: 1}
    assert tracker.pending(TP) == 1


def test_drop_returns_the_offsets_still_to_commit_and_forgets_the_partition() -> None:
    tracker = _tracker(0, 1)
    tracker.complete(TP, 0)
    assert tracker.drop([TP]) == {TP: 1}
    assert tracker.pending(TP) == 0
    tracker.complete(TP, 1)  # a late finish after the revoke is ignored
    assert tracker.committable() == {}


def test_drop_of_an_unknown_partition_is_harmless() -> None:
    assert OffsetTracker().drop([TP]) == {}

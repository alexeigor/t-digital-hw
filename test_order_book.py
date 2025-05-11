import pytest
from main import OrderBook  # Replace 'your_module' with the actual module name

@pytest.fixture
def order_book():
    return OrderBook()


def test_update_from_snapshot(order_book):
    snapshot = {
        "lastUpdateId": 1001,
        "bids": [["100.0", "1.5"], ["99.5", "2.0"], ["99.0", "1.0"]],
        "asks": [["101.0", "1.0"], ["101.5", "2.5"], ["102.0", "1.0"]],
    }

    order_book.update_from_snapshot(snapshot)

    assert order_book.last_update_id == 1001
    assert len(order_book.bids) == 3
    assert len(order_book.asks) == 3
    assert order_book.bids[100.0] == 1.5
    assert order_book.asks[101.0] == 1.0


def test_apply_diff_add_and_remove(order_book):
    # Initial snapshot
    snapshot = {
        "lastUpdateId": 1001,
        "bids": [["100.0", "1.0"]],
        "asks": [["101.0", "1.0"]],
    }
    order_book.update_from_snapshot(snapshot)

    # Apply diff: update bid, remove ask
    order_book.apply_diff(
        bids=[["100.0", "2.0"], ["99.5", "1.0"]],
        asks=[["101.0", "0.0"], ["102.0", "1.5"]],
    )

    assert order_book.bids[100.0] == 2.0
    assert order_book.bids[99.5] == 1.0
    assert 101.0 not in order_book.asks
    assert order_book.asks[102.0] == 1.5


def test_trim(order_book):
    # Add more than 5 bids and asks
    snapshot = {
        "lastUpdateId": 1001,
        "bids": [[str(100 - i), "1.0"] for i in range(10)],  # 100 to 91
        "asks": [[str(101 + i), "1.0"] for i in range(10)],  # 101 to 110
    }
    order_book.update_from_snapshot(snapshot)

    # After trim, only 5 should remain
    assert len(order_book.bids) == 5
    assert len(order_book.asks) == 5

    # Bids should have highest prices
    assert list(order_book.bids.keys()) == [100.0, 99.0, 98.0, 97.0, 96.0]

    # Asks should have lowest prices
    assert list(order_book.asks.keys()) == [101.0, 102.0, 103.0, 104.0, 105.0]


def test_top_levels(order_book):
    snapshot = {
        "lastUpdateId": 1001,
        "bids": [["100.0", "1.0"], ["99.5", "1.5"]],
        "asks": [["101.0", "2.0"], ["101.5", "1.0"]],
    }
    order_book.update_from_snapshot(snapshot)

    bid_levels, ask_levels = order_book.top_levels()

    assert bid_levels == [(100.0, 1.0), (99.5, 1.5)]
    assert ask_levels == [(101.0, 2.0), (101.5, 1.0)]


def test_apply_diff_zero_quantity_removal(order_book):
    snapshot = {
        "lastUpdateId": 1001,
        "bids": [["100.0", "1.0"]],
        "asks": [["101.0", "1.0"]],
    }
    order_book.update_from_snapshot(snapshot)

    # Apply update that removes both levels
    order_book.apply_diff(
        bids=[["100.0", "0"]],
        asks=[["101.0", "0"]],
    )

    assert 100.0 not in order_book.bids
    assert 101.0 not in order_book.asks
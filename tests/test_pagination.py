# Unit tests for coordinator/database.py's pagination/filtering (issue
# #10). Mocks get_conn() (the pooled connection context manager — see
# issue #3) to check the SQL and parameters built for each filter
# combination, without needing a live Postgres.

import sys
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch

sys.path.insert(0, '.')

from coordinator import database as db


def _mock_conn_cursor(rows):
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    return mock_conn, mock_cur


@contextmanager
def _fake_get_conn(mock_conn):
    yield mock_conn


def test_get_jobs_filtered_no_filters():
    print("\n--- TEST 1: no filters -> base query, params are just [limit, skip] ---")
    mock_conn, mock_cur = _mock_conn_cursor([])
    with patch("coordinator.database.get_conn", lambda: _fake_get_conn(mock_conn)):
        db.get_jobs_filtered(skip=0, limit=50)

    query, params = mock_cur.execute.call_args[0]
    assert "WHERE 1=1" in query
    assert "AND status" not in query
    assert "AND created_at" not in query
    assert params == [50, 0]
    print("PASSED")


def test_get_jobs_filtered_with_status():
    print("\n--- TEST 2: status filter adds a clause and a param, in the right order ---")
    mock_conn, mock_cur = _mock_conn_cursor([])
    with patch("coordinator.database.get_conn", lambda: _fake_get_conn(mock_conn)):
        db.get_jobs_filtered(skip=0, limit=50, status="failed")

    query, params = mock_cur.execute.call_args[0]
    assert "AND status = %s" in query
    assert params == ["failed", 50, 0]
    print("PASSED")


def test_get_jobs_filtered_with_since():
    print("\n--- TEST 3: since filter adds a clause and a param ---")
    since = datetime(2026, 1, 1)
    mock_conn, mock_cur = _mock_conn_cursor([])
    with patch("coordinator.database.get_conn", lambda: _fake_get_conn(mock_conn)):
        db.get_jobs_filtered(skip=10, limit=25, since=since)

    query, params = mock_cur.execute.call_args[0]
    assert "AND created_at >= %s" in query
    assert params == [since, 25, 10]
    print("PASSED")


def test_get_jobs_filtered_with_both_filters_combined():
    print("\n--- TEST 4: status + since together -> both clauses, params in order ---")
    since = datetime(2026, 1, 1)
    mock_conn, mock_cur = _mock_conn_cursor([])
    with patch("coordinator.database.get_conn", lambda: _fake_get_conn(mock_conn)):
        db.get_jobs_filtered(skip=0, limit=10, status="completed", since=since)

    query, params = mock_cur.execute.call_args[0]
    assert "AND status = %s" in query
    assert "AND created_at >= %s" in query
    assert params == ["completed", since, 10, 0]
    print("PASSED")


def test_get_jobs_filtered_returns_rows_as_dicts():
    print("\n--- TEST 5: returns the rows the cursor fetched ---")
    mock_conn, mock_cur = _mock_conn_cursor([{"job_id": "1"}, {"job_id": "2"}])
    with patch("coordinator.database.get_conn", lambda: _fake_get_conn(mock_conn)):
        result = db.get_jobs_filtered(skip=0, limit=10)

    assert result == [{"job_id": "1"}, {"job_id": "2"}]
    print("PASSED")


def test_get_workers_paginated_uses_limit_offset():
    print("\n--- TEST 6: worker pagination builds LIMIT/OFFSET with the right params ---")
    mock_conn, mock_cur = _mock_conn_cursor([])
    with patch("coordinator.database.get_conn", lambda: _fake_get_conn(mock_conn)):
        db.get_workers_paginated(skip=5, limit=20)

    query, params = mock_cur.execute.call_args[0]
    assert "LIMIT %s OFFSET %s" in query
    assert params == (20, 5)
    print("PASSED")


if __name__ == "__main__":
    test_get_jobs_filtered_no_filters()
    test_get_jobs_filtered_with_status()
    test_get_jobs_filtered_with_since()
    test_get_jobs_filtered_with_both_filters_combined()
    test_get_jobs_filtered_returns_rows_as_dicts()
    test_get_workers_paginated_uses_limit_offset()
    print("\n✅ All tests passed — pagination/filtering query building is working correctly")

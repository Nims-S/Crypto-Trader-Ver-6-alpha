import unittest
from unittest.mock import patch

import execution


class FakeCursor:
    def __init__(self, fetchone_rows=None):
        self.queries = []
        self.fetchone_rows = list(fetchone_rows or [])

    def execute(self, query, params=None):
        self.queries.append((" ".join(query.split()), params))

    def fetchone(self):
        if self.fetchone_rows:
            return self.fetchone_rows.pop(0)
        return None


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


class ExecutionTests(unittest.TestCase):
    @patch("execution.send_telegram")
    @patch("execution.log_trade_performance")
    def test_manage_position_clamps_partial_closes(self, perf_mock, tg_mock):
        cur = FakeCursor()
        position = {
            "symbol": "BTC/USDT",
            "direction": "LONG",
            "entry": 100.0,
            "sl": 95.0,
            "tp": 110.0,
            "tp2": 112.0,
            "tp3": 0.0,
            "size": 1.0,
            "original_size": 1.0,
            "tp1_hit": False,
            "tp2_hit": False,
            "tp3_hit": False,
            "tp1_close_fraction": 0.8,
            "tp2_close_fraction": 0.8,  # intentionally > remaining after TP1
            "regime": "trend",
            "confidence": 0.9,
            "strategy": "test",
        }

        execution.manage_position(cur, position, price=120.0)

        inserts = [q for q, _ in cur.queries if "INSERT INTO trades" in q]
        self.assertEqual(len(inserts), 2, "Expected TP1 + TP2 trade inserts")

        size_updates = [(q, p) for q, p in cur.queries if "UPDATE positions SET size=%s" in q]
        self.assertTrue(size_updates, "Expected position size updates")
        final_size = float(size_updates[-1][1][0])
        self.assertGreaterEqual(final_size, 0.0)

        deletes = [q for q, _ in cur.queries if "DELETE FROM positions" in q]
        self.assertEqual(len(deletes), 1, "Expected depleted position to be deleted")

        self.assertEqual(perf_mock.call_count, 2)
        self.assertGreaterEqual(tg_mock.call_count, 2)

    @patch("execution.send_telegram")
    @patch("execution.log_trade_performance")
    def test_manage_position_stop_loss_exits_position(self, perf_mock, tg_mock):
        cur = FakeCursor()
        position = {
            "symbol": "ETH/USDT",
            "direction": "LONG",
            "entry": 100.0,
            "sl": 95.0,
            "tp": 110.0,
            "tp2": 115.0,
            "tp3": 120.0,
            "size": 2.0,
            "original_size": 2.0,
            "tp1_hit": False,
            "tp2_hit": False,
            "tp3_hit": False,
            "tp1_close_fraction": 0.3,
            "tp2_close_fraction": 0.4,
            "regime": "range",
            "confidence": 0.5,
            "strategy": "test",
        }

        execution.manage_position(cur, position, price=94.0)

        inserts = [q for q, _ in cur.queries if "INSERT INTO trades" in q]
        deletes = [q for q, _ in cur.queries if "DELETE FROM positions" in q]
        self.assertEqual(len(inserts), 1)
        self.assertEqual(len(deletes), 1)
        self.assertEqual(perf_mock.call_count, 1)
        self.assertEqual(tg_mock.call_count, 1)

    @patch("execution.get_conn")
    def test_update_position_levels_preserves_tp3_if_no_pct(self, get_conn_mock):
        cur = FakeCursor(fetchone_rows=[(100.0, "LONG", 130.0)])
        conn = FakeConn(cur)
        get_conn_mock.return_value = conn

        execution.update_position_levels(
            symbol="SOL/USDT",
            sl_pct=0.02,
            tp1_pct=0.03,
            tp2_pct=0.05,
            tp3_pct=0.0,  # should preserve existing tp3
        )

        update_calls = [(q, p) for q, p in cur.queries if "UPDATE positions" in q]
        self.assertEqual(len(update_calls), 1)
        _, params = update_calls[0]
        updated_tp3 = float(params[3])
        self.assertEqual(updated_tp3, 130.0)
        self.assertTrue(conn.committed)
        self.assertTrue(conn.closed)


if __name__ == "__main__":
    unittest.main()

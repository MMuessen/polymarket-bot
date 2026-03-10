"""Unit tests for PaperAccounting and SessionSnapshot."""
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from app.paper.accounting import PaperAccounting, SessionSnapshot


# ---------------------------------------------------------------------------
# SessionSnapshot.to_dict
# ---------------------------------------------------------------------------

class TestSessionSnapshot:
    def _make(self, **overrides) -> SessionSnapshot:
        defaults = dict(
            computed_at=datetime(2026, 3, 10, 12, 0, 0, tzinfo=timezone.utc),
            starting_balance=1000.0,
            realized_pnl=50.0,
            unrealized_pnl=10.0,
            total_trades=20,
            open_positions=3,
            wins=12,
            losses=8,
            win_rate=0.6,
            equity=1060.0,
            avg_pnl=2.5,
            top_edge=0.03,
        )
        defaults.update(overrides)
        return SessionSnapshot(**defaults)

    def test_to_dict_keys(self):
        d = self._make().to_dict()
        expected_keys = {
            "computed_at", "starting_balance", "realized_pnl", "unrealized_pnl",
            "equity", "total_trades", "open_positions", "wins", "losses",
            "win_rate", "avg_pnl", "top_edge",
        }
        assert expected_keys == set(d.keys())

    def test_to_dict_values_rounded(self):
        snap = self._make(realized_pnl=1.23456789, win_rate=0.666666)
        d = snap.to_dict()
        assert d["realized_pnl"] == round(1.23456789, 4)
        assert d["win_rate"] == round(0.666666, 4)

    def test_to_dict_top_edge_none(self):
        snap = self._make(top_edge=None)
        d = snap.to_dict()
        assert d["top_edge"] is None

    def test_equity_calculation(self):
        snap = self._make(starting_balance=1000.0, realized_pnl=100.0, unrealized_pnl=50.0, equity=1150.0)
        d = snap.to_dict()
        assert d["equity"] == 1150.0


# ---------------------------------------------------------------------------
# PaperAccounting.compute
# ---------------------------------------------------------------------------

class TestPaperAccountingCompute:
    def _make_accounting(self, stats_override=None, positions_override=None):
        trades_repo = AsyncMock()
        positions_repo = AsyncMock()

        stats_override = stats_override or {}
        default_stats = {
            "total_pnl": 50.0,
            "total_trades": 20,
            "wins": 12,
            "losses": 8,
            "win_rate": 0.6,
            "avg_pnl": 2.5,
        }
        default_stats.update(stats_override)
        trades_repo.session_stats.return_value = default_stats

        positions = positions_override if positions_override is not None else [
            {"unrealized_pnl": 5.0},
            {"unrealized_pnl": 10.0},
        ]
        positions_repo.list_all.return_value = positions

        return PaperAccounting(
            trades_repo=trades_repo,
            positions_repo=positions_repo,
            starting_balance=1000.0,
        )

    @pytest.mark.asyncio
    async def test_compute_basic(self):
        acc = self._make_accounting()
        snap = await acc.compute(top_edge=0.02)

        assert snap.realized_pnl == 50.0
        assert snap.unrealized_pnl == 15.0  # 5 + 10
        assert snap.equity == pytest.approx(1065.0)
        assert snap.wins == 12
        assert snap.losses == 8
        assert snap.win_rate == 0.6
        assert snap.top_edge == 0.02
        assert snap.open_positions == 2

    @pytest.mark.asyncio
    async def test_compute_stores_last_snapshot(self):
        acc = self._make_accounting()
        assert acc.last is None
        snap = await acc.compute()
        assert acc.last is snap

    @pytest.mark.asyncio
    async def test_compute_no_positions(self):
        acc = self._make_accounting(positions_override=[])
        snap = await acc.compute()
        assert snap.unrealized_pnl == 0.0
        assert snap.open_positions == 0

    @pytest.mark.asyncio
    async def test_compute_handles_none_values_from_db(self):
        # DB may return None for any field
        acc = self._make_accounting(stats_override={
            "total_pnl": None,
            "total_trades": None,
            "wins": None,
            "losses": None,
            "win_rate": None,
            "avg_pnl": None,
        })
        snap = await acc.compute()
        assert snap.realized_pnl == 0.0
        assert snap.total_trades == 0
        assert snap.wins == 0
        assert snap.losses == 0
        assert snap.win_rate == 0.0
        assert snap.avg_pnl == 0.0

    @pytest.mark.asyncio
    async def test_compute_handles_none_unrealized_pnl_in_position(self):
        acc = self._make_accounting(positions_override=[
            {"unrealized_pnl": None},
            {"unrealized_pnl": 10.0},
        ])
        snap = await acc.compute()
        assert snap.unrealized_pnl == 10.0  # None treated as 0

    @pytest.mark.asyncio
    async def test_open_positions_detail_delegates(self):
        acc = self._make_accounting()
        positions = await acc.open_positions_detail()
        assert positions is not None

    @pytest.mark.asyncio
    async def test_recent_trades_delegates(self):
        acc = self._make_accounting()
        acc._trades.list_recent.return_value = [{"id": 1}]
        trades = await acc.recent_trades(limit=10)
        acc._trades.list_recent.assert_called_once_with(limit=10)
        assert trades == [{"id": 1}]

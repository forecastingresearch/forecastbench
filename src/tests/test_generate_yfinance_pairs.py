"""Tests for scripts/generate_yfinance_pairs.py (pure functions only; no network)."""

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "generate_yfinance_pairs.py"


@pytest.fixture(scope="module")
def gen():
    spec = importlib.util.spec_from_file_location("generate_yfinance_pairs", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestReadPool:
    def test_keeps_active_single_tickers_with_a_summary_only(self, gen, tmp_path):
        """Resolved tickers, pair rows, and tickers Yahoo serves no business summary for (their
        background is "N/A", so a pair naming them would have none either) are left out."""
        rows = [
            {"id": "AAPL", "resolved": False, "background": "Makes phones."},
            {"id": "MRO", "resolved": True, "background": "Oil."},
            {"id": "AAPL_MSFT", "resolved": False, "background": "AAPL: ...\n\nMSFT: ..."},
            {"id": "FISV", "resolved": False, "background": "N/A"},
        ]
        path = tmp_path / "yfinance_questions.jsonl"
        path.write_text("".join(json.dumps(r) + "\n" for r in rows))
        assert gen.read_pool(path) == {"AAPL"}


class TestBuildUniverse:
    def test_swaps_renamed_drops_nullified_and_non_index(self, gen):
        universe = gen.build_universe(
            pool={"AAPL", "FI", "WBA", "OLDCO"},
            sp500=["AAPL", "FISV", "MSFT"],
            nullified_ids={"WBA"},
            ticker_renames=[{"original_ticker": "FI", "replacement_ticker": "FISV"}],
        )
        # FI becomes FISV; WBA is nullified; OLDCO left the index; MSFT is not in the pool.
        assert universe == ["AAPL", "FISV"]


class TestDrawPairs:
    def test_deterministic_unique_and_keeps_existing(self, gen):
        universe = [f"T{i:02d}" for i in range(10)]
        existing = [("T01", "T00")]
        first = gen.draw_pairs(universe, existing, target=5, seed=7)
        second = gen.draw_pairs(universe, existing, target=5, seed=7)
        assert first == second
        assert first[0] == ("T01", "T00")
        assert len(first) == 5
        # Unordered uniqueness: the existing (T01, T00) must not be drawn again as (T00, T01).
        assert len({tuple(sorted(p)) for p in first}) == 5

    def test_orientation_is_random(self, gen):
        """X is not always the alphabetically-first ticker, or "Yes" would always mean the
        earlier letter wins and a yes-leaning forecaster would gain a systematic edge."""
        universe = [f"T{i:02d}" for i in range(12)]
        pairs = gen.draw_pairs(universe, [], target=40, seed=3)
        assert any(x < y for x, y in pairs)
        assert any(x > y for x, y in pairs)

    def test_target_already_met_adds_nothing(self, gen):
        existing = [("A", "B"), ("A", "C")]
        assert gen.draw_pairs(["A", "B", "C", "D"], existing, target=2, seed=1) == existing

    def test_not_enough_candidates_raises(self, gen):
        with pytest.raises(SystemExit, match="Need 2 new pairs"):
            gen.draw_pairs(["A", "B"], [], target=2, seed=1)


class TestRenderPairsBlock:
    def test_rendered_block_round_trips(self, gen):
        pairs = [("A", "B"), ("C", "D")]
        block = gen.render_pairs_block(pairs, seed=42, universe_size=4, today="2026-10-02")
        namespace = {}
        exec(compile(block, "yfinance_questions.py", "exec"), namespace)  # noqa: S102
        assert namespace["YFINANCE_PAIRS"] == pairs
        assert "seed 42" in block
        assert "4 tickers" in block

    def test_replace_keeps_the_hand_written_part(self, gen):
        old = gen.render_pairs_block([("A", "B")], seed=1, universe_size=2, today="2026-01-01")
        module = "def keep_me():\n    return 1\n\n\n" + old
        new = gen.render_pairs_block(
            [("A", "B"), ("C", "D")], seed=2, universe_size=4, today="2026-02-02"
        )
        namespace = {}
        exec(compile(gen.replace_pairs_block(module, new), "m.py", "exec"), namespace)  # noqa: S102
        assert namespace["keep_me"]() == 1
        assert namespace["YFINANCE_PAIRS"] == [("A", "B"), ("C", "D")]
        assert "seed 1" not in gen.replace_pairs_block(module, new)

    def test_replace_appends_when_there_is_no_marker_yet(self, gen):
        block = gen.render_pairs_block([("A", "B")], seed=1, universe_size=2, today="2026-01-01")
        out = gen.replace_pairs_block("X = 1\n", block)
        assert out.startswith("X = 1\n\n\n")
        assert out.endswith(block)

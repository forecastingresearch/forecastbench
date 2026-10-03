"""Tests for share-based allocation in create_question_set."""

import pytest

from curate_questions.create_question_set import main as create_question_set

DATASET_SHARES = {
    "acled": 0.95 / 3,
    "dbnomics": 0.95 / 3,
    "fred": 0.95 / 3,
    "yfinance": 0.05,
}


class TestRoundByLargestRemainder:
    def test_integers_pass_through(self):
        result = create_question_set._round_by_largest_remainder({"a": 5.0, "b": 3.0}, 8)
        assert result == {"a": 5, "b": 3}

    def test_largest_remainders_get_the_extra_items(self):
        result = create_question_set._round_by_largest_remainder(
            {"a": 12.5, "b": 79.1667, "c": 79.1667, "d": 79.1667}, 250
        )
        assert result == {"a": 13, "b": 79, "c": 79, "d": 79}

    def test_ties_go_in_dict_order(self):
        result = create_question_set._round_by_largest_remainder(
            {"a": 5.0, "b": 31.6667, "c": 31.6667, "d": 31.6667}, 100
        )
        assert result == {"a": 5, "b": 32, "c": 32, "d": 31}

    def test_float_noise_does_not_change_the_result(self):
        result = create_question_set._round_by_largest_remainder(
            {"a": 4.999999999, "b": 5.000000001}, 10
        )
        assert result == {"a": 5, "b": 5}


class TestAllocateByShare:
    def test_exact_shares_with_no_shortfall(self):
        result = create_question_set.allocate_by_share(
            data={"a": 100, "b": 100, "c": 100}, shares={"a": 0.5, "b": 0.3, "c": 0.2}, n=10
        )
        assert result == {"a": 5, "b": 3, "c": 2}

    def test_dataset_shares_for_the_llm_set(self):
        data = {source: 1000 for source in DATASET_SHARES}
        result = create_question_set.allocate_by_share(data=data, shares=DATASET_SHARES, n=250)
        assert result == {"acled": 79, "dbnomics": 79, "fred": 79, "yfinance": 13}
        assert sum(result.values()) == 250

    def test_dataset_shares_for_the_human_set(self):
        data = {source: 1000 for source in DATASET_SHARES}
        result = create_question_set.allocate_by_share(data=data, shares=DATASET_SHARES, n=100)
        assert result == {"acled": 32, "dbnomics": 32, "fred": 31, "yfinance": 5}

    def test_capped_source_gives_leftover_to_others_by_share(self):
        result = create_question_set.allocate_by_share(
            data={"a": 10, "b": 100, "c": 100}, shares={"a": 0.5, "b": 0.3, "c": 0.2}, n=100
        )
        # a is capped at 10. The 40 it could not take split 24/16 between b and c (0.3 : 0.2).
        assert result == {"a": 10, "b": 54, "c": 36}

    def test_equal_shares_give_an_even_split(self):
        data = {"a": 1000, "b": 1000, "c": 1000, "d": 1000}
        shares = {key: 0.25 for key in data}
        result = create_question_set.allocate_by_share(data=data, shares=shares, n=250)
        assert result == {"a": 63, "b": 63, "c": 62, "d": 62}

    def test_equal_shares_split_leftover_evenly_when_one_source_is_capped(self):
        data = {"a": 10, "b": 1000, "c": 1000, "d": 1000}
        shares = {key: 0.25 for key in data}
        result = create_question_set.allocate_by_share(data=data, shares=shares, n=250)
        assert result["a"] == 10
        assert sum(result.values()) == 250
        others = [result["b"], result["c"], result["d"]]
        assert max(others) - min(others) <= 1

    def test_two_sources_capped_in_the_same_pass(self):
        data = {"a": 5, "b": 5, "c": 1000, "d": 1000}
        shares = {key: 0.25 for key in data}
        result = create_question_set.allocate_by_share(data=data, shares=shares, n=250)
        assert result == {"a": 5, "b": 5, "c": 120, "d": 120}

    def test_second_source_goes_over_only_after_the_first_is_fixed(self):
        # b fits its first-pass target of 30 but not the 60 it gets after a is fixed at 10.
        result = create_question_set.allocate_by_share(
            data={"a": 10, "b": 40, "c": 1000}, shares={"a": 0.5, "b": 0.3, "c": 0.2}, n=100
        )
        assert result == {"a": 10, "b": 40, "c": 50}

    def test_total_shortfall_raises(self):
        with pytest.raises(ValueError, match="allocated 7/10"):
            create_question_set.allocate_by_share(
                data={"a": 3, "b": 4}, shares={"a": 0.5, "b": 0.5}, n=10
            )

    def test_availability_equal_to_n_returns_everything(self):
        result = create_question_set.allocate_by_share(
            data={"a": 3, "b": 7}, shares={"a": 0.5, "b": 0.5}, n=10
        )
        assert result == {"a": 3, "b": 7}

    def test_shares_are_normalized_when_a_source_is_missing(self):
        # driver drops sources with no questions, so the remaining shares may not sum to 1.
        result = create_question_set.allocate_by_share(
            data={"a": 100, "b": 100}, shares={"a": 0.3, "b": 0.2}, n=10
        )
        assert result == {"a": 6, "b": 4}

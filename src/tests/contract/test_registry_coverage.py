"""Contract: every registry source is implemented, update-only or retired — and acts so."""

import pytest

from sources._metadata import SOURCE_METADATA
from sources.registry import SOURCES
from tests._sources import IMPLEMENTED_SOURCES, RETIRED_SOURCES, UPDATE_ONLY_SOURCES
from tests.factories import make_question_df


def test_registry_sources_partition_into_exactly_one_bucket():
    """Every registry source is in exactly one bucket — never none, never two."""
    buckets = [set(IMPLEMENTED_SOURCES), set(UPDATE_ONLY_SOURCES), set(RETIRED_SOURCES)]
    assert set.union(*buckets) == set(SOURCES)
    for i, bucket in enumerate(buckets):
        for other in buckets[i + 1 :]:
            assert not bucket & other


@pytest.mark.parametrize("name", IMPLEMENTED_SOURCES)
def test_implemented_source_is_fetched_and_updated_nightly(name):
    """A source listed as implemented must be scheduled for both fetch and update."""
    assert SOURCE_METADATA[name]["run_fetch"] and SOURCE_METADATA[name]["run_update"]
    assert not SOURCE_METADATA[name]["dfq_is_frozen"]


@pytest.mark.parametrize("name", UPDATE_ONLY_SOURCES)
def test_update_only_source_refuses_to_fetch(name):
    """No new questions can be pulled in; the published ones keep updating (see conformance)."""
    with pytest.raises(RuntimeError):
        SOURCES[name].fetch()


@pytest.mark.parametrize("name", RETIRED_SOURCES)
def test_retired_source_cannot_be_fetched_and_update_is_a_noop(name):
    """A retired source refuses to fetch, and its no-op update() hands the bank back unchanged."""
    with pytest.raises(RuntimeError):
        SOURCES[name].fetch()

    dfq = make_question_df([{"id": "old-question"}])
    result = SOURCES[name].update(dfq)
    assert result.dfq.equals(dfq)
    assert result.resolution_files == {}

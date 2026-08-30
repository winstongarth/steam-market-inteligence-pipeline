"""nameid_resolver tests. Uses a fake client and recorded fixtures — no network calls."""

from pathlib import Path

import pytest

from ingest.nameid_resolver import NameIdResolutionError, NameIdResolver

FIXTURES = Path(__file__).parent / "fixtures"


class _FakeClient:
    def __init__(self, response_text: str = "<html>no bucket data here</html>") -> None:
        self.response_text = response_text
        self.calls = 0

    async def get_json(self, url, params=None, expect_json=True, headers=None):
        self.calls += 1
        return 200, self.response_text


@pytest.mark.asyncio
async def test_resolve_uses_cache_without_a_request(tmp_path):
    cache_path = tmp_path / "market_bucket_ids.json"
    resolver = NameIdResolver(cache_path=cache_path)
    resolver.seed(730, "AK-47 | Redline (Field-Tested)", "1807209A02300438A4E1F5F503")

    client = _FakeClient()
    result = await resolver.resolve(client, 730, "AK-47 | Redline (Field-Tested)")

    assert result == "1807209A02300438A4E1F5F503"
    assert client.calls == 0


@pytest.mark.asyncio
async def test_resolve_raises_clear_error_when_nothing_found(tmp_path):
    resolver = NameIdResolver(cache_path=tmp_path / "market_bucket_ids.json")
    client = _FakeClient(response_text="<html>bucket page, no matching listing data</html>")

    with pytest.raises(NameIdResolutionError, match="docs/DECISIONS.md"):
        await resolver.resolve(client, 730, "AK-47 | Redline (Field-Tested)")


@pytest.mark.asyncio
async def test_resolve_extracts_from_real_bucket_page_fixture(tmp_path):
    """The fixture is a real (trimmed) excerpt of Valve's bucket page, captured earlier.
    Confirms the extraction regex still works against real recorded data."""
    bucket_page_html = (FIXTURES / "listings_bucket_page_excerpt.html").read_text(encoding="utf-8")
    resolver = NameIdResolver(cache_path=tmp_path / "market_bucket_ids.json")
    client = _FakeClient(response_text=bucket_page_html)

    result = await resolver.resolve(client, 730, "AK-47 | Redline (Battle-Scarred)")
    assert result == "1807209A02300438808080F803"

    # the same fetch also opportunistically cached a second item found on the same page
    assert resolver.get_cached(730, "Sticker | FaZe Clan | Copenhagen 2024") == "18B90930046205080010E238"

    # second call for the first item must hit the cache, not the client again
    client.calls = 0
    result_again = await resolver.resolve(client, 730, "AK-47 | Redline (Battle-Scarred)")
    assert result_again == "1807209A02300438808080F803"
    assert client.calls == 0


def test_seed_from_search_render_result_resolves_commodity_items_for_free(tmp_path):
    """Commodity items (e.g. cases) carry market_bucket_group_id right in search/render —
    no bucket-page fetch needed at all."""
    resolver = NameIdResolver(cache_path=tmp_path / "market_bucket_ids.json")
    result = {
        "hash_name": "Dreams & Nightmares Case",
        "asset_description": {"commodity": 1, "market_bucket_group_id": "G18D2253004"},
    }

    bucket_id = resolver.seed_from_search_render_result(730, result)

    assert bucket_id == "18D2253004"
    assert resolver.get_cached(730, "Dreams & Nightmares Case") == "18D2253004"


def test_seed_from_search_render_result_skips_non_commodity_items(tmp_path):
    resolver = NameIdResolver(cache_path=tmp_path / "market_bucket_ids.json")
    result = {
        "hash_name": "AK-47 | Redline (Field-Tested)",
        "asset_description": {"commodity": 0, "market_bucket_group_id": "G1807209A023004"},
    }

    bucket_id = resolver.seed_from_search_render_result(730, result)

    assert bucket_id is None
    assert resolver.get_cached(730, "AK-47 | Redline (Field-Tested)") is None

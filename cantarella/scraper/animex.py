from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Literal

from curl_cffi import requests

logger = logging.getLogger("animex_scraper")

IMPERSONATE = "chrome"

GRAPHQL_URL = "https://graphql.animex.one/graphql"
ANILIST_GRAPHQL_URL = "https://graphql.anilist.co"
RECENT_URL = "https://graphql.animex.one/api/recent"
EPISODES_URL = "https://pp.animex.one/rest/api/episodes"
SERVERS_URL = "https://pp.animex.one/rest/api/servers"
SOURCES_URL = "https://pp.animex.one/rest/api/sources"

DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Origin": "https://animex.one",
    "Referer": "https://animex.one/",
}

FAST_SEARCH_QUERY = """
query FastSearch($query: String, $limit: Int, $includeAdult: Boolean) {
  catalogAnime(filter: { query: $query, includeAdult: $includeAdult }, limit: $limit) {
    items {
      id
      anilistId
      malId
      titleRomaji
      titleEnglish
      coverImage
      format
      status
      episodeCount
      genres
      seasonYear
      season
      color
      bannerImage
    }
  }
}
"""

ANIME_SCHEDULE_QUERY = """
query ($weekStart: Int, $weekEnd: Int, $page: Int) {
  Page(page: $page) {
    pageInfo { hasNextPage total }
    airingSchedules(airingAt_greater: $weekStart, airingAt_lesser: $weekEnd, sort: TIME) {
      id episode airingAt
      media {
        id idMal
        title { romaji native english userPreferred }
        status format genres bannerImage
        coverImage { extraLarge color }
      }
    }
  }
}
"""


class AnimexAPIError(Exception):
    pass


@dataclass
class AnimexClient:
    session: requests.Session | None = None
    timeout: float = 15.0
    impersonate: str = IMPERSONATE

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session(impersonate=self.impersonate)
        self.session.headers.update(DEFAULT_HEADERS)

    def _get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        resp = self.session.get(
            url, params=params, timeout=self.timeout, impersonate=self.impersonate
        )
        resp.raise_for_status()
        return resp.json()

    def _post_graphql(
        self, query: str, variables: dict[str, Any], url: str = GRAPHQL_URL
    ) -> dict[str, Any]:
        payload = {"query": query, "variables": variables}
        resp = self.session.post(
            url,
            json=payload,
            timeout=self.timeout,
            impersonate=self.impersonate,
        )
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            raise AnimexAPIError(str(data["errors"]))
        return data.get("data", {})

    def search(
        self,
        query: str,
        limit: int = 10,
        include_adult: bool = False,
    ) -> list[dict[str, Any]]:
        data = self._post_graphql(
            FAST_SEARCH_QUERY,
            {"query": query, "limit": limit, "includeAdult": include_adult},
        )
        try:
            return data["catalogAnime"]["items"]
        except (KeyError, TypeError) as exc:
            raise AnimexAPIError(f"Unexpected search response shape: {data}") from exc

    def get_episodes(self, anime_id: str) -> list[dict[str, Any]]:
        data = self._get(EPISODES_URL, params={"id": anime_id})
        if not isinstance(data, list):
            raise AnimexAPIError(f"Unexpected episodes response shape: {data}")
        return data

    def get_episode_count(self, anime_id: str) -> int:
        return len(self.get_episodes(anime_id))

    def get_servers(self, anime_id: str, ep_num: int) -> dict[str, Any]:
        data = self._get(SERVERS_URL, params={"id": anime_id, "epNum": ep_num})
        if "subProviders" not in data and "dubProviders" not in data:
            raise AnimexAPIError(f"Unexpected servers response shape: {data}")
        return data

    def get_default_provider(
        self,
        anime_id: str,
        ep_num: int,
        type_: Literal["sub", "dub"] = "sub",
    ) -> str | None:
        data = self.get_servers(anime_id, ep_num)
        providers = data.get(f"{type_}Providers") or []
        for p in providers:
            if p.get("default"):
                return p.get("id")
        return providers[0]["id"] if providers else None

    def get_sources(
        self,
        anime_id: str,
        ep_num: int,
        type_: Literal["sub", "dub"] = "sub",
        provider_id: str | None = None,
    ) -> dict[str, Any]:
        if provider_id is None:
            provider_id = self.get_default_provider(anime_id, ep_num, type_)
            if provider_id is None:
                raise AnimexAPIError(
                    f"No {type_} providers available for {anime_id} ep {ep_num}"
                )
        params = {
            "id": anime_id,
            "epNum": ep_num,
            "type": type_,
            "providerId": provider_id,
        }
        data = self._get(SOURCES_URL, params=params)
        if "sources" not in data:
            raise AnimexAPIError(f"Unexpected sources response shape: {data}")
        return data

    def get_best_source(
        self,
        anime_id: str,
        ep_num: int,
        type_: Literal["sub", "dub"] = "sub",
        provider_id: str | None = None,
    ) -> dict[str, Any] | None:
        data = self.get_sources(anime_id, ep_num, type_, provider_id)
        sources = data.get("sources") or []
        if not sources:
            return None

        def _quality_rank(s: dict[str, Any]) -> int:
            q = (s.get("quality") or "").rstrip("p")
            return int(q) if q.isdigit() else -1

        return max(sources, key=_quality_rank)

    def get_recent(self, page: int = 1) -> dict[str, Any]:
        data = self._get(RECENT_URL, params={"page": page})
        if "results" not in data:
            raise AnimexAPIError(f"Unexpected recent response shape: {data}")
        return data

    def iter_recent(self, start_page: int = 1, max_pages: int | None = None):
        page = start_page
        pages_fetched = 0
        while True:
            data = self.get_recent(page=page)
            yield from data.get("results", [])
            pages_fetched += 1
            if not data.get("hasNextPage"):
                break
            if max_pages is not None and pages_fetched >= max_pages:
                break
            page += 1

    def get_schedule(
        self,
        week_start: int,
        week_end: int,
        page: int = 1,
    ) -> dict[str, Any]:
        data = self._post_graphql(
            ANIME_SCHEDULE_QUERY,
            {"weekStart": week_start, "weekEnd": week_end, "page": page},
            url=ANILIST_GRAPHQL_URL,
        )
        if "Page" not in data:
            raise AnimexAPIError(f"Unexpected schedule response shape: {data}")
        return data["Page"]

    def get_schedule_for_current_week(self, page: int = 1) -> dict[str, Any]:
        from datetime import datetime, timedelta, timezone

        now = datetime.now(timezone.utc)
        week_start_dt = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        week_end_dt = week_start_dt + timedelta(days=7) - timedelta(seconds=1)
        return self.get_schedule(
            int(week_start_dt.timestamp()), int(week_end_dt.timestamp()), page=page
        )

    def iter_schedule(
        self, week_start: int, week_end: int, start_page: int = 1
    ):
        page = start_page
        while True:
            data = self.get_schedule(week_start, week_end, page=page)
            yield from data.get("airingSchedules", [])
            if not data.get("pageInfo", {}).get("hasNextPage"):
                break
            page += 1


if __name__ == "__main__":

from __future__ import annotations

import asyncio
import logging
import random

# NOTE: adjust to match your actual project layout.
from bot import app
from .animex import AnimexClient, AnimexAPIError
from .animexdl import AnimexDownloader, sc, bi, _card
from search import RANDOM_IMAGES  # shared image pool, defined once in search.py

logger = logging.getLogger("ongoing")

client = AnimexClient()
downloader = AnimexDownloader(client=client)

# TODO: set this to the chat/channel id where auto-processed episodes get posted.
ONGOING_CHAT_ID = 0
POLL_INTERVAL_SECONDS = 300
DEFAULT_QUALITIES = ["360p", "720p", "1080p"]

# Optional persistence so progress survives restarts. If you don't have a
# `db.py` with a Mongo handle named `db`, this just runs in-memory instead.
try:
    from db import db  # e.g. a motor AsyncIOMotorDatabase
except ImportError:
    db = None

_seen: dict[str, int] = {}


async def _load_seen() -> None:
    if db is None:
        return
    async for doc in db.ongoing_progress.find({}):
        _seen[doc["anime_id"]] = doc["last_episode"]


async def _mark_seen(anime_id: str, ep_num: int) -> None:
    _seen[anime_id] = ep_num
    if db is None:
        return
    await db.ongoing_progress.update_one(
        {"anime_id": anime_id},
        {"$set": {"anime_id": anime_id, "last_episode": ep_num}},
        upsert=True,
    )


def _title_of(item: dict) -> str:
    return item.get("titleEnglish") or item.get("titleRomaji") or "Unknown"


async def _process_new_episode(item: dict) -> None:
    anime_id = str(item.get("id") or item.get("anilistId") or "")
    ep_num = item.get("episode") or item.get("epNum") or item.get("latestEpisode")
    if not anime_id or ep_num is None:
        return

    if ep_num <= _seen.get(anime_id, 0):
        return

    title = _title_of(item)
    status = await app.send_message(
        ONGOING_CHAT_ID,
        _card([f"🆕 {sc('new episode detected')}", "", f"🎬 {bi(title)}", f"📁 {sc('episode')} {ep_num}"]),
    )
    try:
        await app.send_photo(ONGOING_CHAT_ID, random.choice(RANDOM_IMAGES))
    except Exception:
        pass

    try:
        await downloader.process_episode(
            app, ONGOING_CHAT_ID, anime_id, ep_num,
            anime_title=title, qualities=DEFAULT_QUALITIES, status_msg=status,
        )
        await _mark_seen(anime_id, ep_num)
    except Exception as exc:
        logger.exception("Failed to process %s ep %s", title, ep_num)
        await status.edit_text(
            _card([f"❌ {sc('failed')}", "", f"{bi(title)} — {sc('episode')} {ep_num}", "", str(exc)])
        )


async def _poll_loop() -> None:
    await _load_seen()
    logger.info("ongoing watcher started, polling every %ss", POLL_INTERVAL_SECONDS)
    while True:
        try:
            data = client.get_recent(page=1)
            for item in data.get("results", []):
                await _process_new_episode(item)
        except AnimexAPIError as exc:
            logger.warning("Failed to fetch recent feed: %s", exc)
        except Exception:
            logger.exception("Unexpected error in ongoing poll loop")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


def start_ongoing_watcher() -> None:
    """Call once after the bot starts, e.g. inside your on_startup hook:

        from ongoing import start_ongoing_watcher
        start_ongoing_watcher()
    """
    asyncio.get_event_loop().create_task(_poll_loop())

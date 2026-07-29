from __future__ import annotations

import asyncio
import logging

from pyrogram import Client
from pyrogram.enums import ParseMode

from cantarella.core.database import db
from cantarella.scraper.animex import AnimexClient, AnimexAPIError
from cantarella.scraper.animexdl import AnimexDownloader, sc
from cantarella.scraper.miruro import browse_anime as miruro_browse_anime, get_anime_episodes as miruro_get_anime_episodes
from cantarella.scraper.mirurodl import MiruroDownloader
from config import SET_INTERVAL, TARGET_CHAT_ID, LOG_CHANNEL

logger = logging.getLogger(__name__)

animex_client = AnimexClient()
animex_downloader = AnimexDownloader(client=animex_client)
miruro_downloader = MiruroDownloader()

# Same compatibility shim as cantarella/telegram/plugins/anime.py — kept here
# too since this module can run standalone before that plugin is imported.
if not hasattr(AnimexClient, "iter_providers"):

    def _iter_providers(self, anime_id, ep_num, type_="sub"):
        data = self.get_servers(anime_id, ep_num)
        return data.get(f"{type_}Providers") or []

    AnimexClient.iter_providers = _iter_providers

DEFAULT_QUALITIES = ["360p", "720p", "1080p"]


def _card(text: str) -> str:
    return f"<blockquote>{text}</blockquote>"


def _title_of(item: dict) -> str:
    return item.get("titleEnglish") or item.get("titleRomaji") or "Unknown"


def _miruro_title_of(media: dict) -> str:
    title = media.get("title") or {}
    return (
        title.get("english") or title.get("romaji")
        or media.get("titleEnglish") or media.get("titleRomaji") or "Unknown"
    )


def _miruro_latest_episode(episodes_data: dict) -> int | None:
    providers = episodes_data.get("providers", {}) or {}
    numbers: list[int] = []
    for block in providers.values():
        eps = block.get("episodes", {}) or {}
        for lst in eps.values():
            for e in lst:
                n = e.get("number")
                if isinstance(n, (int, float)):
                    numbers.append(int(n))
    return max(numbers) if numbers else None


async def _process_new_episode(client_app: Client, chat_id: int, item: dict) -> None:
    anime_id = str(item.get("id") or item.get("anilistId") or "")
    ep_num = item.get("episode") or item.get("epNum") or item.get("latestEpisode")
    if not anime_id or ep_num is None:
        return

    source_name = item.get("_source", "animex")
    ep_identifier = f"{source_name}_{anime_id}_ep_{ep_num}"
    if await db.is_processed(ep_identifier):
        return

    title = item.get("_title") or _title_of(item)
    log_id = int(LOG_CHANNEL) if LOG_CHANNEL else chat_id
    downloader = miruro_downloader if source_name == "miruro" else animex_downloader

    logger.info("New episode detected via %s: '%s' ep %s", source_name, title, ep_num)
    status = await client_app.send_message(
        log_id,
        _card(f"🆕 {sc('new episode detected')}\n🎬 <b>{title}</b>\n📁 {sc('episode')} {ep_num}"),
        parse_mode=ParseMode.HTML,
    )

    try:
        await downloader.process_episode(
            client_app, chat_id, anime_id, ep_num,
            anime_title=title, qualities=DEFAULT_QUALITIES, status_msg=status,
        )
        await db.mark_processed(ep_identifier)
        logger.info("Auto-download finished for '%s' ep %s via %s", title, ep_num, source_name)
    except Exception as exc:
        logger.exception("Failed to process %s ep %s", title, ep_num)
        await status.edit_text(
            _card(f"❌ {sc('failed')}\n<b>{title}</b> — {sc('episode')} {ep_num}\n{exc}"),
            parse_mode=ParseMode.HTML,
        )


async def _fetch_recent_animex() -> list[dict]:
    data = await asyncio.to_thread(animex_client.get_recent, 1)
    items = []
    for item in data.get("results", []):
        item = dict(item)
        item["_source"] = "animex"
        items.append(item)
    return items


async def _fetch_recent_miruro() -> list[dict]:
    """Miruro has no dedicated 'recent releases' feed — approximate it by
    checking the currently-airing (RELEASING) list and pulling each title's
    latest known episode number from its own episodes payload."""
    try:
        data = await asyncio.to_thread(miruro_browse_anime, "RELEASING", "TRENDING_DESC", 1, 20)
    except Exception as exc:
        logger.warning("Failed to fetch miruro RELEASING feed: %s", exc)
        return []

    # browse_anime may return a list directly OR a dict like {"media": [...]}
    if isinstance(data, list):
        media_list = data
    elif isinstance(data, dict):
        media_list = data.get("media") or data.get("results") or data.get("data") or []
    else:
        media_list = []

    items: list[dict] = []
    for media in media_list:
        anilist_id = media.get("id")
        if anilist_id is None:
            continue
        try:
            episodes_data = await asyncio.to_thread(miruro_get_anime_episodes, anilist_id)
        except Exception as exc:
            logger.debug("Skipping miruro title %s: %s", anilist_id, exc)
            continue
        latest = _miruro_latest_episode(episodes_data)
        if latest is None:
            continue
        items.append({
            "id": str(anilist_id),
            "episode": latest,
            "_source": "miruro",
            "_title": _miruro_title_of(media),
        })
    return items


async def check_and_download_ongoing(client_app: Client, chat_id: int) -> None:
    active_source = await db.get_user_setting(0, "active_source", "animex")

    if active_source == "miruro":
        items = await _fetch_recent_miruro()
    else:
        try:
            items = await _fetch_recent_animex()
        except AnimexAPIError as exc:
            logger.warning("Failed to fetch recent feed: %s", exc)
            items = []

    for item in items:
        try:
            await _process_new_episode(client_app, chat_id, item)
        except Exception:
            logger.exception("Error processing recent item: %s", item)


async def ongoing_task(client_app: Client) -> None:
    """Entry point unchanged from before — still started in cantarella/__main__.py
    via `asyncio.create_task(ongoing_task(app))`."""
    if not TARGET_CHAT_ID:
        logger.warning("TARGET_CHAT_ID is not set — ongoing auto-downloads are disabled.")
        return
    try:
        target_chat_id = int(TARGET_CHAT_ID)
    except ValueError:
        logger.warning("TARGET_CHAT_ID must be a valid integer chat ID — ongoing auto-downloads disabled.")
        return

    logger.info("Starting ongoing checker. Interval: %ss, Target chat: %s", SET_INTERVAL, target_chat_id)

    while True:
        ongoing_enabled = await db.get_user_setting(0, "ongoing_enabled", True)
        if ongoing_enabled:
            try:
                await check_and_download_ongoing(client_app, target_chat_id)
            except Exception:
                logger.exception("Error in ongoing task loop")
        await asyncio.sleep(SET_INTERVAL)

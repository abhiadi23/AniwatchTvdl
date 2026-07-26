#@cantarellabots
from __future__ import annotations

import asyncio
import logging

from pyrogram import Client
from pyrogram.enums import ParseMode

from cantarella.core.database import db
from cantarella.scraper.animex import AnimexClient, AnimexAPIError
from cantarella.scraper.animexdl import AnimexDownloader, sc
from config import SET_INTERVAL, TARGET_CHAT_ID, LOG_CHANNEL

logger = logging.getLogger(__name__)

client = AnimexClient()
downloader = AnimexDownloader(client=client)

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


async def _process_new_episode(client_app: Client, chat_id: int, item: dict) -> None:
    anime_id = str(item.get("id") or item.get("anilistId") or "")
    ep_num = item.get("episode") or item.get("epNum") or item.get("latestEpisode")
    if not anime_id or ep_num is None:
        return

    ep_identifier = f"{anime_id}_ep_{ep_num}"
    if await db.is_processed(ep_identifier):
        return

    title = _title_of(item)
    log_id = int(LOG_CHANNEL) if LOG_CHANNEL else chat_id

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
    except Exception as exc:
        logger.exception("Failed to process %s ep %s", title, ep_num)
        await status.edit_text(
            _card(f"❌ {sc('failed')}\n<b>{title}</b> — {sc('episode')} {ep_num}\n{exc}"),
            parse_mode=ParseMode.HTML,
        )


async def check_and_download_ongoing(client_app: Client, chat_id: int) -> None:
    try:
        data = await asyncio.to_thread(client.get_recent, 1)
    except AnimexAPIError as exc:
        logger.warning("Failed to fetch recent feed: %s", exc)
        return
    for item in data.get("results", []):
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
        ongoing_enabled = await db.get_user_setting(0, "ongoing_enabled", False)
        if ongoing_enabled:
            try:
                await check_and_download_ongoing(client_app, target_chat_id)
            except Exception:
                logger.exception("Error in ongoing task loop")
        await asyncio.sleep(SET_INTERVAL)

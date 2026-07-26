from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from pyrogram import Client, filters
from pyrogram.types import Message

# NOTE: adjust to match your actual project layout.
from bot import app
from animex import AnimexClient, AnimexAPIError
from animexdl import sc, bi, _card
from search import RANDOM_IMAGES  # shared image pool, defined once in search.py

client = AnimexClient()


def _today_window() -> tuple[int, int]:
    now = datetime.now(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1) - timedelta(seconds=1)
    return int(start.timestamp()), int(end.timestamp())


def _title_of(media: dict) -> str:
    title = media.get("title") or {}
    return title.get("english") or title.get("romaji") or title.get("userPreferred") or "Unknown"


def _format_schedule(items: list[dict]) -> str:
    if not items:
        return _card([f"📅 {sc('todays schedule')}", "", sc("nothing airing today")])

    lines = [f"📅 {sc('todays schedule')}", ""]
    for it in sorted(items, key=lambda x: x["airingAt"]):
        media = it.get("media") or {}
        air_time = datetime.fromtimestamp(it["airingAt"], tz=timezone.utc).strftime("%H:%M UTC")
        lines.append(f"🎬 {bi(_title_of(media))} — {sc('ep')} {it.get('episode')} • {air_time}")
    return _card(lines)


@app.on_message(filters.command("schedule"))
async def schedule_command(client_app: Client, message: Message):
    status = await message.reply_text(_card([f"⏳ {sc('fetching schedule')}..."]))

    start, end = _today_window()
    all_items: list[dict] = []
    try:
        page = 1
        while True:
            data = client.get_schedule(start, end, page=page)
            all_items.extend(data.get("airingSchedules", []))
            if not data.get("pageInfo", {}).get("hasNextPage"):
                break
            page += 1
    except AnimexAPIError as exc:
        await status.edit_text(_card([f"❌ {sc('failed to fetch schedule')}", "", str(exc)]))
        return

    text = _format_schedule(all_items)
    photo = random.choice(RANDOM_IMAGES)
    try:
        await message.reply_photo(photo, caption=text)
        await status.delete()
    except Exception:
        await status.edit_text(text)

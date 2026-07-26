#@cantarellabots
from __future__ import annotations

import asyncio
import random
from datetime import datetime, timedelta, timezone

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import CallbackQuery, InlineKeyboardMarkup, Message
from cantarella.button import Button as InlineKeyboardButton

from cantarella.core.database import db
from cantarella.core.images import get_random_image
from cantarella.scraper.animex import AnimexClient, AnimexAPIError
from cantarella.scraper.animexdl import AnimexDownloader, sc
from config import OWNER_ID

client = AnimexClient()
downloader = AnimexDownloader(client=client)

# ---------------------------------------------------------------------------
# Compatibility shim: AnimexDownloader.download_with_fallback() calls
# `self.client.iter_providers(anime_id, ep_num, type_)`, which AnimexClient
# doesn't define (only get_servers()/get_default_provider() do). Add it once.
# ---------------------------------------------------------------------------
if not hasattr(AnimexClient, "iter_providers"):

    def _iter_providers(self, anime_id, ep_num, type_="sub"):
        data = self.get_servers(anime_id, ep_num)
        return data.get(f"{type_}Providers") or []

    AnimexClient.iter_providers = _iter_providers

QUALITY_LABELS = {"1": "360p", "2": "720p", "3": "1080p"}

# One active search flow per user (matches this project's core/state.py pattern).
_sessions: dict[int, dict] = {}


def _card(text: str) -> str:
    return f"<blockquote>{text}</blockquote>"


def _ep_number(ep: dict) -> int:
    return ep.get("number") or ep.get("episode") or ep.get("id")


def _title_of(anime: dict) -> str:
    return anime.get("titleEnglish") or anime.get("titleRomaji") or "Unknown"


async def _is_authorized(user_id: int) -> bool:
    return user_id == OWNER_ID or await db.is_admin(user_id)


# ─────────────────────────────────────────────
#  /search
# ─────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("search"))
async def search_command(client_app: Client, message: Message):
    if not await _is_authorized(message.from_user.id):
        return await message.reply_text(
            _card(f"❌ <b>{sc('only admins can search or download anime')}</b>"),
            parse_mode=ParseMode.HTML,
        )

    if len(message.command) < 2:
        return await message.reply_text(
            _card(f"⚠️ <b>{sc('usage')}</b>\n/search &lt;name&gt;"), parse_mode=ParseMode.HTML
        )

    query = " ".join(message.command[1:])
    status = await message.reply_photo(
        get_random_image(),
        caption=_card(f"🔎 <b>{sc('searching')} \"{query}\"...</b>"),
        parse_mode=ParseMode.HTML,
    )

    try:
        results = await asyncio.to_thread(client.search, query, 10)
    except AnimexAPIError as exc:
        return await status.edit_caption(
            _card(f"❌ <b>{sc('search failed')}</b>\n{exc}"), parse_mode=ParseMode.HTML
        )

    if not results:
        return await status.edit_caption(
            _card(f"🚫 {sc('no results for')} \"{query}\""), parse_mode=ParseMode.HTML
        )

    _sessions[message.from_user.id] = {"query": query, "results": results, "selected": {}}

    rows = [
        [InlineKeyboardButton(f"🎬 {_title_of(item)}", callback_data=f"anx_pick:{idx}")]
        for idx, item in enumerate(results)
    ]
    await status.edit_caption(
        _card(f"🔎 <b>{sc('results for')} \"{query}\"</b>\n{sc('tap a title to continue')}"),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


@Client.on_callback_query(filters.regex(r"^anx_pick:"))
async def on_pick_anime(client_app: Client, cq: CallbackQuery):
    session = _sessions.get(cq.from_user.id)
    if not session:
        return await cq.answer("Session expired — run /search again", show_alert=True)

    idx = int(cq.data.split(":")[1])
    anime = session["results"][idx]
    session["anime"] = anime
    session["anime_id"] = str(anime.get("id") or anime.get("anilistId"))
    title = _title_of(anime)

    await cq.answer()
    await cq.message.edit_caption(
        _card(f"📚 {sc('loading episodes for')} <b>{title}</b>"), parse_mode=ParseMode.HTML
    )

    try:
        episodes = await asyncio.to_thread(client.get_episodes, session["anime_id"])
    except AnimexAPIError as exc:
        return await cq.message.edit_caption(
            _card(f"❌ {sc('failed to load episodes')}\n{exc}"), parse_mode=ParseMode.HTML
        )

    session["episodes"] = episodes
    rows, row = [], []
    for ep in episodes:
        row.append(InlineKeyboardButton(str(_ep_number(ep)), callback_data=f"anx_ep:{_ep_number(ep)}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(f"⬇️ {sc('download all')}", callback_data="anx_all")])

    await cq.message.edit_caption(
        _card(
            f"🎬 <b>{title}</b>\n"
            f"📺 {sc('total episodes')}: {len(episodes)}\n"
            f"{sc('pick an episode or download all')}"
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


def _quality_keyboard(session: dict, key: int, target: str) -> InlineKeyboardMarkup:
    chosen = session["selected"].get(key, set())
    row = [
        InlineKeyboardButton(
            f"{'✅ ' if code in chosen else ''}{label}", callback_data=f"anx_q:{target}:{code}"
        )
        for code, label in QUALITY_LABELS.items()
    ]
    done_row = [InlineKeyboardButton(f"✔️ {sc('done')}", callback_data=f"anx_done:{target}")]
    return InlineKeyboardMarkup([row, done_row])


@Client.on_callback_query(filters.regex(r"^anx_ep:"))
async def on_pick_episode(client_app: Client, cq: CallbackQuery):
    session = _sessions.get(cq.from_user.id)
    if not session:
        return await cq.answer("Session expired — run /search again", show_alert=True)

    ep_num = int(cq.data.split(":")[1])
    await cq.answer()
    title = _title_of(session["anime"])
    await cq.message.edit_caption(
        _card(f"🎬 <b>{title}</b>\n📁 {sc('episode')} {ep_num}\n{sc('choose quality, then tap done')}"),
        parse_mode=ParseMode.HTML,
        reply_markup=_quality_keyboard(session, ep_num, str(ep_num)),
    )


@Client.on_callback_query(filters.regex(r"^anx_all$"))
async def on_pick_all(client_app: Client, cq: CallbackQuery):
    session = _sessions.get(cq.from_user.id)
    if not session:
        return await cq.answer("Session expired — run /search again", show_alert=True)

    await cq.answer()
    title = _title_of(session["anime"])
    await cq.message.edit_caption(
        _card(
            f"🎬 <b>{title}</b>\n"
            f"⬇️ {sc('download all')} ({len(session['episodes'])} {sc('episodes')})\n"
            f"{sc('choose quality, then tap done')}"
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=_quality_keyboard(session, -1, "all"),
    )


@Client.on_callback_query(filters.regex(r"^anx_q:"))
async def on_toggle_quality(client_app: Client, cq: CallbackQuery):
    session = _sessions.get(cq.from_user.id)
    if not session:
        return await cq.answer("Session expired — run /search again", show_alert=True)

    _, target, code = cq.data.split(":")
    key = -1 if target == "all" else int(target)
    chosen = session["selected"].setdefault(key, set())
    if code in chosen:
        chosen.discard(code)
        await cq.answer(f"{QUALITY_LABELS[code]} removed")
    else:
        chosen.add(code)
        await cq.answer(f"{QUALITY_LABELS[code]} selected")

    await cq.message.edit_reply_markup(_quality_keyboard(session, key, target))


@Client.on_callback_query(filters.regex(r"^anx_done:"))
async def on_done(client_app: Client, cq: CallbackQuery):
    session = _sessions.get(cq.from_user.id)
    if not session:
        return await cq.answer("Session expired — run /search again", show_alert=True)

    target = cq.data.split(":", 1)[1]
    key = -1 if target == "all" else int(target)
    codes = session["selected"].get(key)
    if not codes:
        return await cq.answer("Pick at least one quality first", show_alert=True)

    qualities = [QUALITY_LABELS[c] for c in sorted(codes)]
    await cq.answer("Starting download")

    title = _title_of(session["anime"])
    anime_id = session["anime_id"]
    chat_id = cq.message.chat.id

    if target == "all":
        eps_to_run = session["episodes"]
        header = f"⬇️ {sc('download all')} — <b>{title}</b>"
    else:
        ep_num = int(target)
        eps_to_run = [e for e in session["episodes"] if _ep_number(e) == ep_num]
        header = f"⬇️ <b>{title}</b> — {sc('episode')} {ep_num}"

    status = await cq.message.reply_text(
        _card(f"{header}\n🎚️ {sc('quality')}: {', '.join(qualities)}"), parse_mode=ParseMode.HTML
    )

    asyncio.create_task(_run_downloads(client_app, chat_id, anime_id, eps_to_run, qualities, title, status))


async def _run_downloads(client_app, chat_id, anime_id, episodes, qualities, title, status):
    for ep in episodes:
        ep_num = _ep_number(ep)
        try:
            await downloader.process_episode(
                client_app, chat_id, anime_id, ep_num,
                anime_title=title, qualities=qualities, status_msg=status,
            )
        except Exception as exc:
            await status.edit_text(
                _card(f"❌ {sc('error on episode')} {ep_num}\n{exc}"), parse_mode=ParseMode.HTML
            )

    try:
        await client_app.send_message(
            chat_id,
            _card(f"🎉 {sc('all downloads finished')}\n<b>{title}</b>"),
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


# ─────────────────────────────────────────────
#  /schedule
# ─────────────────────────────────────────────

def _today_window() -> tuple[int, int]:
    now = datetime.now(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1) - timedelta(seconds=1)
    return int(start.timestamp()), int(end.timestamp())


def _schedule_title(media: dict) -> str:
    title = media.get("title") or {}
    return title.get("english") or title.get("romaji") or title.get("userPreferred") or "Unknown"


@Client.on_message(filters.command(["schedule", "ongoing"]))
async def schedule_command(client_app: Client, message: Message):
    status = await message.reply_text(
        _card(f"⏳ {sc('fetching schedule')}..."), parse_mode=ParseMode.HTML
    )

    start, end = _today_window()
    all_items: list[dict] = []
    try:
        page = 1
        while True:
            data = await asyncio.to_thread(client.get_schedule, start, end, page)
            all_items.extend(data.get("airingSchedules", []))
            if not data.get("pageInfo", {}).get("hasNextPage"):
                break
            page += 1
    except AnimexAPIError as exc:
        return await status.edit_text(
            _card(f"❌ {sc('failed to fetch schedule')}\n{exc}"), parse_mode=ParseMode.HTML
        )

    if not all_items:
        text = _card(f"📅 <b>{sc('todays schedule')}</b>\n{sc('nothing airing today')}")
    else:
        lines = [f"📅 <b>{sc('todays schedule')}</b>", ""]
        for it in sorted(all_items, key=lambda x: x["airingAt"]):
            media = it.get("media") or {}
            air_time = datetime.fromtimestamp(it["airingAt"], tz=timezone.utc).strftime("%H:%M UTC")
            lines.append(f"🎬 <b>{_schedule_title(media)}</b> — {sc('ep')} {it.get('episode')} • {air_time}")
        text = _card("\n".join(lines))

    # Telegram caps photo captions at 1024 chars. A busy day's schedule can
    # blow past that, so only use the full list as the caption when it fits —
    # otherwise send the full list first as plain text, then the pic with a
    # short caption right after (pic goes with the second message).
    CAPTION_LIMIT = 1024
    photo = get_random_image()
    try:
        if len(text) <= CAPTION_LIMIT:
            await message.reply_photo(photo, caption=text, parse_mode=ParseMode.HTML)
        else:
            await message.reply_text(text, parse_mode=ParseMode.HTML)
            header = _card(f"📅 <b>{sc('todays schedule')}</b>")
            await message.reply_photo(photo, caption=header, parse_mode=ParseMode.HTML)
        await status.delete()
    except Exception:
        await status.edit_text(text, parse_mode=ParseMode.HTML)

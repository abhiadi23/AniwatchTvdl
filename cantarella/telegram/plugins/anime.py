from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import CallbackQuery, InlineKeyboardMarkup, Message
from cantarella.button import Button as InlineKeyboardButton

from cantarella.core.database import db
from cantarella.core.images import get_random_image
from cantarella.scraper.animex import AnimexClient, AnimexAPIError
from cantarella.scraper.animexdl import AnimexDownloader, sc
from cantarella.scraper.miruro import (
    search_anime as miruro_search_anime,
    get_anime_episodes as miruro_get_anime_episodes,
    get_schedule as miruro_get_schedule,
)
from cantarella.scraper.mirurodl import MiruroDownloader
from config import OWNER_ID

logger = logging.getLogger(__name__)

animex_client = AnimexClient()
animex_downloader = AnimexDownloader(client=animex_client)
miruro_downloader = MiruroDownloader()

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
#  Source adapters — everything that differs between animex and miruro
#  (search shape, episode listing shape, schedule shape) lives here so
#  the rest of this file can stay source-agnostic.
# ─────────────────────────────────────────────

def _animex_search(query: str, limit: int = 10) -> list[dict]:
    results = animex_client.search(query, limit)
    out = []
    for r in results:
        rid = r.get("id") or r.get("anilistId")
        if rid is None:
            continue
        out.append({"id": str(rid), "title": _title_of(r)})
    return out


def _miruro_search(query: str, limit: int = 10) -> list[dict]:
    results = miruro_search_anime(query)[:limit]
    out = []
    for r in results:
        rid = r.get("id") or r.get("anilistId")
        if rid is None:
            continue
        title_obj = r.get("title") or {}
        title = (
            title_obj.get("english") or title_obj.get("romaji")
            or r.get("titleEnglish") or r.get("titleRomaji") or "Unknown"
        )
        out.append({"id": str(rid), "title": title})
    return out


def _animex_episode_numbers(anime_id: str) -> list[int]:
    episodes = animex_client.get_episodes(anime_id)
    nums = set()
    for e in episodes:
        n = _ep_number(e)
        if n is not None:
            nums.add(int(n))
    return sorted(nums)


def _miruro_episode_numbers(anilist_id: str) -> list[int]:
    data = miruro_get_anime_episodes(anilist_id)
    providers = data.get("providers", {}) or {}
    nums = set()
    for block in providers.values():
        eps = block.get("episodes", {}) or {}
        for lst in eps.values():
            for e in lst:
                n = e.get("number")
                if isinstance(n, (int, float)):
                    nums.add(int(n))
    return sorted(nums)


def _animex_schedule_items(start: int, end: int) -> list[dict]:
    items: list[dict] = []
    page = 1
    while True:
        data = animex_client.get_schedule(start, end, page)
        items.extend(data.get("airingSchedules", []))
        if not data.get("pageInfo", {}).get("hasNextPage"):
            break
        page += 1
    return items


def _miruro_schedule_items(start: int, end: int) -> list[dict]:
    data = miruro_get_schedule(newest=True)

    # get_schedule may return a raw list or a dict wrapper
    if isinstance(data, list):
        raw = data
    elif isinstance(data, dict):
        raw = data.get("airingSchedules") or data.get("schedules") or data.get("data") or []
    else:
        raw = []

    out: list[dict] = []
    for it in raw:
        airing_at = it.get("airingAt")
        if airing_at is None or not (start <= airing_at <= end):
            continue
        media = it.get("media") or it  # some payloads put title fields at top level
        title_obj = media.get("title") or {
            "english": media.get("titleEnglish"),
            "romaji": media.get("titleRomaji"),
        }
        out.append({"airingAt": airing_at, "episode": it.get("episode"), "media": {"title": title_obj}})
    return out


_SOURCES = {
    "animex": {
        "search": _animex_search,
        "episode_numbers": _animex_episode_numbers,
        "schedule": _animex_schedule_items,
        "downloader": animex_downloader,
        "label": "ANIMEX",
    },
    "miruro": {
        "search": _miruro_search,
        "episode_numbers": _miruro_episode_numbers,
        "schedule": _miruro_schedule_items,
        "downloader": miruro_downloader,
        "label": "MIRURO",
    },
}


async def _get_source() -> tuple[str, dict]:
    """Read the user's active_source setting fresh on every /search or
    /schedule call, so flipping it in /manage takes effect immediately —
    an in-flight session keeps whatever source it started with."""
    name = await db.get_user_setting(0, "active_source", "animex")
    if name not in _SOURCES:
        name = "animex"
    return name, _SOURCES[name]


def _target_key(target: str) -> int:
    if target == "all":
        return -1
    if target.startswith("range-"):
        return -2
    return int(target)


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
    source_name, source = await _get_source()
    logger.info("User %s searching '%s' on %s", message.from_user.id, query, source_name)

    status = await message.reply_photo(
        get_random_image(),
        caption=_card(f"🔎 <b>{sc('searching')} \"{query}\"</b> {sc('on')} {source['label']}..."),
        parse_mode=ParseMode.HTML,
    )

    try:
        results = await asyncio.to_thread(source["search"], query, 10)
    except AnimexAPIError as exc:
        return await status.edit_caption(
            _card(f"❌ <b>{sc('search failed')}</b>\n{exc}"), parse_mode=ParseMode.HTML
        )
    except Exception as exc:
        logger.exception("Search failed on %s for '%s'", source_name, query)
        return await status.edit_caption(
            _card(f"❌ <b>{sc('search failed')}</b>\n{exc}"), parse_mode=ParseMode.HTML
        )

    if not results:
        return await status.edit_caption(
            _card(f"🚫 {sc('no results for')} \"{query}\""), parse_mode=ParseMode.HTML
        )

    _sessions[message.from_user.id] = {
        "source": source_name,
        "query": query,
        "results": results,
        "selected": {},
        "awaiting_range": False,
    }
    await _show_results(status, _sessions[message.from_user.id])


def _results_markup(session: dict) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(f"🎬 {r['title']}", callback_data=f"anx_pick:{idx}")]
        for idx, r in enumerate(session["results"])
    ]
    return InlineKeyboardMarkup(rows)


async def _show_results(msg, session: dict) -> None:
    text = _card(
        f"🔎 <b>{sc('results for')} \"{session['query']}\"</b>\n{sc('tap a title to continue')}"
    )
    await msg.edit_caption(text, parse_mode=ParseMode.HTML, reply_markup=_results_markup(session))


@Client.on_callback_query(filters.regex(r"^anx_back_results$"))
async def on_back_results(client_app: Client, cq: CallbackQuery):
    session = _sessions.get(cq.from_user.id)
    if not session:
        return await cq.answer("Session expired — run /search again", show_alert=True)
    await cq.answer()
    await _show_results(cq.message, session)


@Client.on_callback_query(filters.regex(r"^anx_pick:"))
async def on_pick_anime(client_app: Client, cq: CallbackQuery):
    session = _sessions.get(cq.from_user.id)
    if not session:
        return await cq.answer("Session expired — run /search again", show_alert=True)

    idx = int(cq.data.split(":")[1])
    anime = session["results"][idx]
    session["anime"] = anime
    session["selected"] = {}
    title = anime["title"]

    await cq.answer()
    await cq.message.edit_caption(
        _card(f"📚 {sc('loading episodes for')} <b>{title}</b>"), parse_mode=ParseMode.HTML
    )

    source = _SOURCES[session["source"]]
    try:
        numbers = await asyncio.to_thread(source["episode_numbers"], anime["id"])
    except AnimexAPIError as exc:
        return await cq.message.edit_caption(
            _card(f"❌ {sc('failed to load episodes')}\n{exc}"), parse_mode=ParseMode.HTML
        )
    except Exception as exc:
        logger.exception("Failed to load episodes for %s (%s)", title, session["source"])
        return await cq.message.edit_caption(
            _card(f"❌ {sc('failed to load episodes')}\n{exc}"), parse_mode=ParseMode.HTML
        )

    if not numbers:
        return await cq.message.edit_caption(
            _card(f"🚫 {sc('no episodes found for')} <b>{title}</b>"), parse_mode=ParseMode.HTML
        )

    session["episode_numbers"] = numbers
    await _show_episode_list(cq.message, session)


def _episode_list_markup(session: dict) -> InlineKeyboardMarkup:
    rows, row = [], []
    for n in session["episode_numbers"]:
        row.append(InlineKeyboardButton(str(n), callback_data=f"anx_ep:{n}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(f"⬇️ {sc('download all')}", callback_data="anx_all")])
    rows.append([InlineKeyboardButton(f"📊 {sc('range download')}", callback_data="anx_range")])
    rows.append([InlineKeyboardButton(f"⬅️ {sc('back')}", callback_data="anx_back_results")])
    return InlineKeyboardMarkup(rows)


async def _show_episode_list(msg, session: dict) -> None:
    title = session["anime"]["title"]
    numbers = session["episode_numbers"]
    text = _card(
        f"🎬 <b>{title}</b>\n"
        f"📺 {sc('total episodes')}: {len(numbers)}\n"
        f"{sc('pick an episode, download all, or a range')}"
    )
    await msg.edit_caption(text, parse_mode=ParseMode.HTML, reply_markup=_episode_list_markup(session))


def _quality_keyboard(session: dict, key: int, target: str) -> InlineKeyboardMarkup:
    chosen = session["selected"].get(key, set())
    row = [
        InlineKeyboardButton(
            f"{'✅ ' if code in chosen else ''}{label}", callback_data=f"anx_q:{target}:{code}"
        )
        for code, label in QUALITY_LABELS.items()
    ]
    action_row = [
        InlineKeyboardButton(f"✔️ {sc('done')}", callback_data=f"anx_done:{target}"),
        InlineKeyboardButton(f"⬅️ {sc('back')}", callback_data="anx_back_eps"),
    ]
    return InlineKeyboardMarkup([row, action_row])


@Client.on_callback_query(filters.regex(r"^anx_back_eps$"))
async def on_back_eps(client_app: Client, cq: CallbackQuery):
    session = _sessions.get(cq.from_user.id)
    if not session:
        return await cq.answer("Session expired — run /search again", show_alert=True)
    session["awaiting_range"] = False
    await cq.answer()
    await _show_episode_list(cq.message, session)


@Client.on_callback_query(filters.regex(r"^anx_ep:"))
async def on_pick_episode(client_app: Client, cq: CallbackQuery):
    session = _sessions.get(cq.from_user.id)
    if not session:
        return await cq.answer("Session expired — run /search again", show_alert=True)

    ep_num = int(cq.data.split(":")[1])
    await cq.answer()
    title = session["anime"]["title"]
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
    title = session["anime"]["title"]
    await cq.message.edit_caption(
        _card(
            f"🎬 <b>{title}</b>\n"
            f"⬇️ {sc('download all')} ({len(session['episode_numbers'])} {sc('episodes')})\n"
            f"{sc('choose quality, then tap done')}"
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=_quality_keyboard(session, -1, "all"),
    )


@Client.on_callback_query(filters.regex(r"^anx_range$"))
async def on_range_prompt(client_app: Client, cq: CallbackQuery):
    session = _sessions.get(cq.from_user.id)
    if not session:
        return await cq.answer("Session expired — run /search again", show_alert=True)

    await cq.answer()
    session["awaiting_range"] = True
    numbers = session["episode_numbers"]
    lo, hi = numbers[0], numbers[-1]
    await cq.message.edit_caption(
        _card(
            f"📊 {sc('reply with an episode range like')} <code>{lo}-{hi}</code>\n"
            f"{sc('example')}: <code>3-8</code>\n"
            f"{sc('total episodes')}: {len(numbers)}"
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton(f"⬅️ {sc('back')}", callback_data="anx_back_eps")]]
        ),
    )


@Client.on_message(filters.private & filters.text & filters.regex(r"^\s*\d+\s*-\s*\d+\s*$"))
async def on_range_input(client_app: Client, message: Message):
    session = _sessions.get(message.from_user.id)
    if not session or not session.get("awaiting_range"):
        return  # not something we're expecting — let other handlers see it

    session["awaiting_range"] = False
    start_s, end_s = re.split(r"-", message.text.strip())
    start, end = int(start_s), int(end_s)
    if start > end:
        start, end = end, start

    numbers = session["episode_numbers"]
    eps_in_range = [n for n in numbers if start <= n <= end]
    if not eps_in_range:
        return await message.reply_text(
            _card(f"🚫 {sc('no episodes in range')} {start}-{end}"), parse_mode=ParseMode.HTML
        )

    title = session["anime"]["title"]
    target = f"range-{start}-{end}"
    await message.reply_text(
        _card(
            f"🎬 <b>{title}</b>\n"
            f"📊 {sc('range')} {start}-{end} ({len(eps_in_range)} {sc('episodes')})\n"
            f"{sc('choose quality, then tap done')}"
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=_quality_keyboard(session, -2, target),
    )


@Client.on_callback_query(filters.regex(r"^anx_q:"))
async def on_toggle_quality(client_app: Client, cq: CallbackQuery):
    session = _sessions.get(cq.from_user.id)
    if not session:
        return await cq.answer("Session expired — run /search again", show_alert=True)

    _, target, code = cq.data.split(":")
    key = _target_key(target)
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
    key = _target_key(target)
    codes = session["selected"].get(key)
    if not codes:
        return await cq.answer("Pick at least one quality first", show_alert=True)

    qualities = [QUALITY_LABELS[c] for c in sorted(codes)]
    await cq.answer("Starting download")

    title = session["anime"]["title"]
    ref_id = session["anime"]["id"]
    chat_id = cq.message.chat.id
    numbers = session["episode_numbers"]

    if target == "all":
        eps_to_run = numbers
        header = f"⬇️ {sc('download all')} — <b>{title}</b>"
    elif target.startswith("range-"):
        _, start_s, end_s = target.split("-")
        start, end = int(start_s), int(end_s)
        eps_to_run = [n for n in numbers if start <= n <= end]
        header = f"📊 {sc('range')} {start}-{end} — <b>{title}</b>"
    else:
        ep_num = int(target)
        eps_to_run = [ep_num]
        header = f"⬇️ <b>{title}</b> — {sc('episode')} {ep_num}"

    downloader = _SOURCES[session["source"]]["downloader"]

    status = await cq.message.reply_text(
        _card(f"{header}\n🎚️ {sc('quality')}: {', '.join(qualities)}"), parse_mode=ParseMode.HTML
    )

    logger.info(
        "User %s starting %s download of '%s' eps=%s qualities=%s via %s",
        cq.from_user.id, target, title, eps_to_run, qualities, session["source"],
    )
    asyncio.create_task(
        _run_downloads(client_app, downloader, chat_id, ref_id, eps_to_run, qualities, title, status)
    )


async def _run_downloads(client_app, downloader, chat_id, ref_id, episodes, qualities, title, status):
    for ep_num in episodes:
        try:
            await downloader.process_episode(
                client_app, chat_id, ref_id, ep_num,
                anime_title=title, qualities=qualities, status_msg=status,
            )
        except Exception as exc:
            logger.exception("Error downloading ep %s of '%s'", ep_num, title)
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
    source_name, source = await _get_source()
    status = await message.reply_text(
        _card(f"⏳ {sc('fetching schedule from')} {source['label']}..."), parse_mode=ParseMode.HTML
    )

    start, end = _today_window()
    try:
        all_items = await asyncio.to_thread(source["schedule"], start, end)
    except AnimexAPIError as exc:
        return await status.edit_text(
            _card(f"❌ {sc('failed to fetch schedule')}\n{exc}"), parse_mode=ParseMode.HTML
        )
    except Exception as exc:
        logger.exception("Failed to fetch schedule from %s", source_name)
        return await status.edit_text(
            _card(f"❌ {sc('failed to fetch schedule')}\n{exc}"), parse_mode=ParseMode.HTML
        )

    if not all_items:
        text = _card(f"📅 <b>{sc('todays schedule')}</b> ({source['label']})\n{sc('nothing airing today')}")
    else:
        lines = [f"📅 <b>{sc('todays schedule')}</b> ({source['label']})", ""]
        for it in sorted(all_items, key=lambda x: x["airingAt"]):
            media = it.get("media") or {}
            air_time = datetime.fromtimestamp(it["airingAt"], tz=timezone.utc).strftime("%H:%M UTC")
            lines.append(f"🎬 <b>{_schedule_title(media)}</b> — {sc('ep')} {it.get('episode')} • {air_time}")
        text = _card("\n".join(lines))

    # Telegram caps photo captions at 1024 chars. A busy day's schedule can
    # blow past that — previously that case sent the full text AND a photo
    # with a short header as two separate replies, which read as a broken
    # duplicate response. Now: send with a photo when it fits in a caption,
    # otherwise just send the text card alone (single message either way).
    CAPTION_LIMIT = 1024
    try:
        if len(text) <= CAPTION_LIMIT:
            photo = get_random_image()
            await message.reply_photo(photo, caption=text, parse_mode=ParseMode.HTML)
        else:
            await message.reply_text(text, parse_mode=ParseMode.HTML)
        await status.delete()
    except Exception:
        await status.edit_text(text, parse_mode=ParseMode.HTML)

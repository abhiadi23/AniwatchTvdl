from __future__ import annotations

import asyncio
import random
import time
import uuid

from pyrogram import Client, filters
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# NOTE: adjust these two imports to match your actual project layout —
# `app` is assumed to be the already-configured Pyrogram Client instance.
from bot import app
from .animex import AnimexClient, AnimexAPIError
from .animexdl import AnimexDownloader, sc, bi, _card

client = AnimexClient()
downloader = AnimexDownloader(client=client)

# ---------------------------------------------------------------------------
# Compatibility shim: downloader.download_with_fallback() calls
# `self.client.iter_providers(anime_id, ep_num, type_)`, which isn't defined
# on AnimexClient (only get_servers()/get_default_provider() are). This adds
# it once, at import time, so schedule.py / search.py / ongoing.py can all
# just import AnimexClient normally and it works.
# ---------------------------------------------------------------------------
if not hasattr(AnimexClient, "iter_providers"):

    def _iter_providers(self, anime_id, ep_num, type_="sub"):
        data = self.get_servers(anime_id, ep_num)
        return data.get(f"{type_}Providers") or []

    AnimexClient.iter_providers = _iter_providers

# Shared random-image pool — schedule.py and ongoing.py import this too.
RANDOM_IMAGES = [
    "https://img3.teletype.in/files/67/73/67735f4f-933a-41d9-86b9-609fa03b6614.jpeg",
    "https://img3.teletype.in/files/a6/b6/a6b666ef-afa0-4793-bd6b-235265258840.jpeg",
    "https://img3.teletype.in/files/e8/01/e8013193-9299-4cdc-8222-f4e3801a05e8.jpeg",
    "https://img4.teletype.in/files/77/7f/777f2c2d-fa53-4298-9dee-ab39d9bddf81.jpeg",
    "https://img3.teletype.in/files/a1/9e/a19e9352-dfee-471a-ae3f-14eb2e1b975b.jpeg",
    "https://img1.teletype.in/files/84/84/8484934a-a247-4b1a-8f1f-74aac621bea6.jpeg",
    "https://img4.teletype.in/files/b2/89/b289d67c-2299-4cf6-91c3-b84c83c57caa.jpeg",
    "https://img3.teletype.in/files/a0/49/a049a7b1-2924-41c1-95d4-8c466c1a80ad.jpeg",
    "https://img2.teletype.in/files/59/b3/59b3a62e-e2ce-4f00-847d-9910f0498884.jpeg",
    "https://img2.teletype.in/files/91/d8/91d8838b-85ec-45ff-868f-24d66126ce55.jpeg",
    "https://img4.teletype.in/files/71/a5/71a5481f-2398-4520-8229-222d1cf733e7.jpeg",
    "https://img4.teletype.in/files/f4/b0/f4b007ec-fc8c-49fd-a1fb-b0d02985120a.jpeg",
    "https://img4.teletype.in/files/f6/3c/f63cee0d-10ff-4b8d-9ccc-943fa80a1344.jpeg",
    "https://img4.teletype.in/files/77/ff/77ff451d-0c8a-4aeb-aa9a-a1ae7ca74069.jpeg",
    "https://img4.teletype.in/files/bb/e9/bbe9e4f6-6226-4764-8169-b7d368e29e8c.jpeg",
    "https://img2.teletype.in/files/d4/b8/d4b806a2-c534-466f-85cb-f05a9e31dc92.jpeg",
    "https://img4.teletype.in/files/b6/aa/b6aab772-1d39-4b7e-bfe5-8d04b57ac31e.jpeg",
    "https://img4.teletype.in/files/f5/c3/f5c3a05e-ecfb-4a8e-b921-2b264d40d0ce.jpeg",
    "https://img4.teletype.in/files/3f/01/3f0102af-352a-4a0a-abbd-f18919c56dc9.jpeg",
    "https://img4.teletype.in/files/7f/f2/7ff228ef-6e74-4baf-a877-b35c016d6c7b.jpeg",
    "https://img1.teletype.in/files/8b/02/8b02924e-4f24-4ace-8b3f-be2f8044b8ec.jpeg",
    "https://img2.teletype.in/files/dc/16/dc1625b2-410c-48da-98c1-1956b87768e1.jpeg",
    "https://img2.teletype.in/files/97/f3/97f31df6-2cca-4f58-8269-97aebb6d9ea7.jpeg",
    "https://img2.teletype.in/files/97/65/9765707e-1855-429b-89ba-03401b734827.jpeg",
    "https://img4.teletype.in/files/f4/53/f45390f3-e1eb-4570-9d67-c4114db18589.jpeg",
    "https://img1.teletype.in/files/81/26/81265a94-68ff-47ed-b409-fad382e7a627.jpeg",
    "https://img1.teletype.in/files/0a/1b/0a1b5f17-095c-4826-84c8-39a8b9b9deef.jpeg",
    "https://img4.teletype.in/files/f5/94/f594fbe2-b52d-489a-86c9-23b2f2dbe4d7.jpeg",
    "https://img3.teletype.in/files/e3/76/e376be29-065b-4c1a-986d-aba69d08208f.jpeg",
    "https://img1.teletype.in/files/8f/e6/8fe67878-43a3-4b3d-851f-63727a6a2b0b.jpeg",
    "https://img2.teletype.in/files/1a/d3/1ad3fa24-c3bf-4ca8-a7ef-a79286b1e37c.jpeg",
    "https://img1.teletype.in/files/80/1a/801a77ad-bf05-4d7a-96c9-2b1cde09d04f.jpeg",
    "https://ik.imagekit.io/jbxs2z512/naruto_GxcPgSeOy.jpg?updatedAt=1748486799631",
    "https://ik.imagekit.io/jbxs2z512/hd-anime-prr1y1k5gqxfcgpv.jpg?updatedAt=1748487947183",
    "https://ik.imagekit.io/jbxs2z512/dazai-osamu-sunset-rooftop-anime-wallpaper-cover.jpg?updatedAt=1748488276069",
    "https://ik.imagekit.io/jbxs2z512/thumb-1920-736461.png?updatedAt=1748488419323",
]

QUALITY_LABELS = {"1": "360p", "2": "720p", "3": "1080p"}
SESSION_TTL_SECONDS = 30 * 60


class SearchSession:
    def __init__(self, query: str, results: list[dict]):
        self.id = uuid.uuid4().hex[:8]
        self.query = query
        self.results = results
        self.anime: dict | None = None
        self.anime_id: str | None = None
        self.episodes: list[dict] = []
        # key: episode number, or -1 for "download all" -> set of quality codes ("1"/"2"/"3")
        self.selected: dict[int, set[str]] = {}
        self.created = time.monotonic()


SESSIONS: dict[str, SearchSession] = {}


def _cleanup_sessions() -> None:
    now = time.monotonic()
    for sid in [s for s, sess in SESSIONS.items() if now - sess.created > SESSION_TTL_SECONDS]:
        SESSIONS.pop(sid, None)


def _ep_number(ep: dict) -> int:
    return ep.get("number") or ep.get("episode") or ep.get("id")


def _title_of(anime: dict) -> str:
    return anime.get("titleEnglish") or anime.get("titleRomaji") or "Unknown"


def _results_keyboard(session: SearchSession) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(f"🎬 {_title_of(item)}", callback_data=f"sr:{session.id}:{idx}")]
        for idx, item in enumerate(session.results)
    ]
    return InlineKeyboardMarkup(rows)


def _episodes_keyboard(session: SearchSession) -> InlineKeyboardMarkup:
    rows, row = [], []
    for ep in session.episodes:
        row.append(InlineKeyboardButton(str(_ep_number(ep)), callback_data=f"ep:{session.id}:{_ep_number(ep)}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(f"⬇️ {sc('download all')}", callback_data=f"all:{session.id}")])
    return InlineKeyboardMarkup(rows)


def _quality_keyboard(session: SearchSession, key: int, target: str) -> InlineKeyboardMarkup:
    chosen = session.selected.get(key, set())
    row = [
        InlineKeyboardButton(f"{'✅ ' if code in chosen else ''}{label}", callback_data=f"q:{session.id}:{target}:{code}")
        for code, label in QUALITY_LABELS.items()
    ]
    done_row = [InlineKeyboardButton(f"✔️ {sc('done')}", callback_data=f"done:{session.id}:{target}")]
    return InlineKeyboardMarkup([row, done_row])


@app.on_message(filters.command("search"))
async def search_command(client_app: Client, message: Message):
    _cleanup_sessions()
    if len(message.command) < 2:
        await message.reply_text(_card([f"⚠️ {sc('usage')}", "", "/search <name>"]))
        return

    query = " ".join(message.command[1:])
    status = await message.reply_text(_card([f"🔎 {sc('searching')} \"{query}\"..."]))

    try:
        results = client.search(query, limit=10)
    except AnimexAPIError as exc:
        await status.edit_text(_card([f"❌ {sc('search failed')}", "", str(exc)]))
        return

    if not results:
        await status.edit_text(_card([f"🚫 {sc('no results for')} \"{query}\""]))
        return

    session = SearchSession(query, results)
    SESSIONS[session.id] = session

    text = _card([f"🔎 {sc('results for')} \"{query}\"", "", sc("tap a title to continue")])
    await status.edit_text(text, reply_markup=_results_keyboard(session))


@app.on_callback_query(filters.regex(r"^sr:"))
async def on_pick_anime(client_app: Client, cq: CallbackQuery):
    _, sid, idx = cq.data.split(":")
    session = SESSIONS.get(sid)
    if not session:
        await cq.answer("Session expired, search again", show_alert=True)
        return

    anime = session.results[int(idx)]
    session.anime = anime
    session.anime_id = str(anime.get("anilistId") or anime.get("id"))
    title = _title_of(anime)

    await cq.answer()
    await cq.message.edit_text(_card([f"📚 {sc('loading episodes for')}", bi(title)]))

    try:
        episodes = client.get_episodes(session.anime_id)
    except AnimexAPIError as exc:
        await cq.message.edit_text(_card([f"❌ {sc('failed to load episodes')}", "", str(exc)]))
        return

    session.episodes = episodes
    text = _card([
        f"🎬 {bi(title)}", "",
        f"📺 {sc('total episodes')}: {len(episodes)}", "",
        sc("pick an episode or download all"),
    ])
    await cq.message.edit_text(text, reply_markup=_episodes_keyboard(session))


@app.on_callback_query(filters.regex(r"^ep:"))
async def on_pick_episode(client_app: Client, cq: CallbackQuery):
    _, sid, ep_num = cq.data.split(":")
    session = SESSIONS.get(sid)
    if not session:
        await cq.answer("Session expired, search again", show_alert=True)
        return

    ep_num = int(ep_num)
    await cq.answer()
    title = _title_of(session.anime)
    text = _card([f"🎬 {bi(title)}", "", f"📁 {sc('episode')} {ep_num}", "", sc("choose quality, then tap done")])
    await cq.message.edit_text(text, reply_markup=_quality_keyboard(session, ep_num, str(ep_num)))


@app.on_callback_query(filters.regex(r"^all:"))
async def on_pick_all(client_app: Client, cq: CallbackQuery):
    _, sid = cq.data.split(":")
    session = SESSIONS.get(sid)
    if not session:
        await cq.answer("Session expired, search again", show_alert=True)
        return

    await cq.answer()
    title = _title_of(session.anime)
    text = _card([
        f"🎬 {bi(title)}", "",
        f"⬇️ {sc('download all')} ({len(session.episodes)} {sc('episodes')})", "",
        sc("choose quality, then tap done"),
    ])
    await cq.message.edit_text(text, reply_markup=_quality_keyboard(session, -1, "all"))


@app.on_callback_query(filters.regex(r"^q:"))
async def on_toggle_quality(client_app: Client, cq: CallbackQuery):
    _, sid, target, code = cq.data.split(":")
    session = SESSIONS.get(sid)
    if not session:
        await cq.answer("Session expired, search again", show_alert=True)
        return

    key = -1 if target == "all" else int(target)
    chosen = session.selected.setdefault(key, set())
    if code in chosen:
        chosen.discard(code)
        await cq.answer(f"{QUALITY_LABELS[code]} removed")
    else:
        chosen.add(code)
        await cq.answer(f"{QUALITY_LABELS[code]} selected")

    await cq.message.edit_reply_markup(_quality_keyboard(session, key, target))


@app.on_callback_query(filters.regex(r"^done:"))
async def on_done(client_app: Client, cq: CallbackQuery):
    _, sid, target = cq.data.split(":")
    session = SESSIONS.get(sid)
    if not session:
        await cq.answer("Session expired, search again", show_alert=True)
        return

    key = -1 if target == "all" else int(target)
    codes = session.selected.get(key)
    if not codes:
        await cq.answer("Pick at least one quality first", show_alert=True)
        return

    qualities = [QUALITY_LABELS[c] for c in sorted(codes)]
    await cq.answer("Starting download")

    title = _title_of(session.anime)
    chat_id = cq.message.chat.id

    if target == "all":
        eps_to_run = session.episodes
        header = f"⬇️ {sc('download all')} — {bi(title)}"
    else:
        ep_num = int(target)
        eps_to_run = [e for e in session.episodes if _ep_number(e) == ep_num]
        header = f"⬇️ {bi(title)} — {sc('episode')} {ep_num}"

    status = await cq.message.reply_text(_card([header, "", f"🎚️ {sc('quality')}: {', '.join(qualities)}"]))

    asyncio.create_task(
        _run_downloads(client_app, chat_id, session.anime_id, eps_to_run, qualities, title, status)
    )


async def _run_downloads(client_app, chat_id, anime_id, episodes, qualities, title, status):
    for ep in episodes:
        ep_num = _ep_number(ep)
        try:
            await downloader.process_episode(
                client_app, chat_id, anime_id, ep_num,
                anime_title=title, qualities=qualities, status_msg=status,
            )
        except Exception as exc:
            await status.edit_text(_card([f"❌ {sc('error on episode')} {ep_num}", "", str(exc)]))

    photo = random.choice(RANDOM_IMAGES)
    try:
        await client_app.send_photo(
            chat_id, photo, caption=_card([f"🎉 {sc('all downloads finished')}", "", bi(title)])
        )
    except Exception:
        pass

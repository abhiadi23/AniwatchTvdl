from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal, Optional
from urllib.parse import urljoin

from curl_cffi import requests as curl_requests
from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from .miruro import (
    COMMON_HEADERS,
    TOR_PROXY,
    get_anime_episodes,
    get_episode_sources,
    search_anime,
)

logger = logging.getLogger("mirurodl")

DEFAULT_QUALITIES = ["360p", "720p", "1080p"]

# Some providers report a single "auto"/"default" source instead of
# discrete per-quality URLs — that URL is itself an HLS *master*
# playlist listing multiple resolution variants. When we see one of
# these tags we fetch and parse that playlist to get the real,
# individually-downloadable variant URLs.
AUTO_QUALITY_TAGS = {"auto", "default", "multi", "adaptive", ""}

_MASTER_RESOLUTION_RE = re.compile(r"RESOLUTION=(\d+)x(\d+)")
_MASTER_BANDWIDTH_RE = re.compile(r"BANDWIDTH=(\d+)")


def _parse_master_playlist(text: str, base_url: str) -> list[dict[str, Any]]:
    """Parse an HLS master playlist's ``#EXT-X-STREAM-INF`` variants into
    ``{"quality": "1080p", "url": "..."}`` entries, resolving relative
    variant URIs against the master playlist's own URL."""
    variants: list[dict[str, Any]] = []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if not line.startswith("#EXT-X-STREAM-INF"):
            continue
        res_m = _MASTER_RESOLUTION_RE.search(line)
        if res_m:
            quality = f"{int(res_m.group(2))}p"
        else:
            bw_m = _MASTER_BANDWIDTH_RE.search(line)
            quality = f"{int(bw_m.group(1)) // 1000}k" if bw_m else "unknown"
        for nxt in lines[i + 1:]:
            nxt = nxt.strip()
            if not nxt or nxt.startswith("#"):
                continue
            variants.append({"quality": quality, "url": urljoin(base_url, nxt)})
            break
    return variants

PROGRESS_UPDATE_STEP = 1.0   # minimum % change before editing the TG message again
UPLOAD_UPDATE_STEP = 2.0

_SMALL_CAPS_MAP = str.maketrans(
    "abcdefghijklmnopqrstuvwxyz",
    "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘqʀꜱᴛᴜᴠᴡxʏᴢ",
)


def sc(text: str) -> str:
    """Small-caps unicode text."""
    return text.translate(_SMALL_CAPS_MAP)


def bi(text: str) -> str:
    """Bold + italic combined (HTML, matches the bot's default ParseMode.HTML)."""
    return f"<b><i>{text}</i></b>"


# Thumbnail used for every upload — lives at the repo root.
THUMB_PATH = Path(__file__).resolve().parent.parent.parent / "thumb.jpg"

AUDIO_LABELS = {"sub": "Audio: JP", "dub": "Dual Audio"}


def _build_caption(anime_title: str, season: int, ep_num: int, quality: str, type_: str) -> str:
    """Plain, unstyled upload caption — no small-caps/blockquote fonting."""
    audio = AUDIO_LABELS.get(type_, type_.title())
    title = anime_title or "Anime"
    return f"@cantarellabots [S{season}-E{ep_num}] {title} [{quality}] [{audio}]"


def _progress_bar(pct: float, length: int = 10) -> str:
    pct = max(0.0, min(100.0, pct))
    filled = int(round(pct / 100 * length))
    return "●" * filled + "○" * (length - filled)


def _format_size(num_bytes: float) -> str:
    size = float(max(num_bytes, 0))
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}{unit}"
        size /= 1024
    return f"{size:.1f}GB"


def _card(lines: list[str]) -> str:
    """Every line bold+italic, whole block under a real HTML <blockquote> —
    matches the requested Telegram status style (and the ParseMode.HTML the
    client is configured with)."""
    body = "\n".join(bi(line) if line else "" for line in lines)
    return f"<blockquote>{body}</blockquote>"


def _progress_card(
    icon: str,
    action: str,
    ep_num: int,
    quality: str,
    pct: float,
    speed_text: str,
    done_text: str,
    total_text: str,
    footer: str | None = None,
) -> str:
    bar = _progress_bar(pct)
    lines = [
        f"{icon} {sc(action)}...",
        "",
        f"[{bar}] {pct:.1f}%",
        f"📁 {sc('episode')} {ep_num} • {quality}",
        f"⚡ {sc('speed')}: {speed_text}",
        f"📦 {done_text} / {total_text}  {pct:.2f}%",
    ]
    if footer:
        lines.append(footer)
    return _card(lines)


def _server_card(action: str, ep_num: int, quality: str, provider_name: str, idx: int, total: int) -> str:
    lines = [
        f"🔄 {sc(action)}...",
        "",
        f"📁 {sc('episode')} {ep_num} • {quality}",
        f"🌐 {sc('server')}: <code>{provider_name}</code> ({idx}/{total})",
    ]
    return _card(lines)


# ----------------------------------------------------------------------------
# N_m3u8DL-RE output parsing
# ----------------------------------------------------------------------------
# NOTE: these regexes are a best-effort guess at N_m3u8DL-RE's `--no-ansi-color`
# console output (percent / speed / downloaded-total-size). If progress isn't
# showing up, paste a real run's stdout and these patterns can be tightened.

_PCT_RE = re.compile(r"(\d{1,3}(?:\.\d+)?)\s*%")
_SPEED_RE = re.compile(r"([\d.]+\s*[KMGT]i?B/s)", re.IGNORECASE)
_SIZE_PAIR_RE = re.compile(r"([\d.]+\s*[KMGT]i?B)\s*/\s*([\d.]+\s*[KMGT]i?B)", re.IGNORECASE)


def _parse_progress_line(line: str) -> dict[str, Any] | None:
    pct_m = _PCT_RE.search(line)
    if not pct_m:
        return None
    result: dict[str, Any] = {"pct": float(pct_m.group(1))}
    speed_m = _SPEED_RE.search(line)
    if speed_m:
        result["speed"] = speed_m.group(1).replace(" ", "")
    size_m = _SIZE_PAIR_RE.search(line)
    if size_m:
        result["downloaded"] = size_m.group(1).replace(" ", "")
        result["total"] = size_m.group(2).replace(" ", "")
    return result


# ----------------------------------------------------------------------------
# Result container
# ----------------------------------------------------------------------------


@dataclass
class DownloadResult:
    quality: str
    file_path: Path
    provider_id: str
    provider_name: str
    type_: str
    subtitle_paths: list[Path] = field(default_factory=list)


class DownloadFailedError(Exception):
    """Raised when every provider/server has been exhausted for a quality."""


# ----------------------------------------------------------------------------
# Downloader / uploader
# ----------------------------------------------------------------------------


class MiruroDownloader:
    def __init__(
        self,
        work_dir: str | Path = "downloads",
        binary_dir: str | Path = "binary",
    ) -> None:
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.binary_dir = Path(binary_dir)
        self.binary_path = self._get_binary_path()
        # Shared session for subtitle/track fetches (mirrors the headers
        # miruro_scraper.fetch_data uses against the pipe endpoint).
        self.session = curl_requests.Session()
        self.timeout = 15
        self.impersonate = "chrome"

    # -- binary discovery -----------------------------------------------

    def _get_binary_path(self) -> Path:
        candidates = [
            self.binary_dir / "N_m3u8DL-RE",        # Linux (local binary folder)
            self.binary_dir / "N_m3u8DL-RE.exe",     # Windows local
            Path("/usr/local/bin/N_m3u8DL-RE"),       # Docker / Heroku container
        ]
        for p in candidates:
            if p.exists():
                logger.info("Found N_m3u8DL-RE binary at: %s", p)
                return p
        which_path = shutil.which("N_m3u8DL-RE")
        if which_path:
            logger.info("Found N_m3u8DL-RE in PATH: %s", which_path)
            return Path(which_path)
        raise FileNotFoundError(
            f"N_m3u8DL-RE binary not found. Checked: {candidates} and PATH"
        )

    # -- provider / episode lookup ---------------------------------------

    @staticmethod
    def iter_providers(
        episodes_data: dict[str, Any], ep_num: int, type_: str
    ) -> list[dict[str, Any]]:
        """Flatten episodes_data['providers'] into a list of
        {"name": provider_name, "episode_id": ...} entries that actually
        list the requested episode number under sub/dub."""
        providers = episodes_data.get("providers", {}) or {}
        out: list[dict[str, Any]] = []
        for provider_name, provider_block in providers.items():
            episode_list = (provider_block.get("episodes", {}) or {}).get(type_, [])
            entry = next((ep for ep in episode_list if ep.get("number") == ep_num), None)
            if entry is None:
                continue
            out.append({"name": provider_name, "episode_id": entry.get("id")})
        return out

    # -- quality selection -------------------------------------------------

    @staticmethod
    def _select_source_for_quality(
        sources: list[dict[str, Any]], quality: str
    ) -> dict[str, Any] | None:
        target = quality.rstrip("p")
        for s in sources:
            if (s.get("quality") or "").rstrip("p") == target:
                return s

        numeric = [
            (int(q), s)
            for s in sources
            if (q := (s.get("quality") or "").rstrip("p")).isdigit()
        ]
        if not numeric:
            return sources[0] if sources else None
        numeric.sort(key=lambda pair: pair[0])
        target_int = int(target) if target.isdigit() else 0
        lower_or_equal = [pair for pair in numeric if pair[0] <= target_int]
        return (lower_or_equal[-1] if lower_or_equal else numeric[0])[1]

    def _fetch_master_variants(
        self, master_url: str, headers: dict[str, str]
    ) -> list[dict[str, Any]]:
        """Fetch a provider's 'auto'/'default' URL and parse it as an HLS
        master playlist, returning the real per-resolution variant URLs.
        Runs synchronously — call via ``asyncio.to_thread``."""
        resp = self.session.get(
            master_url,
            headers=headers,
            timeout=self.timeout,
            impersonate=self.impersonate,
            proxy=TOR_PROXY,
        )
        resp.raise_for_status()
        return _parse_master_playlist(resp.text, master_url)

    @staticmethod
    def _probe_video(path: Path) -> tuple[int, int, int]:
        """Return (duration_seconds, width, height) via ffprobe. Telegram
        clients render an upload as a proper video player (with thumb and
        duration bar) only when these are supplied explicitly — without
        them, especially for .mkv, Telegram/Pyrogram can silently fall
        back to showing it as a plain document."""
        try:
            proc = subprocess.run(
                [
                    "ffprobe", "-v", "error", "-select_streams", "v:0",
                    "-show_entries", "stream=width,height",
                    "-show_entries", "format=duration",
                    "-of", "json", str(path),
                ],
                capture_output=True, text=True, timeout=30,
            )
            data = json.loads(proc.stdout or "{}")
            stream = (data.get("streams") or [{}])[0]
            width = int(stream.get("width") or 0)
            height = int(stream.get("height") or 0)
            duration = int(float((data.get("format") or {}).get("duration") or 0))
            return duration, width, height
        except Exception as exc:
            logger.warning("ffprobe failed for %s: %s", path.name, exc)
            return 0, 0, 0

    @staticmethod
    def _mp4_alias_for_mime(path: Path) -> Path:
        """Pyrogram picks the upload's mime_type purely from the *path*
        given to send_video(video=...) (via mimetypes.guess_type), not
        from file_name — and Telegram clients decide whether to render
        the inline video-player bubble largely off that mime_type.
        'video/x-matroska' (.mkv) often renders as a plain document even
        with correct duration/width/height, while 'video/mp4' reliably
        shows the video bubble everywhere. So: upload through a same-
        content hardlink named *.mp4 (mime_type becomes video/mp4), but
        keep file_name= pointing at the real .mkv name so the file
        Telegram actually stores/shows is still named/sent as .mkv."""
        if path.suffix.lower() != ".mkv":
            return path
        alias = path.with_suffix(".mp4")
        if alias.exists():
            alias.unlink()
        try:
            os.link(path, alias)
        except OSError:
            shutil.copy2(path, alias)
        return alias

    # -- N_m3u8DL-RE (async, streams live progress) -------------------------

    async def _run_n_m3u8dl(
        self,
        m3u8_url: str,
        headers: dict[str, str],
        out_name: str,
        out_dir: Path,
        on_progress: Optional[Callable[[float, str, str, str], "asyncio.Future"]] = None,
    ) -> Path:
        out_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            str(self.binary_path),
            m3u8_url,
            "--proxy", TOR_PROXY,
            "--save-name", out_name,
            "--save-dir", str(out_dir),
            "--tmp-dir", str(out_dir / "tmp"),
            "--auto-select",
            "--no-ansi-color",
            "--del-after-done", "true",
            "--binary-merge", "false",
            "--check-segments-count", "false",
        ]
        for key, value in headers.items():
            cmd += ["--header", f"{key}: {value}"]

        logger.info("Running N_m3u8DL-RE for %s -> %s (via %s)", out_name, out_dir, TOR_PROXY)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        assert proc.stdout is not None

        last_pct_reported = -1.0
        tail: list[str] = []

        while True:
            line_bytes = await proc.stdout.readline()
            if not line_bytes:
                break
            line = line_bytes.decode(errors="ignore").strip()
            if not line:
                continue
            tail.append(line)
            if len(tail) > 40:
                tail.pop(0)

            parsed = _parse_progress_line(line)
            if parsed and on_progress:
                pct = parsed["pct"]
                if pct - last_pct_reported >= PROGRESS_UPDATE_STEP or pct >= 99.9:
                    last_pct_reported = pct
                    await on_progress(
                        pct,
                        parsed.get("speed", "—"),
                        parsed.get("downloaded", "—"),
                        parsed.get("total", "—"),
                    )

        returncode = await proc.wait()
        if returncode != 0:
            logger.error(
                "N_m3u8DL-RE exited %s for %s: %s",
                returncode, out_name, "\n".join(tail[-20:]),
            )
            raise RuntimeError(f"N_m3u8DL-RE failed with exit code {returncode}")

        for ext in (".mp4", ".mkv", ".ts"):
            candidate = out_dir / f"{out_name}{ext}"
            if candidate.exists():
                return candidate

        matches = [
            p for p in out_dir.glob(f"{out_name}*")
            if p.is_file() and p.suffix not in (".vtt", ".srt")
        ]
        if matches:
            return matches[0]

        raise FileNotFoundError(
            f"N_m3u8DL-RE reported success but no output file was found for {out_name}"
        )

    @staticmethod
    def _remux_to_mkv(src: Path) -> Path:
        """Always deliver the final upload as an .mkv container (matroska
        doesn't need the mp4-only +faststart flag to be streamable)."""
        if src.suffix.lower() == ".mkv":
            return src
        dst = src.with_suffix(".mkv")
        cmd = ["ffmpeg", "-y", "-i", str(src), "-c", "copy", str(dst)]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0 or not dst.exists():
            logger.warning(
                "ffmpeg mkv remux failed, keeping original file for %s: %s",
                src.name, proc.stderr[-1000:],
            )
            return src
        src.unlink(missing_ok=True)
        return dst

    # -- subtitles ------------------------------------------------------

    async def _fetch_track_subtitles(
        self, tracks: list[dict[str, Any]], out_name: str, out_dir: Path
    ) -> list[Path]:
        """Some providers return explicit subtitle track URLs in the
        sources payload's `tracks` / `subtitles` field. Providers that only
        hardsub return an empty/null tracks list, so this is a no-op for those."""
        saved: list[Path] = []
        for i, track in enumerate(tracks or []):
            # Miruro uses "url" in tracks and "file" in subtitles — accept both
            url = track.get("url") or track.get("file")
            if not url:
                continue
            lang = track.get("lang") or track.get("language") or track.get("label") or f"sub{i}"
            dest = out_dir / f"{out_name}.{lang}.vtt"
            try:
                resp = await asyncio.to_thread(
                    self.session.get,
                    url,
                    timeout=self.timeout,
                    impersonate=self.impersonate,
                    proxy=TOR_PROXY,
                )
                resp.raise_for_status()
                dest.write_bytes(resp.content)
                saved.append(dest)
            except Exception as exc:
                logger.warning("Failed to fetch subtitle track %s: %s", url, exc)
        return saved

    @staticmethod
    def _find_local_vtt_files(out_name: str, out_dir: Path) -> list[Path]:
        """N_m3u8DL-RE's --auto-select can itself pull embedded subtitle
        renditions from the m3u8 (soft-sub providers) and save them as
        .vtt next to the video."""
        return sorted(p for p in out_dir.glob(f"{out_name}*.vtt") if p.is_file())

    # -- status message helpers --------------------------------------------

    @staticmethod
    async def _edit_status(status_msg: Message | None, text: str) -> None:
        if status_msg is None:
            return
        try:
            await status_msg.edit_text(text, parse_mode=ParseMode.HTML)
        except Exception as exc:
            logger.debug("Could not edit status message: %s", exc)

    # -- per-quality download with server fallback --------------------------

    async def download_with_fallback(
        self,
        anilist_id: Any,
        ep_num: int,
        quality: str,
        type_: Literal["sub", "dub"] = "sub",
        anime_title: str = "",
        status_msg: Message | None = None,
        episodes_data: dict[str, Any] | None = None,
    ) -> DownloadResult:
        if episodes_data is None:
            episodes_data = await asyncio.to_thread(get_anime_episodes, anilist_id)

        providers = self.iter_providers(episodes_data, ep_num, type_)
        if not providers:
            raise DownloadFailedError(f"No {type_} providers listed for {anilist_id} ep {ep_num}")

        total = len(providers)
        out_name = f"{anilist_id}_ep{ep_num}_{type_}_{quality}"
        last_error: Exception | None = None

        for idx, provider in enumerate(providers, start=1):
            provider_name = provider["name"]
            episode_id = provider["episode_id"]
            provider_id = provider_name.lower()

            action = "connecting to server" if idx == 1 else "server unavailable, switching"
            await self._edit_status(
                status_msg, _server_card(action, ep_num, quality, provider_name, idx, total)
            )

            try:
                data = await asyncio.to_thread(
                    get_episode_sources, episode_id, provider_id, type_, anilist_id
                )
            except Exception as exc:
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                if status_code == 404:
                    logger.info("Provider %s 404'd, trying next server", provider_name)
                else:
                    logger.warning("Provider %s failed (%s), trying next server", provider_name, exc)
                last_error = exc
                continue

            # -----------------------------------------------------------------
            # FIX: Miruro returns "streams", not "sources". We also keep only
            # HLS entries because N_m3u8DL-RE cannot consume embed pages.
            # -----------------------------------------------------------------
            streams = data.get("streams") or []
            hls_streams = [s for s in streams if s.get("type") == "hls"]
            if not hls_streams:
                logger.info("Provider %s had no HLS streams, trying next server", provider_name)
                continue

            # Base headers mirror what miruro_scraper.fetch_data sends to
            # the pipe endpoint, then anything the sources response itself
            # supplies (e.g. per-CDN auth headers) is layered on top.
            headers = {
                **COMMON_HEADERS,
                **(data.get("headers") or {}),
            }

            chosen = self._select_source_for_quality(hls_streams, quality)
            if not chosen or not chosen.get("url"):
                logger.info("Provider %s had no usable %s stream, trying next server", provider_name, quality)
                continue

            # Remember the referer attached to this specific stream (needed for CDN auth)
            stream_referer = chosen.get("referer")

            if (chosen.get("quality") or "").strip().lower() in AUTO_QUALITY_TAGS:
                # This provider only exposes a single adaptive/master
                # playlist instead of discrete per-quality URLs — pull the
                # real resolution variants out of it and re-pick the
                # closest match to what was requested.
                try:
                    variants = await asyncio.to_thread(
                        self._fetch_master_variants, chosen["url"], headers
                    )
                except Exception as exc:
                    logger.warning(
                        "Failed to expand auto/master playlist for %s: %s", provider_name, exc
                    )
                    variants = []
                if variants:
                    resolved = self._select_source_for_quality(variants, quality)
                    if resolved and resolved.get("url"):
                        chosen = resolved

            actual_quality = chosen.get("quality") or quality
            if stream_referer:
                headers["Referer"] = stream_referer

            logger.info(
                "Ep %s: provider %s resolved -> quality=%s url=%s...",
                ep_num, provider_name, actual_quality, chosen["url"][:80],
            )

            async def _on_progress(pct: float, speed: str, done: str, tot: str) -> None:
                await self._edit_status(
                    status_msg,
                    _progress_card(
                        "📥", "downloading", ep_num, actual_quality, pct, speed, done, tot,
                        footer=f"🌐 {sc('server')}: <code>{provider_name}</code>",
                    ),
                )

            logger.info("Ep %s: starting download via %s (%s)", ep_num, provider_name, actual_quality)
            try:
                file_path = await self._run_n_m3u8dl(
                    chosen["url"], headers, out_name, self.work_dir, on_progress=_on_progress
                )
                logger.info("Ep %s: download finished -> %s, remuxing to mkv", ep_num, file_path.name)
                file_path = await asyncio.to_thread(self._remux_to_mkv, file_path)
            except Exception as exc:
                logger.warning("Download via %s failed (%s), trying next server", provider_name, exc)
                last_error = exc
                continue

            # -----------------------------------------------------------------
            # FIX: Collect subtitles from both "tracks" and "subtitles" keys.
            # -----------------------------------------------------------------
            all_subtitle_tracks = (data.get("tracks") or []) + (data.get("subtitles") or [])
            subtitle_paths = await self._fetch_track_subtitles(
                all_subtitle_tracks, out_name, self.work_dir
            )
            subtitle_paths += [
                p for p in self._find_local_vtt_files(out_name, self.work_dir)
                if p not in subtitle_paths
            ]
            logger.info(
                "Ep %s: ready for upload -> %s (%s, %s)",
                ep_num, file_path.name, actual_quality,
                _format_size(file_path.stat().st_size) if file_path.exists() else "?",
            )

            return DownloadResult(
                quality=actual_quality,
                file_path=file_path,
                provider_id=provider_id,
                provider_name=provider_name,
                type_=type_,
                subtitle_paths=subtitle_paths,
            )

        raise DownloadFailedError(
            f"All {total} providers exhausted for {anilist_id} ep {ep_num} at {quality}"
        ) from last_error

    # -- upload --------------------------------------------------------------

    async def upload_result(
        self,
        app: Client,
        chat_id: int | str,
        result: DownloadResult,
        ep_num: int,
        anime_title: str = "",
        season: int = 1,
        status_msg: Message | None = None,
        caption: str | None = None,
    ) -> list[Message]:
        caption = caption or _build_caption(
            anime_title, season, ep_num, result.quality, result.type_
        )

        last_reported = -100.0
        last_time = time.monotonic()
        last_bytes = 0

        async def progress(current: int, total: int) -> None:
            nonlocal last_reported, last_time, last_bytes
            pct = (current / total * 100) if total else 0.0
            if pct - last_reported < UPLOAD_UPDATE_STEP and pct < 99.9:
                return
            now = time.monotonic()
            elapsed = max(now - last_time, 1e-6)
            speed_bps = max(current - last_bytes, 0) / elapsed
            last_reported, last_time, last_bytes = pct, now, current
            await self._edit_status(
                status_msg,
                _progress_card(
                    "📤", "uploading", ep_num, result.quality, pct,
                    f"{_format_size(speed_bps)}/s",
                    _format_size(current), _format_size(total),
                ),
            )

        sent_messages: list[Message] = []
        duration, width, height = await asyncio.to_thread(self._probe_video, result.file_path)
        upload_path = await asyncio.to_thread(self._mp4_alias_for_mime, result.file_path)
        logger.info(
            "Uploading %s -> chat %s (ep %s, %s, %dx%d, %ds, mime-alias=%s)",
            result.file_path.name, chat_id, ep_num, result.quality, width, height, duration,
            upload_path.name,
        )
        try:
            sent = await app.send_video(
                chat_id=chat_id,
                video=str(upload_path),
                file_name=result.file_path.name,
                thumb=str(THUMB_PATH) if THUMB_PATH.exists() else None,
                caption=caption,
                parse_mode=ParseMode.HTML,
                supports_streaming=True,
                duration=duration,
                width=width,
                height=height,
                progress=progress,
            )
        finally:
            if upload_path != result.file_path:
                upload_path.unlink(missing_ok=True)
        sent_messages.append(sent)
        logger.info("Uploaded ep %s -> message_id=%s", ep_num, sent.id)

        for sub_path in result.subtitle_paths:
            try:
                sub_sent = await app.send_document(
                    chat_id=chat_id,
                    document=str(sub_path),
                    caption=f"{bi(sc('subtitle'))} • {sub_path.stem}",
                    parse_mode=ParseMode.HTML,
                )
                sent_messages.append(sub_sent)
            except Exception as exc:
                logger.warning("Failed to upload subtitle %s: %s", sub_path, exc)

        return sent_messages

    # -- full pipeline: all 3 qualities, download + upload each -------------

    async def process_episode(
        self,
        app: Client,
        chat_id: int | str,
        anilist_id: Any,
        ep_num: int,
        anime_title: str = "",
        season: int = 1,
        qualities: list[str] | None = None,
        type_: Literal["sub", "dub"] = "sub",
        status_msg: Message | None = None,
        cleanup: bool = True,
    ) -> list[Message]:
        qualities = qualities or DEFAULT_QUALITIES
        sent_messages: list[Message] = []
        logger.info(
            "Processing '%s' ep %s (anilist_id=%s, type=%s, qualities=%s)",
            anime_title, ep_num, anilist_id, type_, qualities,
        )

        # Fetch episodes once and reuse across all requested qualities,
        # instead of re-hitting the episodes endpoint per quality.
        episodes_data = await asyncio.to_thread(get_anime_episodes, anilist_id)

        for quality in qualities:
            try:
                result = await self.download_with_fallback(
                    anilist_id, ep_num, quality,
                    type_=type_, anime_title=anime_title, status_msg=status_msg,
                    episodes_data=episodes_data,
                )
            except DownloadFailedError as exc:
                logger.warning("Skipping %s for ep %s: %s", quality, ep_num, exc)
                await self._edit_status(
                    status_msg,
                    _card([f"⚠️ {sc('skipped')}", "", f"{quality} — {sc('all servers failed')}"]),
                )
                continue

            sent = await self.upload_result(
                app, chat_id, result, ep_num,
                anime_title=anime_title, season=season, status_msg=status_msg,
            )
            sent_messages.extend(sent)

            if cleanup:
                result.file_path.unlink(missing_ok=True)
                for sub_path in result.subtitle_paths:
                    sub_path.unlink(missing_ok=True)

        title_display = anime_title or sc("episode")
        ep_label = sc("episode")
        await self._edit_status(status_msg, _card([f"✅ {sc('done')}", "", f"{title_display} — {ep_label} {ep_num}"]))
        logger.info(
            "Finished '%s' ep %s: %d message(s) sent", anime_title, ep_num, len(sent_messages)
        )
        return sent_messages

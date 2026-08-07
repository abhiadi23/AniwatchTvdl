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

from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from .animex import AnimexClient

logger = logging.getLogger("animexdl")

DEFAULT_QUALITIES = ["360p", "720p", "1080p"]

# Some providers report a single "auto"/"default" source instead of
# discrete per-quality URLs. Two different things can be going on here:
#
#   1. It's genuinely an HLS *master* playlist listing multiple resolution
#      variants — we fetch and parse it to get the real per-quality URLs.
#   2. It's actually just ONE fixed-resolution stream that happens to be
#      *tagged* "auto" (confirmed in the wild: the CDN URL itself embeds
#      a resolution like ".../720p1142.../..." and the "playlist" has no
#      #EXT-X-STREAM-INF variants at all — there's nothing to expand).
#
# Case 2 used to silently re-download the identical file three times (once
# per requested quality) and label it whatever was requested rather than
# what it actually was. See `single_stream` handling below.
AUTO_QUALITY_TAGS = {"auto", "default", "multi", "adaptive", ""}

_RESOLUTION_RE = re.compile(r"RESOLUTION=(\d+)x(\d+)")
_BANDWIDTH_RE = re.compile(r"BANDWIDTH=(\d+)")


def _parse_master_playlist(text: str, base_url: str) -> list[dict[str, Any]]:
    """Parse an HLS master playlist's ``#EXT-X-STREAM-INF`` variants into
    ``{"quality": "1080p", "url": "..."}`` entries, resolving relative
    variant URIs against the master playlist's own URL. Returns an empty
    list if the fetched playlist has no #EXT-X-STREAM-INF lines at all —
    that means it wasn't actually a master playlist (see AUTO_QUALITY_TAGS
    comment above)."""
    variants: list[dict[str, Any]] = []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if not line.startswith("#EXT-X-STREAM-INF"):
            continue
        res_m = _RESOLUTION_RE.search(line)
        if res_m:
            quality = f"{int(res_m.group(2))}p"
        else:
            bw_m = _BANDWIDTH_RE.search(line)
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
# console output (percent / speed / downloaded-total-size). I haven't been able
# to confirm the exact live format — if progress isn't showing up, paste a
# real run's stdout and I'll tighten these patterns to match it exactly.

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
    # True when this provider only ever exposed ONE actual stream (its
    # "auto"-tagged source had no expandable master-playlist variants).
    # process_episode uses this to stop requesting further qualities for
    # this episode instead of re-downloading the identical file again.
    single_stream: bool = False


class DownloadFailedError(Exception):
    """Raised when every provider/server has been exhausted for a quality."""


# ----------------------------------------------------------------------------
# Downloader / uploader
# ----------------------------------------------------------------------------


class AnimexDownloader:
    def __init__(
        self,
        client: AnimexClient | None = None,
        work_dir: str | Path = "downloads",
        binary_dir: str | Path = "binary",
    ) -> None:
        self.client = client or AnimexClient()
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.binary_dir = Path(binary_dir)
        self.binary_path = self._get_binary_path()

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
        resp = self.client.session.get(
            master_url,
            headers=headers,
            timeout=self.client.timeout,
            impersonate=self.client.impersonate,
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

        logger.info("Running N_m3u8DL-RE for %s -> %s", out_name, out_dir)
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

    @staticmethod
    def _lang_from_subtitle_name(path: Path) -> str:
        """Best-effort language tag from the subtitle filename.
        `_fetch_track_subtitles` saves as `{out_name}.{lang}.vtt`, so the
        second-to-last dot-segment is the lang code there. N_m3u8DL-RE's own
        naming for embedded-track vtts it pulls out itself isn't confirmed —
        if muxed tracks show up untitled/mislabeled, paste a real filename
        from `--auto-select` output and I'll tighten this."""
        stem = path.stem  # strips .vtt
        parts = stem.rsplit(".", 1)
        return parts[1] if len(parts) == 2 and parts[1] else "und"

    def _mux_subtitles(self, video_path: Path, subtitle_paths: list[Path]) -> Path | None:
        """Soft-mux subtitle tracks into an .mkv alongside the video —
        stream copy, no re-encode — so players show them automatically
        instead of relying on a separately-uploaded .vtt document. Matroska
        accepts WebVTT natively, so `-c:s copy` doesn't need to touch
        codec/format. Only ever called when subtitle_paths is non-empty,
        i.e. confirmed soft-sub providers; hard-sub providers (tracks=null,
        no local vtt) never reach this path. Returns None on ffmpeg failure
        so the caller can fall back to the old separate-upload behavior."""
        dst = video_path.with_suffix(".mkv")
        cmd = ["ffmpeg", "-y", "-i", str(video_path)]
        for sub in subtitle_paths:
            cmd += ["-i", str(sub)]
        cmd += ["-map", "0:v", "-map", "0:a?"]
        for i in range(1, len(subtitle_paths) + 1):
            cmd += ["-map", f"{i}:s"]
        cmd += ["-c:v", "copy", "-c:a", "copy", "-c:s", "copy"]
        for i, sub in enumerate(subtitle_paths):
            lang = self._lang_from_subtitle_name(sub)
            cmd += [f"-metadata:s:s:{i}", f"language={lang}", f"-metadata:s:s:{i}", f"title={lang}"]
        cmd += [str(dst)]

        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0 or not dst.exists():
            logger.warning(
                "ffmpeg subtitle mux failed, falling back to separate subtitle upload for %s: %s",
                video_path.name, proc.stderr[-1000:],
            )
            return None
        return dst

    # -- subtitles ------------------------------------------------------

    async def _fetch_track_subtitles(
        self, tracks: list[dict[str, Any]], out_name: str, out_dir: Path
    ) -> list[Path]:
        """Some soft-sub providers return explicit subtitle track URLs in
        the sources payload's `tracks` field (confirmed: hard-sub providers
        like 'uwu' return tracks=null, so this is a no-op for those)."""
        saved: list[Path] = []
        for i, track in enumerate(tracks or []):
            url = track.get("url")
            if not url:
                continue
            lang = track.get("lang") or track.get("language") or track.get("label") or f"sub{i}"
            dest = out_dir / f"{out_name}.{lang}.vtt"
            try:
                resp = await asyncio.to_thread(
                    self.client.session.get,
                    url,
                    timeout=self.client.timeout,
                    impersonate=self.client.impersonate,
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
        anime_id: str,
        ep_num: int,
        quality: str,
        type_: Literal["sub", "dub"] = "sub",
        anime_title: str = "",
        status_msg: Message | None = None,
    ) -> DownloadResult:
        providers = self.client.iter_providers(anime_id, ep_num, type_)
        if not providers:
            raise DownloadFailedError(f"No {type_} providers listed for {anime_id} ep {ep_num}")

        total = len(providers)
        out_name = f"{anime_id}_ep{ep_num}_{type_}_{quality}"
        last_error: Exception | None = None

        for idx, provider in enumerate(providers, start=1):
            provider_id = provider.get("id")
            provider_name = provider.get("tip") or str(provider_id)

            action = "connecting to server" if idx == 1 else "server unavailable, switching"
            await self._edit_status(
                status_msg, _server_card(action, ep_num, quality, provider_name, idx, total)
            )

            try:
                data = self.client.get_sources(anime_id, ep_num, type_, provider_id)
            except Exception as exc:
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                if status_code == 404:
                    logger.info("Provider %s 404'd, trying next server", provider_name)
                else:
                    logger.warning("Provider %s failed (%s), trying next server", provider_name, exc)
                last_error = exc
                continue

            sources = data.get("sources") or []
            if not sources:
                logger.info("Provider %s had no sources, trying next server", provider_name)
                continue

            # Base headers come from the same client/session that talked to
            # the animex.one API (Origin/Referer/User-Agent all match), then
            # anything the sources response itself supplies (e.g. per-CDN
            # auth headers) is layered on top. Computed before quality
            # selection since the auto/master-playlist expansion below
            # also needs them (to fetch the playlist itself).
            headers = {
                **self.client.download_headers(),
                **(data.get("headers") or {}),
            }

            chosen = self._select_source_for_quality(sources, quality)
            if not chosen or not chosen.get("url"):
                logger.info("Provider %s had no usable %s source, trying next server", provider_name, quality)
                continue

            # Whether we managed to expand an "auto"-tagged source into
            # real per-resolution variants via the master-playlist parse.
            resolved_via_master = False
            is_auto_tagged = (chosen.get("quality") or "").strip().lower() in AUTO_QUALITY_TAGS

            if is_auto_tagged:
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
                        resolved_via_master = True
                else:
                    # No exception, just nothing to expand — this "auto" tag
                    # was mislabeling a single fixed-resolution stream, not
                    # a real adaptive master playlist. Downloading it once
                    # is all that's possible; requesting it again under a
                    # different quality label would just re-fetch the same
                    # bytes.
                    logger.info(
                        "Provider %s's 'auto' source has no expandable variants — "
                        "it's a single fixed-quality stream, not a master playlist (ep %s)",
                        provider_name, ep_num,
                    )

            single_stream = is_auto_tagged and not resolved_via_master

            actual_quality = chosen.get("quality") or quality
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
            except Exception as exc:
                logger.warning("Download via %s failed (%s), trying next server", provider_name, exc)
                last_error = exc
                continue
            logger.info("Ep %s: download finished -> %s", ep_num, file_path.name)

            if single_stream:
                # Relabel from the literal "auto" tag to the stream's real
                # resolution so captions/status don't say "[auto]".
                _, _, probed_h = await asyncio.to_thread(self._probe_video, file_path)
                if probed_h:
                    actual_quality = f"{probed_h}p"
                    logger.info(
                        "Ep %s: single-stream provider — probed actual resolution as %s",
                        ep_num, actual_quality,
                    )

            subtitle_paths = await self._fetch_track_subtitles(
                data.get("tracks") or [], out_name, self.work_dir
            )
            subtitle_paths += [
                p for p in self._find_local_vtt_files(out_name, self.work_dir)
                if p not in subtitle_paths
            ]

            if subtitle_paths:
                # Soft-sub: mux tracks into the container instead of
                # uploading .vtt separately. On success the subs are now
                # embedded, so subtitle_paths is cleared and the source
                # .vtt files are deleted. On failure we fall back to the
                # old behavior (remux for faststart, upload vtt as docs).
                logger.info("Ep %s: muxing %d subtitle track(s) into container", ep_num, len(subtitle_paths))
                muxed = await asyncio.to_thread(self._mux_subtitles, file_path, subtitle_paths)
                if muxed is not None:
                    for sub_path in subtitle_paths:
                        sub_path.unlink(missing_ok=True)
                    file_path = muxed
                    subtitle_paths = []
                    logger.info("Ep %s: subtitle mux OK -> %s", ep_num, file_path.name)
                else:
                    logger.warning("Ep %s: subtitle mux failed, falling back to plain mkv remux", ep_num)
                    file_path = await asyncio.to_thread(self._remux_to_mkv, file_path)
            else:
                file_path = await asyncio.to_thread(self._remux_to_mkv, file_path)
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
                single_stream=single_stream,
            )

        raise DownloadFailedError(
            f"All {total} providers exhausted for {anime_id} ep {ep_num} at {quality}"
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

        # Only reached now if subtitle muxing failed and we fell back to
        # the old separate-upload path (see download_with_fallback).
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
        anime_id: str,
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
            "Processing '%s' ep %s (anime_id=%s, type=%s, qualities=%s)",
            anime_title, ep_num, anime_id, type_, qualities,
        )

        for quality in qualities:
            try:
                result = await self.download_with_fallback(
                    anime_id, ep_num, quality,
                    type_=type_, anime_title=anime_title, status_msg=status_msg,
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

            if result.single_stream:
                # This provider only ever had one real stream — the other
                # requested qualities would just be identical re-downloads
                # under a different label, so stop here instead of hitting
                # N_m3u8DL-RE again for the same bytes.
                skipped = [q for q in qualities if q != quality]
                if skipped:
                    logger.info(
                        "Ep %s: only one stream available (%s) — skipping remaining "
                        "requested qualities %s (same provider, no adaptive variants)",
                        ep_num, result.quality, skipped,
                    )
                    await self._edit_status(
                        status_msg,
                        _card([
                            f"ℹ️ {sc('single quality available')}",
                            "",
                            f"{sc('only')} {result.quality} {sc('is available for this episode')}",
                            f"{sc('skipped')}: {', '.join(skipped)}",
                        ]),
                    )
                break

        title_display = anime_title or sc("episode")
        ep_label = sc("episode")
        await self._edit_status(status_msg, _card([f"✅ {sc('done')}", "", f"{title_display} — {ep_label} {ep_num}"]))
        logger.info(
            "Finished '%s' ep %s: %d message(s) sent", anime_title, ep_num, len(sent_messages)
        )
        return sent_messages

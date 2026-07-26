from __future__ import annotations

import asyncio
import logging
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal, Optional

from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from .animex import AnimexClient

logger = logging.getLogger("animexdl")

DEFAULT_QUALITIES = ["360p", "720p", "1080p"]

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
    def _remux_faststart(src: Path) -> Path:
        dst = src.with_name(src.stem + "_fs" + src.suffix)
        cmd = ["ffmpeg", "-y", "-i", str(src), "-c", "copy", "-movflags", "+faststart", str(dst)]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0 or not dst.exists():
            logger.warning(
                "ffmpeg +faststart remux failed, keeping original file for %s: %s",
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

            chosen = self._select_source_for_quality(sources, quality)
            if not chosen or not chosen.get("url"):
                logger.info("Provider %s had no usable %s source, trying next server", provider_name, quality)
                continue

            actual_quality = chosen.get("quality") or quality

            # Base headers come from the same client/session that talked to
            # the animex.one API (Origin/Referer/User-Agent all match), then
            # anything the sources response itself supplies (e.g. per-CDN
            # auth headers) is layered on top.
            headers = {
                **self.client.download_headers(),
                **(data.get("headers") or {}),
            }

            async def _on_progress(pct: float, speed: str, done: str, tot: str) -> None:
                await self._edit_status(
                    status_msg,
                    _progress_card(
                        "📥", "downloading", ep_num, actual_quality, pct, speed, done, tot,
                        footer=f"🌐 {sc('server')}: <code>{provider_name}</code>",
                    ),
                )

            try:
                file_path = await self._run_n_m3u8dl(
                    chosen["url"], headers, out_name, self.work_dir, on_progress=_on_progress
                )
            except Exception as exc:
                logger.warning("Download via %s failed (%s), trying next server", provider_name, exc)
                last_error = exc
                continue

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
                muxed = await asyncio.to_thread(self._mux_subtitles, file_path, subtitle_paths)
                if muxed is not None:
                    for sub_path in subtitle_paths:
                        sub_path.unlink(missing_ok=True)
                    file_path = muxed
                    subtitle_paths = []
                else:
                    file_path = await asyncio.to_thread(self._remux_faststart, file_path)
            else:
                file_path = await asyncio.to_thread(self._remux_faststart, file_path)

            return DownloadResult(
                quality=actual_quality,
                file_path=file_path,
                provider_id=provider_id,
                provider_name=provider_name,
                type_=type_,
                subtitle_paths=subtitle_paths,
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
        status_msg: Message | None = None,
        caption: str | None = None,
    ) -> list[Message]:
        caption = caption or (
            f"{bi(sc(anime_title or 'anime'))}\n"
            f"{bi(sc(result.type_))} • {bi(result.quality)}"
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
        sent = await app.send_video(
            chat_id=chat_id,
            video=str(result.file_path),
            caption=caption,
            parse_mode=ParseMode.HTML,
            supports_streaming=True,
            progress=progress,
        )
        sent_messages.append(sent)

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
        qualities: list[str] | None = None,
        type_: Literal["sub", "dub"] = "sub",
        status_msg: Message | None = None,
        cleanup: bool = True,
    ) -> list[Message]:
        qualities = qualities or DEFAULT_QUALITIES
        sent_messages: list[Message] = []

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
                anime_title=anime_title, status_msg=status_msg,
            )
            sent_messages.extend(sent)

            if cleanup:
                result.file_path.unlink(missing_ok=True)
                for sub_path in result.subtitle_paths:
                    sub_path.unlink(missing_ok=True)

        title_display = anime_title or sc("episode")
        ep_label = sc("episode")
        await self._edit_status(status_msg, _card([f"✅ {sc('done')}", "", f"{title_display} — {ep_label} {ep_num}"]))
        return sent_messages

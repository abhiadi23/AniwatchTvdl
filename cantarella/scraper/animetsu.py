#@cantarellabots
from cantarella.core.proxy import get_random_proxy, get_proxy_dict
from curl_cffi import requests as c_requests
import json
import re
import subprocess
import shutil
import os
from pathlib import Path

class AnimetsuScraper:
    BASE_URL = "https://animetsu.live"
    API_URL = f"{BASE_URL}/v2/api"
    PROXY_URL = "https://swiftstream.top/proxy" # Fallback if need_proxy is true

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Origin": BASE_URL,
        "Referer": f"{BASE_URL}/",
    }

    def __init__(self, download_path="anime_downloads", progress_queue=None):
        self.download_path = Path(download_path)
        self.download_path.mkdir(exist_ok=True)
        self.progress_queue = progress_queue
        self.binary_path = self._get_binary_path()
        self.proxy = get_random_proxy()
        self.session = c_requests.Session()
        proxy_dict = get_proxy_dict(self.proxy)
        if proxy_dict:
            self.session.proxies.update(proxy_dict)

    def _get_binary_path(self):
        candidates = [
            Path("binary") / "N_m3u8DL-RE",
            Path("binary") / "N_m3u8DL-RE.exe",
            Path("/usr/local/bin/N_m3u8DL-RE"),
        ]
        for p in candidates:
            if p.exists(): return p
        which_path = shutil.which("N_m3u8DL-RE")
        if which_path: return Path(which_path)
        return None

    def search_anime(self, query):
        url = f"{self.API_URL}/anime/search?query={query.replace(' ', '+')}"
        try:
            resp = self.session.get(url, headers=self.HEADERS, impersonate="chrome120")
            if resp.status_code == 200:
                data = resp.json()
                results = []
                for item in data.get('results', []):
                    title_dict = item.get('title', {})
                    title = title_dict.get('english') or title_dict.get('romaji') or title_dict.get('native')
                    results.append({
                        'title': title,
                        'id': item.get('id'),
                        'type': item.get('type', 'Anime').upper(),
                        'url': f"{self.BASE_URL}/anime/{item.get('id')}"
                    })
                return results
        except Exception as e:
            print(f"Animetsu search error: {e}")
        return []

    def get_anime_info(self, anime_id):
        url = f"{self.API_URL}/anime/info/{anime_id}"
        try:
            resp = self.session.get(url, headers=self.HEADERS, impersonate="chrome120")
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            print(f"Animetsu info error: {e}")
        return None

    def list_episodes(self, anime_id):
        # anime_id might be a URL or just the ID
        if "/" in anime_id:
            anime_id = anime_id.split('/')[-1]

        url = f"{self.API_URL}/anime/eps/{anime_id}"
        try:
            resp = self.session.get(url, headers=self.HEADERS, impersonate="chrome120")
            if resp.status_code == 200:
                data = resp.json()
                results = []
                for ep in data:
                    results.append({
                        'title': ep.get('name') or f"Episode {ep.get('ep_num')}",
                        'url': f"{self.BASE_URL}/watch/{anime_id}/{ep.get('ep_num')}",
                        'ep_number': str(ep.get('ep_num')),
                        'ep_id': ep.get('id')
                    })
                return results
        except Exception as e:
            print(f"Animetsu list episodes error: {e}")
        return []

    def get_episode_servers(self, anime_id, ep_num):
        url = f"{self.API_URL}/anime/servers/{anime_id}/{ep_num}"
        try:
            resp = self.session.get(url, headers=self.HEADERS, impersonate="chrome120")
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            print(f"Animetsu servers error: {e}")
        return []

    def get_schedule(self, date_str=None):
        url = f"{self.API_URL}/anime/schedule"
        if date_str:
            url += f"?date={date_str}"
        try:
            resp = self.session.get(url, headers=self.HEADERS, impersonate="chrome120")
            if resp.status_code == 200:
                data = resp.json()
                results = []
                seen_ids = set()
                from datetime import datetime
                for item in data:
                    anime_id = item.get('id')
                    if not anime_id or anime_id in seen_ids:
                        continue
                    seen_ids.add(anime_id)

                    title_dict = item.get('title', {})
                    title = title_dict.get('english') or title_dict.get('romaji') or title_dict.get('native')

                    airing_at = item.get('airing_at')
                    time_str = "Unknown"
                    if airing_at:
                        # Animetsu provides timestamp in milliseconds
                        dt = datetime.fromtimestamp(airing_at / 1000)
                        time_str = dt.strftime("%H:%M")

                    results.append({
                        'id': item.get('id'),
                        'title': title,
                        'time': time_str,
                        'ep': item.get('airing_ep')
                    })
                return results
        except Exception as e:
            print(f"Animetsu schedule error: {e}")
        return []

    def get_episode_sources(self, anime_id, ep_num, server='default', source_type='sub'):
        url = f"{self.API_URL}/anime/oppai/{anime_id}/{ep_num}?server={server}&source_type={source_type}"
        try:
            resp = self.session.get(url, headers=self.HEADERS, impersonate="chrome120")
            if resp.status_code == 200:
                data = resp.json()
                for src in data.get('sources', []):
                    if src.get('need_proxy'):
                        src['url'] = f"{self.PROXY_URL}{src['url']}"
                    elif src['url'].startswith('/'):
                        src['url'] = f"{self.BASE_URL}{src['url']}"
                return data
        except Exception as e:
            print(f"Animetsu sources error: {e}")
        return None

    def fetch_recently_updated(self, page=1, per_page=12):
        url = f"{self.API_URL}/anime/recent?page={page}&per_page={per_page}"
        try:
            resp = self.session.get(url, headers=self.HEADERS, impersonate="chrome120")
            if resp.status_code == 200:
                data = resp.json()
                results = []
                for item in data.get('results', []):
                    title_dict = item.get('title', {})
                    title = title_dict.get('english') or title_dict.get('romaji') or title_dict.get('native')
                    results.append({
                        'title': title,
                        'id': item.get('id'),
                        'url': f"{self.BASE_URL}/anime/{item.get('id')}",
                        'ep_num': item.get('ep_num'),
                        'aired_at': item.get('aired_at')
                    })
                return results
        except Exception as e:
            print(f"Animetsu recent error: {e}")
        return []

    def get_home_sections(self):
        url = f"{self.API_URL}/anime/home"
        try:
            resp = self.session.get(url, headers=self.HEADERS, impersonate="chrome120")
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            print(f"Animetsu home sections error: {e}")
        return {}

    def download_episode(self, url, quality="auto", name_override=None, season_override=None, ep_num_override=None):
        # URL format: https://animetsu.live/watch/anime_id/ep_num
        parts = url.split('/')
        if "watch" in parts:
            idx = parts.index("watch")
            anime_id = parts[idx+1]
            ep_num = parts[idx+2]
        else:
            anime_id = parts[-2]
            ep_num = parts[-1]

        info = self.get_anime_info(anime_id)
        if not info:
            if self.progress_queue: self.progress_queue.put({'error': 'Could not fetch anime info from Animetsu.'})
            return False

        title_dict = info.get('title', {})
        anime_name = name_override or title_dict.get('english') or title_dict.get('romaji') or title_dict.get('native')

        # 1. Fetch Sub & Dub data
        sub_data = self.get_episode_sources(anime_id, ep_num, source_type='sub')
        dub_data = self.get_episode_sources(anime_id, ep_num, source_type='dub')

        is_dub_only = False
        if not sub_data or not sub_data.get('sources'):
            # If no sub, try using dub as primary
            if dub_data and dub_data.get('sources'):
                sub_data = dub_data
                dub_data = None
                is_dub_only = True

        if not sub_data or not sub_data.get('sources'):
            if self.progress_queue: self.progress_queue.put({'error': 'Could not find video sources on Animetsu.'})
            return False

        # 2. Select Source and Quality
        sources = sub_data['sources']
        selected_source = sources[0]
        if quality != "auto":
            for src in sources:
                if quality in src.get('quality', ''):
                    selected_source = src
                    break

        m3u8_url = selected_source['url']
        qual_str = selected_source.get('quality', 'auto').replace('p', '')

        # 3. Preparation
        def sanitize(name): return re.sub(r'[\\/*?:"<>|]', "", name)
        try: from config import FORMAT
        except ImportError: FORMAT = "[S{season}-E{episode}] {title} [{quality}] [{audio}]"

        if is_dub_only:
            audio_label = "EN"
        elif dub_data and dub_data.get('sources'):
            audio_label = "Dual Audio"
        else:
            audio_label = "JP"

        base_filename = sanitize(FORMAT.format(
            season=season_override or "1",
            episode=ep_num_override or ep_num,
            title=anime_name,
            quality=f"{qual_str}p",
            audio=audio_label
        ))

        task_dir = self.download_path / f"animetsu_{anime_id}_{ep_num}_{qual_str}"
        task_dir.mkdir(exist_ok=True)

        video_temp = task_dir / f"{base_filename}_sub.mkv"
        audio_temp = task_dir / f"{base_filename}_dub.mkv"
        final_file = self.download_path / f"{base_filename}.mkv"

        if self.progress_queue:
            self.progress_queue.put({'status': f"📥 **Downloading (Animetsu): {anime_name} [{qual_str}p]**\nPlease wait..."})

        # 4. Download tracks
        def run_n_m3u8dl(dl_url, save_name, dl_type='sub'):
            cmd = [
                str(self.binary_path), dl_url,
                "--save-dir", str(task_dir),
                "--save-name", save_name,
                "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "-H", f"Referer: {self.BASE_URL}/",
                "--check-segments-count", "False",
                "-mt", "--thread-count", "50",
                "--download-retry-count", "5",
                "--auto-select"
            ]
            if self.proxy: cmd.extend(["--custom-proxy", self.proxy])

            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
            while True:
                line = process.stdout.readline()
                if not line: break
                line = line.decode('utf-8', errors='replace').strip()

                if "%" in line and self.progress_queue:
                    percent_match = re.search(r"(\d+(\.\d+)?)%", line)
                    if percent_match:
                        # Improved parsing for speed and sizes
                        parts = re.split(r"\d+(\.\d+)?%", line)
                        speed_match = None
                        if len(parts) > 1:
                            after_percent = parts[-1]
                            speed_match = re.search(r"(\d+(\.\d+)?\s*[MKG]?i?(B/s|bps|b/s|bit/s))", after_percent, re.I)
                        if not speed_match:
                            speed_match = re.search(r"(\d+(\.\d+)?\s*[MKG]?i?(B/s|bps|b/s|bit/s))", line, re.I)

                        size_match = re.search(r"(\d+(\.\d+)?\s*\S+)\s*/\s*(\d+(\.\d+)?\s*\S+)", line, re.I)

                        self.progress_queue.put({
                            'percent': f"{percent_match.group(1)}%",
                            'speed': speed_match.group(1) if speed_match else "0 MB/s",
                            'downloaded': size_match.group(1) if size_match else "0 MB",
                            'total': size_match.group(3) if size_match else "0 MB",
                            'type': dl_type,
                            'title': f"Episode {ep_num}"
                        })
            process.wait()
            return process.returncode == 0

        # Download Sub (Main Video)
        if not run_n_m3u8dl(m3u8_url, f"{base_filename}_sub", 'sub'):
            if self.progress_queue: self.progress_queue.put({'error': 'Video download failed'})
            return False

        # Download Dub (Audio Only) if available
        dub_downloaded = False
        if dub_data and dub_data.get('sources'):
            dub_m3u8 = dub_data['sources'][0]['url']
            if run_n_m3u8dl(dub_m3u8, f"{base_filename}_dub", 'dub'):
                dub_downloaded = True

        # 5. Extract Subs
        sub_files = []
        if sub_data.get('subs'):
            for i, s in enumerate(sub_data['subs']):
                lang = s.get('lang', f'sub_{i}').lower().replace(' ', '_')
                sub_path = task_dir / f"{base_filename}_{lang}.vtt"
                try:
                    r = self.session.get(s['url'], timeout=10)
                    if r.status_code == 200:
                        with open(sub_path, 'wb') as f: f.write(r.content)
                        sub_files.append((sub_path, lang))
                except: pass

        # 6. Merge with ffmpeg
        # Identify downloaded files
        for f in task_dir.iterdir():
            if f.name.startswith(f"{base_filename}_sub."): f.rename(video_temp)
            elif f.name.startswith(f"{base_filename}_dub."): f.rename(audio_temp)

        if not video_temp.exists():
            if self.progress_queue: self.progress_queue.put({'error': 'Video file missing after download.'})
            return False

        if not dub_downloaded and not sub_files:
            video_temp.replace(final_file)
        else:
            if self.progress_queue: self.progress_queue.put({'status': f"🎬 **Merging Tracks for: Episode {ep_num}**"})
            cmd = ['ffmpeg', '-y', '-i', str(video_temp)]
            if dub_downloaded: cmd.extend(['-i', str(audio_temp)])
            for s_path, _ in sub_files: cmd.extend(['-i', str(s_path)])

            cmd.extend(['-map', '0:v', '-map', '0:a'])
            if dub_downloaded: cmd.extend(['-map', '1:a:0'])

            sub_offset = 2 if dub_downloaded else 1
            for i in range(len(sub_files)): cmd.extend(['-map', f'{i + sub_offset}:s'])

            cmd.extend(['-c', 'copy', '-c:s', 'srt'])
            cmd.extend(['-metadata:s:a:0', 'language=jpn', '-metadata:s:a:0', 'title=Japanese'])
            if dub_downloaded: cmd.extend(['-metadata:s:a:1', 'language=eng', '-metadata:s:a:1', 'title=English'])
            if sub_files: cmd.extend(['-disposition:s:0', 'default'])

            cmd.append(str(final_file))
            try: subprocess.run(cmd, check=True, capture_output=True)
            except: video_temp.replace(final_file)

        shutil.rmtree(task_dir, ignore_errors=True)
        if self.progress_queue:
            self.progress_queue.put({'finished': True, 'filename': str(final_file), 'title': base_filename})
        return True

if __name__ == '__main__':
    scraper = AnimetsuScraper()
    print(scraper.search_anime('naruto'))

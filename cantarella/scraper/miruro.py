import base64
import gzip
import json
import os
import zlib

from curl_cffi import requests

# ==========================================
# CONFIGURATION
# ==========================================
API_ENDPOINT = "https://www.miruro.to/api/secure/pipe"
REFERER_URL = "https://www.miruro.to/"

TOR_PROXY = os.environ.get("TOR_PROXY", "socks5://127.0.0.1:9050")
"""SOCKS5 proxy used for every pipe request and for N_m3u8DL-RE downloads."""

COMMON_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Referer": REFERER_URL,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
}

# XOR obfuscation key from VITE_PIPE_OBF_KEY (hex -> bytes)
OBF_KEY = bytes.fromhex("71951034f8fbcf53d89db52ceb3dc22c")


# ==========================================
# HELPERS
# ==========================================
def base64url_decode(s: str) -> bytes:
    """Decode base64url string (no padding, -_ instead of +/)."""
    b64 = s.replace('-', '+').replace('_', '/')
    pad = 4 - (len(b64) % 4)
    if pad != 4:
        b64 += '=' * pad
    return base64.b64decode(b64)


def base64url_encode(data: bytes) -> str:
    """Encode bytes to base64url string (no padding)."""
    return base64.b64encode(data).decode('ascii').replace('+', '-').replace('/', '_').rstrip('=')


def xor_decrypt(data: bytes, key: bytes) -> bytes:
    """XOR decrypt with repeating key."""
    return bytes(data[i] ^ key[i % len(key)] for i in range(len(data)))


def decompress_auto(data: bytes) -> bytes:
    """Auto-detect gzip/zlib/deflate and decompress."""
    if len(data) < 2:
        return data
    # gzip
    if data[0] == 0x1f and data[1] == 0x8b:
        return gzip.decompress(data)
    # zlib (auto window bits)
    return zlib.decompress(data, zlib.MAX_WBITS | 32)


# ==========================================
# PAYLOAD BUILDERS
# ==========================================
def build_search_payload(anime_name: str) -> str:
    """Build the search dictionary and encode it as base64url JSON."""
    payload = {
        "path": "search",
        "method": "GET",
        "query": {
            "q": anime_name,
            "limit": 15,
            "offset": 0,
            "type": "ANIME"
        },
        "body": None,
        "version": "0.2.0"
    }
    json_str = json.dumps(payload, separators=(',', ':'))
    return base64url_encode(json_str.encode('utf-8'))


def build_browse_payload(status: str = "RELEASING", sort: str = "TRENDING_DESC",
                          page: int = 1, per_page: int = 12) -> str:
    """Build the search/browse dictionary and encode it as base64url JSON."""
    payload = {
        "path": "search/browse",
        "method": "GET",
        "query": {
            "type": "ANIME",
            "status": status,
            "sort": sort,
            "page": page,
            "perPage": per_page
        },
        "body": None,
        "version": "0.2.0"
    }
    json_str = json.dumps(payload, separators=(',', ':'))
    return base64url_encode(json_str.encode('utf-8'))


def build_schedule_payload(sort=None, newest: bool = True) -> str:
    """Build the schedule dictionary and encode it as base64url JSON."""
    if sort is None:
        sort = ["TIME_DESC"]
    payload = {
        "path": "schedule",
        "method": "GET",
        "query": {
            "sort": sort,
            "newest": newest
        },
        "body": None,
        "version": "0.2.0"
    }
    json_str = json.dumps(payload, separators=(',', ':'))
    return base64url_encode(json_str.encode('utf-8'))


def build_episodes_payload(anilist_id) -> str:
    """Build the episodes dictionary and encode it as base64url JSON."""
    payload = {
        "path": "episodes",
        "method": "GET",
        "query": {"anilistId": anilist_id},
        "body": None,
        "version": "0.2.0"
    }
    json_str = json.dumps(payload, separators=(',', ':'))
    return base64url_encode(json_str.encode('utf-8'))


def build_sources_payload(episode_id: str, provider: str, category: str, anilist_id) -> str:
    """Build the sources dictionary and encode it as base64url JSON."""
    payload = {
        "path": "sources",
        "method": "GET",
        "query": {
            "episodeId": episode_id,
            "provider": provider,
            "category": category,
            "anilistId": anilist_id
        },
        "body": None,
        "version": "0.2.0"
    }
    json_str = json.dumps(payload, separators=(',', ':'))
    return base64url_encode(json_str.encode('utf-8'))


# ==========================================
# REQUEST HANDLING
# ==========================================
def fetch_data(e_param: str) -> tuple[str, dict]:
    """Send the base64-encoded query to the secure pipe endpoint via Tor."""
    response = requests.get(
        API_ENDPOINT,
        params={"e": e_param},
        headers=COMMON_HEADERS,
        proxy=TOR_PROXY,
        impersonate="chrome",
        timeout=15
    )
    response.raise_for_status()
    return response.text, dict(response.headers)


def parse_response(raw_response: str, headers: dict) -> dict:
    """Parse standard JSON or handle obfuscated responses."""
    obf_type = headers.get('x-obfuscated')
    if obf_type == '2' and OBF_KEY:
        encrypted = base64url_decode(raw_response.strip())
        decrypted = xor_decrypt(encrypted, OBF_KEY)
        decompressed = decompress_auto(decrypted)
        return json.loads(decompressed.decode('utf-8'))
    return json.loads(raw_response.strip())


def search_anime(anime_name: str) -> list:
    """Run the search step and return the list of results."""
    e_param = build_search_payload(anime_name)
    raw_response, headers = fetch_data(e_param)
    result = parse_response(raw_response, headers)

    if isinstance(result, dict):
        for key in ("results", "data", "media"):
            if key in result and isinstance(result[key], list):
                return result[key]
        return []
    if isinstance(result, list):
        return result
    return []


def browse_anime(status: str = "RELEASING", sort: str = "TRENDING_DESC",
                  page: int = 1, per_page: int = 12) -> dict:
    """Run the search/browse step and return the decrypted payload."""
    e_param = build_browse_payload(status, sort, page, per_page)
    raw_response, headers = fetch_data(e_param)
    return parse_response(raw_response, headers)


def get_schedule(sort=None, newest: bool = True) -> dict:
    """Run the schedule step and return the decrypted payload."""
    e_param = build_schedule_payload(sort, newest)
    raw_response, headers = fetch_data(e_param)
    return parse_response(raw_response, headers)


def get_anime_episodes(anilist_id) -> dict:
    """Run the episodes step and return the decrypted payload."""
    e_param = build_episodes_payload(anilist_id)
    raw_response, headers = fetch_data(e_param)
    return parse_response(raw_response, headers)


def get_episode_sources(episode_id: str, provider: str, category: str, anilist_id) -> dict:
    """Run the sources step for one specific provider/episode."""
    e_param = build_sources_payload(episode_id, provider, category, anilist_id)
    raw_response, headers = fetch_data(e_param)
    return parse_response(raw_response, headers)

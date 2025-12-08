"""
COMPLETE Discord Music Bot with Premium UI and Enhanced Features
COMPLETE REWRITE with Maki-style UI and Advanced Management
"""

import asyncio
import csv
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Any, Set
import math
import random
import itertools

import aiohttp
import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands, tasks
from discord.ui import Button, View, Select, Modal, TextInput
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urljoin, quote, unquote
import base64
import hashlib
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ========== EXTENDED AUDIO FORMATS ==========
AUDIO_EXTENSIONS = {
    '.mp3': 'audio/mpeg',
    '.mp4': 'video/mp4',
    '.m4a': 'audio/mp4',
    '.flac': 'audio/flac',
    '.wav': 'audio/wav',
    '.ogg': 'audio/ogg',
    '.aac': 'audio/aac',
    '.webm': 'audio/webm',
    '.wma': 'audio/x-ms-wma',
    '.opus': 'audio/opus',
    '.mka': 'audio/x-matroska',
    '.mkv': 'video/x-matroska',
    '.mov': 'video/quicktime',
    '.avi': 'video/x-msvideo',
    '.mpg': 'video/mpeg',
    '.mpeg': 'video/mpeg',
    '.wmv': 'video/x-ms-wmv',
    '.3gp': 'video/3gpp',
    '.aiff': 'audio/x-aiff',
    '.alac': 'audio/alac',
    '.amr': 'audio/amr',
    '.au': 'audio/basic',
    '.mid': 'audio/midi',
    '.midi': 'audio/midi',
    '.ra': 'audio/vnd.rn-realaudio',
    '.rm': 'audio/vnd.rn-realaudio',
    '.swf': 'application/x-shockwave-flash',
}

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('data/bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create data directory
Path("data").mkdir(exist_ok=True)

# ========== UNIVERSAL CLOUD STORAGE RESOLVER ==========
class CloudStorageResolver:
    """Universal resolver for cloud storage links (kept from original)"""
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.cache: Dict[str, Dict] = {}
        self.cache_file = "data/link_cache.json"
        self.load_cache()
        
        self.last_request = 0
        self.request_delay = 1.0
        
        self.base_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }
    
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
        self.session = aiohttp.ClientSession(
            headers=self.base_headers,
            timeout=timeout
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def load_cache(self):
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.cache = json.load(f)
                logger.info(f"Loaded {len(self.cache)} cached links")
            else:
                self.cache = {}
        except Exception as e:
            logger.error(f"Failed to load cache: {e}")
            self.cache = {}
    
    def save_cache(self):
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
    
    async def rate_limit(self):
        current_time = time.time()
        elapsed = current_time - self.last_request
        
        if elapsed < self.request_delay:
            await asyncio.sleep(self.request_delay - elapsed)
        
        self.last_request = time.time()
    
    def identify_service(self, url: str) -> str:
        url_lower = url.lower()
        
        if 'dropbox.com' in url_lower:
            return 'dropbox'
        elif 'drive.google.com' in url_lower:
            return 'google_drive'
        elif 'mediafire.com' in url_lower:
            return 'mediafire'
        elif 'mega.nz' in url_lower or 'mega.co.nz' in url_lower:
            return 'mega'
        elif 'terabox.com' in url_lower or '4funbox' in url_lower:
            return 'terabox'
        elif 'onedrive.live.com' in url_lower:
            return 'onedrive'
        elif 'pixeldrain.com' in url_lower:
            return 'pixeldrain'
        elif 'anonfiles.com' in url_lower:
            return 'anonfiles'
        elif 'file.io' in url_lower:
            return 'fileio'
        elif 'transfer.sh' in url_lower:
            return 'transfersh'
        elif 'github.com' in url_lower or 'raw.githubusercontent.com' in url_lower:
            return 'github'
        elif 'sourceforge.net' in url_lower:
            return 'sourceforge'
        else:
            return 'direct'
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def resolve_link(self, share_link: str) -> Optional[str]:
        share_link = share_link.strip()
        
        if share_link in self.cache:
            cached = self.cache[share_link]
            if 'expires' in cached:
                expires = datetime.fromisoformat(cached['expires'])
                if datetime.now() < expires:
                    logger.info(f"Using cached link for {share_link}")
                    return cached['direct_link']
            del self.cache[share_link]
        
        await self.rate_limit()
        logger.info(f"Resolving link: {share_link}")
        service = self.identify_service(share_link)
        
        try:
            if service == 'dropbox':
                direct_link = await self._resolve_dropbox(share_link)
            elif service == 'google_drive':
                direct_link = await self._resolve_google_drive(share_link)
            elif service == 'mediafire':
                direct_link = await self._resolve_mediafire(share_link)
            elif service == 'mega':
                direct_link = await self._resolve_mega(share_link)
            elif service == 'onedrive':
                direct_link = await self._resolve_onedrive(share_link)
            elif service == 'terabox':
                direct_link = await self._resolve_terabox(share_link)
            elif service in ['pixeldrain', 'anonfiles', 'fileio', 'transfersh', 'github', 'sourceforge']:
                direct_link = await self._resolve_simple_direct(share_link)
            else:
                direct_link = share_link
            
            if direct_link:
                if await self._test_direct_link(direct_link):
                    self.cache[share_link] = {
                        'direct_link': direct_link,
                        'service': service,
                        'resolved_at': datetime.now().isoformat(),
                        'expires': (datetime.now() + timedelta(days=7)).isoformat()
                    }
                    self.save_cache()
                    logger.info(f"Successfully resolved {service} link")
                    return direct_link
                else:
                    logger.error(f"Direct link test failed: {direct_link}")
        except Exception as e:
            logger.error(f"Service resolver failed: {e}")
        
        try:
            html_link = await self._extract_from_html(share_link)
            if html_link and await self._test_direct_link(html_link):
                self.cache[share_link] = {
                    'direct_link': html_link,
                    'service': service + '_html',
                    'resolved_at': datetime.now().isoformat(),
                    'expires': (datetime.now() + timedelta(days=3)).isoformat()
                }
                self.save_cache()
                return html_link
        except Exception as e:
            logger.debug(f"HTML extraction failed: {e}")
        
        logger.error(f"All resolution methods failed for: {share_link}")
        return None
    
    async def _test_direct_link(self, url: str) -> bool:
        try:
            async with self.session.head(url, allow_redirects=True, timeout=10) as response:
                return response.status in [200, 206]
        except:
            return False
    
    async def _resolve_dropbox(self, share_link: str) -> Optional[str]:
        if '?dl=0' in share_link:
            direct_link = share_link.replace('?dl=0', '?dl=1')
        elif '?dl=' in share_link:
            direct_link = re.sub(r'\?dl=\d', '?dl=1', share_link)
        else:
            if '?' in share_link:
                direct_link = share_link + '&dl=1'
            else:
                direct_link = share_link + '?dl=1'
        
        raw_link = direct_link.replace('?dl=1', '?raw=1')
        
        if await self._test_direct_link(direct_link):
            return direct_link
        elif await self._test_direct_link(raw_link):
            return raw_link
        
        try:
            async with self.session.get(share_link) as response:
                if response.status == 200:
                    html = await response.text()
                    patterns = [
                        r'"downloadUrl":"([^"]+)"',
                        r'href="(https://uc[^"]+)"',
                        r'<a[^>]+href="([^"]+)"[^>]*>.*?Download.*?</a>',
                        r'content="(https://[^"]+)"[^>]*property="og:video"',
                        r'content="(https://[^"]+)"[^>]*property="og:audio"',
                    ]
                    
                    for pattern in patterns:
                        matches = re.findall(pattern, html, re.IGNORECASE)
                        for url in matches:
                            if 'dropboxusercontent.com' in url or 'dl.dropboxusercontent.com' in url:
                                return url
        except Exception as e:
            logger.error(f"Dropbox HTML parsing failed: {e}")
        
        return None
    
    async def _resolve_google_drive(self, share_link: str) -> Optional[str]:
        file_id = None
        patterns = [
            r'/d/([a-zA-Z0-9_-]+)',
            r'id=([a-zA-Z0-9_-]+)',
            r'/file/d/([a-zA-Z0-9_-]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, share_link)
            if match:
                file_id = match.group(1)
                break
        
        if not file_id:
            return None
        
        direct_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        
        if await self._test_direct_link(direct_url):
            return direct_url
        
        try:
            async with self.session.get(direct_url) as response:
                if response.status == 200:
                    html = await response.text()
                    token_pattern = r'confirm=([a-zA-Z0-9_-]+)'
                    match = re.search(token_pattern, html)
                    
                    if match:
                        confirm_token = match.group(1)
                        confirmed_url = f"{direct_url}&confirm={confirm_token}"
                        if await self._test_direct_link(confirmed_url):
                            return confirmed_url
        except Exception as e:
            logger.error(f"Google Drive token extraction failed: {e}")
        
        return None
    
    async def _resolve_mediafire(self, share_link: str) -> Optional[str]:
        if 'download' in share_link and 'mediafire.com' in share_link:
            return share_link
        
        try:
            async with self.session.get(share_link) as response:
                if response.status == 200:
                    try:
                        html = await response.text()
                    except UnicodeDecodeError:
                        content_type = response.headers.get('content-type', '').lower()
                        if content_type.startswith('audio/') or content_type.startswith('application/'):
                            return share_link
                        else:
                            html_bytes = await response.read()
                            try:
                                html = html_bytes.decode('utf-8', errors='ignore')
                            except:
                                return share_link
                    
                    patterns = [
                        r'href="(https?://download\d*\.mediafire\.com/[^"]+)"',
                        r'"download_link":\s*"([^"]+)"',
                        r'kNO\s*=\s*["\']([^"\']+)["\']',
                        r'downloadUrl\s*:\s*["\']([^"\']+)["\']',
                        r'direct_link.*?:.*?"([^"]+)"',
                    ]
                    
                    for pattern in patterns:
                        matches = re.findall(pattern, html, re.IGNORECASE)
                        for url in matches:
                            url = url.replace('\\/', '/')
                            if 'mediafire.com' in url and '/download/' in url:
                                return url
                    
                    try:
                        soup = BeautifulSoup(html, 'html.parser')
                        download_button = soup.find('a', {'id': 'downloadButton'})
                        if download_button and download_button.get('href'):
                            url = download_button['href']
                            if url.startswith('//'):
                                url = 'https:' + url
                            return url
                    except:
                        pass
                        
        except Exception as e:
            logger.error(f"MediaFire resolution failed: {e}")
        
        return None
    
    async def _resolve_mega(self, share_link: str) -> Optional[str]:
        return share_link
    
    async def _resolve_onedrive(self, share_link: str) -> Optional[str]:
        if '?id=' in share_link:
            direct_link = share_link.replace('?id=', '/download?')
            if await self._test_direct_link(direct_link):
                return direct_link
        
        if '?' in share_link:
            direct_link = share_link + '&download=1'
        else:
            direct_link = share_link + '?download=1'
        
        return direct_link
    
    async def _resolve_terabox(self, share_link: str) -> Optional[str]:
        try:
            async with self.session.get(share_link) as response:
                if response.status == 200:
                    html = await response.text()
                    patterns = [
                        r'"dlink":"([^"]+)"',
                        r'downloadUrl["\']?\s*:\s*["\']([^"\']+)["\']',
                        r'<a[^>]+href="([^"]+)"[^>]*>.*?Download.*?</a>',
                        r'<source[^>]+src="([^"]+)"',
                    ]
                    
                    for pattern in patterns:
                        matches = re.findall(pattern, html, re.IGNORECASE)
                        for url in matches:
                            if 'terabox.com' in url or '4funbox' in url:
                                url = url.replace('\\/', '/')
                                return url
        except Exception as e:
            logger.error(f"TeraBox resolution failed: {e}")
        
        return None
    
    async def _resolve_simple_direct(self, share_link: str) -> str:
        return share_link
    
    async def _extract_from_html(self, url: str) -> Optional[str]:
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    content_type = response.headers.get('content-type', '').lower()
                    
                    if any(media_type in content_type for media_type in ['audio/', 'video/', 'application/octet-stream']):
                        return url
                    
                    try:
                        html = await response.text()
                    except UnicodeDecodeError:
                        return None
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    for tag in soup.find_all(['audio', 'video', 'source']):
                        src = tag.get('src')
                        if src and self._is_media_url(src):
                            return self._make_absolute(url, src)
                    
                    for link in soup.find_all('a', href=True):
                        href = link.get('href')
                        download_attr = link.get('download')
                        
                        if download_attr and href and self._is_media_url(href):
                            return self._make_absolute(url, href)
                        
                        link_text = link.get_text().lower()
                        if ('download' in link_text or 'dl' in link_text) and href:
                            if self._is_media_url(href):
                                return self._make_absolute(url, href)
                    
                    meta_refresh = soup.find('meta', {'http-equiv': 'refresh'})
                    if meta_refresh and meta_refresh.get('content'):
                        content = meta_refresh['content']
                        if 'url=' in content.lower():
                            redirect_url = content.split('url=', 1)[1].strip()
                            return self._make_absolute(url, redirect_url)
                    
                    json_patterns = [
                        r'"url"\s*:\s*"([^"]+)"',
                        r'"download_url"\s*:\s*"([^"]+)"',
                        r'"direct_link"\s*:\s*"([^"]+)"',
                        r'"src"\s*:\s*"([^"]+)"',
                    ]
                    
                    for pattern in json_patterns:
                        matches = re.findall(pattern, html)
                        for match in matches:
                            if self._is_media_url(match):
                                return self._make_absolute(url, match)
        
        except Exception as e:
            logger.error(f"HTML extraction failed: {e}")
        
        return None
    
    def _is_media_url(self, url: str) -> bool:
        url_lower = url.lower()
        for ext in AUDIO_EXTENSIONS.keys():
            if url_lower.endswith(ext):
                return True
        return False
    
    def _make_absolute(self, base_url: str, url: str) -> str:
        if url.startswith('http://') or url.startswith('https://'):
            return url
        elif url.startswith('//'):
            return 'https:' + url
        elif url.startswith('/'):
            parsed = urlparse(base_url)
            return f"{parsed.scheme}://{parsed.netloc}{url}"
        else:
            return urljoin(base_url, url)
        
# ========== BASE MUSIC PLAYER CLASS ==========
class MusicPlayer:
    """Base Music Player Class with Core Functionality"""
    
    def __init__(self, bot, guild_id: int):
        self.bot = bot
        self.guild_id = guild_id
        self.voice_client: Optional[discord.VoiceClient] = None
        self.current_channel: Optional[discord.TextChannel] = None
        
        # Queue and History
        self.queue: List[Dict] = []
        self.current_track: Optional[Dict] = None
        self.history: List[Dict] = []
        self.max_history_size = 50
        
        # Playback state
        self.is_playing = False
        self.is_paused = False
        self.volume = 0.5
        self.loop_mode = 'off'  # off, track, queue
        
        # Preloading and cache
        self.preloading: Dict[str, Dict] = {}
        self.cache_dir = Path("data/music_cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Download speed control (bytes per second)
        self.download_speed = int(os.getenv('DOWNLOAD_SPEED', 2097152))  # Default 2 MB/s
        
        # Messages for updates
        self.now_playing_message: Optional[discord.Message] = None
        self.queue_message: Optional[discord.Message] = None
        self.loading_message: Optional[discord.Message] = None
        
        # Background tasks
        self.background_downloads: Dict[str, asyncio.Task] = {}
        
        # Event loop for thread safety
        self.loop = asyncio.get_event_loop()
    
    def get_cache_path(self, filename: str) -> Path:
        """Get cache path for filename (sanitized)"""
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        safe_filename = safe_filename[:200]  # Limit length
        return self.cache_dir / safe_filename
    
    def is_cached(self, filename: str) -> bool:
        """Check if file is cached"""
        return self.get_cache_path(filename).exists()
    
    async def download_to_cache(self, track: Dict, update_db: bool = True) -> Optional[Path]:
        """
        Download track to cache with speed control
        Returns: Path to cached file or None if failed
        """
        cache_path = self.get_cache_path(track['filename'])
        
        # Already cached
        if cache_path.exists():
            logger.debug(f"Already cached: {track['filename']}")
            return cache_path
        
        direct_link = track.get('direct_link')
        if not direct_link:
            logger.error(f"No direct link for {track['filename']}")
            return None
        
        logger.info(f"Downloading to cache: {track['filename']}")
        
        try:
            # Try multiple download methods
            methods = [
                self._download_direct,
                self._download_with_redirects,
                self._download_any,
            ]
            
            for method in methods:
                try:
                    logger.debug(f"Trying download method: {method.__name__}")
                    result = await method(direct_link, cache_path)
                    if result:
                        # Update database if requested
                        if update_db:
                            await self._update_cache_status(track['filename'], str(cache_path))
                        return cache_path
                    else:
                        # Delete partially downloaded file
                        if cache_path.exists():
                            try:
                                cache_path.unlink()
                            except:
                                pass
                except Exception as e:
                    logger.debug(f"Download method {method.__name__} failed: {e}")
                    continue
            
            logger.error(f"All download methods failed for: {track['filename']}")
            return None
                    
        except Exception as e:
            logger.error(f"Download error for {track['filename']}: {e}")
            # Clean up partially downloaded file
            if cache_path.exists():
                try:
                    cache_path.unlink()
                except:
                    pass
            return None
    
    async def _download_direct(self, url: str, cache_path: Path) -> bool:
        """Direct download with better error handling"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*',
            'Accept-Encoding': 'identity',
            'Range': 'bytes=0-',
        }
        
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, timeout=180, allow_redirects=True) as response:
                    if response.status in [200, 206]:
                        total_size = int(response.headers.get('content-length', 0))
                        downloaded = 0
                        start_time = time.time()
                        
                        # Check if it's actually media content
                        content_type = response.headers.get('content-type', '').lower()
                        if not any(media_type in content_type for media_type in ['audio/', 'video/', 'application/octet-stream']):
                            logger.warning(f"Content type {content_type} might not be media")
                        
                        with open(cache_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(8192):
                                if not chunk:
                                    continue
                                
                                f.write(chunk)
                                downloaded += len(chunk)
                                
                                # Speed control
                                if self.download_speed > 0:
                                    expected_time = downloaded / self.download_speed
                                    actual_time = time.time() - start_time
                                    
                                    if actual_time < expected_time:
                                        await asyncio.sleep(expected_time - actual_time)
                        
                        if downloaded > 0:
                            logger.info(f"Downloaded {cache_path.name} ({downloaded/1024/1024:.2f} MB)")
                            return True
                        else:
                            logger.error("Downloaded 0 bytes")
                            return False
                    else:
                        logger.error(f"Direct download failed: {response.status}")
                        return False
                        
        except aiohttp.ClientError as e:
            logger.error(f"Download client error: {e}")
            return False
        except asyncio.TimeoutError:
            logger.error("Download timeout")
            return False
        except Exception as e:
            logger.error(f"Download error: {e}")
            return False
    
    async def _download_with_redirects(self, url: str, cache_path: Path) -> bool:
        """Download with manual redirect following"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*',
        }
        
        # Follow redirects manually
        current_url = url
        max_redirects = 5
        redirect_count = 0
        
        while redirect_count < max_redirects:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(current_url, allow_redirects=False, timeout=30) as response:
                    if response.status in [301, 302, 303, 307, 308]:
                        # Follow redirect
                        redirect_url = response.headers.get('Location')
                        if redirect_url:
                            current_url = self._make_absolute(current_url, redirect_url)
                            redirect_count += 1
                            continue
                        else:
                            logger.error("Redirect without Location header")
                            return False
                    elif response.status == 200:
                        # Download the file
                        total_size = int(response.headers.get('content-length', 0))
                        downloaded = 0
                        start_time = time.time()
                        
                        with open(cache_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(8192):
                                if not chunk:
                                    continue
                                
                                f.write(chunk)
                                downloaded += len(chunk)
                                
                                # Speed control
                                if self.download_speed > 0:
                                    expected_time = downloaded / self.download_speed
                                    actual_time = time.time() - start_time
                                    
                                    if actual_time < expected_time:
                                        await asyncio.sleep(expected_time - actual_time)
                        
                        logger.info(f"Downloaded via redirects: {cache_path.name}")
                        return True
                    else:
                        logger.error(f"Download failed: {response.status}")
                        return False
        
        logger.error(f"Too many redirects: {redirect_count}")
        return False
    
    async def _download_any(self, url: str, cache_path: Path) -> bool:
        """Try to download any URL regardless of content type"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*',
            'Range': 'bytes=0-',
        }
        
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, timeout=180, allow_redirects=True) as response:
                    if response.status in [200, 206, 302, 307, 308]:
                        downloaded = 0
                        start_time = time.time()
                        
                        with open(cache_path, 'wb') as f:
                            async for chunk in response.content.iter_any():
                                if not chunk:
                                    continue
                                
                                f.write(chunk)
                                downloaded += len(chunk)
                                
                                # Basic speed control
                                if self.download_speed > 0 and downloaded > 1024:
                                    expected_time = downloaded / self.download_speed
                                    actual_time = time.time() - start_time
                                    
                                    if actual_time < expected_time:
                                        await asyncio.sleep(expected_time - actual_time)
                        
                        if downloaded > 1024:  # At least 1KB
                            logger.info(f"Downloaded {cache_path.name} ({downloaded/1024/1024:.2f} MB)")
                            return True
                        else:
                            logger.warning(f"Small download: {downloaded} bytes")
                            return False
                    else:
                        logger.error(f"Download failed with status: {response.status}")
                        return False
                        
        except Exception as e:
            logger.error(f"Download any error: {e}")
            return False
    
    def _make_absolute(self, base_url: str, url: str) -> str:
        """Make URL absolute"""
        if url.startswith('http://') or url.startswith('https://'):
            return url
        elif url.startswith('//'):
            return 'https:' + url
        elif url.startswith('/'):
            parsed = urlparse(base_url)
            return f"{parsed.scheme}://{parsed.netloc}{url}"
        else:
            return urljoin(base_url, url)
    
    async def _update_cache_status(self, filename: str, cache_path: str):
        """Update cache status in database"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                # First check if track exists in database
                cursor = await db.execute(
                    "SELECT filename FROM track_stats WHERE filename = ?",
                    (filename,)
                )
                exists = await cursor.fetchone()
                
                if exists:
                    # Update existing record
                    await db.execute('''
                        UPDATE track_stats 
                        SET is_cached = 1, cache_path = ?, last_cached = ?
                        WHERE filename = ?
                    ''', (
                        cache_path,
                        datetime.now().isoformat(),
                        filename
                    ))
                else:
                    # Insert new record
                    await db.execute('''
                        INSERT INTO track_stats 
                        (filename, title, artist, genre, direct_link, service, is_cached, cache_path, last_cached)
                        VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                    ''', (
                        filename,
                        "Unknown Title",
                        "Unknown Artist",
                        "Unknown",
                        "",
                        "unknown",
                        cache_path,
                        datetime.now().isoformat()
                    ))
                
                await db.commit()
                logger.info(f"Updated cache status for {filename}")
                
        except Exception as e:
            logger.error(f"Failed to update cache status: {e}")
            pass
    
    async def join_voice(self, voice_channel: discord.VoiceChannel) -> bool:
        """Join voice channel"""
        try:
            if self.voice_client and self.voice_client.is_connected():
                await self.voice_client.move_to(voice_channel)
            else:
                self.voice_client = await voice_channel.connect()
            return True
        except Exception as e:
            logger.error(f"Failed to join voice: {e}")
            return False
    
    async def leave_voice(self):
        """Leave voice channel"""
        if self.voice_client and self.voice_client.is_connected():
            await self.voice_client.disconnect()
        self.voice_client = None
        self.is_playing = False
        self.is_paused = False
    
    async def play_track(self, track: Dict, interaction: Optional[discord.Interaction] = None):
        """Play a track with caching and progress display"""
        try:
            # Add current track to history
            if self.current_track:
                self.history.append(self.current_track)
                if len(self.history) > self.max_history_size:
                    self.history.pop(0)
            
            # Check cache and download if needed
            if not self.is_cached(track['filename']):
                # Send loading message
                if interaction and self.current_channel:
                    embed = discord.Embed(
                        title="⏳ Downloading...",
                        description=f"**{track['title']}** by {track.get('artist', 'Unknown')}",
                        color=discord.Color.blue()
                    )
                    embed.add_field(name="Status", value="Downloading to cache for smooth playback...", inline=False)
                    self.loading_message = await self.current_channel.send(embed=embed)
                
                # Download with progress
                cache_path = await self.download_to_cache(track)
                if not cache_path:
                    if self.loading_message:
                        try:
                            await self.loading_message.delete()
                        except:
                            pass
                        self.loading_message = None
                    
                    if interaction:
                        await interaction.followup.send(f"❌ Failed to download: {track['title']}", ephemeral=True)
                    return
                
                # Delete loading message
                if self.loading_message:
                    try:
                        await self.loading_message.delete()
                    except:
                        pass
                    self.loading_message = None
            
            # Update play stats
            await self.update_play_stats(track['filename'])
            
            # Set current track
            self.current_track = track
            self.is_playing = True
            self.is_paused = False
            
            # Create audio source
            cache_path = self.get_cache_path(track['filename'])
            
            # Use simple FFmpeg options without reconnect
            audio_source = discord.FFmpegPCMAudio(
                str(cache_path),
                options=f'-vn -af volume={self.volume}'
            )
            
            # Play with after callback
            def after_callback(error):
                if error:
                    logger.error(f"Playback error: {error}")
                
                # Schedule the async after_track method in the event loop
                asyncio.run_coroutine_threadsafe(self._after_track_async(error, track), self.loop)
            
            self.voice_client.play(audio_source, after=after_callback)
            
            # Send now playing embed
            if interaction:
                await self.send_now_playing(interaction, track)
            
            logger.info(f"Now playing: {track['title']} by {track.get('artist', 'Unknown')}")
            
            # Start background preloading of queue
            if self.queue:
                asyncio.create_task(self._preload_queue_background())
            
        except Exception as e:
            logger.error(f"Play error: {e}")
            if interaction:
                await interaction.followup.send(f"❌ Error playing track: {str(e)[:200]}", ephemeral=True)
    
    async def _after_track_async(self, error, track):
        """Async callback after track finishes playing"""
        if error:
            logger.error(f"Playback error: {error}")
        
        # Update now playing message to show finished track
        await self._show_finished_track(track)
        
        # Handle loop modes
        if self.loop_mode == 'track' and track:
            await self._replay_track(track)
        elif self.loop_mode == 'queue' and track:
            self.queue.append(track)
            await self.play_next()
        else:
            await self.play_next()
    
    async def _show_finished_track(self, track):
        """Update now playing message to show finished track"""
        try:
            if self.now_playing_message:
                embed = discord.Embed(
                    title="✅ Finished Playing",
                    description=f"**{track['title']}** by {track.get('artist', 'Unknown')}",
                    color=discord.Color.green()
                )
                
                # Add to history display
                if self.history:
                    recent = self.history[-1] if len(self.history) > 0 else track
                    embed.add_field(
                        name="Recently Played",
                        value=f"{recent['title'][:50]}...",
                        inline=True
                    )
                
                await self.now_playing_message.edit(embed=embed, view=None)
                self.now_playing_message = None
                
        except Exception as e:
            logger.error(f"Failed to update finished track: {e}")
    
    async def _replay_track(self, track):
        """Replay the current track"""
        await asyncio.sleep(1)
        await self.play_track(track)
    
    async def play_next(self):
        """Play next track in queue"""
        if not self.queue:
            self.is_playing = False
            self.current_track = None
            
            # Send queue empty message
            if self.current_channel:
                embed = discord.Embed(
                    title="Queue Complete",
                    description="The queue has finished playing.",
                    color=discord.Color.blue()
                )
                await self.current_channel.send(embed=embed)
            
            return
        
        # Get next track
        next_track = self.queue.pop(0)
        await self.play_track(next_track)
    
    async def play_previous(self, interaction: Optional[discord.Interaction] = None) -> bool:
        """Play previous track from history"""
        if not self.history:
            if interaction:
                await interaction.response.send_message("No previous tracks in history", ephemeral=True)
            return False
        
        # Get last track from history
        previous_track = self.history.pop()
        
        # Add current track to front of queue if exists
        if self.current_track:
            self.queue.insert(0, self.current_track)
        
        # Play previous track
        await self.play_track(previous_track, interaction)
        return True
    
    async def remove_from_queue(self, positions: List[int]) -> List[Dict]:
        """Remove tracks from queue by positions"""
        removed = []
        
        # Sort positions in reverse to avoid index shifting
        for pos in sorted(positions, reverse=True):
            if 1 <= pos <= len(self.queue):
                removed.append(self.queue.pop(pos - 1))
        
        return removed
    
    async def update_play_stats(self, filename: str):
        """Update play statistics in database"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                await db.execute('''
                    UPDATE track_stats 
                    SET plays = COALESCE(plays, 0) + 1, last_played = ?
                    WHERE filename = ?
                ''', (datetime.now().isoformat(), filename))
                await db.commit()
        except Exception as e:
            logger.error(f"Failed to update play stats: {e}")
    
    async def update_skip_stats(self, filename: str):
        """Update skip statistics in database"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                await db.execute(
                    "UPDATE track_stats SET skips = COALESCE(skips, 0) + 1 WHERE filename = ?",
                    (filename,)
                )
                await db.commit()
        except Exception as e:
            logger.error(f"Failed to update skip stats: {e}")
    
    async def send_now_playing(self, interaction: discord.Interaction, track: Dict):
        """Send now playing embed with interactive controls"""
        embed = discord.Embed(
            title="🎵 Now Playing",
            color=discord.Color.green()
        )
        
        # Track info
        embed.add_field(name="Title", value=track['title'], inline=False)
        embed.add_field(name="Artist", value=track.get('artist', 'Unknown'), inline=True)
        
        if track.get('duration'):
            embed.add_field(name="Duration", value=track['duration'], inline=True)
        
        if track.get('genre'):
            embed.add_field(name="Genre", value=track['genre'], inline=True)
        
        # Cache status
        cache_status = "✅ Cached" if self.is_cached(track['filename']) else "⏳ Streaming"
        embed.add_field(name="Cache", value=cache_status, inline=True)
        
        # Queue info
        if self.queue:
            next_tracks = []
            for i, t in enumerate(self.queue[:3], 1):
                track_status = "✅" if self.is_cached(t['filename']) else "⏳"
                next_tracks.append(f"`{i}.` {track_status} {t['title'][:30]}...")
            
            if next_tracks:
                embed.add_field(
                    name="Up Next",
                    value="\n".join(next_tracks),
                    inline=False
                )
        
        # History info
        if self.history:
            embed.add_field(
                name="History",
                value=f"{len(self.history)} previous tracks",
                inline=True
            )
        
        # Playback info
        embed.set_footer(text=f"Volume: {int(self.volume * 100)}% | Loop: {self.loop_mode}")
        
        # Create control buttons
        view = MusicControls(self)
        
        # Send or update message
        if self.now_playing_message:
            try:
                await self.now_playing_message.edit(embed=embed, view=view)
            except:
                self.now_playing_message = await interaction.followup.send(embed=embed, view=view)
        else:
            self.now_playing_message = await interaction.followup.send(embed=embed, view=view)
    
    async def _preload_queue_background(self):
        """Preload queued tracks in background"""
        if not self.queue or len(self.queue) <= 1:
            return
        
        # Create status message
        if self.current_channel:
            embed = discord.Embed(
                title="🔄 Preloading Queue",
                description=f"Preloading {len(self.queue)} tracks in background...",
                color=discord.Color.blue()
            )
            status_msg = await self.current_channel.send(embed=embed)
        
        # Preload tracks
        preloaded_count = 0
        for i, track in enumerate(self.queue):
            try:
                if not self.is_cached(track['filename']):
                    # Update status every 3 tracks
                    if self.current_channel and i % 3 == 0:
                        embed = discord.Embed(
                            title="🔄 Preloading Queue",
                            description=f"Downloading {i+1}/{len(self.queue)}",
                            color=discord.Color.blue()
                        )
                        embed.add_field(name="Current", value=track['title'][:50], inline=False)
                        embed.add_field(name="Speed", value=f"{self.download_speed/1024:.0f} KB/s", inline=True)
                        await status_msg.edit(embed=embed)
                    
                    # Download with controlled speed
                    await self.download_to_cache(track, update_db=False)
                    preloaded_count += 1
                    
                    # Small delay between downloads
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Failed to preload {track['filename']}: {e}")
                continue
        
        # Update completion status
        if self.current_channel and preloaded_count > 0:
            embed = discord.Embed(
                title="✅ Queue Preloaded",
                description=f"Preloaded {preloaded_count} tracks for smooth playback",
                color=discord.Color.green()
            )
            await status_msg.edit(embed=embed)
    
    async def preload_playlist(self, playlist_tracks: List[Dict], playlist_name: str) -> Dict:
        """
        Preload a playlist to cache
        Returns: Dict with preload results
        """
        logger.info(f"Starting preload for playlist '{playlist_name}' with {len(playlist_tracks)} tracks")
        
        # Initialize preload status
        self.preloading[playlist_name] = {
            'status': 'preloading',
            'progress': 0,
            'total': len(playlist_tracks),
            'cached': 0,
            'skipped': 0,
            'failed': 0,
            'started_at': datetime.now().isoformat()
        }
        
        cached_count = 0
        skipped_count = 0
        failed_count = 0
        
        # Preload each track
        for i, track in enumerate(playlist_tracks):
            try:
                if self.is_cached(track['filename']):
                    skipped_count += 1
                else:
                    result = await self.download_to_cache(track, update_db=True)
                    if result:
                        cached_count += 1
                    else:
                        failed_count += 1
                
                # Update progress
                self.preloading[playlist_name]['progress'] = i + 1
                self.preloading[playlist_name]['cached'] = cached_count
                self.preloading[playlist_name]['skipped'] = skipped_count
                self.preloading[playlist_name]['failed'] = failed_count
                
                # Small delay to prevent rate limiting
                await asyncio.sleep(0.3)
                
            except Exception as e:
                logger.error(f"Failed to preload {track.get('filename', 'unknown')}: {e}")
                failed_count += 1
        
        # Update final status
        self.preloading[playlist_name]['status'] = 'completed'
        self.preloading[playlist_name]['completed_at'] = datetime.now().isoformat()
        
        result = {
            'playlist_name': playlist_name,
            'total_tracks': len(playlist_tracks),
            'cached': cached_count,
            'already_cached': skipped_count,
            'failed': failed_count,
            'duration': (datetime.now() - datetime.fromisoformat(
                self.preloading[playlist_name]['started_at']
            )).total_seconds()
        }
        
        logger.info(f"Preload completed for '{playlist_name}': {cached_count} new, {skipped_count} already cached")
        return result
    
    def get_preload_progress_embed(self, playlist_name: str) -> discord.Embed:
        """Get embed showing preload progress"""
        if playlist_name not in self.preloading:
            embed = discord.Embed(
                title="Preload Not Found",
                description=f"No preload in progress for '{playlist_name}'",
                color=discord.Color.red()
            )
            return embed
        
        status = self.preloading[playlist_name]
        
        if status['status'] == 'completed':
            embed = discord.Embed(
                title=f"✅ Preload Complete: {playlist_name}",
                color=discord.Color.green()
            )
            
            duration = timedelta(seconds=status.get('duration', 0))
            embed.add_field(name="Total Tracks", value=str(status['total']), inline=True)
            embed.add_field(name="Newly Cached", value=str(status['cached']), inline=True)
            embed.add_field(name="Already Cached", value=str(status['skipped']), inline=True)
            embed.add_field(name="Failed", value=str(status['failed']), inline=True)
            embed.add_field(name="Duration", value=str(duration), inline=True)
            
        else:
            embed = discord.Embed(
                title=f"🔄 Preloading: {playlist_name}",
                color=discord.Color.blue()
            )
            
            progress = status['progress']
            total = status['total']
            percentage = (progress / total * 100) if total > 0 else 0
            
            # Progress bar
            bars = 20
            filled_bars = int(percentage / 100 * bars)
            progress_bar = "█" * filled_bars + "░" * (bars - filled_bars)
            
            embed.description = f"```[{progress_bar}] {percentage:.1f}%```"
            embed.add_field(name="Progress", value=f"{progress}/{total}", inline=True)
            embed.add_field(name="Cached", value=str(status['cached']), inline=True)
            embed.add_field(name="Skipped", value=str(status['skipped']), inline=True)
            embed.add_field(name="Failed", value=str(status['failed']), inline=True)
        
        return embed

# ========== NEW PREMIUM UI COMPONENTS ==========
class PaginatedSelect(Select):
    """Paginated select menu with next/previous buttons"""
    
    def __init__(self, items: List[Dict], placeholder: str, page: int = 0, items_per_page: int = 20, 
                 min_values: int = 1, max_values: int = 1, custom_id: str = None):
        self.all_items = items
        self.page = page
        self.items_per_page = items_per_page
        self.total_pages = max(1, math.ceil(len(items) / items_per_page))
        
        start_idx = page * items_per_page
        end_idx = start_idx + items_per_page
        page_items = items[start_idx:end_idx]
        
        options = []
        for item in page_items:
            label = item.get('label', str(item))[:100]
            description = item.get('description', '')[:100]
            value = item.get('value', str(item))
            options.append(discord.SelectOption(label=label, description=description, value=value))
        
        super().__init__(
            placeholder=f"{placeholder} (Page {page + 1}/{self.total_pages})",
            options=options,
            min_values=min_values,
            max_values=max_values,
            custom_id=custom_id
        )

class PaginatedView(View):
    """View with paginated select and navigation buttons"""
    
    def __init__(self, items: List[Dict], placeholder: str, callback, author: discord.Member,
                 items_per_page: int = 20, timeout: float = 180.0):
        super().__init__(timeout=timeout)
        self.items = items
        self.placeholder = placeholder
        self.callback_func = callback
        self.author = author
        self.items_per_page = items_per_page
        self.current_page = 0
        self.total_pages = max(1, math.ceil(len(items) / items_per_page))
        
        self.update_select()
    
    def update_select(self):
        # Clear existing select
        for item in self.children:
            if isinstance(item, Select):
                self.remove_item(item)
        
        # Create new paginated select
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_items = self.items[start_idx:end_idx]
        
        select = PaginatedSelect(
            page_items,
            self.placeholder,
            self.current_page,
            self.items_per_page,
            custom_id=f"page_select_{self.current_page}"
        )
        
        async def select_callback(interaction: discord.Interaction):
            if interaction.user != self.author:
                await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
                return
            await self.callback_func(interaction, select)
        
        select.callback = select_callback
        self.add_item(select)
        
        # Update navigation buttons
        self.update_buttons()
    
    def update_buttons(self):
        # Clear existing navigation buttons
        navigation_buttons = []
        for item in self.children[:]:
            if isinstance(item, Button) and item.custom_id in ['prev_page', 'next_page', 'close_page']:
                self.remove_item(item)
                navigation_buttons.append(item)
        
        # Add navigation buttons
        prev_button = Button(
            label="◀ Previous",
            style=discord.ButtonStyle.grey,
            disabled=self.current_page == 0,
            custom_id="prev_page"
        )
        
        next_button = Button(
            label="Next ▶",
            style=discord.ButtonStyle.grey,
            disabled=self.current_page >= self.total_pages - 1,
            custom_id="next_page"
        )
        
        close_button = Button(
            label="✖ Close",
            style=discord.ButtonStyle.red,
            custom_id="close_page"
        )
        
        async def prev_callback(interaction: discord.Interaction):
            if interaction.user != self.author:
                await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
                return
            self.current_page -= 1
            self.update_select()
            await interaction.response.edit_message(view=self)
        
        async def next_callback(interaction: discord.Interaction):
            if interaction.user != self.author:
                await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
                return
            self.current_page += 1
            self.update_select()
            await interaction.response.edit_message(view=self)
        
        async def close_callback(interaction: discord.Interaction):
            if interaction.user != self.author:
                await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
                return
            await interaction.message.delete()
        
        prev_button.callback = prev_callback
        next_button.callback = next_callback
        close_button.callback = close_callback
        
        self.add_item(prev_button)
        self.add_item(next_button)
        self.add_item(close_button)
    
    async def on_timeout(self):
        try:
            for item in self.children:
                item.disabled = True
            await self.message.edit(view=self)
        except:
            pass

# ========== NEW NOW PLAYING UI (Maki Style) ==========
class PremiumNowPlayingView(View):
    """Premium Now Playing View that auto-updates like Maki"""
    
    def __init__(self, player, timeout=None):  # No timeout for permanent display
        super().__init__(timeout=None)  # Permanent, won't timeout
        self.player = player
        self.update_task = None
        self.last_track_id = None
        self.is_active = True
    
    async def start_updates(self, message: discord.Message):
        """Start periodic updates for the now playing display"""
        self.message = message
        
        # Start update task if not already running
        if not self.update_task:
            self.update_task = asyncio.create_task(self._update_loop())
    
    async def _update_loop(self):
        """Continuously update the now playing display"""
        while self.is_active and self.player.voice_client and self.player.voice_client.is_connected():
            try:
                # Update if track changed
                current_track_id = self.player.current_track.get('filename') if self.player.current_track else None
                
                if current_track_id != self.last_track_id:
                    await self._update_display()
                    self.last_track_id = current_track_id
                
                # Update progress bar periodically
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Now playing update error: {e}")
                await asyncio.sleep(10)
        
        # Cleanup when done
        self.is_active = False
    
    async def _update_display(self):
        """Update the now playing message"""
        try:
            embed = await self._create_now_playing_embed()
            await self.message.edit(embed=embed, view=self)
        except Exception as e:
            logger.error(f"Failed to update now playing: {e}")
    
    async def _create_now_playing_embed(self) -> discord.Embed:
        """Create premium now playing embed"""
        player = self.player
        
        if not player.current_track:
            embed = discord.Embed(
                title="🎵 Now Playing",
                description="*No track playing*",
                color=discord.Color.blue()
            )
            return embed
        
        track = player.current_track
        
        # Create premium embed
        embed = discord.Embed(
            title=f"🎵 **{track['title'][:100]}**",
            description=f"👤 **{track.get('artist', 'Unknown Artist')}**",
            color=discord.Color.green()
        )
        
        # Add progress bar
        if player.voice_client and player.voice_client.is_playing():
            if hasattr(player.voice_client.source, '_start'):
                elapsed = time.time() - player.voice_client.source._start
                duration = track.get('duration_seconds', 180)  # Default 3 minutes
                
                # Create progress bar
                progress_length = 20
                progress = int((elapsed / duration) * progress_length) if duration > 0 else 0
                progress = min(progress, progress_length)
                
                progress_bar = "▬" * progress + "🔘" + "▬" * (progress_length - progress)
                
                # Format time
                elapsed_str = time.strftime('%M:%S', time.gmtime(elapsed))
                duration_str = time.strftime('%M:%S', time.gmtime(duration))
                
                embed.add_field(
                    name="Progress",
                    value=f"```{progress_bar}```\n`{elapsed_str} / {duration_str}`",
                    inline=False
                )
        
        # Add track info
        if track.get('genre'):
            embed.add_field(name="Genre", value=track['genre'], inline=True)
        
        embed.add_field(name="Status", value="▶️ Playing" if player.is_playing else "⏸️ Paused", inline=True)
        embed.add_field(name="Loop", value=self._get_loop_emoji(), inline=True)
        
        # Queue info
        if player.queue:
            next_track = player.queue[0] if len(player.queue) > 0 else None
            if next_track:
                embed.add_field(
                    name="Up Next",
                    value=f"**{next_track['title'][:50]}**",
                    inline=False
                )
        
        # Cache status
        cache_status = "✅ Cached" if player.is_cached(track['filename']) else "⏳ Streaming"
        embed.add_field(name="Cache", value=cache_status, inline=True)
        
        # Volume
        embed.add_field(name="Volume", value=f"{int(player.volume * 100)}%", inline=True)
        
        # Footer with queue info
        footer_text = f"🔢 Queue: {len(player.queue)} track(s)"
        if player.history:
            footer_text += f" | 📜 History: {len(player.history)}"
        
        embed.set_footer(text=footer_text)
        
        return embed
    
    def _get_loop_emoji(self) -> str:
        loop_modes = {
            'off': '❌',
            'track': '🔂',
            'queue': '🔁'
        }
        return loop_modes.get(self.player.loop_mode, '❌')
    
    @discord.ui.button(label="⏮️", style=discord.ButtonStyle.grey, row=0, custom_id="np_previous")
    async def previous_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        if self.player.history:
            await self.player.play_previous()
    
    @discord.ui.button(label="⏸️", style=discord.ButtonStyle.grey, row=0, custom_id="np_pause")
    async def pause_button(self, interaction: discord.Interaction, button: Button):
        if self.player.voice_client and self.player.voice_client.is_playing():
            self.player.voice_client.pause()
            self.player.is_paused = True
            button.label = "▶️"
            await interaction.response.edit_message(view=self)
    
    @discord.ui.button(label="▶️", style=discord.ButtonStyle.grey, row=0, custom_id="np_resume")
    async def resume_button(self, interaction: discord.Interaction, button: Button):
        if self.player.voice_client and self.player.voice_client.is_paused():
            self.player.voice_client.resume()
            self.player.is_paused = False
            # Find and update pause button
            for child in self.children:
                if child.custom_id == "np_pause":
                    child.label = "⏸️"
            await interaction.response.edit_message(view=self)
    
    @discord.ui.button(label="⏭️", style=discord.ButtonStyle.grey, row=0, custom_id="np_skip")
    async def skip_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        if self.player.voice_client and self.player.voice_client.is_playing():
            self.player.voice_client.stop()
            if self.player.current_track:
                await self.player.update_skip_stats(self.player.current_track['filename'])
    
    @discord.ui.button(label="🔁", style=discord.ButtonStyle.grey, row=0, custom_id="np_loop")
    async def loop_button(self, interaction: discord.Interaction, button: Button):
        modes = ['off', 'track', 'queue']
        current_idx = modes.index(self.player.loop_mode)
        next_idx = (current_idx + 1) % len(modes)
        self.player.loop_mode = modes[next_idx]
        
        mode_labels = {'off': '🔁', 'track': '🔂', 'queue': '🔁'}
        button.label = mode_labels[modes[next_idx]]
        await interaction.response.edit_message(view=self)
    
    @discord.ui.button(label="🔀", style=discord.ButtonStyle.grey, row=1, custom_id="np_shuffle")
    async def shuffle_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        if len(self.player.queue) >= 2:
            random.shuffle(self.player.queue)
            await interaction.followup.send("🔀 Queue shuffled", ephemeral=True)
    
    @discord.ui.button(label="📋", style=discord.ButtonStyle.grey, row=1, custom_id="np_queue")
    async def queue_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        await self.player.show_queue(interaction)
    
    @discord.ui.button(label="🔊", style=discord.ButtonStyle.grey, row=1, custom_id="np_volume")
    async def volume_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.send_message(
            "Enter volume level (1-100):",
            ephemeral=True,
            delete_after=30
        )
        
        def check(m):
            return m.author == interaction.user and m.channel == interaction.channel and m.content.isdigit()
        
        try:
            msg = await self.player.bot.wait_for('message', timeout=30, check=check)
            level = int(msg.content)
            
            if 1 <= level <= 100:
                self.player.volume = level / 100
                if self.player.voice_client and self.player.voice_client.source:
                    self.player.voice_client.source.volume = self.player.volume
                
                await interaction.followup.send(f"🔊 Volume set to {level}%", ephemeral=True)
                await msg.delete()
            else:
                await interaction.followup.send("Volume must be between 1 and 100", ephemeral=True)
        except asyncio.TimeoutError:
            await interaction.followup.send("Volume change timed out", ephemeral=True)
    
    @discord.ui.button(label="⏹️", style=discord.ButtonStyle.red, row=1, custom_id="np_stop")
    async def stop_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        if self.player.voice_client:
            self.player.voice_client.stop()
            self.player.queue.clear()
            await interaction.followup.send("⏹️ Playback stopped", ephemeral=True)
    
    async def on_timeout(self):
        # This won't happen since we set timeout=None
        pass
    
    def stop(self):
        """Stop the update loop"""
        self.is_active = False
        if self.update_task:
            self.update_task.cancel()

# ========== MANAGE MUSIC UI COMPONENTS ==========
class ManageMusicMainView(View):
    """Main Manage Music Panel with Dropdown"""
    
    def __init__(self, cog, author: discord.Member, timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
        
        # Create dropdown
        select = Select(
            placeholder="Select action...",
            options=[
                discord.SelectOption(label="🎵 Add Music", value="add", description="Add new music to library"),
                discord.SelectOption(label="🗑️ Remove", value="remove", description="Remove music or playlists"),
                discord.SelectOption(label="⚙️ Manage", value="manage", description="Manage library and cache"),
                discord.SelectOption(label="📊 Stats", value="stats", description="View statistics"),
                discord.SelectOption(label="❓ Help", value="help", description="Show help for this panel")
            ]
        )
        select.callback = self.dropdown_callback
        self.add_item(select)
        
        # Close button
        close_btn = Button(label="✖ Close", style=discord.ButtonStyle.red, row=1)
        async def close_callback(interaction: discord.Interaction):
            if interaction.user != self.author:
                await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
                return
            await interaction.message.delete()
        close_btn.callback = close_callback
        self.add_item(close_btn)
    
    async def dropdown_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        value = interaction.data['values'][0]
        
        if value == "add":
            view = AddPanelView(self.cog, self.author)
            embed = discord.Embed(
                title="🎵 Add Content",
                description="Select what you want to add:",
                color=discord.Color.green()
            )
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        
        elif value == "remove":
            view = RemovePanelView(self.cog, self.author)
            embed = discord.Embed(
                title="🗑️ Remove Content",
                description="Select what you want to remove:",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        
        elif value == "manage":
            view = ManagePanelView(self.cog, self.author)
            embed = discord.Embed(
                title="⚙️ Manage Content",
                description="Select management action:",
                color=discord.Color.blue()
            )
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        
        elif value == "stats":
            await self.show_stats(interaction)
        
        elif value == "help":
            await self.show_help(interaction)
    
    async def show_stats(self, interaction: discord.Interaction):
        """Show library statistics"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                # Get total tracks
                cursor = await db.execute("SELECT COUNT(*) FROM track_stats")
                total_tracks = (await cursor.fetchone())[0]
                
                # Get cached tracks
                cursor = await db.execute("SELECT COUNT(*) FROM track_stats WHERE is_cached = 1")
                cached_tracks = (await cursor.fetchone())[0]
                
                # Get total playlists
                cursor = await db.execute("SELECT COUNT(*) FROM playlists WHERE user_id = ?", (self.author.id,))
                total_playlists = (await cursor.fetchone())[0]
                
                # Get most played tracks
                cursor = await db.execute("""
                    SELECT title, artist, plays 
                    FROM track_stats 
                    WHERE plays > 0 
                    ORDER BY plays DESC 
                    LIMIT 5
                """)
                top_tracks = await cursor.fetchall()
                
                # Get cache size
                cache_dir = Path("data/music_cache")
                cache_size = 0
                if cache_dir.exists():
                    for f in cache_dir.glob('**/*'):
                        if f.is_file():
                            cache_size += f.stat().st_size
                
                embed = discord.Embed(
                    title="📊 Library Statistics",
                    description=f"**Total Tracks:** {total_tracks}\n"
                              f"**Cached Tracks:** {cached_tracks}\n"
                              f"**Your Playlists:** {total_playlists}\n"
                              f"**Cache Size:** {cache_size/1024/1024:.2f} MB",
                    color=discord.Color.purple()
                )
                
                if top_tracks:
                    top_list = ""
                    for i, (title, artist, plays) in enumerate(top_tracks, 1):
                        top_list += f"{i}. **{title[:30]}** - {artist[:20]} ({plays} plays)\n"
                    embed.add_field(name="Top Played Tracks", value=top_list, inline=False)
                
                await interaction.followup.send(embed=embed, ephemeral=True)
                
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            await interaction.followup.send("❌ Failed to get statistics", ephemeral=True)
    
    async def show_help(self, interaction: discord.Interaction):
        """Show help for manage music panel"""
        embed = discord.Embed(
            title="❓ Manage Music Help",
            description="This panel allows you to manage your music library.",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="🎵 Add",
            value="• **Add Music**: Add individual tracks\n"
                  "• **Add Playlist**: Create new playlists\n"
                  "• **Add to Playlist**: Add tracks to existing playlists",
            inline=False
        )
        
        embed.add_field(
            name="🗑️ Remove",
            value="• **Remove Music**: Delete tracks from library\n"
                  "• **Remove Playlist**: Delete entire playlists\n"
                  "• **Remove from Playlist**: Remove tracks from playlists",
            inline=False
        )
        
        embed.add_field(
            name="⚙️ Manage",
            value="• **Preload**: Cache tracks for smooth playback\n"
                  "• **Unload**: Remove tracks from cache\n"
                  "• **Edit**: Edit track or playlist info",
            inline=False
        )
        
        embed.set_footer(text="All changes are saved immediately")
        await interaction.followup.send(embed=embed, ephemeral=True)

# ========== ADD PANEL VIEW ==========
class AddPanelView(View):
    """Panel for adding content"""
    
    def __init__(self, cog, author: discord.Member, timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
    
    @discord.ui.button(label="➕ Add Music", style=discord.ButtonStyle.green, row=0)
    async def add_music_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        modal = AddMusicModal(self.cog)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="📁 Add Playlist", style=discord.ButtonStyle.green, row=0)
    async def add_playlist_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        modal = AddPlaylistModal(self.cog, self.author)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="🎵 Add to Playlist", style=discord.ButtonStyle.blurple, row=1)
    async def add_to_playlist_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get available playlists and tracks
        playlists = await self.cog.get_user_playlists(self.author.id)
        all_tracks = await self.cog.get_all_tracks()
        
        if not playlists:
            await interaction.followup.send("❌ You don't have any playlists. Create one first!", ephemeral=True)
            return
        
        if not all_tracks:
            await interaction.followup.send("❌ No tracks in library. Add some music first!", ephemeral=True)
            return
        
        # Create view for selecting playlist and tracks
        view = AddToPlaylistView(self.cog, self.author, playlists, all_tracks)
        embed = discord.Embed(
            title="🎵 Add to Playlist",
            description="Select a playlist and tracks to add:",
            color=discord.Color.green()
        )
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.grey, row=2)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = ManageMusicMainView(self.cog, self.author)
        embed = discord.Embed(
            title="🎵 Manage Music",
            description="Select an action from the dropdown:",
            color=discord.Color.blue()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    @discord.ui.button(label="✖ Close", style=discord.ButtonStyle.red, row=2)
    async def close_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== ADD MUSIC MODAL ==========
class AddMusicModal(Modal, title="➕ Add Music"):
    """Modal for adding music"""
    
    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog
        
        self.title_input = TextInput(
            label="Track Title",
            placeholder="Enter track title...",
            required=True,
            max_length=200
        )
        
        self.artist_input = TextInput(
            label="Artist",
            placeholder="Enter artist name...",
            required=True,
            max_length=200
        )
        
        self.link_input = TextInput(
            label="Download Link",
            placeholder="Dropbox, Google Drive, MediaFire, etc...",
            required=True,
            max_length=500
        )
        
        self.genre_input = TextInput(
            label="Genre (Optional)",
            placeholder="Pop, Rock, Electronic, etc...",
            required=False,
            max_length=100
        )
        
        self.add_item(self.title_input)
        self.add_item(self.artist_input)
        self.add_item(self.link_input)
        self.add_item(self.genre_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        title = self.title_input.value.strip()
        artist = self.artist_input.value.strip()
        link = self.link_input.value.strip()
        genre = self.genre_input.value.strip() or "Unknown"
        
        # Validate link
        if not link.startswith('http'):
            embed = discord.Embed(
                title="❌ Invalid Link",
                description="Please provide a valid HTTP/HTTPS link",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        
        # Create safe filename
        safe_title = re.sub(r'[^\w\s\-\.\(\)\[\]]', '', title)
        safe_artist = re.sub(r'[^\w\s\-\.\(\)\[\]]', '', artist)
        filename = f"{safe_artist} - {safe_title}.mp3"
        
        # Show processing message
        embed = discord.Embed(
            title="🔗 Processing...",
            description=f"Adding **{title}** by {artist}",
            color=discord.Color.blue()
        )
        embed.set_footer(text="This may take a moment...")
        msg = await interaction.followup.send(embed=embed, ephemeral=True)
        
        try:
            # Resolve link
            async with CloudStorageResolver() as resolver:
                service = resolver.identify_service(link)
                direct_link = await resolver.resolve_link(link)
                
                if not direct_link:
                    embed = discord.Embed(
                        title="❌ Link Resolution Failed",
                        description="Could not get direct download link.",
                        color=discord.Color.red()
                    )
                    await msg.edit(embed=embed)
                    return
                
                # Check if already exists
                existing = await self.cog.get_track_by_filename(filename)
                if existing:
                    embed = discord.Embed(
                        title="⚠️ Track Already Exists",
                        description=f"**{existing['title']}** is already in the library",
                        color=discord.Color.orange()
                    )
                    await msg.edit(embed=embed)
                    return
                
                # Test link
                async with aiohttp.ClientSession() as session:
                    async with session.head(direct_link, allow_redirects=True, timeout=10) as test_response:
                        if test_response.status != 200:
                            embed = discord.Embed(
                                title="❌ Download Failed",
                                description=f"Link returned status: {test_response.status}",
                                color=discord.Color.red()
                            )
                            await msg.edit(embed=embed)
                            return
                
                # Add to database
                async with aiosqlite.connect("data/music_bot.db") as db:
                    await db.execute('''
                        INSERT INTO track_stats 
                        (filename, title, artist, genre, direct_link, service, added_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        filename,
                        title,
                        artist,
                        genre,
                        direct_link,
                        service,
                        datetime.now().isoformat()
                    ))
                    await db.commit()
                
                # Update index
                await self.cog._add_to_json_index({
                    'filename': filename,
                    'title': title,
                    'artist': artist,
                    'genre': genre,
                    'direct_link': direct_link,
                    'service': service,
                    'added_date': datetime.now().isoformat()
                })
                
                # Success
                embed = discord.Embed(
                    title="✅ Track Added",
                    description=f"**{title}** by {artist}",
                    color=discord.Color.green()
                )
                embed.add_field(name="Status", value="✅ Ready to play", inline=True)
                embed.add_field(name="Service", value=service.replace('_', ' ').title(), inline=True)
                embed.set_footer(text="Use /play to play this track")
                
                await msg.edit(embed=embed)
                
        except Exception as e:
            logger.error(f"Failed to add track: {e}", exc_info=True)
            embed = discord.Embed(
                title="❌ Failed to Add Track",
                description=f"Error: {str(e)[:500]}",
                color=discord.Color.red()
            )
            await msg.edit(embed=embed)

# ========== ADD PLAYLIST MODAL ==========
class AddPlaylistModal(Modal, title="📁 Create Playlist"):
    """Modal for creating a playlist"""
    
    def __init__(self, cog, author: discord.Member):
        super().__init__(timeout=None)
        self.cog = cog
        self.author = author
        
        self.name_input = TextInput(
            label="Playlist Name",
            placeholder="Enter playlist name...",
            required=True,
            max_length=100
        )
        
        self.description_input = TextInput(
            label="Description (Optional)",
            placeholder="Enter playlist description...",
            required=False,
            max_length=200,
            style=discord.TextStyle.long
        )
        
        self.add_item(self.name_input)
        self.add_item(self.description_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        name = self.name_input.value.strip()
        description = self.description_input.value.strip()
        
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                # Check if exists
                cursor = await db.execute(
                    "SELECT id FROM playlists WHERE name = ? AND user_id = ?",
                    (name, self.author.id)
                )
                existing = await cursor.fetchone()
                
                if existing:
                    embed = discord.Embed(
                        title="❌ Playlist Exists",
                        description=f"You already have a playlist named '{name}'",
                        color=discord.Color.orange()
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
                
                # Create playlist
                await db.execute(
                    "INSERT INTO playlists (name, user_id, description) VALUES (?, ?, ?)",
                    (name, self.author.id, description)
                )
                await db.commit()
                
                embed = discord.Embed(
                    title="✅ Playlist Created",
                    description=f"Created playlist: **{name}**",
                    color=discord.Color.green()
                )
                if description:
                    embed.add_field(name="Description", value=description, inline=False)
                embed.set_footer(text=f"Owner: {self.author.display_name}")
                
                await interaction.followup.send(embed=embed, ephemeral=True)
                
        except Exception as e:
            logger.error(f"Failed to create playlist: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description=f"Failed to create playlist: {e}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

# ========== ADD TO PLAYLIST VIEW (PAGINATED) ==========
class AddToPlaylistView(View):
    """View for adding tracks to playlist with pagination"""
    
    def __init__(self, cog, author: discord.Member, playlists: List[Dict], tracks: List[Dict], timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
        self.playlists = playlists
        self.tracks = tracks
        
        # Current pages
        self.playlist_page = 0
        self.track_page = 0
        
        # Selections
        self.selected_playlist = None
        self.selected_tracks = set()
        
        # Update the view
        self.update_view()
    
    def update_view(self):
        """Update the view with current selections"""
        # Clear existing items
        self.clear_items()
        
        # Playlist select (single selection)
        playlist_select = self.create_playlist_select()
        self.add_item(playlist_select)
        
        # Track select (multiple selection)
        track_select = self.create_track_select()
        self.add_item(track_select)
        
        # Action buttons
        confirm_btn = Button(
            label="✅ Confirm",
            style=discord.ButtonStyle.green,
            disabled=not (self.selected_playlist and self.selected_tracks),
            row=2
        )
        confirm_btn.callback = self.confirm_callback
        
        clear_btn = Button(
            label="🗑️ Clear Selection",
            style=discord.ButtonStyle.grey,
            row=2
        )
        clear_btn.callback = self.clear_callback
        
        back_btn = Button(
            label="🔙 Back",
            style=discord.ButtonStyle.grey,
            row=3
        )
        back_btn.callback = self.back_callback
        
        close_btn = Button(
            label="✖ Close",
            style=discord.ButtonStyle.red,
            row=3
        )
        close_btn.callback = self.close_callback
        
        self.add_item(confirm_btn)
        self.add_item(clear_btn)
        self.add_item(back_btn)
        self.add_item(close_btn)
        
        # Navigation buttons for playlists
        if len(self.playlists) > 20:
            nav_row = self.create_navigation_buttons("playlist", 4)
            for btn in nav_row:
                self.add_item(btn)
        
        # Navigation buttons for tracks
        if len(self.tracks) > 20:
            nav_row = self.create_navigation_buttons("track", 5)
            for btn in nav_row:
                self.add_item(btn)
    
    def create_playlist_select(self) -> Select:
        """Create playlist select dropdown"""
        items_per_page = 20
        start_idx = self.playlist_page * items_per_page
        end_idx = start_idx + items_per_page
        page_items = self.playlists[start_idx:end_idx]
        
        options = []
        for i, playlist in enumerate(page_items):
            playlist_id = start_idx + i
            label = playlist['name'][:90]
            description = f"by {playlist.get('owner', 'You')}"
            if playlist.get('track_count'):
                description = f"{playlist['track_count']} tracks"
            
            options.append(discord.SelectOption(
                label=label,
                description=description[:95],
                value=str(playlist_id),
                default=str(playlist_id) == str(self.selected_playlist) if self.selected_playlist else False
            ))
        
        placeholder = f"Select Playlist (Page {self.playlist_page + 1}/{math.ceil(len(self.playlists)/20)})"
        select = Select(
            placeholder=placeholder,
            options=options,
            min_values=1,
            max_values=1
        )
        select.callback = self.playlist_select_callback
        return select
    
    def create_track_select(self) -> Select:
        """Create track select dropdown"""
        items_per_page = 20
        start_idx = self.track_page * items_per_page
        end_idx = start_idx + items_per_page
        page_items = self.tracks[start_idx:end_idx]
        
        options = []
        for i, track in enumerate(page_items):
            track_id = start_idx + i
            label = track['title'][:90]
            description = track.get('artist', 'Unknown Artist')[:95]
            
            options.append(discord.SelectOption(
                label=label,
                description=description,
                value=str(track_id),
                default=str(track_id) in self.selected_tracks
            ))
        
        placeholder = f"Select Tracks (Page {self.track_page + 1}/{math.ceil(len(self.tracks)/20)})"
        select = Select(
            placeholder=placeholder,
            options=options,
            min_values=0,
            max_values=min(20, len(page_items))
        )
        select.callback = self.track_select_callback
        return select
    
    def create_navigation_buttons(self, target: str, row: int) -> List[Button]:
        """Create navigation buttons for pagination"""
        if target == "playlist":
            total_pages = math.ceil(len(self.playlists) / 20)
            current_page = self.playlist_page
        else:
            total_pages = math.ceil(len(self.tracks) / 20)
            current_page = self.track_page
        
        prev_btn = Button(
            label="◀ Prev",
            style=discord.ButtonStyle.grey,
            disabled=current_page == 0,
            custom_id=f"{target}_prev",
            row=row
        )
        
        next_btn = Button(
            label="Next ▶",
            style=discord.ButtonStyle.grey,
            disabled=current_page >= total_pages - 1,
            custom_id=f"{target}_next",
            row=row
        )
        
        async def prev_callback(interaction: discord.Interaction):
            if interaction.user != self.author:
                await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
                return
            
            if target == "playlist":
                self.playlist_page -= 1
            else:
                self.track_page -= 1
            
            self.update_view()
            await interaction.response.edit_message(view=self)
        
        async def next_callback(interaction: discord.Interaction):
            if interaction.user != self.author:
                await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
                return
            
            if target == "playlist":
                self.playlist_page += 1
            else:
                self.track_page += 1
            
            self.update_view()
            await interaction.response.edit_message(view=self)
        
        prev_btn.callback = prev_callback
        next_btn.callback = next_callback
        
        return [prev_btn, next_btn]
    
    async def playlist_select_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
            return
        
        self.selected_playlist = interaction.data['values'][0]
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def track_select_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
            return
        
        self.selected_tracks = set(interaction.data['values'])
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def confirm_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get selected playlist
        playlist_idx = int(self.selected_playlist)
        playlist = self.playlists[playlist_idx]
        
        # Get selected tracks
        track_indices = [int(idx) for idx in self.selected_tracks]
        selected_tracks = [self.tracks[idx] for idx in track_indices]
        
        try:
            added_count = 0
            already_in_playlist = 0
            
            async with aiosqlite.connect("data/music_bot.db") as db:
                # Get playlist ID
                cursor = await db.execute(
                    "SELECT id FROM playlists WHERE name = ? AND user_id = ?",
                    (playlist['name'], self.author.id)
                )
                playlist_id = (await cursor.fetchone())[0]
                
                # Get existing tracks in playlist
                cursor = await db.execute(
                    "SELECT track_filename FROM playlist_tracks WHERE playlist_id = ?",
                    (playlist_id,)
                )
                existing_tracks = {row[0] for row in await cursor.fetchall()}
                
                # Add each track
                for track in selected_tracks:
                    if track['filename'] in existing_tracks:
                        already_in_playlist += 1
                        continue
                    
                    # Get next position
                    cursor = await db.execute(
                        "SELECT MAX(position) FROM playlist_tracks WHERE playlist_id = ?",
                        (playlist_id,)
                    )
                    max_pos = await cursor.fetchone()
                    next_pos = (max_pos[0] or 0) + 1
                    
                    # Insert into playlist
                    await db.execute(
                        "INSERT INTO playlist_tracks (playlist_id, track_filename, position) VALUES (?, ?, ?)",
                        (playlist_id, track['filename'], next_pos)
                    )
                    added_count += 1
                
                await db.commit()
            
            # Create result embed
            embed = discord.Embed(
                title="✅ Tracks Added to Playlist",
                color=discord.Color.green()
            )
            embed.add_field(name="Playlist", value=playlist['name'], inline=True)
            embed.add_field(name="Added", value=str(added_count), inline=True)
            
            if already_in_playlist > 0:
                embed.add_field(name="Already in Playlist", value=str(already_in_playlist), inline=True)
            
            if added_count > 0:
                track_list = "\n".join([f"• {t['title'][:50]}..." for t in selected_tracks[:3]])
                if added_count > 3:
                    track_list += f"\n... and {added_count - 3} more"
                
                embed.add_field(name="Added Tracks", value=track_list, inline=False)
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
            # Update the view to reflect changes
            self.selected_tracks.clear()
            self.update_view()
            await interaction.edit_original_response(view=self)
            
        except Exception as e:
            logger.error(f"Failed to add tracks to playlist: {e}")
            embed = discord.Embed(
                title="❌ Failed to Add Tracks",
                description=f"Error: {str(e)[:500]}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
    
    async def clear_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.selected_tracks.clear()
        self.selected_playlist = None
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def back_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = AddPanelView(self.cog, self.author)
        embed = discord.Embed(
            title="🎵 Add Content",
            description="Select what you want to add:",
            color=discord.Color.green()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    async def close_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== REMOVE PANEL VIEW ==========
class RemovePanelView(View):
    """Panel for removing content"""
    
    def __init__(self, cog, author: discord.Member, timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
    
    @discord.ui.button(label="🗑️ Remove Music", style=discord.ButtonStyle.red, row=0)
    async def remove_music_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get all tracks
        tracks = await self.cog.get_all_tracks()
        if not tracks:
            await interaction.followup.send("❌ No tracks in library to remove.", ephemeral=True)
            return
        
        view = RemoveMusicView(self.cog, self.author, tracks)
        embed = discord.Embed(
            title="🗑️ Remove Music",
            description="Select tracks to remove from library:",
            color=discord.Color.red()
        )
        embed.set_footer(text="⚠️ This will delete tracks from database and cache")
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="📁 Remove Playlist", style=discord.ButtonStyle.red, row=0)
    async def remove_playlist_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get user playlists
        playlists = await self.cog.get_user_playlists(self.author.id)
        if not playlists:
            await interaction.followup.send("❌ You don't have any playlists to remove.", ephemeral=True)
            return
        
        view = RemovePlaylistView(self.cog, self.author, playlists)
        embed = discord.Embed(
            title="🗑️ Remove Playlist",
            description="Select playlists to remove:",
            color=discord.Color.red()
        )
        embed.set_footer(text="⚠️ This will delete playlists but not the tracks")
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="🎵 Remove from Playlist", style=discord.ButtonStyle.red, row=1)
    async def remove_from_playlist_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get user playlists
        playlists = await self.cog.get_user_playlists(self.author.id)
        if not playlists:
            await interaction.followup.send("❌ You don't have any playlists.", ephemeral=True)
            return
        
        view = SelectPlaylistToRemoveFromView(self.cog, self.author, playlists)
        embed = discord.Embed(
            title="🎵 Remove from Playlist",
            description="Select a playlist to remove tracks from:",
            color=discord.Color.orange()
        )
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.grey, row=2)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = ManageMusicMainView(self.cog, self.author)
        embed = discord.Embed(
            title="🎵 Manage Music",
            description="Select an action from the dropdown:",
            color=discord.Color.blue()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    @discord.ui.button(label="✖ Close", style=discord.ButtonStyle.red, row=2)
    async def close_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== REMOVE MUSIC VIEW ==========
class RemoveMusicView(View):
    """View for removing music with pagination"""
    
    def __init__(self, cog, author: discord.Member, tracks: List[Dict], timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
        self.tracks = tracks
        self.selected_tracks = set()
        self.current_page = 0
        self.items_per_page = 20
        
        self.update_view()
    
    def update_view(self):
        """Update the view with current page"""
        self.clear_items()
        
        # Create track select
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_tracks = self.tracks[start_idx:end_idx]
        
        options = []
        for i, track in enumerate(page_tracks):
            track_idx = start_idx + i
            label = track['title'][:90]
            description = track.get('artist', 'Unknown Artist')[:95]
            
            options.append(discord.SelectOption(
                label=label,
                description=description,
                value=str(track_idx),
                default=str(track_idx) in self.selected_tracks
            ))
        
        select = Select(
            placeholder=f"Select Tracks to Remove (Page {self.current_page + 1}/{math.ceil(len(self.tracks)/self.items_per_page)})",
            options=options,
            min_values=0,
            max_values=len(page_tracks)
        )
        select.callback = self.select_callback
        self.add_item(select)
        
        # Action buttons
        remove_btn = Button(
            label="🗑️ Remove Selected",
            style=discord.ButtonStyle.red,
            disabled=len(self.selected_tracks) == 0,
            row=1
        )
        remove_btn.callback = self.remove_callback
        
        select_all_btn = Button(
            label="✅ Select All on Page",
            style=discord.ButtonStyle.grey,
            row=1
        )
        select_all_btn.callback = self.select_all_callback
        
        clear_btn = Button(
            label="🗑️ Clear Selection",
            style=discord.ButtonStyle.grey,
            row=2
        )
        clear_btn.callback = self.clear_callback
        
        # Navigation buttons
        if len(self.tracks) > self.items_per_page:
            nav_row = 3
            prev_btn = Button(
                label="◀ Previous",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page == 0,
                row=nav_row
            )
            prev_btn.callback = self.prev_callback
            
            next_btn = Button(
                label="Next ▶",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page >= math.ceil(len(self.tracks)/self.items_per_page) - 1,
                row=nav_row
            )
            next_btn.callback = self.next_callback
            
            self.add_item(prev_btn)
            self.add_item(next_btn)
        
        back_btn = Button(
            label="🔙 Back",
            style=discord.ButtonStyle.grey,
            row=4
        )
        back_btn.callback = self.back_callback
        
        close_btn = Button(
            label="✖ Close",
            style=discord.ButtonStyle.red,
            row=4
        )
        close_btn.callback = self.close_callback
        
        self.add_item(remove_btn)
        self.add_item(select_all_btn)
        self.add_item(clear_btn)
        self.add_item(back_btn)
        self.add_item(close_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
            return
        
        self.selected_tracks = set(interaction.data['values'])
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def select_all_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        
        for i in range(start_idx, min(end_idx, len(self.tracks))):
            self.selected_tracks.add(str(i))
        
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def remove_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get selected tracks
        track_indices = [int(idx) for idx in self.selected_tracks]
        selected_tracks = [self.tracks[idx] for idx in track_indices]
        
        try:
            removed_count = 0
            failed_count = 0
            
            async with aiosqlite.connect("data/music_bot.db") as db:
                for track in selected_tracks:
                    try:
                        # Remove from database
                        await db.execute(
                            "DELETE FROM track_stats WHERE filename = ?",
                            (track['filename'],)
                        )
                        
                        # Remove from playlists
                        await db.execute(
                            "DELETE FROM playlist_tracks WHERE track_filename = ?",
                            (track['filename'],)
                        )
                        
                        # Remove from cache
                        cache_path = Path("data/music_cache") / re.sub(r'[<>:"/\\|?*]', '_', track['filename'])[:200]
                        if cache_path.exists():
                            cache_path.unlink()
                        
                        removed_count += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to remove track {track['filename']}: {e}")
                        failed_count += 1
                
                await db.commit()
            
            # Update index
            await self.cog.update_track_index()
            
            # Result embed
            embed = discord.Embed(
                title="🗑️ Music Removed",
                color=discord.Color.green() if removed_count > 0 else discord.Color.red()
            )
            embed.add_field(name="Removed", value=str(removed_count), inline=True)
            embed.add_field(name="Failed", value=str(failed_count), inline=True)
            
            if removed_count > 0:
                # Update local list
                for idx in sorted(track_indices, reverse=True):
                    del self.tracks[idx]
                
                # Reset selection and page
                self.selected_tracks.clear()
                self.current_page = min(self.current_page, math.ceil(len(self.tracks)/self.items_per_page) - 1)
                self.update_view()
                
                track_list = "\n".join([f"• {t['title'][:50]}..." for t in selected_tracks[:3]])
                if removed_count > 3:
                    track_list += f"\n... and {removed_count - 3} more"
                
                embed.add_field(name="Removed Tracks", value=track_list, inline=False)
                embed.set_footer(text="Tracks removed from database, cache, and all playlists")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            await interaction.edit_original_response(view=self)
            
        except Exception as e:
            logger.error(f"Failed to remove tracks: {e}")
            embed = discord.Embed(
                title="❌ Failed to Remove Tracks",
                description=f"Error: {str(e)[:500]}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
    
    async def clear_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.selected_tracks.clear()
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def prev_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page -= 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def next_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page += 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def back_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = RemovePanelView(self.cog, self.author)
        embed = discord.Embed(
            title="🗑️ Remove Content",
            description="Select what you want to remove:",
            color=discord.Color.red()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    async def close_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== REMOVE PLAYLIST VIEW ==========
class RemovePlaylistView(View):
    """View for removing playlists with pagination"""
    
    def __init__(self, cog, author: discord.Member, playlists: List[Dict], timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
        self.playlists = playlists
        self.selected_playlists = set()
        self.current_page = 0
        self.items_per_page = 20
        
        self.update_view()
    
    def update_view(self):
        """Update the view with current page"""
        self.clear_items()
        
        # Create playlist select
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_playlists = self.playlists[start_idx:end_idx]
        
        options = []
        for i, playlist in enumerate(page_playlists):
            playlist_idx = start_idx + i
            label = playlist['name'][:90]
            description = f"{playlist.get('track_count', 0)} tracks"
            
            options.append(discord.SelectOption(
                label=label,
                description=description,
                value=str(playlist_idx),
                default=str(playlist_idx) in self.selected_playlists
            ))
        
        select = Select(
            placeholder=f"Select Playlists to Remove (Page {self.current_page + 1}/{math.ceil(len(self.playlists)/self.items_per_page)})",
            options=options,
            min_values=0,
            max_values=len(page_playlists)
        )
        select.callback = self.select_callback
        self.add_item(select)
        
        # Action buttons
        remove_btn = Button(
            label="🗑️ Remove Selected",
            style=discord.ButtonStyle.red,
            disabled=len(self.selected_playlists) == 0,
            row=1
        )
        remove_btn.callback = self.remove_callback
        
        select_all_btn = Button(
            label="✅ Select All on Page",
            style=discord.ButtonStyle.grey,
            row=1
        )
        select_all_btn.callback = self.select_all_callback
        
        clear_btn = Button(
            label="🗑️ Clear Selection",
            style=discord.ButtonStyle.grey,
            row=2
        )
        clear_btn.callback = self.clear_callback
        
        # Navigation buttons
        if len(self.playlists) > self.items_per_page:
            nav_row = 3
            prev_btn = Button(
                label="◀ Previous",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page == 0,
                row=nav_row
            )
            prev_btn.callback = self.prev_callback
            
            next_btn = Button(
                label="Next ▶",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page >= math.ceil(len(self.playlists)/self.items_per_page) - 1,
                row=nav_row
            )
            next_btn.callback = self.next_callback
            
            self.add_item(prev_btn)
            self.add_item(next_btn)
        
        back_btn = Button(
            label="🔙 Back",
            style=discord.ButtonStyle.grey,
            row=4
        )
        back_btn.callback = self.back_callback
        
        close_btn = Button(
            label="✖ Close",
            style=discord.ButtonStyle.red,
            row=4
        )
        close_btn.callback = self.close_callback
        
        self.add_item(remove_btn)
        self.add_item(select_all_btn)
        self.add_item(clear_btn)
        self.add_item(back_btn)
        self.add_item(close_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
            return
        
        self.selected_playlists = set(interaction.data['values'])
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def select_all_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        
        for i in range(start_idx, min(end_idx, len(self.playlists))):
            self.selected_playlists.add(str(i))
        
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def remove_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get selected playlists
        playlist_indices = [int(idx) for idx in self.selected_playlists]
        selected_playlists = [self.playlists[idx] for idx in playlist_indices]
        
        try:
            removed_count = 0
            failed_count = 0
            
            async with aiosqlite.connect("data/music_bot.db") as db:
                for playlist in selected_playlists:
                    try:
                        # Delete playlist (cascade will delete playlist_tracks)
                        await db.execute(
                            "DELETE FROM playlists WHERE name = ? AND user_id = ?",
                            (playlist['name'], self.author.id)
                        )
                        removed_count += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to remove playlist {playlist['name']}: {e}")
                        failed_count += 1
                
                await db.commit()
            
            # Result embed
            embed = discord.Embed(
                title="🗑️ Playlists Removed",
                color=discord.Color.green() if removed_count > 0 else discord.Color.red()
            )
            embed.add_field(name="Removed", value=str(removed_count), inline=True)
            embed.add_field(name="Failed", value=str(failed_count), inline=True)
            
            if removed_count > 0:
                # Update local list
                for idx in sorted(playlist_indices, reverse=True):
                    del self.playlists[idx]
                
                # Reset selection and page
                self.selected_playlists.clear()
                self.current_page = min(self.current_page, math.ceil(len(self.playlists)/self.items_per_page) - 1)
                self.update_view()
                
                playlist_list = "\n".join([f"• {p['name']}" for p in selected_playlists[:3]])
                if removed_count > 3:
                    playlist_list += f"\n... and {removed_count - 3} more"
                
                embed.add_field(name="Removed Playlists", value=playlist_list, inline=False)
                embed.set_footer(text="Playlists removed. Tracks remain in library.")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            await interaction.edit_original_response(view=self)
            
        except Exception as e:
            logger.error(f"Failed to remove playlists: {e}")
            embed = discord.Embed(
                title="❌ Failed to Remove Playlists",
                description=f"Error: {str(e)[:500]}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
    
    async def clear_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.selected_playlists.clear()
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def prev_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page -= 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def next_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page += 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def back_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = RemovePanelView(self.cog, self.author)
        embed = discord.Embed(
            title="🗑️ Remove Content",
            description="Select what you want to remove:",
            color=discord.Color.red()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    async def close_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== SELECT PLAYLIST TO REMOVE FROM VIEW ==========
class SelectPlaylistToRemoveFromView(View):
    """View for selecting a playlist to remove tracks from"""
    
    def __init__(self, cog, author: discord.Member, playlists: List[Dict], timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
        self.playlists = playlists
        self.selected_playlist = None
        self.current_page = 0
        self.items_per_page = 20
        
        self.update_view()
    
    def update_view(self):
        """Update the view with current page"""
        self.clear_items()
        
        # Create playlist select
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_playlists = self.playlists[start_idx:end_idx]
        
        options = []
        for i, playlist in enumerate(page_playlists):
            playlist_idx = start_idx + i
            label = playlist['name'][:90]
            description = f"{playlist.get('track_count', 0)} tracks"
            
            options.append(discord.SelectOption(
                label=label,
                description=description,
                value=str(playlist_idx),
                default=str(playlist_idx) == str(self.selected_playlist) if self.selected_playlist else False
            ))
        
        select = Select(
            placeholder=f"Select Playlist (Page {self.current_page + 1}/{math.ceil(len(self.playlists)/self.items_per_page)})",
            options=options,
            min_values=1,
            max_values=1
        )
        select.callback = self.select_callback
        self.add_item(select)
        
        # Continue button
        continue_btn = Button(
            label="➡️ Continue",
            style=discord.ButtonStyle.green,
            disabled=self.selected_playlist is None,
            row=1
        )
        continue_btn.callback = self.continue_callback
        
        # Navigation buttons
        if len(self.playlists) > self.items_per_page:
            nav_row = 2
            prev_btn = Button(
                label="◀ Previous",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page == 0,
                row=nav_row
            )
            prev_btn.callback = self.prev_callback
            
            next_btn = Button(
                label="Next ▶",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page >= math.ceil(len(self.playlists)/self.items_per_page) - 1,
                row=nav_row
            )
            next_btn.callback = self.next_callback
            
            self.add_item(prev_btn)
            self.add_item(next_btn)
        
        back_btn = Button(
            label="🔙 Back",
            style=discord.ButtonStyle.grey,
            row=3
        )
        back_btn.callback = self.back_callback
        
        close_btn = Button(
            label="✖ Close",
            style=discord.ButtonStyle.red,
            row=3
        )
        close_btn.callback = self.close_callback
        
        self.add_item(continue_btn)
        self.add_item(back_btn)
        self.add_item(close_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
            return
        
        self.selected_playlist = interaction.data['values'][0]
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def continue_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get selected playlist
        playlist_idx = int(self.selected_playlist)
        playlist = self.playlists[playlist_idx]
        
        # Get tracks from this playlist
        tracks = await self.cog.get_playlist_tracks(self.author.id, playlist['name'])
        if not tracks:
            await interaction.followup.send(f"❌ Playlist '{playlist['name']}' is empty.", ephemeral=True)
            return
        
        view = RemoveFromPlaylistView(self.cog, self.author, playlist, tracks)
        embed = discord.Embed(
            title=f"🗑️ Remove from {playlist['name']}",
            description="Select tracks to remove from this playlist:",
            color=discord.Color.orange()
        )
        embed.set_footer(text="⚠️ This will only remove tracks from the playlist, not from library")
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    async def prev_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page -= 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def next_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page += 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def back_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = RemovePanelView(self.cog, self.author)
        embed = discord.Embed(
            title="🗑️ Remove Content",
            description="Select what you want to remove:",
            color=discord.Color.red()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    async def close_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== REMOVE FROM PLAYLIST VIEW ==========
class RemoveFromPlaylistView(View):
    """View for removing tracks from a specific playlist"""
    
    def __init__(self, cog, author: discord.Member, playlist: Dict, tracks: List[Dict], timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
        self.playlist = playlist
        self.tracks = tracks
        self.selected_tracks = set()
        self.current_page = 0
        self.items_per_page = 20
        
        self.update_view()
    
    def update_view(self):
        """Update the view with current page"""
        self.clear_items()
        
        # Create track select
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_tracks = self.tracks[start_idx:end_idx]
        
        options = []
        for i, track in enumerate(page_tracks):
            track_idx = start_idx + i
            label = track['title'][:90]
            description = track.get('artist', 'Unknown Artist')[:95]
            
            options.append(discord.SelectOption(
                label=label,
                description=description,
                value=str(track_idx),
                default=str(track_idx) in self.selected_tracks
            ))
        
        select = Select(
            placeholder=f"Select Tracks to Remove (Page {self.current_page + 1}/{math.ceil(len(self.tracks)/self.items_per_page)})",
            options=options,
            min_values=0,
            max_values=len(page_tracks)
        )
        select.callback = self.select_callback
        self.add_item(select)
        
        # Action buttons
        remove_btn = Button(
            label="🗑️ Remove Selected",
            style=discord.ButtonStyle.red,
            disabled=len(self.selected_tracks) == 0,
            row=1
        )
        remove_btn.callback = self.remove_callback
        
        select_all_btn = Button(
            label="✅ Select All on Page",
            style=discord.ButtonStyle.grey,
            row=1
        )
        select_all_btn.callback = self.select_all_callback
        
        clear_btn = Button(
            label="🗑️ Clear Selection",
            style=discord.ButtonStyle.grey,
            row=2
        )
        clear_btn.callback = self.clear_callback
        
        # Navigation buttons
        if len(self.tracks) > self.items_per_page:
            nav_row = 3
            prev_btn = Button(
                label="◀ Previous",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page == 0,
                row=nav_row
            )
            prev_btn.callback = self.prev_callback
            
            next_btn = Button(
                label="Next ▶",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page >= math.ceil(len(self.tracks)/self.items_per_page) - 1,
                row=nav_row
            )
            next_btn.callback = self.next_callback
            
            self.add_item(prev_btn)
            self.add_item(next_btn)
        
        back_btn = Button(
            label="🔙 Back to Playlists",
            style=discord.ButtonStyle.grey,
            row=4
        )
        back_btn.callback = self.back_callback
        
        close_btn = Button(
            label="✖ Close",
            style=discord.ButtonStyle.red,
            row=4
        )
        close_btn.callback = self.close_callback
        
        self.add_item(remove_btn)
        self.add_item(select_all_btn)
        self.add_item(clear_btn)
        self.add_item(back_btn)
        self.add_item(close_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
            return
        
        self.selected_tracks = set(interaction.data['values'])
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def select_all_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        
        for i in range(start_idx, min(end_idx, len(self.tracks))):
            self.selected_tracks.add(str(i))
        
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def remove_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get selected tracks
        track_indices = [int(idx) for idx in self.selected_tracks]
        selected_tracks = [self.tracks[idx] for idx in track_indices]
        
        try:
            removed_count = 0
            failed_count = 0
            
            async with aiosqlite.connect("data/music_bot.db") as db:
                # Get playlist ID
                cursor = await db.execute(
                    "SELECT id FROM playlists WHERE name = ? AND user_id = ?",
                    (self.playlist['name'], self.author.id)
                )
                playlist_id = (await cursor.fetchone())[0]
                
                for track in selected_tracks:
                    try:
                        # Remove from playlist
                        await db.execute(
                            "DELETE FROM playlist_tracks WHERE playlist_id = ? AND track_filename = ?",
                            (playlist_id, track['filename'])
                        )
                        removed_count += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to remove track {track['filename']} from playlist: {e}")
                        failed_count += 1
                
                await db.commit()
            
            # Result embed
            embed = discord.Embed(
                title="🗑️ Tracks Removed from Playlist",
                color=discord.Color.green() if removed_count > 0 else discord.Color.red()
            )
            embed.add_field(name="Playlist", value=self.playlist['name'], inline=True)
            embed.add_field(name="Removed", value=str(removed_count), inline=True)
            embed.add_field(name="Failed", value=str(failed_count), inline=True)
            
            if removed_count > 0:
                # Update local list
                for idx in sorted(track_indices, reverse=True):
                    del self.tracks[idx]
                
                # Reset selection and page
                self.selected_tracks.clear()
                self.current_page = min(self.current_page, math.ceil(len(self.tracks)/self.items_per_page) - 1)
                self.update_view()
                
                track_list = "\n".join([f"• {t['title'][:50]}..." for t in selected_tracks[:3]])
                if removed_count > 3:
                    track_list += f"\n... and {removed_count - 3} more"
                
                embed.add_field(name="Removed Tracks", value=track_list, inline=False)
                embed.set_footer(text="Tracks removed from playlist only")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            await interaction.edit_original_response(view=self)
            
        except Exception as e:
            logger.error(f"Failed to remove tracks from playlist: {e}")
            embed = discord.Embed(
                title="❌ Failed to Remove Tracks",
                description=f"Error: {str(e)[:500]}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
    
    async def clear_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.selected_tracks.clear()
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def prev_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page -= 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def next_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page += 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def back_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = SelectPlaylistToRemoveFromView(self.cog, self.author, await self.cog.get_user_playlists(self.author.id))
        embed = discord.Embed(
            title="🎵 Remove from Playlist",
            description="Select a playlist to remove tracks from:",
            color=discord.Color.orange()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    async def close_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== MANAGE PANEL VIEW ==========
class ManagePanelView(View):
    """Panel for management actions (preload, unload, edit)"""
    
    def __init__(self, cog, author: discord.Member, timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
    
    @discord.ui.button(label="🔄 Preload", style=discord.ButtonStyle.green, row=0)
    async def preload_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = PreloadView(self.cog, self.author)
        embed = discord.Embed(
            title="🔄 Preload Content",
            description="Select content to preload to cache:",
            color=discord.Color.green()
        )
        embed.set_footer(text="Preloading caches files for smooth playback")
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="🗑️ Unload", style=discord.ButtonStyle.red, row=0)
    async def unload_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = UnloadView(self.cog, self.author)
        embed = discord.Embed(
            title="🗑️ Unload from Cache",
            description="Select content to remove from cache:",
            color=discord.Color.red()
        )
        embed.set_footer(text="Unloading frees up disk space")
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="✏️ Edit", style=discord.ButtonStyle.blurple, row=1)
    async def edit_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = EditView(self.cog, self.author)
        embed = discord.Embed(
            title="✏️ Edit Content",
            description="Select content to edit:",
            color=discord.Color.blue()
        )
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.grey, row=2)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = ManageMusicMainView(self.cog, self.author)
        embed = discord.Embed(
            title="🎵 Manage Music",
            description="Select an action from the dropdown:",
            color=discord.Color.blue()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    @discord.ui.button(label="✖ Close", style=discord.ButtonStyle.red, row=2)
    async def close_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== PRELOAD VIEW ==========
class PreloadView(View):
    """View for preloading content"""
    
    def __init__(self, cog, author: discord.Member, timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
    
    @discord.ui.button(label="🎵 Preload Music", style=discord.ButtonStyle.green, row=0)
    async def preload_music_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get all tracks (including cache status)
        all_tracks = await self.cog.get_all_tracks_with_cache_status()
        if not all_tracks:
            await interaction.followup.send("❌ No tracks in library.", ephemeral=True)
            return
        
        # Filter for tracks not cached
        tracks_to_preload = [t for t in all_tracks if not t.get('is_cached', False)]
        if not tracks_to_preload:
            await interaction.followup.send("✅ All tracks are already cached!", ephemeral=True)
            return
        
        view = PreloadMusicView(self.cog, self.author, tracks_to_preload)
        embed = discord.Embed(
            title="🔄 Preload Music",
            description=f"Select tracks to preload ({len(tracks_to_preload)} not cached):",
            color=discord.Color.green()
        )
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="📁 Preload Playlist", style=discord.ButtonStyle.green, row=0)
    async def preload_playlist_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get user playlists
        playlists = await self.cog.get_user_playlists(self.author.id)
        if not playlists:
            await interaction.followup.send("❌ You don't have any playlists.", ephemeral=True)
            return
        
        view = PreloadPlaylistView(self.cog, self.author, playlists)
        embed = discord.Embed(
            title="🔄 Preload Playlist",
            description="Select playlists to preload:",
            color=discord.Color.green()
        )
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.grey, row=1)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = ManagePanelView(self.cog, self.author)
        embed = discord.Embed(
            title="⚙️ Manage Content",
            description="Select management action:",
            color=discord.Color.blue()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    @discord.ui.button(label="✖ Close", style=discord.ButtonStyle.red, row=1)
    async def close_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== PRELOAD MUSIC VIEW ==========
class PreloadMusicView(View):
    """View for preloading specific music tracks"""
    
    def __init__(self, cog, author: discord.Member, tracks: List[Dict], timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
        self.tracks = tracks
        self.selected_tracks = set()
        self.current_page = 0
        self.items_per_page = 20
        
        self.update_view()
    
    def update_view(self):
        """Update the view with current page"""
        self.clear_items()
        
        # Create track select
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_tracks = self.tracks[start_idx:end_idx]
        
        options = []
        for i, track in enumerate(page_tracks):
            track_idx = start_idx + i
            label = track['title'][:90]
            description = track.get('artist', 'Unknown Artist')[:95]
            
            options.append(discord.SelectOption(
                label=label,
                description=description,
                value=str(track_idx),
                default=str(track_idx) in self.selected_tracks
            ))
        
        select = Select(
            placeholder=f"Select Tracks to Preload (Page {self.current_page + 1}/{math.ceil(len(self.tracks)/self.items_per_page)})",
            options=options,
            min_values=0,
            max_values=len(page_tracks)
        )
        select.callback = self.select_callback
        self.add_item(select)
        
        # Action buttons
        preload_btn = Button(
            label="🔄 Preload Selected",
            style=discord.ButtonStyle.green,
            disabled=len(self.selected_tracks) == 0,
            row=1
        )
        preload_btn.callback = self.preload_callback
        
        select_all_btn = Button(
            label="✅ Select All on Page",
            style=discord.ButtonStyle.grey,
            row=1
        )
        select_all_btn.callback = self.select_all_callback
        
        clear_btn = Button(
            label="🗑️ Clear Selection",
            style=discord.ButtonStyle.grey,
            row=2
        )
        clear_btn.callback = self.clear_callback
        
        # Navigation buttons
        if len(self.tracks) > self.items_per_page:
            nav_row = 3
            prev_btn = Button(
                label="◀ Previous",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page == 0,
                row=nav_row
            )
            prev_btn.callback = self.prev_callback
            
            next_btn = Button(
                label="Next ▶",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page >= math.ceil(len(self.tracks)/self.items_per_page) - 1,
                row=nav_row
            )
            next_btn.callback = self.next_callback
            
            self.add_item(prev_btn)
            self.add_item(next_btn)
        
        back_btn = Button(
            label="🔙 Back",
            style=discord.ButtonStyle.grey,
            row=4
        )
        back_btn.callback = self.back_callback
        
        close_btn = Button(
            label="✖ Close",
            style=discord.ButtonStyle.red,
            row=4
        )
        close_btn.callback = self.close_callback
        
        self.add_item(preload_btn)
        self.add_item(select_all_btn)
        self.add_item(clear_btn)
        self.add_item(back_btn)
        self.add_item(close_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
            return
        
        self.selected_tracks = set(interaction.data['values'])
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def select_all_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        
        for i in range(start_idx, min(end_idx, len(self.tracks))):
            self.selected_tracks.add(str(i))
        
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def preload_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get selected tracks
        track_indices = [int(idx) for idx in self.selected_tracks]
        selected_tracks = [self.tracks[idx] for idx in track_indices]
        
        # Create loading embed
        embed = discord.Embed(
            title="🔄 Preloading Tracks...",
            description=f"Preloading {len(selected_tracks)} tracks...",
            color=discord.Color.blue()
        )
        status_msg = await interaction.followup.send(embed=embed, ephemeral=True)
        
        try:
            # Preload tracks
            player = self.cog.get_player(interaction.guild.id)
            cached_count = 0
            failed_count = 0
            
            for i, track in enumerate(selected_tracks):
                try:
                    # Update status every 5 tracks
                    if i % 5 == 0:
                        embed.description = f"Preloading {i+1}/{len(selected_tracks)} tracks..."
                        await status_msg.edit(embed=embed)
                    
                    # Download to cache
                    cache_path = player.get_cache_path(track['filename'])
                    if not cache_path.exists():
                        # Use player's download method
                        success = await player.download_to_cache(track, update_db=True)
                        if success:
                            cached_count += 1
                        else:
                            failed_count += 1
                    else:
                        # Already cached
                        cached_count += 1
                    
                    # Small delay
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Failed to preload {track['filename']}: {e}")
                    failed_count += 1
            
            # Result embed
            embed = discord.Embed(
                title="✅ Preload Complete",
                color=discord.Color.green() if cached_count > 0 else discord.Color.red()
            )
            embed.add_field(name="Total Tracks", value=str(len(selected_tracks)), inline=True)
            embed.add_field(name="Cached", value=str(cached_count), inline=True)
            embed.add_field(name="Failed", value=str(failed_count), inline=True)
            
            if cached_count > 0:
                embed.set_footer(text=f"Added {cached_count} new tracks to cache")
            else:
                embed.set_footer(text="No new tracks were cached")
            
            await status_msg.edit(embed=embed)
            
        except Exception as e:
            logger.error(f"Preload failed: {e}")
            embed = discord.Embed(
                title="❌ Preload Failed",
                description=f"Error: {str(e)[:500]}",
                color=discord.Color.red()
            )
            await status_msg.edit(embed=embed)
    
    async def clear_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.selected_tracks.clear()
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def prev_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page -= 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def next_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page += 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def back_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = PreloadView(self.cog, self.author)
        embed = discord.Embed(
            title="🔄 Preload Content",
            description="Select content to preload to cache:",
            color=discord.Color.green()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    async def close_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== PRELOAD PLAYLIST VIEW ==========
class PreloadPlaylistView(View):
    """View for preloading playlists"""
    
    def __init__(self, cog, author: discord.Member, playlists: List[Dict], timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
        self.playlists = playlists
        self.selected_playlists = set()
        self.current_page = 0
        self.items_per_page = 20
        
        self.update_view()
    
    def update_view(self):
        """Update the view with current page"""
        self.clear_items()
        
        # Create playlist select
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_playlists = self.playlists[start_idx:end_idx]
        
        options = []
        for i, playlist in enumerate(page_playlists):
            playlist_idx = start_idx + i
            label = playlist['name'][:90]
            description = f"{playlist.get('track_count', 0)} tracks"
            
            options.append(discord.SelectOption(
                label=label,
                description=description,
                value=str(playlist_idx),
                default=str(playlist_idx) in self.selected_playlists
            ))
        
        select = Select(
            placeholder=f"Select Playlists to Preload (Page {self.current_page + 1}/{math.ceil(len(self.playlists)/self.items_per_page)})",
            options=options,
            min_values=0,
            max_values=len(page_playlists)
        )
        select.callback = self.select_callback
        self.add_item(select)
        
        # Action buttons
        preload_btn = Button(
            label="🔄 Preload Selected",
            style=discord.ButtonStyle.green,
            disabled=len(self.selected_playlists) == 0,
            row=1
        )
        preload_btn.callback = self.preload_callback
        
        select_all_btn = Button(
            label="✅ Select All on Page",
            style=discord.ButtonStyle.grey,
            row=1
        )
        select_all_btn.callback = self.select_all_callback
        
        clear_btn = Button(
            label="🗑️ Clear Selection",
            style=discord.ButtonStyle.grey,
            row=2
        )
        clear_btn.callback = self.clear_callback
        
        # Navigation buttons
        if len(self.playlists) > self.items_per_page:
            nav_row = 3
            prev_btn = Button(
                label="◀ Previous",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page == 0,
                row=nav_row
            )
            prev_btn.callback = self.prev_callback
            
            next_btn = Button(
                label="Next ▶",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page >= math.ceil(len(self.playlists)/self.items_per_page) - 1,
                row=nav_row
            )
            next_btn.callback = self.next_callback
            
            self.add_item(prev_btn)
            self.add_item(next_btn)
        
        back_btn = Button(
            label="🔙 Back",
            style=discord.ButtonStyle.grey,
            row=4
        )
        back_btn.callback = self.back_callback
        
        close_btn = Button(
            label="✖ Close",
            style=discord.ButtonStyle.red,
            row=4
        )
        close_btn.callback = self.close_callback
        
        self.add_item(preload_btn)
        self.add_item(select_all_btn)
        self.add_item(clear_btn)
        self.add_item(back_btn)
        self.add_item(close_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
            return
        
        self.selected_playlists = set(interaction.data['values'])
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def select_all_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        
        for i in range(start_idx, min(end_idx, len(self.playlists))):
            self.selected_playlists.add(str(i))
        
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def preload_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get selected playlists
        playlist_indices = [int(idx) for idx in self.selected_playlists]
        selected_playlists = [self.playlists[idx] for idx in playlist_indices]
        
        # Create loading embed
        embed = discord.Embed(
            title="🔄 Preloading Playlists...",
            description=f"Preloading {len(selected_playlists)} playlists...",
            color=discord.Color.blue()
        )
        status_msg = await interaction.followup.send(embed=embed, ephemeral=True)
        
        try:
            total_tracks = 0
            cached_count = 0
            failed_count = 0
            
            player = self.cog.get_player(interaction.guild.id)
            
            for playlist_idx, playlist in enumerate(selected_playlists):
                # Get playlist tracks
                tracks = await self.cog.get_playlist_tracks(self.author.id, playlist['name'])
                total_tracks += len(tracks)
                
                # Update status
                embed.description = f"Preloading playlist {playlist_idx+1}/{len(selected_playlists)}: {playlist['name']}"
                await status_msg.edit(embed=embed)
                
                # Preload each track
                for i, track in enumerate(tracks):
                    try:
                        cache_path = player.get_cache_path(track['filename'])
                        if not cache_path.exists():
                            success = await player.download_to_cache(track, update_db=True)
                            if success:
                                cached_count += 1
                            else:
                                failed_count += 1
                        else:
                            cached_count += 1
                        
                        # Small delay
                        await asyncio.sleep(0.3)
                        
                    except Exception as e:
                        logger.error(f"Failed to preload {track['filename']}: {e}")
                        failed_count += 1
            
            # Result embed
            embed = discord.Embed(
                title="✅ Playlists Preloaded",
                color=discord.Color.green() if cached_count > 0 else discord.Color.red()
            )
            embed.add_field(name="Playlists", value=str(len(selected_playlists)), inline=True)
            embed.add_field(name="Total Tracks", value=str(total_tracks), inline=True)
            embed.add_field(name="Newly Cached", value=str(cached_count), inline=True)
            embed.add_field(name="Failed", value=str(failed_count), inline=True)
            
            if cached_count > 0:
                playlist_names = ", ".join([p['name'] for p in selected_playlists[:3]])
                if len(selected_playlists) > 3:
                    playlist_names += f" and {len(selected_playlists)-3} more"
                embed.add_field(name="Playlists", value=playlist_names, inline=False)
            
            await status_msg.edit(embed=embed)
            
        except Exception as e:
            logger.error(f"Playlist preload failed: {e}")
            embed = discord.Embed(
                title="❌ Playlist Preload Failed",
                description=f"Error: {str(e)[:500]}",
                color=discord.Color.red()
            )
            await status_msg.edit(embed=embed)
    
    async def clear_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.selected_playlists.clear()
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def prev_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page -= 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def next_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page += 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def back_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = PreloadView(self.cog, self.author)
        embed = discord.Embed(
            title="🔄 Preload Content",
            description="Select content to preload to cache:",
            color=discord.Color.green()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    async def close_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== UNLOAD VIEW ==========
class UnloadView(View):
    """View for unloading content from cache"""
    
    def __init__(self, cog, author: discord.Member, timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
    
    @discord.ui.button(label="🎵 Unload Music", style=discord.ButtonStyle.red, row=0)
    async def unload_music_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get cached tracks only
        all_tracks = await self.cog.get_all_tracks_with_cache_status()
        cached_tracks = [t for t in all_tracks if t.get('is_cached', False)]
        
        if not cached_tracks:
            await interaction.followup.send("✅ No tracks are cached.", ephemeral=True)
            return
        
        view = UnloadMusicView(self.cog, self.author, cached_tracks)
        embed = discord.Embed(
            title="🗑️ Unload Music from Cache",
            description=f"Select tracks to unload ({len(cached_tracks)} cached):",
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="📁 Unload Playlist", style=discord.ButtonStyle.red, row=0)
    async def unload_playlist_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get user playlists
        playlists = await self.cog.get_user_playlists(self.author.id)
        if not playlists:
            await interaction.followup.send("❌ You don't have any playlists.", ephemeral=True)
            return
        
        view = UnloadPlaylistView(self.cog, self.author, playlists)
        embed = discord.Embed(
            title="🗑️ Unload Playlist from Cache",
            description="Select playlists to unload:",
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.grey, row=1)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = ManagePanelView(self.cog, self.author)
        embed = discord.Embed(
            title="⚙️ Manage Content",
            description="Select management action:",
            color=discord.Color.blue()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    @discord.ui.button(label="✖ Close", style=discord.ButtonStyle.red, row=1)
    async def close_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== UNLOAD MUSIC VIEW ==========
class UnloadMusicView(View):
    """View for unloading specific music tracks from cache"""
    
    def __init__(self, cog, author: discord.Member, tracks: List[Dict], timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
        self.tracks = tracks
        self.selected_tracks = set()
        self.current_page = 0
        self.items_per_page = 20
        
        self.update_view()
    
    def update_view(self):
        """Update the view with current page"""
        self.clear_items()
        
        # Create track select
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_tracks = self.tracks[start_idx:end_idx]
        
        options = []
        for i, track in enumerate(page_tracks):
            track_idx = start_idx + i
            label = track['title'][:90]
            description = track.get('artist', 'Unknown Artist')[:95]
            
            options.append(discord.SelectOption(
                label=label,
                description=description,
                value=str(track_idx),
                default=str(track_idx) in self.selected_tracks
            ))
        
        select = Select(
            placeholder=f"Select Tracks to Unload (Page {self.current_page + 1}/{math.ceil(len(self.tracks)/self.items_per_page)})",
            options=options,
            min_values=0,
            max_values=len(page_tracks)
        )
        select.callback = self.select_callback
        self.add_item(select)
        
        # Action buttons
        unload_btn = Button(
            label="🗑️ Unload Selected",
            style=discord.ButtonStyle.red,
            disabled=len(self.selected_tracks) == 0,
            row=1
        )
        unload_btn.callback = self.unload_callback
        
        select_all_btn = Button(
            label="✅ Select All on Page",
            style=discord.ButtonStyle.grey,
            row=1
        )
        select_all_btn.callback = self.select_all_callback
        
        clear_btn = Button(
            label="🗑️ Clear Selection",
            style=discord.ButtonStyle.grey,
            row=2
        )
        clear_btn.callback = self.clear_callback
        
        # Navigation buttons
        if len(self.tracks) > self.items_per_page:
            nav_row = 3
            prev_btn = Button(
                label="◀ Previous",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page == 0,
                row=nav_row
            )
            prev_btn.callback = self.prev_callback
            
            next_btn = Button(
                label="Next ▶",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page >= math.ceil(len(self.tracks)/self.items_per_page) - 1,
                row=nav_row
            )
            next_btn.callback = self.next_callback
            
            self.add_item(prev_btn)
            self.add_item(next_btn)
        
        back_btn = Button(
            label="🔙 Back",
            style=discord.ButtonStyle.grey,
            row=4
        )
        back_btn.callback = self.back_callback
        
        close_btn = Button(
            label="✖ Close",
            style=discord.ButtonStyle.red,
            row=4
        )
        close_btn.callback = self.close_callback
        
        self.add_item(unload_btn)
        self.add_item(select_all_btn)
        self.add_item(clear_btn)
        self.add_item(back_btn)
        self.add_item(close_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
            return
        
        self.selected_tracks = set(interaction.data['values'])
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def select_all_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        
        for i in range(start_idx, min(end_idx, len(self.tracks))):
            self.selected_tracks.add(str(i))
        
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def unload_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get selected tracks
        track_indices = [int(idx) for idx in self.selected_tracks]
        selected_tracks = [self.tracks[idx] for idx in track_indices]
        
        # Create loading embed
        embed = discord.Embed(
            title="🗑️ Unloading Tracks...",
            description=f"Unloading {len(selected_tracks)} tracks from cache...",
            color=discord.Color.orange()
        )
        status_msg = await interaction.followup.send(embed=embed, ephemeral=True)
        
        try:
            unloaded_count = 0
            failed_count = 0
            freed_bytes = 0
            
            player = self.cog.get_player(interaction.guild.id)
            
            for i, track in enumerate(selected_tracks):
                try:
                    # Get cache path
                    cache_path = player.get_cache_path(track['filename'])
                    
                    if cache_path.exists():
                        # Get file size before deletion
                        file_size = cache_path.stat().st_size
                        
                        # Delete file
                        cache_path.unlink()
                        
                        # Update database
                        async with aiosqlite.connect("data/music_bot.db") as db:
                            await db.execute(
                                "UPDATE track_stats SET is_cached = 0, cache_path = NULL WHERE filename = ?",
                                (track['filename'],)
                            )
                            await db.commit()
                        
                        unloaded_count += 1
                        freed_bytes += file_size
                    
                    # Update status every 5 tracks
                    if i % 5 == 0:
                        embed.description = f"Unloading {i+1}/{len(selected_tracks)} tracks..."
                        await status_msg.edit(embed=embed)
                    
                except Exception as e:
                    logger.error(f"Failed to unload {track['filename']}: {e}")
                    failed_count += 1
            
            # Result embed
            embed = discord.Embed(
                title="✅ Unload Complete",
                color=discord.Color.green() if unloaded_count > 0 else discord.Color.red()
            )
            embed.add_field(name="Total Selected", value=str(len(selected_tracks)), inline=True)
            embed.add_field(name="Unloaded", value=str(unloaded_count), inline=True)
            embed.add_field(name="Failed", value=str(failed_count), inline=True)
            embed.add_field(name="Freed Space", value=f"{freed_bytes/1024/1024:.2f} MB", inline=True)
            
            if unloaded_count > 0:
                embed.set_footer(text=f"Freed {freed_bytes/1024/1024:.2f} MB of disk space")
            
            await status_msg.edit(embed=embed)
            
        except Exception as e:
            logger.error(f"Unload failed: {e}")
            embed = discord.Embed(
                title="❌ Unload Failed",
                description=f"Error: {str(e)[:500]}",
                color=discord.Color.red()
            )
            await status_msg.edit(embed=embed)
    
    async def clear_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.selected_tracks.clear()
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def prev_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page -= 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def next_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page += 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def back_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = UnloadView(self.cog, self.author)
        embed = discord.Embed(
            title="🗑️ Unload from Cache",
            description="Select content to remove from cache:",
            color=discord.Color.red()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    async def close_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== UNLOAD PLAYLIST VIEW ==========
class UnloadPlaylistView(View):
    """View for unloading playlists from cache"""
    
    def __init__(self, cog, author: discord.Member, playlists: List[Dict], timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
        self.playlists = playlists
        self.selected_playlists = set()
        self.current_page = 0
        self.items_per_page = 20
        
        self.update_view()
    
    def update_view(self):
        """Update the view with current page"""
        self.clear_items()
        
        # Create playlist select
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_playlists = self.playlists[start_idx:end_idx]
        
        options = []
        for i, playlist in enumerate(page_playlists):
            playlist_idx = start_idx + i
            label = playlist['name'][:90]
            description = f"{playlist.get('track_count', 0)} tracks"
            
            options.append(discord.SelectOption(
                label=label,
                description=description,
                value=str(playlist_idx),
                default=str(playlist_idx) in self.selected_playlists
            ))
        
        select = Select(
            placeholder=f"Select Playlists to Unload (Page {self.current_page + 1}/{math.ceil(len(self.playlists)/self.items_per_page)})",
            options=options,
            min_values=0,
            max_values=len(page_playlists)
        )
        select.callback = self.select_callback
        self.add_item(select)
        
        # Action buttons
        unload_btn = Button(
            label="🗑️ Unload Selected",
            style=discord.ButtonStyle.red,
            disabled=len(self.selected_playlists) == 0,
            row=1
        )
        unload_btn.callback = self.unload_callback
        
        select_all_btn = Button(
            label="✅ Select All on Page",
            style=discord.ButtonStyle.grey,
            row=1
        )
        select_all_btn.callback = self.select_all_callback
        
        clear_btn = Button(
            label="🗑️ Clear Selection",
            style=discord.ButtonStyle.grey,
            row=2
        )
        clear_btn.callback = self.clear_callback
        
        # Navigation buttons
        if len(self.playlists) > self.items_per_page:
            nav_row = 3
            prev_btn = Button(
                label="◀ Previous",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page == 0,
                row=nav_row
            )
            prev_btn.callback = self.prev_callback
            
            next_btn = Button(
                label="Next ▶",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page >= math.ceil(len(self.playlists)/self.items_per_page) - 1,
                row=nav_row
            )
            next_btn.callback = self.next_callback
            
            self.add_item(prev_btn)
            self.add_item(next_btn)
        
        back_btn = Button(
            label="🔙 Back",
            style=discord.ButtonStyle.grey,
            row=4
        )
        back_btn.callback = self.back_callback
        
        close_btn = Button(
            label="✖ Close",
            style=discord.ButtonStyle.red,
            row=4
        )
        close_btn.callback = self.close_callback
        
        self.add_item(unload_btn)
        self.add_item(select_all_btn)
        self.add_item(clear_btn)
        self.add_item(back_btn)
        self.add_item(close_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
            return
        
        self.selected_playlists = set(interaction.data['values'])
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def select_all_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        
        for i in range(start_idx, min(end_idx, len(self.playlists))):
            self.selected_playlists.add(str(i))
        
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def unload_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get selected playlists
        playlist_indices = [int(idx) for idx in self.selected_playlists]
        selected_playlists = [self.playlists[idx] for idx in playlist_indices]
        
        # Create loading embed
        embed = discord.Embed(
            title="🗑️ Unloading Playlists...",
            description=f"Unloading {len(selected_playlists)} playlists from cache...",
            color=discord.Color.orange()
        )
        status_msg = await interaction.followup.send(embed=embed, ephemeral=True)
        
        try:
            total_unloaded = 0
            failed_count = 0
            freed_bytes = 0
            
            player = self.cog.get_player(interaction.guild.id)
            
            for playlist_idx, playlist in enumerate(selected_playlists):
                # Get playlist tracks
                tracks = await self.cog.get_playlist_tracks(self.author.id, playlist['name'])
                
                # Update status
                embed.description = f"Unloading playlist {playlist_idx+1}/{len(selected_playlists)}: {playlist['name']}"
                await status_msg.edit(embed=embed)
                
                # Unload each track
                for track in tracks:
                    try:
                        cache_path = player.get_cache_path(track['filename'])
                        
                        if cache_path.exists():
                            # Get file size before deletion
                            file_size = cache_path.stat().st_size
                            
                            # Delete file
                            cache_path.unlink()
                            
                            # Update database
                            async with aiosqlite.connect("data/music_bot.db") as db:
                                await db.execute(
                                    "UPDATE track_stats SET is_cached = 0, cache_path = NULL WHERE filename = ?",
                                    (track['filename'],)
                                )
                                await db.commit()
                            
                            total_unloaded += 1
                            freed_bytes += file_size
                        
                    except Exception as e:
                        logger.error(f"Failed to unload {track['filename']}: {e}")
                        failed_count += 1
            
            # Result embed
            embed = discord.Embed(
                title="✅ Playlists Unloaded",
                color=discord.Color.green() if total_unloaded > 0 else discord.Color.red()
            )
            embed.add_field(name="Playlists", value=str(len(selected_playlists)), inline=True)
            embed.add_field(name="Tracks Unloaded", value=str(total_unloaded), inline=True)
            embed.add_field(name="Failed", value=str(failed_count), inline=True)
            embed.add_field(name="Freed Space", value=f"{freed_bytes/1024/1024:.2f} MB", inline=True)
            
            if total_unloaded > 0:
                playlist_names = ", ".join([p['name'] for p in selected_playlists[:3]])
                if len(selected_playlists) > 3:
                    playlist_names += f" and {len(selected_playlists)-3} more"
                embed.add_field(name="Playlists", value=playlist_names, inline=False)
            
            await status_msg.edit(embed=embed)
            
        except Exception as e:
            logger.error(f"Playlist unload failed: {e}")
            embed = discord.Embed(
                title="❌ Playlist Unload Failed",
                description=f"Error: {str(e)[:500]}",
                color=discord.Color.red()
            )
            await status_msg.edit(embed=embed)
    
    async def clear_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.selected_playlists.clear()
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def prev_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page -= 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def next_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page += 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def back_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = UnloadView(self.cog, self.author)
        embed = discord.Embed(
            title="🗑️ Unload from Cache",
            description="Select content to remove from cache:",
            color=discord.Color.red()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    async def close_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== EDIT VIEW ==========
class EditView(View):
    """View for editing content"""
    
    def __init__(self, cog, author: discord.Member, timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
    
    @discord.ui.button(label="🎵 Edit Music", style=discord.ButtonStyle.blurple, row=0)
    async def edit_music_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get all tracks
        tracks = await self.cog.get_all_tracks()
        if not tracks:
            await interaction.followup.send("❌ No tracks in library.", ephemeral=True)
            return
        
        view = SelectMusicToEditView(self.cog, self.author, tracks)
        embed = discord.Embed(
            title="✏️ Edit Music",
            description="Select a track to edit:",
            color=discord.Color.blue()
        )
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="📁 Edit Playlist", style=discord.ButtonStyle.blurple, row=0)
    async def edit_playlist_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Get user playlists
        playlists = await self.cog.get_user_playlists(self.author.id)
        if not playlists:
            await interaction.followup.send("❌ You don't have any playlists.", ephemeral=True)
            return
        
        view = SelectPlaylistToEditView(self.cog, self.author, playlists)
        embed = discord.Embed(
            title="✏️ Edit Playlist",
            description="Select a playlist to edit:",
            color=discord.Color.blue()
        )
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.grey, row=1)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = ManagePanelView(self.cog, self.author)
        embed = discord.Embed(
            title="⚙️ Manage Content",
            description="Select management action:",
            color=discord.Color.blue()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    @discord.ui.button(label="✖ Close", style=discord.ButtonStyle.red, row=1)
    async def close_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== SELECT MUSIC TO EDIT VIEW ==========
class SelectMusicToEditView(View):
    """View for selecting music to edit"""
    
    def __init__(self, cog, author: discord.Member, tracks: List[Dict], timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
        self.tracks = tracks
        self.selected_track = None
        self.current_page = 0
        self.items_per_page = 20
        
        self.update_view()
    
    def update_view(self):
        """Update the view with current page"""
        self.clear_items()
        
        # Create track select
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_tracks = self.tracks[start_idx:end_idx]
        
        options = []
        for i, track in enumerate(page_tracks):
            track_idx = start_idx + i
            label = track['title'][:90]
            description = track.get('artist', 'Unknown Artist')[:95]
            
            options.append(discord.SelectOption(
                label=label,
                description=description,
                value=str(track_idx),
                default=str(track_idx) == str(self.selected_track) if self.selected_track else False
            ))
        
        select = Select(
            placeholder=f"Select Track to Edit (Page {self.current_page + 1}/{math.ceil(len(self.tracks)/self.items_per_page)})",
            options=options,
            min_values=1,
            max_values=1
        )
        select.callback = self.select_callback
        self.add_item(select)
        
        # Edit button
        edit_btn = Button(
            label="✏️ Edit Selected",
            style=discord.ButtonStyle.blurple,
            disabled=self.selected_track is None,
            row=1
        )
        edit_btn.callback = self.edit_callback
        
        # Navigation buttons
        if len(self.tracks) > self.items_per_page:
            nav_row = 2
            prev_btn = Button(
                label="◀ Previous",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page == 0,
                row=nav_row
            )
            prev_btn.callback = self.prev_callback
            
            next_btn = Button(
                label="Next ▶",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page >= math.ceil(len(self.tracks)/self.items_per_page) - 1,
                row=nav_row
            )
            next_btn.callback = self.next_callback
            
            self.add_item(prev_btn)
            self.add_item(next_btn)
        
        back_btn = Button(
            label="🔙 Back",
            style=discord.ButtonStyle.grey,
            row=3
        )
        back_btn.callback = self.back_callback
        
        close_btn = Button(
            label="✖ Close",
            style=discord.ButtonStyle.red,
            row=3
        )
        close_btn.callback = self.close_callback
        
        self.add_item(edit_btn)
        self.add_item(back_btn)
        self.add_item(close_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
            return
        
        self.selected_track = interaction.data['values'][0]
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def edit_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        # Get selected track
        track_idx = int(self.selected_track)
        track = self.tracks[track_idx]
        
        # Open edit modal
        modal = EditMusicModal(self.cog, track)
        await interaction.response.send_modal(modal)
    
    async def prev_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page -= 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def next_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page += 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def back_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = EditView(self.cog, self.author)
        embed = discord.Embed(
            title="✏️ Edit Content",
            description="Select content to edit:",
            color=discord.Color.blue()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    async def close_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== EDIT MUSIC MODAL ==========
class EditMusicModal(Modal, title="✏️ Edit Music"):
    """Modal for editing music metadata"""
    
    def __init__(self, cog, track: Dict):
        super().__init__(timeout=None)
        self.cog = cog
        self.track = track
        
        self.title_input = TextInput(
            label="Track Title",
            default=track['title'],
            required=True,
            max_length=200
        )
        
        self.artist_input = TextInput(
            label="Artist",
            default=track.get('artist', ''),
            required=True,
            max_length=200
        )
        
        self.genre_input = TextInput(
            label="Genre",
            default=track.get('genre', ''),
            required=False,
            max_length=100
        )
        
        self.add_item(self.title_input)
        self.add_item(self.artist_input)
        self.add_item(self.genre_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        new_title = self.title_input.value.strip()
        new_artist = self.artist_input.value.strip()
        new_genre = self.genre_input.value.strip() or "Unknown"
        
        # Generate new filename
        safe_title = re.sub(r'[^\w\s\-\.\(\)\[\]]', '', new_title)
        safe_artist = re.sub(r'[^\w\s\-\.\(\)\[\]]', '', new_artist)
        new_filename = f"{safe_artist} - {safe_title}.mp3"
        
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                # Check if new filename would conflict
                if new_filename != self.track['filename']:
                    cursor = await db.execute(
                        "SELECT filename FROM track_stats WHERE filename = ?",
                        (new_filename,)
                    )
                    conflict = await cursor.fetchone()
                    
                    if conflict:
                        embed = discord.Embed(
                            title="❌ Filename Conflict",
                            description=f"A track with filename '{new_filename}' already exists.",
                            color=discord.Color.red()
                        )
                        await interaction.followup.send(embed=embed, ephemeral=True)
                        return
                
                # Update database
                await db.execute('''
                    UPDATE track_stats 
                    SET title = ?, artist = ?, genre = ?, filename = ?
                    WHERE filename = ?
                ''', (
                    new_title,
                    new_artist,
                    new_genre,
                    new_filename,
                    self.track['filename']
                ))
                
                # Update playlist references if filename changed
                if new_filename != self.track['filename']:
                    await db.execute(
                        "UPDATE playlist_tracks SET track_filename = ? WHERE track_filename = ?",
                        (new_filename, self.track['filename'])
                    )
                    
                    # Rename cache file if exists
                    old_cache_path = Path("data/music_cache") / re.sub(r'[<>:"/\\|?*]', '_', self.track['filename'])[:200]
                    new_cache_path = Path("data/music_cache") / re.sub(r'[<>:"/\\|?*]', '_', new_filename)[:200]
                    
                    if old_cache_path.exists():
                        old_cache_path.rename(new_cache_path)
                
                await db.commit()
            
            # Update index
            await self.cog.update_track_index()
            
            # Success embed
            embed = discord.Embed(
                title="✅ Track Updated",
                description=f"**{self.track['title']}** → **{new_title}**",
                color=discord.Color.green()
            )
            embed.add_field(name="Artist", value=f"{self.track.get('artist', 'Unknown')} → {new_artist}", inline=True)
            embed.add_field(name="Genre", value=f"{self.track.get('genre', 'Unknown')} → {new_genre}", inline=True)
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Failed to edit track: {e}")
            embed = discord.Embed(
                title="❌ Failed to Update Track",
                description=f"Error: {str(e)[:500]}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

# ========== SELECT PLAYLIST TO EDIT VIEW ==========
class SelectPlaylistToEditView(View):
    """View for selecting a playlist to edit"""
    
    def __init__(self, cog, author: discord.Member, playlists: List[Dict], timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.author = author
        self.playlists = playlists
        self.selected_playlist = None
        self.current_page = 0
        self.items_per_page = 20
        
        self.update_view()
    
    def update_view(self):
        """Update the view with current page"""
        self.clear_items()
        
        # Create playlist select
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_playlists = self.playlists[start_idx:end_idx]
        
        options = []
        for i, playlist in enumerate(page_playlists):
            playlist_idx = start_idx + i
            label = playlist['name'][:90]
            description = f"{playlist.get('track_count', 0)} tracks"
            
            options.append(discord.SelectOption(
                label=label,
                description=description,
                value=str(playlist_idx),
                default=str(playlist_idx) == str(self.selected_playlist) if self.selected_playlist else False
            ))
        
        select = Select(
            placeholder=f"Select Playlist to Edit (Page {self.current_page + 1}/{math.ceil(len(self.playlists)/self.items_per_page)})",
            options=options,
            min_values=1,
            max_values=1
        )
        select.callback = self.select_callback
        self.add_item(select)
        
        # Edit button
        edit_btn = Button(
            label="✏️ Edit Selected",
            style=discord.ButtonStyle.blurple,
            disabled=self.selected_playlist is None,
            row=1
        )
        edit_btn.callback = self.edit_callback
        
        # Navigation buttons
        if len(self.playlists) > self.items_per_page:
            nav_row = 2
            prev_btn = Button(
                label="◀ Previous",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page == 0,
                row=nav_row
            )
            prev_btn.callback = self.prev_callback
            
            next_btn = Button(
                label="Next ▶",
                style=discord.ButtonStyle.grey,
                disabled=self.current_page >= math.ceil(len(self.playlists)/self.items_per_page) - 1,
                row=nav_row
            )
            next_btn.callback = self.next_callback
            
            self.add_item(prev_btn)
            self.add_item(next_btn)
        
        back_btn = Button(
            label="🔙 Back",
            style=discord.ButtonStyle.grey,
            row=3
        )
        back_btn.callback = self.back_callback
        
        close_btn = Button(
            label="✖ Close",
            style=discord.ButtonStyle.red,
            row=3
        )
        close_btn.callback = self.close_callback
        
        self.add_item(edit_btn)
        self.add_item(back_btn)
        self.add_item(close_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this menu!", ephemeral=True)
            return
        
        self.selected_playlist = interaction.data['values'][0]
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def edit_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        # Get selected playlist
        playlist_idx = int(self.selected_playlist)
        playlist = self.playlists[playlist_idx]
        
        # Open edit modal
        modal = EditPlaylistModal(self.cog, playlist, self.author)
        await interaction.response.send_modal(modal)
    
    async def prev_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page -= 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def next_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        self.current_page += 1
        self.update_view()
        await interaction.response.edit_message(view=self)
    
    async def back_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        
        await interaction.response.defer()
        view = EditView(self.cog, self.author)
        embed = discord.Embed(
            title="✏️ Edit Content",
            description="Select content to edit:",
            color=discord.Color.blue()
        )
        await interaction.edit_original_response(embed=embed, view=view)
    
    async def close_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
            return
        await interaction.message.delete()

# ========== EDIT PLAYLIST MODAL ==========
class EditPlaylistModal(Modal, title="✏️ Edit Playlist"):
    """Modal for editing playlist metadata"""
    
    def __init__(self, cog, playlist: Dict, author: discord.Member):
        super().__init__(timeout=None)
        self.cog = cog
        self.playlist = playlist
        self.author = author
        
        self.name_input = TextInput(
            label="Playlist Name",
            default=playlist['name'],
            required=True,
            max_length=100
        )
        
        self.description_input = TextInput(
            label="Description",
            default=playlist.get('description', ''),
            required=False,
            max_length=200,
            style=discord.TextStyle.long
        )
        
        self.add_item(self.name_input)
        self.add_item(self.description_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        new_name = self.name_input.value.strip()
        new_description = self.description_input.value.strip()
        
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                # Check if new name conflicts with existing playlist (excluding current)
                cursor = await db.execute(
                    "SELECT id FROM playlists WHERE name = ? AND user_id = ? AND name != ?",
                    (new_name, self.author.id, self.playlist['name'])
                )
                conflict = await cursor.fetchone()
                
                if conflict:
                    embed = discord.Embed(
                        title="❌ Playlist Name Conflict",
                        description=f"You already have a playlist named '{new_name}'.",
                        color=discord.Color.red()
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
                
                # Update playlist
                await db.execute('''
                    UPDATE playlists 
                    SET name = ?, description = ?
                    WHERE name = ? AND user_id = ?
                ''', (
                    new_name,
                    new_description,
                    self.playlist['name'],
                    self.author.id
                ))
                
                await db.commit()
            
            # Success embed
            embed = discord.Embed(
                title="✅ Playlist Updated",
                description=f"**{self.playlist['name']}** → **{new_name}**",
                color=discord.Color.green()
            )
            
            if new_description:
                embed.add_field(name="Description", value=new_description, inline=False)
            
            embed.set_footer(text=f"Owner: {self.author.display_name}")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Failed to edit playlist: {e}")
            embed = discord.Embed(
                title="❌ Failed to Update Playlist",
                description=f"Error: {str(e)[:500]}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

# ========== ENHANCED MUSIC PLAYER WITH NEW UI ==========
class EnhancedMusicPlayer(MusicPlayer):
    """Enhanced Music Player with Premium UI Integration"""
    
    def __init__(self, bot, guild_id: int):
        super().__init__(bot, guild_id)
        self.now_playing_view: Optional[PremiumNowPlayingView] = None
        self.auto_update_task: Optional[asyncio.Task] = None
        self.last_embed_update = 0
        self.update_interval = 5  # Update embed every 5 seconds
    
    async def play_track(self, track: Dict, interaction: Optional[discord.Interaction] = None):
        """Play a track with premium UI"""
        try:
            # Add current track to history
            if self.current_track:
                self.history.append(self.current_track)
                if len(self.history) > self.max_history_size:
                    self.history.pop(0)
            
            # Clear previous now playing view
            if self.now_playing_view:
                self.now_playing_view.stop()
                self.now_playing_view = None
            
            # Check cache and download if needed
            if not self.is_cached(track['filename']):
                if interaction and self.current_channel:
                    embed = discord.Embed(
                        title="⏳ Downloading...",
                        description=f"**{track['title']}** by {track.get('artist', 'Unknown')}",
                        color=discord.Color.blue()
                    )
                    embed.add_field(name="Status", value="Downloading to cache for smooth playback...", inline=False)
                    self.loading_message = await self.current_channel.send(embed=embed)
                
                cache_path = await self.download_to_cache(track)
                if not cache_path:
                    if self.loading_message:
                        try:
                            await self.loading_message.delete()
                        except:
                            pass
                        self.loading_message = None
                    
                    if interaction:
                        await interaction.followup.send(f"❌ Failed to download: {track['title']}", ephemeral=True)
                    return
                
                if self.loading_message:
                    try:
                        await self.loading_message.delete()
                    except:
                        pass
                    self.loading_message = None
            
            # Update play stats
            await self.update_play_stats(track['filename'])
            
            # Set current track
            self.current_track = track
            self.is_playing = True
            self.is_paused = False
            
            # Create audio source with extended format support
            cache_path = self.get_cache_path(track['filename'])
            
            # Check file extension for appropriate options
            file_ext = cache_path.suffix.lower()
            
            # Basic FFmpeg options that work for most formats
            options = f'-vn -af volume={self.volume}'
            
            # Special handling for certain formats
            if file_ext in ['.flac', '.wav', '.alac']:
                # Lossless formats
                options = f'-vn -acodec pcm_s16le -af volume={self.volume}'
            elif file_ext in ['.m4a', '.mp4']:
                # AAC/MP4 formats
                options = f'-vn -c:a aac -af volume={self.volume}'
            
            audio_source = discord.FFmpegPCMAudio(
                str(cache_path),
                options=options
            )
            
            # Set volume
            audio_source.volume = self.volume
            
            # Play with after callback
            def after_callback(error):
                if error:
                    logger.error(f"Playback error: {error}")
                
                # Schedule async after_track method
                asyncio.run_coroutine_threadsafe(self._after_track_async(error, track), self.loop)
            
            self.voice_client.play(audio_source, after=after_callback)
            
            # Create and send premium now playing embed
            if interaction:
                await self.send_premium_now_playing(interaction, track)
            
            logger.info(f"Now playing: {track['title']} by {track.get('artist', 'Unknown')}")
            
            # Start background preloading
            if self.queue:
                asyncio.create_task(self._preload_queue_background())
            
        except Exception as e:
            logger.error(f"Play error: {e}")
            if interaction:
                await interaction.followup.send(f"❌ Error playing track: {str(e)[:200]}", ephemeral=True)
    
    async def send_premium_now_playing(self, interaction: discord.Interaction, track: Dict):
        """Send premium now playing embed with auto-updating view"""
        embed = await self._create_premium_embed(track)
        
        # Create premium view
        self.now_playing_view = PremiumNowPlayingView(self)
        
        # Send or update message
        if self.now_playing_message:
            try:
                await self.now_playing_message.delete()
            except:
                pass
        
        self.now_playing_message = await interaction.followup.send(embed=embed, view=self.now_playing_view)
        
        # Start auto-updates
        await self.now_playing_view.start_updates(self.now_playing_message)
    
    async def _create_premium_embed(self, track: Dict) -> discord.Embed:
        """Create premium now playing embed with progress bar"""
        embed = discord.Embed(
            title=f"🎵 **{track['title'][:100]}**",
            description=f"👤 **{track.get('artist', 'Unknown Artist')}**",
            color=discord.Color.green()
        )
        
        # File info
        if track.get('filename'):
            file_ext = Path(track['filename']).suffix.upper()
            embed.add_field(name="Format", value=file_ext, inline=True)
        
        # Cache status
        cache_status = "✅ Cached" if self.is_cached(track['filename']) else "⏳ Streaming"
        embed.add_field(name="Cache", value=cache_status, inline=True)
        
        # Playback info
        status = "▶️ Playing" if self.is_playing else "⏸️ Paused"
        embed.add_field(name="Status", value=status, inline=True)
        
        # Volume
        embed.add_field(name="Volume", value=f"{int(self.volume * 100)}%", inline=True)
        
        # Loop mode
        loop_modes = {'off': '❌', 'track': '🔂', 'queue': '🔁'}
        embed.add_field(name="Loop", value=loop_modes.get(self.loop_mode, '❌'), inline=True)
        
        # Queue info
        if self.queue:
            next_tracks = []
            for i, t in enumerate(self.queue[:2], 1):
                cache_icon = "✅" if self.is_cached(t['filename']) else "⏳"
                next_tracks.append(f"`{i}.` {cache_icon} {t['title'][:30]}...")
            
            if next_tracks:
                embed.add_field(
                    name="Up Next",
                    value="\n".join(next_tracks),
                    inline=False
                )
        
        # History info
        if self.history:
            embed.add_field(
                name="History",
                value=f"{len(self.history)} previous tracks",
                inline=True
            )
        
        # Footer with queue count
        embed.set_footer(text=f"🔢 Queue: {len(self.queue)} track(s) | 📜 History: {len(self.history)}")
        
        return embed
    
    async def show_queue(self, interaction: discord.Interaction):
        """Show queue with premium UI"""
        if not self.queue and not self.current_track:
            embed = discord.Embed(
                title="Queue is Empty",
                description="Add tracks with `/play`",
                color=discord.Color.blue()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        
        embed = discord.Embed(
            title="📋 Music Queue",
            color=discord.Color.blue()
        )
        
        # Current track
        if self.current_track:
            status = "▶️ Playing" if self.is_playing else "⏸️ Paused"
            cache_icon = "✅" if self.is_cached(self.current_track['filename']) else "⏳"
            
            embed.add_field(
                name=f"{status}",
                value=f"{cache_icon} **{self.current_track['title']}** by {self.current_track.get('artist', 'Unknown')}",
                inline=False
            )
        
        # Queue
        if self.queue:
            queue_text = ""
            for i, track in enumerate(self.queue[:10], 1):
                cache_icon = "✅" if self.is_cached(track['filename']) else "⏳"
                duration = track.get('duration', 'Unknown')
                queue_text += f"`{i}.` {cache_icon} **{track['title'][:40]}** - {track.get('artist', 'Unknown')[:20]} ({duration})\n"
            
            if len(self.queue) > 10:
                queue_text += f"\n... and {len(self.queue) - 10} more tracks"
            
            embed.add_field(name="Up Next", value=queue_text, inline=False)
        
        # Playback info
        footer_text = f"Volume: {int(self.volume * 100)}% | Loop: {self.loop_mode}"
        embed.set_footer(text=footer_text)
        
        # Create queue controls
        view = PremiumQueueControls(self)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    async def _after_track_async(self, error, track):
        """Async callback after track finishes"""
        if error:
            logger.error(f"Playback error: {error}")
        
        # Clean up old now playing view
        if self.now_playing_view:
            self.now_playing_view.stop()
            self.now_playing_view = None
        
        # Handle loop modes
        if self.loop_mode == 'track' and track:
            await asyncio.sleep(1)
            await self.play_track(track)
        elif self.loop_mode == 'queue' and track:
            self.queue.append(track)
            await self.play_next()
        else:
            await self.play_next()
    
    async def play_next(self):
        """Play next track in queue with premium UI"""
        if not self.queue:
            self.is_playing = False
            self.current_track = None
            
            # Clean up now playing view
            if self.now_playing_view:
                self.now_playing_view.stop()
                self.now_playing_view = None
            
            # Send completion message
            if self.current_channel:
                embed = discord.Embed(
                    title="Queue Complete",
                    description="The queue has finished playing.",
                    color=discord.Color.blue()
                )
                await self.current_channel.send(embed=embed)
            
            return
        
        # Get next track
        next_track = self.queue.pop(0)
        
        # Use a dummy interaction to trigger the premium UI
        # In practice, this would be called from a button or command
        await self.play_track(next_track)
    
    async def play_previous(self, interaction: Optional[discord.Interaction] = None) -> bool:
        """Play previous track from history with premium UI"""
        if not self.history:
            if interaction:
                await interaction.response.send_message("No previous tracks in history", ephemeral=True)
            return False
        
        # Get last track from history
        previous_track = self.history.pop()
        
        # Add current track to front of queue if exists
        if self.current_track:
            self.queue.insert(0, self.current_track)
        
        # Play previous track
        if interaction:
            await self.play_track(previous_track, interaction)
        else:
            await self.play_track(previous_track)
        
        return True

# ========== PREMIUM QUEUE CONTROLS ==========
class PremiumQueueControls(View):
    """Premium queue controls with removal options"""
    
    def __init__(self, player: EnhancedMusicPlayer):
        super().__init__(timeout=180)
        self.player = player
    
    @discord.ui.button(label="🗑️ Remove", style=discord.ButtonStyle.red, row=0)
    async def remove_button(self, interaction: discord.Interaction, button: Button):
        """Open modal to remove tracks"""
        modal = PremiumRemoveTracksModal(self.player)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="🔀 Shuffle", style=discord.ButtonStyle.grey, row=0)
    async def shuffle_button(self, interaction: discord.Interaction, button: Button):
        """Shuffle queue"""
        if len(self.player.queue) < 2:
            await interaction.response.send_message("Need at least 2 tracks to shuffle", ephemeral=True)
            return
        
        random.shuffle(self.player.queue)
        await interaction.response.send_message("🔀 Queue shuffled", ephemeral=True)
    
    @discord.ui.button(label="🗑️ Clear", style=discord.ButtonStyle.red, row=0)
    async def clear_button(self, interaction: discord.Interaction, button: Button):
        """Clear entire queue"""
        if not self.player.queue:
            await interaction.response.send_message("Queue is already empty", ephemeral=True)
            return
        
        self.player.queue.clear()
        await interaction.response.send_message("🗑️ Queue cleared", ephemeral=True)
    
    @discord.ui.button(label="🔙 Back", style=discord.ButtonStyle.grey, row=1)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        """Go back to now playing"""
        await interaction.response.defer()
        await interaction.delete_original_response()
    
    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.grey, row=1)
    async def refresh_button(self, interaction: discord.Interaction, button: Button):
        """Refresh queue view"""
        await interaction.response.defer()
        
        embed = discord.Embed(
            title="📋 Music Queue",
            color=discord.Color.blue()
        )
        
        # Current track
        if self.player.current_track:
            status = "▶️ Playing" if self.player.is_playing else "⏸️ Paused"
            cache_icon = "✅" if self.player.is_cached(self.player.current_track['filename']) else "⏳"
            
            embed.add_field(
                name=f"{status}",
                value=f"{cache_icon} **{self.player.current_track['title']}** by {self.player.current_track.get('artist', 'Unknown')}",
                inline=False
            )
        
        # Queue
        if self.player.queue:
            queue_text = ""
            for i, track in enumerate(self.player.queue[:10], 1):
                cache_icon = "✅" if self.player.is_cached(track['filename']) else "⏳"
                duration = track.get('duration', 'Unknown')
                queue_text += f"`{i}.` {cache_icon} **{track['title'][:40]}** - {track.get('artist', 'Unknown')[:20]} ({duration})\n"
            
            if len(self.player.queue) > 10:
                queue_text += f"\n... and {len(self.player.queue) - 10} more tracks"
            
            embed.add_field(name="Up Next", value=queue_text, inline=False)
        
        embed.set_footer(text=f"Total: {len(self.player.queue)} tracks")
        await interaction.edit_original_response(embed=embed, view=self)

# ========== PREMIUM REMOVE TRACKS MODAL ==========
class PremiumRemoveTracksModal(Modal, title="Remove Tracks from Queue"):
    """Premium modal for removing tracks"""
    
    def __init__(self, player: EnhancedMusicPlayer):
        super().__init__(timeout=None)
        self.player = player
        
        self.track_numbers = TextInput(
            label="Track Numbers to Remove",
            placeholder="Example: 1,3,5-7 or 'all'",
            required=True,
            max_length=50
        )
        self.add_item(self.track_numbers)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        input_text = self.track_numbers.value.strip().lower()
        
        if input_text == 'all':
            # Remove all tracks
            removed_count = len(self.player.queue)
            self.player.queue.clear()
            
            embed = discord.Embed(
                title="Queue Cleared",
                description=f"Removed all {removed_count} tracks from queue",
                color=discord.Color.green()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        
        # Parse track numbers
        positions = []
        try:
            for part in input_text.split(','):
                part = part.strip()
                if '-' in part:
                    start, end = map(int, part.split('-'))
                    positions.extend(range(start, end + 1))
                else:
                    positions.append(int(part))
        except ValueError:
            await interaction.followup.send(
                "❌ Invalid format. Use: 1,3,5-7 or 'all'",
                ephemeral=True
            )
            return
        
        # Remove tracks
        removed = []
        for pos in sorted(positions, reverse=True):
            if 1 <= pos <= len(self.player.queue):
                removed.append(self.player.queue.pop(pos - 1))
        
        if removed:
            removed_list = "\n".join([f"• {t['title']}" for t in removed[:5]])
            if len(removed) > 5:
                removed_list += f"\n... and {len(removed) - 5} more"
            
            embed = discord.Embed(
                title="✅ Tracks Removed",
                description=f"Removed {len(removed)} tracks from queue:\n{removed_list}",
                color=discord.Color.green()
            )
        else:
            embed = discord.Embed(
                title="❌ No Tracks Removed",
                description="No tracks found at those positions",
                color=discord.Color.red()
            )
        
        await interaction.followup.send(embed=embed, ephemeral=True)

# ========== UPDATED MUSIC COG ==========
class Music(commands.Cog):
    """Complete Music Cog with Premium UI and Enhanced Management"""
    
    def __init__(self, bot):
        self.bot = bot
        self.players: Dict[int, EnhancedMusicPlayer] = {}
        
        # Initialize database with migration
        self.init_database()
        
        # Start background tasks
        self.cache_cleanup_task.start()
        self.index_update_task.start()
        logger.info("Music cog initialized with premium UI and management system")
    
    def init_database(self):
        """Initialize SQLite database with enhanced schema"""
        db_path = "data/music_bot.db"
        Path("data").mkdir(parents=True, exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check and update track_stats table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='track_stats'")
        if cursor.fetchone():
            # Check for new columns
            cursor.execute("PRAGMA table_info(track_stats)")
            columns = {row[1]: row for row in cursor.fetchall()}
            
            # Add description column if missing
            if 'description' not in columns:
                cursor.execute('ALTER TABLE track_stats ADD COLUMN description TEXT')
                logger.info("Added 'description' column to track_stats table")
        else:
            # Create new track_stats table
            cursor.execute('''
                CREATE TABLE track_stats (
                    filename TEXT PRIMARY KEY,
                    title TEXT NOT NULL DEFAULT 'Unknown Title',
                    artist TEXT NOT NULL DEFAULT 'Unknown Artist',
                    genre TEXT DEFAULT 'Unknown',
                    description TEXT,
                    direct_link TEXT,
                    service TEXT DEFAULT 'unknown',
                    plays INTEGER DEFAULT 0,
                    skips INTEGER DEFAULT 0,
                    is_cached INTEGER DEFAULT 0,
                    cache_path TEXT,
                    last_cached TEXT,
                    last_played TEXT,
                    added_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            logger.info("Created track_stats table")
        
        # Check and update playlists table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='playlists'")
        if not cursor.fetchone():
            cursor.execute('''
                CREATE TABLE playlists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    description TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(name, user_id)
                )
            ''')
            logger.info("Created playlists table")
        
        # Check playlist_tracks table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='playlist_tracks'")
        if not cursor.fetchone():
            cursor.execute('''
                CREATE TABLE playlist_tracks (
                    playlist_id INTEGER,
                    track_filename TEXT,
                    position INTEGER,
                    added_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (playlist_id) REFERENCES playlists(id) ON DELETE CASCADE,
                    FOREIGN KEY (track_filename) REFERENCES track_stats(filename) ON DELETE CASCADE,
                    PRIMARY KEY (playlist_id, track_filename)
                )
            ''')
            logger.info("Created playlist_tracks table")
        
        # Create indexes
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_track_stats_filename ON track_stats(filename)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_track_stats_title ON track_stats(title)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_track_stats_artist ON track_stats(artist)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_track_stats_is_cached ON track_stats(is_cached)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_playlists_user_id ON playlists(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_playlist_tracks_playlist_id ON playlist_tracks(playlist_id)')
            logger.info("Created database indexes")
        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
        
        conn.commit()
        conn.close()
        logger.info("Database initialization complete")
    
    def get_player(self, guild_id: int) -> EnhancedMusicPlayer:
        """Get or create enhanced music player for guild"""
        if guild_id not in self.players:
            self.players[guild_id] = EnhancedMusicPlayer(self.bot, guild_id)
        return self.players[guild_id]
    
    # ========== SLASH COMMANDS ==========
    
    @commands.hybrid_command(name="managemusic", description="Manage your music library")
    async def manage_music(self, ctx: commands.Context):
        """Open the music management panel"""
        view = ManageMusicMainView(self, ctx.author)
        embed = discord.Embed(
            title="🎵 Manage Music",
            description="Select an action from the dropdown:",
            color=discord.Color.blue()
        )
        embed.set_footer(text="This panel will timeout after 5 minutes of inactivity")
        await ctx.send(embed=embed, view=view, ephemeral=True)
    
    async def play_autocomplete(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        """Autocomplete for /play command"""
        if not current or len(current) < 2:
            return []
        
        try:
            tracks = await self.search_tracks(current, limit=25)
            choices = []
            
            for track in tracks:
                display_name = f"{track['title']} - {track.get('artist', 'Unknown')}"
                if len(display_name) > 100:
                    display_name = display_name[:97] + "..."
                
                choices.append(
                    app_commands.Choice(
                        name=display_name,
                        value=track['title']
                    )
                )
            
            return choices[:25]
        
        except Exception as e:
            logger.error(f"Autocomplete error: {e}")
            return []
    
    @commands.hybrid_command(name="play", description="Play a track from the library")
    @app_commands.describe(query="Track name, artist, or search query")
    @app_commands.autocomplete(query=play_autocomplete)
    async def play(self, ctx: commands.Context, *, query: str):
        """Play music from the library with premium UI"""
        
        # Check voice channel
        if not ctx.author.voice:
            embed = discord.Embed(
                title="❌ Not in Voice Channel",
                description="Please join a voice channel first!",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        await ctx.defer()
        
        player = self.get_player(ctx.guild.id)
        player.current_channel = ctx.channel
        
        # Join voice channel
        if not await player.join_voice(ctx.author.voice.channel):
            embed = discord.Embed(
                title="❌ Connection Failed",
                description="Could not join voice channel",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        # Search for tracks
        tracks = await self.search_tracks(query, limit=25)
        if not tracks:
            embed = discord.Embed(
                title="❌ No Tracks Found",
                description=f"No track found for: **{query}**",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        # Single track found
        if len(tracks) == 1:
            track = tracks[0]
            
            # Send downloading message
            embed = discord.Embed(
                title="⏳ Loading Track...",
                description=f"**{track['title']}** by {track.get('artist', 'Unknown')}",
                color=discord.Color.blue()
            )
            loading_msg = await ctx.send(embed=embed)
            
            # Download to cache
            cache_result = await player.download_to_cache(track)
            if not cache_result:
                embed = discord.Embed(
                    title="❌ Download Failed",
                    description=f"Could not download: {track['title']}",
                    color=discord.Color.red()
                )
                await loading_msg.edit(embed=embed)
                return
            
            await loading_msg.delete()
            
            # Add to queue or play immediately
            if player.is_playing or player.is_paused:
                player.queue.append(track)
                embed = discord.Embed(
                    title="✅ Added to Queue",
                    description=f"**{track['title']}** by {track.get('artist', 'Unknown')}",
                    color=discord.Color.green()
                )
                embed.add_field(name="Position", value=f"#{len(player.queue)}", inline=True)
                embed.add_field(name="Cache", value="✅ Ready", inline=True)
                await ctx.send(embed=embed)
            else:
                await player.play_track(track, ctx.interaction)
        
        # Multiple tracks found - show selection
        else:
            view = TrackSelectView(tracks, player, ctx.author)
            embed = discord.Embed(
                title="🔍 Multiple Tracks Found",
                description=f"Found {len(tracks)} tracks for: **{query}**",
                color=discord.Color.blue()
            )
            embed.add_field(
                name="Select a track:",
                value="Choose from the dropdown below",
                inline=False
            )
            await ctx.send(embed=embed, view=view)
    
    @commands.hybrid_command(name="testlink", description="Test if a cloud storage link works")
    @app_commands.describe(link="Share link to test")
    async def test_link(self, ctx: commands.Context, *, link: str):
        """Test if a cloud storage link can be resolved"""
        await ctx.defer()
        
        embed = discord.Embed(
            title="🔗 Testing Link...",
            description="Attempting to resolve the link...",
            color=discord.Color.blue()
        )
        embed.add_field(name="Link", value=f"`{link[:50]}...`", inline=False)
        
        msg = await ctx.send(embed=embed)
        
        try:
            async with CloudStorageResolver() as resolver:
                service = resolver.identify_service(link)
                
                embed.add_field(name="Detected Service", value=service.replace('_', ' ').title(), inline=True)
                await msg.edit(embed=embed)
                
                direct_link = await resolver.resolve_link(link)
                
                if not direct_link:
                    embed = discord.Embed(
                        title="❌ Link Resolution Failed",
                        description="Could not get direct download link.",
                        color=discord.Color.red()
                    )
                    await msg.edit(embed=embed)
                    return
                
                # Test the link
                test_embed = discord.Embed(
                    title="🔍 Testing Direct Link...",
                    description=f"Testing: `{direct_link[:50]}...`",
                    color=discord.Color.blue()
                )
                await msg.edit(embed=test_embed)
                
                async with aiohttp.ClientSession() as session:
                    async with session.head(direct_link, allow_redirects=True, timeout=10) as response:
                        status = response.status
                        content_type = response.headers.get('content-type', 'unknown')
                        content_length = response.headers.get('content-length', 'unknown')
                        
                        if status == 200:
                            embed = discord.Embed(
                                title="✅ Link Works!",
                                description="This link can be used in the Manage Music panel",
                                color=discord.Color.green()
                            )
                        else:
                            embed = discord.Embed(
                                title="⚠️ Link Issues",
                                description="The link resolves but may have issues",
                                color=discord.Color.orange()
                            )
                        
                        embed.add_field(name="Service", value=service.replace('_', ' ').title(), inline=True)
                        embed.add_field(name="Status", value=str(status), inline=True)
                        embed.add_field(name="Content Type", value=content_type[:50], inline=True)
                        
                        if content_length != 'unknown':
                            size_mb = int(content_length) / 1024 / 1024
                            embed.add_field(name="File Size", value=f"{size_mb:.2f} MB", inline=True)
                        
                        embed.add_field(name="Direct Link", value=f"`{direct_link[:100]}...`", inline=False)
                        
                        await msg.edit(embed=embed)
                        
        except Exception as e:
            logger.error(f"Link test failed: {e}")
            embed = discord.Embed(
                title="❌ Test Failed",
                description=f"Error: {str(e)[:500]}",
                color=discord.Color.red()
            )
            await msg.edit(embed=embed)
    
    # ========== TEXT COMMANDS (Keep for compatibility) ==========
    
    @commands.command(name="skip", description="Skip current track")
    async def skip(self, ctx: commands.Context):
        """Skip current track"""
        player = self.get_player(ctx.guild.id)
        
        if not player.voice_client or not player.voice_client.is_playing():
            embed = discord.Embed(
                title="❌ Nothing Playing",
                description="No track to skip",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        player.voice_client.stop()
        if player.current_track:
            await player.update_skip_stats(player.current_track['filename'])
        
        embed = discord.Embed(
            title="⏭️ Skipped",
            description="Track skipped",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
    
    @commands.command(name="pause", description="Pause current track")
    async def pause(self, ctx: commands.Context):
        """Pause playback"""
        player = self.get_player(ctx.guild.id)
        
        if player.voice_client and player.voice_client.is_playing():
            player.voice_client.pause()
            player.is_paused = True
            
            embed = discord.Embed(
                title="⏸️ Paused",
                description="Playback paused",
                color=discord.Color.blue()
            )
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Not Playing",
                description="Nothing is playing",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
    
    @commands.command(name="resume", description="Resume playback")
    async def resume(self, ctx: commands.Context):
        """Resume playback"""
        player = self.get_player(ctx.guild.id)
        
        if player.voice_client and player.voice_client.is_paused():
            player.voice_client.resume()
            player.is_paused = False
            
            embed = discord.Embed(
                title="▶️ Resumed",
                description="Playback resumed",
                color=discord.Color.green()
            )
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Not Paused",
                description="Playback is not paused",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
    
    @commands.command(name="stop", description="Stop playback and clear queue")
    async def stop(self, ctx: commands.Context):
        """Stop playback"""
        player = self.get_player(ctx.guild.id)
        
        if player.voice_client:
            player.voice_client.stop()
            player.queue.clear()
            player.history.clear()
            player.is_playing = False
            player.is_paused = False
            
            # Clean up now playing view
            if player.now_playing_view:
                player.now_playing_view.stop()
                player.now_playing_view = None
            
            embed = discord.Embed(
                title="⏹️ Stopped",
                description="Playback stopped, queue cleared",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Not Playing",
                description="Nothing is playing",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
    
    @commands.command(name="queue", aliases=["q"], description="Show current queue")
    async def queue(self, ctx: commands.Context):
        """Show music queue"""
        player = self.get_player(ctx.guild.id)
        await player.show_queue(ctx.interaction)
    
    @commands.command(name="nowplaying", aliases=["np"], description="Show current track")
    async def nowplaying(self, ctx: commands.Context):
        """Show currently playing track"""
        player = self.get_player(ctx.guild.id)
        
        if not player.current_track:
            embed = discord.Embed(
                title="❌ Not Playing",
                description="No track is currently playing",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        embed = discord.Embed(
            title="🎵 Now Playing",
            color=discord.Color.green()
        )
        
        track = player.current_track
        embed.add_field(name="Title", value=track['title'], inline=False)
        embed.add_field(name="Artist", value=track.get('artist', 'Unknown'), inline=True)
        
        if track.get('genre'):
            embed.add_field(name="Genre", value=track['genre'], inline=True)
        
        if player.queue:
            embed.add_field(
                name="Queue",
                value=f"{len(player.queue)} track(s) waiting",
                inline=False
            )
        
        embed.set_footer(text=f"Volume: {int(player.volume * 100)}% | Loop: {player.loop_mode}")
        await ctx.send(embed=embed)
    
    @commands.command(name="previous", aliases=["prev"], description="Play previous track")
    async def previous(self, ctx: commands.Context):
        """Play previous track"""
        player = self.get_player(ctx.guild.id)
        
        if not ctx.author.voice:
            embed = discord.Embed(
                title="❌ Not in Voice Channel",
                description="Please join a voice channel first!",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        if not player.voice_client or not player.voice_client.is_connected():
            embed = discord.Embed(
                title="❌ Not Connected",
                description="Bot is not connected to voice",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        await ctx.defer()
        success = await player.play_previous(ctx.interaction)
        
        if not success:
            embed = discord.Embed(
                title="❌ No Previous Track",
                description="History is empty",
                color=discord.Color.orange()
            )
            await ctx.send(embed=embed)
    
    @commands.command(name="volume", aliases=["vol"], description="Adjust volume")
    async def volume(self, ctx: commands.Context, level: int):
        """Adjust playback volume"""
        player = self.get_player(ctx.guild.id)
        
        if not 1 <= level <= 100:
            embed = discord.Embed(
                title="❌ Invalid Volume",
                description="Volume must be between 1 and 100",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        player.volume = level / 100
        
        if player.voice_client and player.voice_client.source:
            player.voice_client.source.volume = player.volume
        
        embed = discord.Embed(
            title="🔊 Volume Adjusted",
            description=f"Volume set to **{level}%**",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
    
    @commands.command(name="loop", description="Set loop mode")
    async def loop(self, ctx: commands.Context, mode: str = None):
        """Set loop mode"""
        player = self.get_player(ctx.guild.id)
        
        if mode:
            if mode.lower() in ['off', 'track', 'queue']:
                player.loop_mode = mode.lower()
            else:
                embed = discord.Embed(
                    title="❌ Invalid Mode",
                    description="Valid modes: off, track, queue",
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed)
                return
        
        mode_emojis = {'off': '❌', 'track': '🔂', 'queue': '🔁'}
        embed = discord.Embed(
            title="Loop Mode",
            description=f"Current: {mode_emojis[player.loop_mode]} **{player.loop_mode}**",
            color=discord.Color.blue()
        )
        await ctx.send(embed=embed)
    
    @commands.command(name="shuffle", description="Shuffle the queue")
    async def shuffle(self, ctx: commands.Context):
        """Shuffle queue"""
        player = self.get_player(ctx.guild.id)
        
        if len(player.queue) < 2:
            embed = discord.Embed(
                title="❌ Not Enough Tracks",
                description="Need at least 2 tracks in queue to shuffle",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        random.shuffle(player.queue)
        
        embed = discord.Embed(
            title="🔀 Queue Shuffled",
            description=f"Shuffled {len(player.queue)} tracks",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
    
    @commands.command(name="remove", description="Remove tracks from queue")
    async def remove(self, ctx: commands.Context, *, positions: str):
        """Remove tracks from queue by position"""
        player = self.get_player(ctx.guild.id)
        
        input_text = positions.strip().lower()
        
        if input_text == 'all':
            # Remove all tracks
            removed_count = len(player.queue)
            player.queue.clear()
            
            embed = discord.Embed(
                title="✅ Queue Cleared",
                description=f"Removed all {removed_count} tracks from queue",
                color=discord.Color.green()
            )
            await ctx.send(embed=embed)
            return
        
        # Parse track numbers
        try:
            positions_list = []
            for part in input_text.split(','):
                part = part.strip()
                if '-' in part:
                    start, end = map(int, part.split('-'))
                    positions_list.extend(range(start, end + 1))
                else:
                    positions_list.append(int(part))
        except ValueError:
            embed = discord.Embed(
                title="❌ Invalid Format",
                description="Use: `1,3,5-7` or `all`\nExample: `e!remove 2,4-6`",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        # Remove tracks
        removed = []
        for pos in sorted(positions_list, reverse=True):
            if 1 <= pos <= len(player.queue):
                removed.append(player.queue.pop(pos - 1))
        
        if removed:
            removed_list = "\n".join([f"`{i+1}.` **{t['title']}**" for i, t in enumerate(removed[:5])])
            if len(removed) > 5:
                removed_list += f"\n... and {len(removed) - 5} more"
            
            embed = discord.Embed(
                title="✅ Tracks Removed",
                description=f"Removed {len(removed)} tracks from queue:\n{removed_list}",
                color=discord.Color.green()
            )
        else:
            embed = discord.Embed(
                title="❌ No Tracks Removed",
                description="No tracks found at those positions",
                color=discord.Color.red()
            )
        
        await ctx.send(embed=embed)

    # ========== HELPER METHODS FOR DATABASE OPERATIONS ==========
    
    async def get_all_tracks(self) -> List[Dict]:
        """Get all tracks from database"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                cursor = await db.execute('''
                    SELECT filename, title, artist, genre, direct_link, service, is_cached
                    FROM track_stats 
                    ORDER BY title, artist
                ''')
                
                rows = await cursor.fetchall()
                
                tracks = []
                for row in rows:
                    tracks.append({
                        'filename': row[0],
                        'title': row[1] or "Unknown Title",
                        'artist': row[2] or "Unknown Artist",
                        'genre': row[3] or "Unknown",
                        'direct_link': row[4] or '',
                        'service': row[5] or 'unknown',
                        'is_cached': bool(row[6])
                    })
                
                return tracks
                
        except Exception as e:
            logger.error(f"Failed to get all tracks: {e}")
            return []
    
    async def get_all_tracks_with_cache_status(self) -> List[Dict]:
        """Get all tracks with detailed cache status"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                cursor = await db.execute('''
                    SELECT filename, title, artist, genre, is_cached, cache_path, plays
                    FROM track_stats 
                    ORDER BY title, artist
                ''')
                
                rows = await cursor.fetchall()
                
                tracks = []
                for row in rows:
                    tracks.append({
                        'filename': row[0],
                        'title': row[1] or "Unknown Title",
                        'artist': row[2] or "Unknown Artist",
                        'genre': row[3] or "Unknown",
                        'is_cached': bool(row[4]),
                        'cache_path': row[5],
                        'plays': row[6] or 0
                    })
                
                return tracks
                
        except Exception as e:
            logger.error(f"Failed to get tracks with cache status: {e}")
            return []
    
    async def get_user_playlists(self, user_id: int) -> List[Dict]:
        """Get all playlists for a user"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                cursor = await db.execute('''
                    SELECT p.name, p.description, 
                           COUNT(pt.track_filename) as track_count
                    FROM playlists p
                    LEFT JOIN playlist_tracks pt ON p.id = pt.playlist_id
                    WHERE p.user_id = ?
                    GROUP BY p.id
                    ORDER BY p.name
                ''', (user_id,))
                
                rows = await cursor.fetchall()
                
                playlists = []
                for name, description, track_count in rows:
                    playlists.append({
                        'name': name,
                        'description': description or '',
                        'track_count': track_count,
                        'owner': 'You'
                    })
                
                return playlists
                
        except Exception as e:
            logger.error(f"Failed to get user playlists: {e}")
            return []
    
    async def get_track_by_filename(self, filename: str) -> Optional[Dict]:
        """Get track by filename from database"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                cursor = await db.execute('''
                    SELECT filename, title, artist, genre, direct_link, service, is_cached
                    FROM track_stats 
                    WHERE filename = ?
                ''', (filename,))
                
                row = await cursor.fetchone()
                
                if row:
                    return {
                        'filename': row[0],
                        'title': row[1] or "Unknown Title",
                        'artist': row[2] or "Unknown Artist",
                        'genre': row[3] or "Unknown",
                        'direct_link': row[4] or '',
                        'service': row[5] or 'unknown',
                        'is_cached': bool(row[6])
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Database error: {e}")
            return None
    
    async def get_playlist_tracks(self, user_id: int, playlist_name: str) -> List[Dict]:
        """Get all tracks from a playlist"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                cursor = await db.execute(
                    """
                    SELECT ts.filename, ts.title, ts.artist, ts.direct_link, ts.genre, ts.service, ts.is_cached
                    FROM playlists p
                    JOIN playlist_tracks pt ON p.id = pt.playlist_id
                    JOIN track_stats ts ON pt.track_filename = ts.filename
                    WHERE p.name = ? AND p.user_id = ?
                    ORDER BY pt.position
                    """,
                    (playlist_name, user_id)
                )
                rows = await cursor.fetchall()
                
                tracks = []
                for row in rows:
                    tracks.append({
                        'filename': row[0],
                        'title': row[1] or "Unknown Title",
                        'artist': row[2] or "Unknown Artist",
                        'direct_link': row[3] or '',
                        'genre': row[4] or "Unknown",
                        'service': row[5] or 'unknown',
                        'is_cached': bool(row[6])
                    })
                
                return tracks
                
        except Exception as e:
            logger.error(f"Failed to get playlist tracks: {e}")
            return []
    
    async def search_tracks(self, query: str, limit: int = 25) -> List[Dict]:
        """Search for multiple tracks in database with improved search"""
        try:
            # Load index for faster search
            index_file = "data/music_index.json"
            if not Path(index_file).exists():
                # Create index from database
                await self.update_track_index()
                if not Path(index_file).exists():
                    return []
            
            with open(index_file, 'r', encoding='utf-8') as f:
                index = json.load(f)
            
            # Clean query
            query = query.lower().strip()
            
            if not query:
                # Return random tracks for empty query
                return random.sample(index, min(10, len(index))) if index else []
            
            # Score each track with fuzzy matching
            scored_tracks = []
            
            for track in index:
                score = 0
                
                # Exact matches
                if track.get('filename', '').lower() == query:
                    score += 100
                
                if track.get('title', '').lower() == query:
                    score += 50
                
                if track.get('artist', '').lower() == query:
                    score += 40
                
                # Partial matches with weight
                title = track.get('title', '').lower()
                artist = track.get('artist', '').lower()
                filename = track.get('filename', '').lower()
                genre = track.get('genre', '').lower()
                
                # Check if query is in any field
                if query in title:
                    # Weight by position in title
                    pos = title.find(query)
                    score += 30 - (pos / 10)  # Earlier matches score higher
                
                if query in artist:
                    score += 20
                
                if query in filename:
                    score += 10
                
                if query in genre:
                    score += 5
                
                # Word-by-word matching
                query_words = query.split()
                title_words = title.split()
                artist_words = artist.split()
                
                # Check for matching words
                for word in query_words:
                    if word in title_words:
                        score += 15
                    if word in artist_words:
                        score += 10
                
                if score > 0:
                    scored_tracks.append((score, track))
            
            # Sort by score
            scored_tracks.sort(key=lambda x: x[0], reverse=True)
            
            # Return top results
            return [track for score, track in scored_tracks[:limit]]
            
        except Exception as e:
            logger.error(f"Search error: {e}")
            return []
    
    async def update_track_index(self):
        """Update the track index from database"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                cursor = await db.execute('''
                    SELECT filename, title, artist, genre, service, is_cached, plays
                    FROM track_stats
                    ORDER BY title, artist
                ''')
                
                rows = await cursor.fetchall()
                
                index = []
                for row in rows:
                    index.append({
                        'filename': row[0],
                        'title': row[1] or "Unknown Title",
                        'artist': row[2] or "Unknown Artist",
                        'genre': row[3] or "Unknown",
                        'service': row[4] or 'unknown',
                        'is_cached': bool(row[5]),
                        'plays': row[6] or 0
                    })
                
                # Save to file
                with open("data/music_index.json", 'w', encoding='utf-8') as f:
                    json.dump(index, f, indent=2, ensure_ascii=False)
                
                logger.info(f"Updated track index with {len(index)} tracks")
                
        except Exception as e:
            logger.error(f"Failed to update track index: {e}")
    
    async def _add_to_json_index(self, track: Dict):
        """Add track to JSON index"""
        try:
            index_file = "data/music_index.json"
            
            if Path(index_file).exists():
                with open(index_file, 'r', encoding='utf-8') as f:
                    index = json.load(f)
            else:
                index = []
            
            # Check if already exists
            for existing in index:
                if existing['filename'] == track['filename']:
                    # Update existing
                    existing.update(track)
                    break
            else:
                # Add new
                index.append(track)
            
            with open(index_file, 'w', encoding='utf-8') as f:
                json.dump(index, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Added/updated track in index: {track['filename']}")
                
        except Exception as e:
            logger.error(f"Failed to add to JSON index: {e}")
            raise
    
    async def get_cached_tracks_count(self) -> Tuple[int, int]:
        """Get count of cached vs total tracks"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                # Total tracks
                cursor = await db.execute("SELECT COUNT(*) FROM track_stats")
                total = (await cursor.fetchone())[0]
                
                # Cached tracks
                cursor = await db.execute("SELECT COUNT(*) FROM track_stats WHERE is_cached = 1")
                cached = (await cursor.fetchone())[0]
                
                return total, cached
                
        except Exception as e:
            logger.error(f"Failed to get cached count: {e}")
            return 0, 0
    
    async def get_cache_size(self) -> float:
        """Get total cache size in MB"""
        cache_dir = Path("data/music_cache")
        if not cache_dir.exists():
            return 0.0
        
        total_size = 0
        for f in cache_dir.glob('**/*'):
            if f.is_file():
                total_size += f.stat().st_size
        
        return total_size / 1024 / 1024
    
    # ========== BACKGROUND TASKS ==========
    
    @tasks.loop(hours=6)
    async def cache_cleanup_task(self):
        """Automatically clean up cache when over limit"""
        try:
            await self.cleanup_cache()
        except Exception as e:
            logger.error(f"Cache cleanup task failed: {e}")
    
    @tasks.loop(hours=1)
    async def index_update_task(self):
        """Periodically update the track index"""
        try:
            await self.update_track_index()
        except Exception as e:
            logger.error(f"Index update task failed: {e}")
    
    async def cleanup_cache(self):
        """Clean up cache based on track scores and usage"""
        try:
            db_path = "data/music_bot.db"
            async with aiosqlite.connect(db_path) as db:
                # Get tracks with cache info, ordered by score
                cursor = await db.execute('''
                    SELECT filename, cache_path, plays, skips, last_played, 
                           (COALESCE(plays, 0) - COALESCE(skips, 0) * 2) as score,
                           julianday('now') - julianday(COALESCE(last_played, added_date)) as days_since_played
                    FROM track_stats 
                    WHERE is_cached = 1 AND cache_path IS NOT NULL
                    ORDER BY score ASC, days_since_played DESC
                ''')
                cached_tracks = await cursor.fetchall()
                
                # Calculate current cache size
                cache_dir = Path("data/music_cache")
                total_size = sum(f.stat().st_size for f in cache_dir.glob('**/*') if f.is_file())
                max_size = int(os.getenv('MAX_CACHE_SIZE', 10737418240))  # 10GB
                
                # Remove tracks until under 80% capacity
                removed = 0
                freed_bytes = 0
                
                for track in cached_tracks:
                    if total_size <= max_size * 0.8:  # Stop at 80% capacity
                        break
                    
                    cache_path = Path(track[1])
                    if cache_path.exists():
                        file_size = cache_path.stat().st_size
                        
                        try:
                            cache_path.unlink()
                            total_size -= file_size
                            freed_bytes += file_size
                            removed += 1
                            
                            # Update database
                            await db.execute(
                                "UPDATE track_stats SET is_cached = 0, cache_path = NULL WHERE filename = ?",
                                (track[0],)
                            )
                            
                        except Exception as e:
                            logger.error(f"Failed to delete {cache_path}: {e}")
                
                await db.commit()
                
                if removed > 0:
                    logger.info(f"Cache cleanup: Removed {removed} files, freed {freed_bytes/1024/1024:.2f} MB")
                    
        except Exception as e:
            logger.error(f"Cache cleanup failed: {e}")
    
    @tasks.loop(hours=24)
    async def stats_cleanup_task(self):
        """Clean up old statistics and optimize database"""
        try:
            db_path = "data/music_bot.db"
            async with aiosqlite.connect(db_path) as db:
                # Remove tracks that haven't been played in 90 days and aren't cached
                cursor = await db.execute('''
                    DELETE FROM track_stats 
                    WHERE last_played IS NOT NULL 
                    AND julianday('now') - julianday(last_played) > 90 
                    AND is_cached = 0
                ''')
                deleted = cursor.rowcount
                
                # Update index
                await db.execute("VACUUM")
                await db.commit()
                
                if deleted > 0:
                    logger.info(f"Stats cleanup: Removed {deleted} old tracks")
                    await self.update_track_index()
                    
        except Exception as e:
            logger.error(f"Stats cleanup failed: {e}")
    
    # ========== EVENT LISTENERS ==========
    
    @commands.Cog.listener()
    async def on_voice_state_update(self, member, before, after):
        """Handle voice state updates for auto-disconnect"""
        # Bot was disconnected from voice
        if member == self.bot.user and before.channel and not after.channel:
            guild_id = before.channel.guild.id
            if guild_id in self.players:
                player = self.players[guild_id]
                player.is_playing = False
                player.is_paused = False
                
                # Clean up now playing view
                if player.now_playing_view:
                    player.now_playing_view.stop()
                    player.now_playing_view = None
                
                # Clear messages
                if player.now_playing_message:
                    try:
                        await player.now_playing_message.delete()
                    except:
                        pass
                    player.now_playing_message = None
                
                logger.info(f"Bot disconnected from voice in guild {guild_id}")
        
        # Auto-leave if alone in voice channel for 5 minutes
        elif member != self.bot.user and before.channel and before.channel.guild.id in self.players:
            guild_id = before.channel.guild.id
            player = self.players[guild_id]
            
            if player.voice_client and player.voice_client.is_connected():
                # Check if bot is alone in voice
                voice_channel = player.voice_client.channel
                if len(voice_channel.members) == 1:  # Only bot
                    # Start timeout task
                    asyncio.create_task(self._auto_disconnect_timeout(guild_id, voice_channel))
    
    async def _auto_disconnect_timeout(self, guild_id: int, voice_channel: discord.VoiceChannel):
        """Auto-disconnect after being alone in voice for 5 minutes"""
        await asyncio.sleep(300)  # 5 minutes
        
        if guild_id in self.players:
            player = self.players[guild_id]
            
            # Check if still alone
            if (player.voice_client and 
                player.voice_client.is_connected() and 
                len(voice_channel.members) == 1):
                
                await player.leave_voice()
                
                # Send message
                if player.current_channel:
                    embed = discord.Embed(
                        title="👋 Auto-disconnected",
                        description="Left voice channel due to inactivity",
                        color=discord.Color.blue()
                    )
                    await player.current_channel.send(embed=embed)
    
    @commands.Cog.listener()
    async def on_guild_remove(self, guild):
        """Clean up when bot leaves a guild"""
        guild_id = guild.id
        if guild_id in self.players:
            player = self.players[guild_id]
            
            # Disconnect from voice
            if player.voice_client and player.voice_client.is_connected():
                await player.voice_client.disconnect()
            
            # Clean up
            if player.now_playing_view:
                player.now_playing_view.stop()
            
            # Remove player
            del self.players[guild_id]
            logger.info(f"Cleaned up player for guild {guild_id}")
    
    # ========== COG MANAGEMENT ==========
    
    async def cog_unload(self):
        """Cleanup on cog unload"""
        logger.info("Unloading Music cog...")
        
        # Stop all background tasks
        self.cache_cleanup_task.cancel()
        self.index_update_task.cancel()
        self.stats_cleanup_task.cancel()
        
        # Disconnect all voice clients
        for guild_id, player in self.players.items():
            try:
                if player.voice_client and player.voice_client.is_connected():
                    await player.voice_client.disconnect()
                
                # Stop now playing views
                if player.now_playing_view:
                    player.now_playing_view.stop()
                
                logger.info(f"Cleaned up player for guild {guild_id}")
            except Exception as e:
                logger.error(f"Error cleaning up player for guild {guild_id}: {e}")
        
        # Clear players dict
        self.players.clear()
        
        logger.info("Music cog unloaded")
    
    async def cog_before_invoke(self, ctx):
        """Before command invocation"""
        # Ensure database exists
        self.init_database()
    
    @commands.Cog.listener()
    async def on_ready(self):
        """When bot is ready"""
        logger.info(f"Music cog ready! Loaded with premium UI and management system")
        
        # Start stats cleanup task
        self.stats_cleanup_task.start()
        
        # Update index
        await self.update_track_index()
        
        # Report status
        total_tracks, cached_tracks = await self.get_cached_tracks_count()
        cache_size = await self.get_cache_size()
        
        logger.info(f"Library: {total_tracks} total tracks, {cached_tracks} cached ({cache_size:.2f} MB)")

# ========== TRACK SELECT VIEW (Updated) ==========
class TrackSelectView(View):
    """View for selecting tracks from search results with pagination"""
    
    def __init__(self, tracks: List[Dict], player: EnhancedMusicPlayer, author: discord.Member):
        super().__init__(timeout=60)
        self.tracks = tracks
        self.player = player
        self.author = author
        self.current_page = 0
        self.items_per_page = 20
        
        self.update_select()
    
    def update_select(self):
        """Update the select dropdown with current page"""
        # Clear existing select
        for item in self.children[:]:
            if isinstance(item, Select):
                self.remove_item(item)
        
        # Create page items
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_tracks = self.tracks[start_idx:end_idx]
        
        options = []
        for i, track in enumerate(page_tracks):
            track_idx = start_idx + i
            label = track['title'][:90]
            description = f"by {track.get('artist', 'Unknown')}"
            
            options.append(
                discord.SelectOption(
                    label=label,
                    description=description[:95],
                    value=str(track_idx)
                )
            )
        
        select = discord.ui.Select(
            placeholder=f"Select a track to play... (Page {self.current_page + 1}/{math.ceil(len(self.tracks)/self.items_per_page)})",
            options=options
        )
        select.callback = self.select_callback
        self.add_item(select)
        
        # Add navigation if needed
        if len(self.tracks) > self.items_per_page:
            self.add_navigation_buttons()
    
    def add_navigation_buttons(self):
        """Add navigation buttons for pagination"""
        # Clear existing navigation buttons
        nav_buttons = []
        for item in self.children[:]:
            if isinstance(item, Button) and item.custom_id in ['prev_track', 'next_track']:
                self.remove_item(item)
                nav_buttons.append(item)
        
        # Add new navigation buttons
        prev_button = Button(
            label="◀ Previous",
            style=discord.ButtonStyle.grey,
            disabled=self.current_page == 0,
            custom_id="prev_track",
            row=1
        )
        
        next_button = Button(
            label="Next ▶",
            style=discord.ButtonStyle.grey,
            disabled=self.current_page >= math.ceil(len(self.tracks)/self.items_per_page) - 1,
            custom_id="next_track",
            row=1
        )
        
        async def prev_callback(interaction: discord.Interaction):
            if interaction.user != self.author:
                await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
                return
            self.current_page -= 1
            self.update_select()
            await interaction.response.edit_message(view=self)
        
        async def next_callback(interaction: discord.Interaction):
            if interaction.user != self.author:
                await interaction.response.send_message("❌ You can't use this button!", ephemeral=True)
                return
            self.current_page += 1
            self.update_select()
            await interaction.response.edit_message(view=self)
        
        prev_button.callback = prev_callback
        next_button.callback = next_callback
        
        self.add_item(prev_button)
        self.add_item(next_button)
    
    async def select_callback(self, interaction: discord.Interaction):
        if interaction.user != self.author:
            await interaction.response.send_message("You can't use this menu!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        selected_idx = int(interaction.data['values'][0])
        track = self.tracks[selected_idx]
        
        # Send loading message
        embed = discord.Embed(
            title="⏳ Loading Track...",
            description=f"**{track['title']}** by {track.get('artist', 'Unknown')}",
            color=discord.Color.blue()
        )
        msg = await interaction.followup.send(embed=embed)
        
        # Download to cache
        cache_result = await self.player.download_to_cache(track)
        if not cache_result:
            embed = discord.Embed(
                title="❌ Download Failed",
                description=f"Could not download: {track['title']}",
                color=discord.Color.red()
            )
            await msg.edit(embed=embed)
            return
        
        # Add to queue or play immediately
        if self.player.is_playing or self.player.is_paused:
            self.player.queue.append(track)
            embed = discord.Embed(
                title="✅ Added to Queue",
                description=f"**{track['title']}** by {track.get('artist', 'Unknown')}",
                color=discord.Color.green()
            )
            embed.add_field(name="Position", value=f"#{len(self.player.queue)}", inline=True)
            embed.add_field(name="Cache", value="✅ Ready", inline=True)
            await msg.edit(embed=embed)
        else:
            await msg.delete()
            await self.player.play_track(track, interaction)

# ========== SETUP FUNCTION ==========
async def setup(bot):
    """Setup function for the cog"""
    # Ensure data directory exists
    Path("data").mkdir(exist_ok=True)
    Path("data/music_cache").mkdir(exist_ok=True)
    
    # Load existing cache status
    cache_dir = Path("data/music_cache")
    if cache_dir.exists():
        cached_files = [f.name for f in cache_dir.glob('*') if f.is_file()]
        logger.info(f"Found {len(cached_files)} cached files")
    
    # Add cog to bot
    await bot.add_cog(Music(bot))
    
    logger.info("""
    ╔══════════════════════════════════════════════════════════════╗
    ║                    🎵 MUSIC BOT LOADED 🎵                    ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                                                              ║
    ║  ✅ Premium Now Playing UI (Maki-style)                      ║
    ║  ✅ Complete Management System (/managemusic)                ║
    ║  ✅ 25+ Audio Format Support                                 ║
    ║  ✅ Universal Cloud Storage Support                          ║
    ║  ✅ Paginated Selection Menus                                ║
    ║  ✅ Auto-disconnect & Cleanup                                ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    # Create welcome embed
    embed = discord.Embed(
        title="🎵 Music Bot Ready!",
        description="Premium music bot with complete management system has been loaded.",
        color=discord.Color.green()
    )
    
    embed.add_field(name="Main Commands", value="`/play` - Play music\n`/managemusic` - Manage library\n`/testlink` - Test cloud links", inline=False)
    embed.add_field(name="Playback Controls", value="`skip`, `pause`, `resume`, `stop`, `queue`, `nowplaying`, `volume`, `loop`, `shuffle`", inline=False)
    embed.add_field(name="Features", value="• Premium Maki-style UI\n• 25+ audio formats\n• Cloud storage support\n• Auto-caching\n• Playlist management", inline=False)
    
    # Find first text channel to send welcome message
    for guild in bot.guilds:
        for channel in guild.text_channels:
            if channel.permissions_for(guild.me).send_messages:
                try:
                    await channel.send(embed=embed)
                    logger.info(f"Sent welcome message to {guild.name} in #{channel.name}")
                    break
                except:
                    continue

# ========== ADDITIONAL UTILITY FUNCTIONS ==========
def generate_track_embed(track: Dict, player: EnhancedMusicPlayer) -> discord.Embed:
    """Generate a standard track embed for various uses"""
    embed = discord.Embed(
        title=f"🎵 {track['title'][:200]}",
        description=f"👤 **{track.get('artist', 'Unknown Artist')}**",
        color=discord.Color.green()
    )
    
    # Add track info
    if track.get('genre'):
        embed.add_field(name="Genre", value=track['genre'], inline=True)
    
    if track.get('service'):
        service_name = track['service'].replace('_', ' ').title()
        embed.add_field(name="Service", value=service_name, inline=True)
    
    # Cache status
    cache_status = "✅ Cached" if player.is_cached(track['filename']) else "⏳ Not Cached"
    embed.add_field(name="Cache", value=cache_status, inline=True)
    
    # File info
    file_ext = Path(track['filename']).suffix.upper()
    embed.add_field(name="Format", value=file_ext, inline=True)
    
    return embed

def format_duration(seconds: int) -> str:
    """Format seconds to MM:SS or HH:MM:SS"""
    if seconds < 3600:
        return time.strftime('%M:%S', time.gmtime(seconds))
    else:
        return time.strftime('%H:%M:%S', time.gmtime(seconds))

def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe filesystem use"""
    # Remove invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Limit length
    sanitized = sanitized[:200]
    # Remove leading/trailing spaces and dots
    sanitized = sanitized.strip('. ')
    return sanitized

def get_file_extension(url: str) -> str:
    """Extract file extension from URL"""
    # Try to get extension from URL path
    parsed = urlparse(url)
    path = parsed.path
    
    # Check for common audio extensions
    for ext in AUDIO_EXTENSIONS.keys():
        if path.lower().endswith(ext):
            return ext
    
    # Default to .mp3
    return '.mp3'

async def download_with_progress(url: str, filepath: Path, update_callback = None) -> bool:
    """Download file with progress callback"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': '*/*',
    }
    
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, timeout=180, allow_redirects=True) as response:
                if response.status in [200, 206]:
                    total_size = int(response.headers.get('content-length', 0))
                    downloaded = 0
                    start_time = time.time()
                    
                    with open(filepath, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            if not chunk:
                                continue
                            
                            f.write(chunk)
                            downloaded += len(chunk)
                            
                            # Call update callback if provided
                            if update_callback:
                                await update_callback(downloaded, total_size, start_time)
                    
                    return downloaded > 0
                else:
                    logger.error(f"Download failed with status: {response.status}")
                    return False
                    
    except Exception as e:
        logger.error(f"Download error: {e}")
        # Clean up partially downloaded file
        if filepath.exists():
            try:
                filepath.unlink()
            except:
                pass
        return False

# ========== ERROR HANDLERS ==========
class MusicError(Exception):
    """Base exception for music bot errors"""
    pass

class DownloadError(MusicError):
    """Error during download"""
    pass

class CacheError(MusicError):
    """Error with cache management"""
    pass

class DatabaseError(MusicError):
    """Error with database operations"""
    pass

def error_handler(func):
    """Decorator for error handling in music commands"""
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except DownloadError as e:
            logger.error(f"Download error in {func.__name__}: {e}")
            raise commands.CommandError(f"Download failed: {str(e)[:200]}")
        except CacheError as e:
            logger.error(f"Cache error in {func.__name__}: {e}")
            raise commands.CommandError(f"Cache error: {str(e)[:200]}")
        except DatabaseError as e:
            logger.error(f"Database error in {func.__name__}: {e}")
            raise commands.CommandError(f"Database error: {str(e)[:200]}")
        except discord.errors.HTTPException as e:
            logger.error(f"Discord HTTP error in {func.__name__}: {e}")
            raise commands.CommandError(f"Discord API error: {str(e)[:200]}")
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}", exc_info=True)
            raise commands.CommandError(f"Unexpected error: {str(e)[:200]}")
    
    return wrapper

# ========== CONFIGURATION ==========
class MusicConfig:
    """Configuration for music bot"""
    
    # Cache settings
    MAX_CACHE_SIZE = int(os.getenv('MAX_CACHE_SIZE', 10737418240))  # 10GB
    DOWNLOAD_SPEED = int(os.getenv('DOWNLOAD_SPEED', 2097152))  # 2 MB/s
    
    # Audio settings
    DEFAULT_VOLUME = 0.5
    SUPPORTED_FORMATS = list(AUDIO_EXTENSIONS.keys())
    
    # UI settings
    ITEMS_PER_PAGE = 20
    AUTO_DISCONNECT_TIMEOUT = 300  # 5 minutes
    
    # Database settings
    DATABASE_PATH = "data/music_bot.db"
    INDEX_PATH = "data/music_index.json"
    CACHE_DIR = "data/music_cache"
    
    @classmethod
    def validate(cls):
        """Validate configuration"""
        # Ensure directories exist
        Path("data").mkdir(exist_ok=True)
        Path(cls.CACHE_DIR).mkdir(exist_ok=True)
        
        # Validate cache size
        if cls.MAX_CACHE_SIZE < 104857600:  # 100MB
            logger.warning(f"MAX_CACHE_SIZE is very small: {cls.MAX_CACHE_SIZE/1024/1024:.1f} MB")
        
        logger.info(f"Music configuration loaded:")
        logger.info(f"  • Max cache: {cls.MAX_CACHE_SIZE/1024/1024/1024:.1f} GB")
        logger.info(f"  • Download speed: {cls.DOWNLOAD_SPEED/1024} KB/s")
        logger.info(f"  • Supported formats: {len(cls.SUPPORTED_FORMATS)}")
        logger.info(f"  • Items per page: {cls.ITEMS_PER_PAGE}")

# Validate configuration on import
MusicConfig.validate()

logger.info("""
🎵 MUSIC BOT SYSTEM COMPLETE 🎵

✓ Premium UI with auto-updating Now Playing
✓ Complete /managemusic panel with paginated menus
✓ Support for 25+ audio formats
✓ Universal cloud storage resolver
✓ Advanced caching system with preload/unload
✓ Playlist management with add/remove/edit
✓ Database with statistics and indexing
✓ Background maintenance tasks
✓ Auto-disconnect and cleanup
✓ Error handling and user feedback

The bot is ready for deployment with production-grade features!
""")

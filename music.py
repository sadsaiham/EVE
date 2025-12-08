"""
COMPLETE Discord Music Bot with Universal Cloud Storage Support
FIXED VERSION with Database Migration and Autocomplete
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
from typing import Dict, List, Optional, Tuple, Union, Any
import math
import random

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

# ========== Universal Cloud Storage Link Resolver ==========
class CloudStorageResolver:
    """Universal resolver for cloud storage links"""
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.cache: Dict[str, Dict] = {}
        self.cache_file = "data/link_cache.json"
        self.load_cache()
        
        # Rate limiting
        self.last_request = 0
        self.request_delay = 1.0
        
        # Headers that work for most services
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
        """Async context manager entry"""
        timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
        self.session = aiohttp.ClientSession(
            headers=self.base_headers,
            timeout=timeout
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    def load_cache(self):
        """Load cached links from file"""
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
        """Save cache to file"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
    
    async def rate_limit(self):
        """Implement rate limiting"""
        current_time = time.time()
        elapsed = current_time - self.last_request
        
        if elapsed < self.request_delay:
            await asyncio.sleep(self.request_delay - elapsed)
        
        self.last_request = time.time()
    
    def identify_service(self, url: str) -> str:
        """Identify which cloud service the URL belongs to"""
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
        """
        Resolve any cloud storage link to direct download link
        Returns: Direct download URL or None if failed
        """
        # Clean and normalize
        share_link = share_link.strip()
        
        # Check cache
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
        
        # Identify service
        service = self.identify_service(share_link)
        logger.info(f"Identified service: {service}")
        
        # Try service-specific resolver
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
                # Try as direct link first
                direct_link = share_link
            
            if direct_link:
                # Test if the link works
                if await self._test_direct_link(direct_link):
                    # Cache the result
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
        
        # Fallback: Try to extract direct link from HTML
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
        """Test if a direct link works"""
        try:
            async with self.session.head(url, allow_redirects=True, timeout=10) as response:
                return response.status in [200, 206]
        except:
            return False
    
    async def _resolve_dropbox(self, share_link: str) -> Optional[str]:
        """Resolve Dropbox share link to direct download"""
        # Dropbox formats:
        # https://www.dropbox.com/s/FILE_ID/FILENAME?dl=0
        # https://www.dropbox.com/scl/fi/FILE_ID/FILENAME?rlkey=KEY&dl=0
        
        # Method 1: Change dl=0 to dl=1
        if '?dl=0' in share_link:
            direct_link = share_link.replace('?dl=0', '?dl=1')
        elif '?dl=' in share_link:
            direct_link = re.sub(r'\?dl=\d', '?dl=1', share_link)
        else:
            # Add dl=1 parameter
            if '?' in share_link:
                direct_link = share_link + '&dl=1'
            else:
                direct_link = share_link + '?dl=1'
        
        # Also try raw=1 for some links
        raw_link = direct_link.replace('?dl=1', '?raw=1')
        
        # Test both
        if await self._test_direct_link(direct_link):
            return direct_link
        elif await self._test_direct_link(raw_link):
            return raw_link
        
        # Method 2: Extract from HTML
        try:
            async with self.session.get(share_link) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Look for direct download URLs in Dropbox HTML
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
        """Resolve Google Drive share link"""
        # Extract file ID
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
        
        # Method 1: Direct download URL
        direct_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        
        # Test direct link
        if await self._test_direct_link(direct_url):
            return direct_url
        
        # Method 2: Try to get confirm token for large files
        try:
            async with self.session.get(direct_url) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Look for confirm token
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
        """Resolve MediaFire share link"""
        # Check if it's already a direct download link
        if 'download' in share_link and 'mediafire.com' in share_link:
            # Already a direct download link
            return share_link
        
        try:
            async with self.session.get(share_link) as response:
                if response.status == 200:
                    # Try to read as text first for HTML parsing
                    try:
                        html = await response.text()
                    except UnicodeDecodeError:
                        # If it's binary data, it might already be the file
                        if response.headers.get('content-type', '').startswith('audio/'):
                            # It's an audio file, return the link
                            return share_link
                        elif response.headers.get('content-type', '').startswith('application/'):
                            # It's likely the file itself
                            return share_link
                        else:
                            # Try different encoding
                            html_bytes = await response.read()
                            try:
                                html = html_bytes.decode('utf-8', errors='ignore')
                            except:
                                # If all else fails, return the link
                                return share_link
                    
                    # MediaFire has direct download link in button
                    # Try multiple patterns
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
                            # Clean the URL
                            url = url.replace('\\/', '/')
                            if 'mediafire.com' in url and '/download/' in url:
                                return url
                    
                    # Try BeautifulSoup for button
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
        """Resolve MEGA.nz share link (simplified)"""
        # MEGA is complex, we'll just return the original link
        # and hope the downloader can handle it
        return share_link
    
    async def _resolve_onedrive(self, share_link: str) -> Optional[str]:
        """Resolve OneDrive share link"""
        # Change ?id= to /download?
        if '?id=' in share_link:
            direct_link = share_link.replace('?id=', '/download?')
            if await self._test_direct_link(direct_link):
                return direct_link
        
        # Try to add download parameter
        if '?' in share_link:
            direct_link = share_link + '&download=1'
        else:
            direct_link = share_link + '?download=1'
        
        return direct_link
    
    async def _resolve_terabox(self, share_link: str) -> Optional[str]:
        """Resolve TeraBox share link"""
        # TeraBox is difficult, but we can try
        try:
            async with self.session.get(share_link) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Look for direct URLs
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
                                # Clean URL
                                url = url.replace('\\/', '/')
                                return url
        except Exception as e:
            logger.error(f"TeraBox resolution failed: {e}")
        
        return None
    
    async def _resolve_simple_direct(self, share_link: str) -> str:
        """For services that already provide direct downloads"""
        return share_link
    
    async def _extract_from_html(self, url: str) -> Optional[str]:
        """Extract direct download link from any HTML page"""
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    # Check content type
                    content_type = response.headers.get('content-type', '').lower()
                    
                    # If it's already media content, return the URL
                    if any(media_type in content_type for media_type in ['audio/', 'video/', 'application/octet-stream']):
                        return url
                    
                    # Otherwise, try to parse as HTML
                    try:
                        html = await response.text()
                    except UnicodeDecodeError:
                        # If we can't decode as text, then it's not HTML, so return None
                        return None
                    
                    # Look for audio/video sources
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Check audio/video tags
                    for tag in soup.find_all(['audio', 'video', 'source']):
                        src = tag.get('src')
                        if src and self._is_media_url(src):
                            return self._make_absolute(url, src)
                    
                    # Check links with download attribute
                    for link in soup.find_all('a', href=True):
                        href = link.get('href')
                        download_attr = link.get('download')
                        
                        if download_attr and href and self._is_media_url(href):
                            return self._make_absolute(url, href)
                        
                        # Check link text for download
                        link_text = link.get_text().lower()
                        if ('download' in link_text or 'dl' in link_text) and href:
                            if self._is_media_url(href):
                                return self._make_absolute(url, href)
                    
                    # Check for meta refresh (redirects)
                    meta_refresh = soup.find('meta', {'http-equiv': 'refresh'})
                    if meta_refresh and meta_refresh.get('content'):
                        content = meta_refresh['content']
                        if 'url=' in content.lower():
                            redirect_url = content.split('url=', 1)[1].strip()
                            return self._make_absolute(url, redirect_url)
                    
                    # Look for JSON data with URLs
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
        """Check if URL looks like a media file"""
        media_extensions = [
            '.mp3', '.mp4', '.m4a', '.flac', '.wav', '.ogg', '.aac',
            '.webm', '.wma', '.opus', '.mka', '.mkv', '.mov', '.avi',
            '.mpg', '.mpeg', '.wmv'
        ]
        
        url_lower = url.lower()
        return any(url_lower.endswith(ext) for ext in media_extensions)
    
    def _make_absolute(self, base_url: str, url: str) -> str:
        """Convert relative URL to absolute"""
        if url.startswith('http://') or url.startswith('https://'):
            return url
        elif url.startswith('//'):
            return 'https:' + url
        elif url.startswith('/'):
            parsed = urlparse(base_url)
            return f"{parsed.scheme}://{parsed.netloc}{url}"
        else:
            return urljoin(base_url, url)

# ========== Enhanced Music Player ==========
class MusicPlayer:
    """Enhanced Music Player with All Features"""
    
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
        # Sanitize filename
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
                self._download_any,  # Added fallback method
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
    
    async def _download_with_session(self, url: str, cache_path: Path) -> bool:
        """Download with persistent session"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
        }
        
        timeout = aiohttp.ClientTimeout(total=300, connect=30, sock_read=60)
        
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            async with session.get(url, allow_redirects=True) as response:
                if response.status == 200:
                    total_size = int(response.headers.get('content-length', 0))
                    downloaded = 0
                    start_time = time.time()
                    last_update = start_time
                    
                    with open(cache_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(16384):
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
                            
                            # Log progress
                            current_time = time.time()
                            if current_time - last_update >= 5:
                                speed = downloaded / (current_time - start_time)
                                logger.debug(f"Downloading: {downloaded/1024/1024:.2f} MB ({speed/1024:.1f} KB/s)")
                                last_update = current_time
                    
                    download_time = time.time() - start_time
                    speed = downloaded / download_time if download_time > 0 else 0
                    logger.info(f"Download complete: {cache_path.name} ({speed/1024:.1f} KB/s)")
                    return True
                else:
                    logger.error(f"Session download failed: {response.status}")
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
            # Don't raise the error, just log it
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
            
            # Create audio source - FIXED: removed reconnect options
            cache_path = self.get_cache_path(track['filename'])
            
            # Use simple FFmpeg options without reconnect
            audio_source = discord.FFmpegPCMAudio(
                str(cache_path),
                options=f'-vn -af volume={self.volume}'
            )
            
            # Play with after callback - FIXED: use thread-safe coroutine execution
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

# ========== Control Views ==========
class MusicControls(View):
    """Interactive music control buttons"""
    
    def __init__(self, player: MusicPlayer):
        super().__init__(timeout=180)  # 3 minute timeout
        self.player = player
    
    @discord.ui.button(label="⏮️ Previous", style=discord.ButtonStyle.grey, row=0)
    async def previous_button(self, interaction: discord.Interaction, button: Button):
        if not self.player.voice_client or not self.player.voice_client.is_connected():
            await interaction.response.send_message("Not connected to voice", ephemeral=True)
            return
        
        await interaction.response.defer()
        success = await self.player.play_previous()
        
        if not success:
            await interaction.followup.send("No previous track available", ephemeral=True)
    
    @discord.ui.button(label="⏸️ Pause", style=discord.ButtonStyle.grey, row=0)
    async def pause_button(self, interaction: discord.Interaction, button: Button):
        if self.player.voice_client and self.player.voice_client.is_playing():
            self.player.voice_client.pause()
            self.player.is_paused = True
            await interaction.response.send_message("⏸️ Paused", ephemeral=True)
        else:
            await interaction.response.send_message("Nothing is playing", ephemeral=True)
    
    @discord.ui.button(label="▶️ Resume", style=discord.ButtonStyle.grey, row=0)
    async def resume_button(self, interaction: discord.Interaction, button: Button):
        if self.player.voice_client and self.player.voice_client.is_paused():
            self.player.voice_client.resume()
            self.player.is_paused = False
            await interaction.response.send_message("▶️ Resumed", ephemeral=True)
        else:
            await interaction.response.send_message("Nothing is paused", ephemeral=True)
    
    @discord.ui.button(label="⏭️ Skip", style=discord.ButtonStyle.grey, row=0)
    async def skip_button(self, interaction: discord.Interaction, button: Button):
        if self.player.voice_client and self.player.voice_client.is_playing():
            self.player.voice_client.stop()
            if self.player.current_track:
                await self.player.update_skip_stats(self.player.current_track['filename'])
            await interaction.response.send_message("⏭️ Skipped", ephemeral=True)
        else:
            await interaction.response.send_message("Nothing to skip", ephemeral=True)
    
    @discord.ui.button(label="🔁 Loop", style=discord.ButtonStyle.grey, row=1)
    async def loop_button(self, interaction: discord.Interaction, button: Button):
        modes = ['off', 'track', 'queue']
        current_idx = modes.index(self.player.loop_mode)
        next_idx = (current_idx + 1) % len(modes)
        self.player.loop_mode = modes[next_idx]
        
        mode_emojis = {'off': '❌', 'track': '🔂', 'queue': '🔁'}
        await interaction.response.send_message(
            f"{mode_emojis[modes[next_idx]]} Loop mode: **{modes[next_idx]}**",
            ephemeral=True
        )
    
    @discord.ui.button(label="🔀 Shuffle", style=discord.ButtonStyle.grey, row=1)
    async def shuffle_button(self, interaction: discord.Interaction, button: Button):
        if len(self.player.queue) < 2:
            await interaction.response.send_message("Need at least 2 tracks to shuffle", ephemeral=True)
            return
        
        random.shuffle(self.player.queue)
        await interaction.response.send_message("🔀 Queue shuffled", ephemeral=True)
    
    @discord.ui.button(label="📋 Queue", style=discord.ButtonStyle.blurple, row=1)
    async def queue_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        await self.show_queue(interaction)
    
    @discord.ui.button(label="🗑️ Clear", style=discord.ButtonStyle.red, row=1)
    async def clear_button(self, interaction: discord.Interaction, button: Button):
        if not self.player.queue:
            await interaction.response.send_message("Queue is already empty", ephemeral=True)
            return
        
        self.player.queue.clear()
        await interaction.response.send_message("🗑️ Queue cleared", ephemeral=True)
    
    async def show_queue(self, interaction: discord.Interaction):
        """Show queue with interactive controls"""
        player = self.player
        
        if not player.queue and not player.current_track:
            embed = discord.Embed(
                title="Queue is Empty",
                description="Add tracks with `/play`",
                color=discord.Color.blue()
            )
            await interaction.followup.send(embed=embed)
            return
        
        embed = discord.Embed(
            title="📋 Music Queue",
            color=discord.Color.blue()
        )
        
        # Add current track
        if player.current_track:
            status = "▶️ Playing" if player.is_playing else "⏸️ Paused"
            embed.add_field(
                name=f"{status}",
                value=f"**{player.current_track['title']}** by {player.current_track.get('artist', 'Unknown')}",
                inline=False
            )
        
        # Add queue
        if player.queue:
            queue_text = ""
            for i, track in enumerate(player.queue[:10], 1):
                cache_status = "✅" if player.is_cached(track['filename']) else "⏳"
                queue_text += f"`{i}.` {cache_status} **{track['title'][:40]}** - {track.get('artist', 'Unknown')[:20]}\n"
            
            if len(player.queue) > 10:
                queue_text += f"\n... and {len(player.queue) - 10} more tracks"
            
            embed.add_field(name="Up Next", value=queue_text, inline=False)
        
        # Add playback info
        footer_text = f"Volume: {int(player.volume * 100)}% | Loop: {player.loop_mode}"
        if player.history:
            footer_text += f" | History: {len(player.history)}"
        
        embed.set_footer(text=footer_text)
        
        # Create queue controls
        view = QueueControls(player)
        await interaction.followup.send(embed=embed, view=view)

class QueueControls(View):
    """Queue controls with removal options"""
    
    def __init__(self, player: MusicPlayer):
        super().__init__(timeout=180)
        self.player = player
    
    @discord.ui.button(label="❌ Remove", style=discord.ButtonStyle.red)
    async def remove_button(self, interaction: discord.Interaction, button: Button):
        """Open modal to remove tracks"""
        modal = RemoveTracksModal(self.player)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.grey)
    async def refresh_button(self, interaction: discord.Interaction, button: Button):
        """Refresh queue view"""
        await interaction.response.defer()
        
        embed = discord.Embed(
            title="📋 Music Queue",
            color=discord.Color.blue()
        )
        
        # Add current track
        if self.player.current_track:
            status = "▶️ Playing" if self.player.is_playing else "⏸️ Paused"
            embed.add_field(
                name=f"{status}",
                value=f"**{self.player.current_track['title']}** by {self.player.current_track.get('artist', 'Unknown')}",
                inline=False
            )
        
        # Add queue
        if self.player.queue:
            queue_text = ""
            for i, track in enumerate(self.player.queue[:10], 1):
                cache_status = "✅" if self.player.is_cached(track['filename']) else "⏳"
                queue_text += f"`{i}.` {cache_status} **{track['title'][:40]}** - {track.get('artist', 'Unknown')[:20]}\n"
            
            if len(self.player.queue) > 10:
                queue_text += f"\n... and {len(self.player.queue) - 10} more tracks"
            
            embed.add_field(name="Up Next", value=queue_text, inline=False)
        
        embed.set_footer(text=f"Total: {len(self.player.queue)} tracks")
        await interaction.edit_original_response(embed=embed, view=self)

class RemoveTracksModal(Modal, title="Remove Tracks from Queue"):
    """Modal for removing tracks by position"""
    
    def __init__(self, player: MusicPlayer):
        super().__init__()
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
        removed = await self.player.remove_from_queue(positions)
        
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

class TrackSelectView(View):
    """View for selecting tracks from search results"""
    
    def __init__(self, tracks: List[Dict], player: MusicPlayer, author: discord.Member):
        super().__init__(timeout=60)
        self.tracks = tracks
        self.player = player
        self.author = author
        
        # Create select dropdown
        options = []
        for i, track in enumerate(tracks[:25]):  # Discord limit
            label = track['title'][:90] if len(track['title']) > 90 else track['title']
            desc = f"by {track.get('artist', 'Unknown')}"
            options.append(
                discord.SelectOption(
                    label=label,
                    description=desc[:95],
                    value=str(i)
                )
            )
        
        select = discord.ui.Select(
            placeholder="Select a track to play...",
            options=options
        )
        select.callback = self.select_callback
        self.add_item(select)
    
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

# ========== Main Music Cog ==========
class Music(commands.Cog):
    """Complete Music Cog with Universal Cloud Storage Support"""
    
    def __init__(self, bot):
        self.bot = bot
        self.players: Dict[int, MusicPlayer] = {}
        
        # Initialize database with migration
        self.init_database()
        
        # Start background tasks
        self.cache_cleanup_task.start()
        logger.info("Music cog initialized with universal cloud storage support")
    
    def init_database(self):
        """Initialize SQLite database with migration support"""
        db_path = "data/music_bot.db"
        Path("data").mkdir(parents=True, exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # ========== MIGRATION: Check and update existing tables ==========
        
        # First, check if track_stats table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='track_stats'")
        if cursor.fetchone():
            # Table exists, check columns
            cursor.execute("PRAGMA table_info(track_stats)")
            columns = {row[1]: row for row in cursor.fetchall()}
            
            # Add missing columns
            if 'service' not in columns:
                cursor.execute('ALTER TABLE track_stats ADD COLUMN service TEXT DEFAULT "unknown"')
                logger.info("Added 'service' column to track_stats table")
            
            if 'genre' not in columns:
                cursor.execute('ALTER TABLE track_stats ADD COLUMN genre TEXT')
                logger.info("Added 'genre' column to track_stats table")
            
            if 'is_cached' not in columns:
                cursor.execute('ALTER TABLE track_stats ADD COLUMN is_cached INTEGER DEFAULT 0')
                logger.info("Added 'is_cached' column to track_stats table")
            
            if 'cache_path' not in columns:
                cursor.execute('ALTER TABLE track_stats ADD COLUMN cache_path TEXT')
                logger.info("Added 'cache_path' column to track_stats table")
            
            if 'last_cached' not in columns:
                cursor.execute('ALTER TABLE track_stats ADD COLUMN last_cached TEXT')
                logger.info("Added 'last_cached' column to track_stats table")
            
            if 'last_played' not in columns:
                cursor.execute('ALTER TABLE track_stats ADD COLUMN last_played TEXT')
                logger.info("Added 'last_played' column to track_stats table")
            
            if 'plays' not in columns:
                cursor.execute('ALTER TABLE track_stats ADD COLUMN plays INTEGER DEFAULT 0')
                logger.info("Added 'plays' column to track_stats table")
            
            if 'skips' not in columns:
                cursor.execute('ALTER TABLE track_stats ADD COLUMN skips INTEGER DEFAULT 0')
                logger.info("Added 'skips' column to track_stats table")
        
        else:
            # Create new track_stats table
            cursor.execute('''
                CREATE TABLE track_stats (
                    filename TEXT PRIMARY KEY,
                    title TEXT NOT NULL DEFAULT 'Unknown Title',
                    artist TEXT NOT NULL DEFAULT 'Unknown Artist',
                    genre TEXT DEFAULT 'Unknown',
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
            logger.info("Created track_stats table with default values")
        
        # Check if playlists table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='playlists'")
        if not cursor.fetchone():
            cursor.execute('''
                CREATE TABLE playlists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(name, user_id)
                )
            ''')
            logger.info("Created playlists table")
        
        # Check if playlist_tracks table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='playlist_tracks'")
        if cursor.fetchone():
            # Check if it's the old version with track_id
            cursor.execute("PRAGMA table_info(playlist_tracks)")
            columns = {row[1]: row for row in cursor.fetchall()}
            
            if 'track_id' in columns and 'track_filename' not in columns:
                # Old version, drop and recreate
                cursor.execute('DROP TABLE playlist_tracks')
                logger.info("Dropped old playlist_tracks table with track_id column")
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
                logger.info("Created new playlist_tracks table with track_filename")
            elif 'track_filename' not in columns:
                # Some other issue, drop and recreate
                cursor.execute('DROP TABLE playlist_tracks')
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
                logger.info("Recreated playlist_tracks table")
        else:
            # Create new playlist_tracks table
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
        
        # Create indexes for better performance
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_track_stats_filename ON track_stats(filename)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_track_stats_title ON track_stats(title)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_track_stats_artist ON track_stats(artist)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_playlists_user_id ON playlists(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_playlist_tracks_playlist_id ON playlist_tracks(playlist_id)')
            logger.info("Created database indexes")
        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
        
        conn.commit()
        conn.close()
        logger.info("Database initialization complete")
    
    def get_player(self, guild_id: int) -> MusicPlayer:
        """Get or create music player for guild"""
        if guild_id not in self.players:
            self.players[guild_id] = MusicPlayer(self.bot, guild_id)
        return self.players[guild_id]
    
    # ========== SLASH COMMANDS with Autocomplete ==========
    
    async def play_autocomplete(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        """Autocomplete for /play command"""
        if not current or len(current) < 2:
            return []
        
        try:
            tracks = await self.search_tracks(current, limit=25)
            choices = []
            
            for track in tracks:
                # Create a display name
                display_name = f"{track['title']} - {track.get('artist', 'Unknown')}"
                if len(display_name) > 100:
                    display_name = display_name[:97] + "..."
                
                choices.append(
                    app_commands.Choice(
                        name=display_name,
                        value=track['title']  # Use title as value for searching
                    )
                )
            
            return choices[:25]  # Discord limit
        
        except Exception as e:
            logger.error(f"Autocomplete error: {e}")
            return []
    
    @commands.hybrid_command(name="play", description="Play a track from the library")
    @app_commands.describe(query="Track name, artist, or search query")
    @app_commands.autocomplete(query=play_autocomplete)
    async def play(self, ctx: commands.Context, *, query: str):
        """Play music from the library"""
        
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
    
    @commands.hybrid_command(name="add", description="Add a track from any cloud storage")
    @app_commands.describe(
        title="Title of the track",
        artist="Artist name",
        link="Share link (Dropbox, Google Drive, etc.)",
        genre="Genre (optional)"
    )
    async def add_track(
        self, 
        ctx: commands.Context, 
        title: str, 
        artist: str, 
        link: str,
        genre: str = "Unknown"
    ):
        """Manually add a track from any cloud storage"""
        
        # Validate link
        if not link.startswith('http'):
            embed = discord.Embed(
                title="❌ Invalid Link",
                description="Please provide a valid HTTP/HTTPS link",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        await ctx.defer()
        
        # Create safe filename
        safe_title = re.sub(r'[^\w\s\-\.\(\)\[\]]', '', title)
        safe_artist = re.sub(r'[^\w\s\-\.\(\)\[\]]', '', artist)
        filename = f"{safe_artist} - {safe_title}.mp3"
        
        # Show processing message
        embed = discord.Embed(
            title="🔗 Processing Link...",
            description="Resolving cloud storage link...",
            color=discord.Color.blue()
        )
        embed.add_field(name="Title", value=title, inline=False)
        embed.add_field(name="Artist", value=artist, inline=True)
        embed.add_field(name="Genre", value=genre, inline=True)
        embed.add_field(name="Link", value=f"[Click to open]({link})", inline=False)
        
        msg = await ctx.send(embed=embed)
        
        try:
            # Resolve the link
            async with CloudStorageResolver() as resolver:
                # Identify service
                service = resolver.identify_service(link)
                embed.add_field(name="Service", value=service.replace('_', ' ').title(), inline=True)
                await msg.edit(embed=embed)
                
                direct_link = await resolver.resolve_link(link)
                
                if not direct_link:
                    embed = discord.Embed(
                        title="❌ Link Resolution Failed",
                        description="Could not get direct download link. Possible reasons:\n\n"
                                  "1. **Invalid or expired link**\n"
                                  "2. **File requires authentication**\n"
                                  "3. **Service not supported**\n"
                                  "4. **File is too large**\n\n"
                                  "**Try these fixes:**\n"
                                  "1. Make sure the link is publicly accessible\n"
                                  "2. Try a different cloud service\n"
                                  "3. Use a direct download link if possible",
                        color=discord.Color.red()
                    )
                    await msg.edit(embed=embed)
                    return
                
                logger.info(f"Resolved link: {direct_link[:100]}...")
                
                # Check if track already exists
                existing = await self.get_track_by_filename(filename)
                if existing:
                    embed = discord.Embed(
                        title="⚠️ Track Already Exists",
                        description=f"**{existing['title']}** is already in the library",
                        color=discord.Color.orange()
                    )
                    await msg.edit(embed=embed)
                    return
                
                # Test the direct link
                test_embed = discord.Embed(
                    title="🔍 Testing Download Link...",
                    description="Verifying the resolved link works...",
                    color=discord.Color.blue()
                )
                await msg.edit(embed=test_embed)
                
                async with aiohttp.ClientSession() as session:
                    async with session.head(direct_link, allow_redirects=True, timeout=10) as test_response:
                        if test_response.status != 200:
                            embed = discord.Embed(
                                title="❌ Download Link Invalid",
                                description=f"The resolved download link doesn't work.\n"
                                          f"Status: {test_response.status}",
                                color=discord.Color.red()
                            )
                            await msg.edit(embed=embed)
                            return
                
                # Add to database
                async with aiosqlite.connect("data/music_bot.db") as db:
                    # Check if service column exists, if not add it
                    cursor = await db.execute("PRAGMA table_info(track_stats)")
                    columns = {row[1] for row in await cursor.fetchall()}
                    
                    if 'service' in columns:
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
                    else:
                        # Fallback without service column
                        await db.execute('''
                            INSERT INTO track_stats 
                            (filename, title, artist, genre, direct_link, added_date)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            filename,
                            title,
                            artist,
                            genre,
                            direct_link,
                            datetime.now().isoformat()
                        ))
                    await db.commit()
                
                # Add to JSON index
                await self._add_to_json_index({
                    'filename': filename,
                    'title': title,
                    'artist': artist,
                    'genre': genre,
                    'direct_link': direct_link,
                    'service': service,
                    'added_date': datetime.now().isoformat()
                })
                
                # Success message
                embed = discord.Embed(
                    title="✅ Track Added Successfully!",
                    description=f"**{title}** by {artist}",
                    color=discord.Color.green()
                )
                embed.add_field(name="Service", value=service.replace('_', ' ').title(), inline=True)
                embed.add_field(name="Status", value="✅ Ready to play", inline=True)
                embed.add_field(name="Cache", value="⏳ Will cache on first play", inline=True)
                embed.set_footer(text="Use /play to play this track")
                
                await msg.edit(embed=embed)
                
        except Exception as e:
            logger.error(f"Failed to add track: {e}", exc_info=True)
            embed = discord.Embed(
                title="❌ Failed to Add Track",
                description=f"Error: {str(e)[:500]}\n\nPlease check your link and try again.",
                color=discord.Color.red()
            )
            await msg.edit(embed=embed)
    
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
                # Identify service
                service = resolver.identify_service(link)
                
                # Update embed
                embed.add_field(name="Detected Service", value=service.replace('_', ' ').title(), inline=True)
                await msg.edit(embed=embed)
                
                # Resolve link
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
                                description="This link can be used with `/add` command",
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
    
    # ========== TEXT COMMANDS ==========
    
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
        
        if not player.queue and not player.current_track:
            embed = discord.Embed(
                title="Queue is Empty",
                description="Add tracks with `/play`",
                color=discord.Color.blue()
            )
            await ctx.send(embed=embed)
            return
        
        embed = discord.Embed(
            title="📋 Music Queue",
            color=discord.Color.blue()
        )
        
        # Current track
        if player.current_track:
            status = "▶️ Playing" if player.is_playing else "⏸️ Paused"
            embed.add_field(
                name=f"{status}",
                value=f"**{player.current_track['title']}** by {player.current_track.get('artist', 'Unknown')}",
                inline=False
            )
        
        # Queue
        if player.queue:
            queue_text = ""
            for i, track in enumerate(player.queue[:10], 1):
                cache_status = "✅" if player.is_cached(track['filename']) else "⏳"
                queue_text += f"`{i}.` {cache_status} **{track['title'][:40]}** - {track.get('artist', 'Unknown')[:20]}\n"
            
            if len(player.queue) > 10:
                queue_text += f"\n... and {len(player.queue) - 10} more tracks"
            
            embed.add_field(name="Up Next", value=queue_text, inline=False)
        
        embed.set_footer(text=f"Total: {len(player.queue)} tracks | Loop: {player.loop_mode}")
        await ctx.send(embed=embed)
    
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
        removed = await player.remove_from_queue(positions_list)
        
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
    
    # ========== PLAYLIST COMMANDS ==========
    
    @commands.group(name="playlist", invoke_without_command=True)
    async def playlist(self, ctx: commands.Context):
        """Playlist management commands"""
        embed = discord.Embed(
            title="📁 Playlist Commands",
            description="Available playlist commands:",
            color=discord.Color.blue()
        )
        embed.add_field(
            name="Create/Manage",
            value="`create`, `add`, `remove`, `delete`, `list`, `show`",
            inline=False
        )
        embed.add_field(
            name="Play/Preload",
            value="`play`, `preload`, `preloadstatus`",
            inline=False
        )
        await ctx.send(embed=embed)
    
    @playlist.command(name="create", description="Create a new playlist")
    async def playlist_create(self, ctx: commands.Context, name: str):
        """Create a new playlist"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                # Check if playlist already exists
                cursor = await db.execute(
                    "SELECT id FROM playlists WHERE name = ? AND user_id = ?",
                    (name, ctx.author.id)
                )
                existing = await cursor.fetchone()
                
                if existing:
                    embed = discord.Embed(
                        title="❌ Playlist Exists",
                        description=f"You already have a playlist named '{name}'",
                        color=discord.Color.orange()
                    )
                    await ctx.send(embed=embed)
                    return
                
                # Create playlist
                await db.execute(
                    "INSERT INTO playlists (name, user_id) VALUES (?, ?)",
                    (name, ctx.author.id)
                )
                await db.commit()
                
                embed = discord.Embed(
                    title="✅ Playlist Created",
                    description=f"Created playlist: **{name}**",
                    color=discord.Color.green()
                )
                embed.set_footer(text=f"Owner: {ctx.author.display_name}")
                await ctx.send(embed=embed)
                
        except Exception as e:
            logger.error(f"Failed to create playlist: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description=f"Failed to create playlist: {e}",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
    
    @playlist.command(name="add", description="Add track to playlist")
    async def playlist_add(self, ctx: commands.Context, playlist_name: str, *, track_query: str):
        """Add track to playlist"""
        await ctx.defer()
        
        # Search for track
        tracks = await self.search_tracks(track_query, limit=1)
        if not tracks:
            embed = discord.Embed(
                title="❌ Track Not Found",
                description=f"No track found for: {track_query}",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        track = tracks[0]
        
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                # Get playlist ID
                cursor = await db.execute(
                    "SELECT id FROM playlists WHERE name = ? AND user_id = ?",
                    (playlist_name, ctx.author.id)
                )
                playlist = await cursor.fetchone()
                
                if not playlist:
                    embed = discord.Embed(
                        title="❌ Playlist Not Found",
                        description=f"You don't have a playlist named '{playlist_name}'",
                        color=discord.Color.red()
                    )
                    await ctx.send(embed=embed)
                    return
                
                playlist_id = playlist[0]
                
                # Check if track exists in database
                cursor = await db.execute(
                    "SELECT filename FROM track_stats WHERE filename = ?",
                    (track['filename'],)
                )
                existing_track = await cursor.fetchone()
                
                if not existing_track:
                    # Add track to database first
                    await db.execute(
                        """
                        INSERT INTO track_stats (filename, title, artist, direct_link)
                        VALUES (?, ?, ?, ?)
                        """,
                        (track['filename'], track['title'], track.get('artist', 'Unknown'), track.get('direct_link', ''))
                    )
                    await db.commit()
                
                # Check if track already in playlist
                cursor = await db.execute(
                    "SELECT 1 FROM playlist_tracks WHERE playlist_id = ? AND track_filename = ?",
                    (playlist_id, track['filename'])
                )
                existing = await cursor.fetchone()
                
                if existing:
                    embed = discord.Embed(
                        title="⚠️ Track Already in Playlist",
                        description=f"'{track['title']}' is already in '{playlist_name}'",
                        color=discord.Color.orange()
                    )
                    await ctx.send(embed=embed)
                    return
                
                # Get next position
                cursor = await db.execute(
                    "SELECT MAX(position) FROM playlist_tracks WHERE playlist_id = ?",
                    (playlist_id,)
                )
                max_pos = await cursor.fetchone()
                next_pos = (max_pos[0] or 0) + 1
                
                # Add to playlist
                await db.execute(
                    "INSERT INTO playlist_tracks (playlist_id, track_filename, position) VALUES (?, ?, ?)",
                    (playlist_id, track['filename'], next_pos)
                )
                await db.commit()
                
                embed = discord.Embed(
                    title="✅ Track Added to Playlist",
                    description=f"Added **{track['title']}** to **{playlist_name}**",
                    color=discord.Color.green()
                )
                embed.add_field(name="Position", value=str(next_pos), inline=True)
                embed.add_field(name="Artist", value=track.get('artist', 'Unknown'), inline=True)
                await ctx.send(embed=embed)
                
        except Exception as e:
            logger.error(f"Failed to add track to playlist: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description=f"Failed to add track: {e}",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
    
    @playlist.command(name="list", description="List your playlists")
    async def playlist_list(self, ctx: commands.Context):
        """List all your playlists"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                cursor = await db.execute(
                    """
                    SELECT p.name, COUNT(pt.track_filename) as track_count
                    FROM playlists p
                    LEFT JOIN playlist_tracks pt ON p.id = pt.playlist_id
                    WHERE p.user_id = ?
                    GROUP BY p.id
                    ORDER BY p.name
                    """,
                    (ctx.author.id,)
                )
                playlists = await cursor.fetchall()
                
                if not playlists:
                    embed = discord.Embed(
                        title="📁 No Playlists",
                        description="You haven't created any playlists yet.\nUse `e!playlist create <name>` to create one.",
                        color=discord.Color.blue()
                    )
                    await ctx.send(embed=embed)
                    return
                
                embed = discord.Embed(
                    title="📁 Your Playlists",
                    description=f"Found {len(playlists)} playlist(s)",
                    color=discord.Color.blue()
                )
                
                for name, track_count in playlists:
                    embed.add_field(
                        name=name,
                        value=f"🎵 {track_count} tracks",
                        inline=True
                    )
                
                await ctx.send(embed=embed)
                
        except Exception as e:
            logger.error(f"Failed to list playlists: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description=f"Failed to list playlists: {e}",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
    
    @playlist.command(name="play", description="Play a playlist")
    async def playlist_play(self, ctx: commands.Context, playlist_name: str, shuffle: bool = False):
        """Play a playlist"""
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
        
        # Join voice
        if not await player.join_voice(ctx.author.voice.channel):
            embed = discord.Embed(
                title="❌ Connection Failed",
                description="Could not join voice channel",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        # Get playlist tracks
        tracks = await self.get_playlist_tracks(ctx.author.id, playlist_name)
        if not tracks:
            embed = discord.Embed(
                title="❌ Playlist Empty or Not Found",
                description=f"Playlist '{playlist_name}' is empty or doesn't exist",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        # Shuffle if requested
        if shuffle:
            random.shuffle(tracks)
        
        # Add tracks to queue or play immediately
        if player.is_playing or player.is_paused:
            player.queue.extend(tracks)
            embed = discord.Embed(
                title="✅ Playlist Added to Queue",
                description=f"Added **{playlist_name}** ({len(tracks)} tracks) to queue",
                color=discord.Color.green()
            )
            
            if shuffle:
                embed.add_field(name="Shuffled", value="✅ Yes", inline=True)
            
            await ctx.send(embed=embed)
        else:
            # Play first track, add rest to queue
            first_track = tracks.pop(0)
            player.queue.extend(tracks)
            
            await player.play_track(first_track, ctx.interaction)
            
            # Send playlist info
            embed = discord.Embed(
                title="🎵 Playing Playlist",
                description=f"**{playlist_name}** ({len(tracks) + 1} tracks)",
                color=discord.Color.green()
            )
            
            if shuffle:
                embed.add_field(name="Shuffled", value="✅ Yes", inline=True)
            
            embed.set_footer(text=f"First track: {first_track['title']}")
            await ctx.send(embed=embed)
    
    @playlist.command(name="preload", description="Preload playlist to cache")
    async def playlist_preload(self, ctx: commands.Context, playlist_name: str):
        """Preload a playlist to cache for smooth playback"""
        await ctx.defer()
        
        player = self.get_player(ctx.guild.id)
        
        # Check if already preloading
        if playlist_name in player.preloading and player.preloading[playlist_name]['status'] == 'preloading':
            embed = player.get_preload_progress_embed(playlist_name)
            await ctx.send(embed=embed)
            return
        
        # Get playlist tracks
        tracks = await self.get_playlist_tracks(ctx.author.id, playlist_name)
        if not tracks:
            embed = discord.Embed(
                title="❌ Playlist Empty or Not Found",
                description=f"Playlist '{playlist_name}' is empty or doesn't exist",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        # Send starting message
        embed = discord.Embed(
            title="🔄 Starting Preload",
            description=f"Preloading **{playlist_name}** ({len(tracks)} tracks) to cache...",
            color=discord.Color.blue()
        )
        embed.add_field(name="Status", value="Initializing...", inline=True)
        embed.set_footer(text="Use e!playlist preloadstatus to check progress")
        
        status_msg = await ctx.send(embed=embed)
        
        # Start preload in background
        async def preload_task():
            try:
                result = await player.preload_playlist(tracks, playlist_name)
                
                # Send completion message
                final_embed = player.get_preload_progress_embed(playlist_name)
                await status_msg.edit(embed=final_embed)
                
                # Also send to channel
                await ctx.channel.send(
                    f"✅ Preload complete for **{playlist_name}**!",
                    embed=final_embed
                )
                
            except Exception as e:
                logger.error(f"Preload task failed: {e}")
                error_embed = discord.Embed(
                    title="❌ Preload Failed",
                    description=f"Error preloading playlist: {e}",
                    color=discord.Color.red()
                )
                await status_msg.edit(embed=error_embed)
        
        # Start task
        asyncio.create_task(preload_task())
    
    @playlist.command(name="preloadstatus", description="Check preload progress")
    async def playlist_preloadstatus(self, ctx: commands.Context, playlist_name: str = None):
        """Check preload progress for playlists"""
        player = self.get_player(ctx.guild.id)
        
        if playlist_name:
            # Show specific playlist preload status
            embed = player.get_preload_progress_embed(playlist_name)
            await ctx.send(embed=embed)
        else:
            # Show all preloads
            if not player.preloading:
                embed = discord.Embed(
                    title="📊 No Active Preloads",
                    description="No playlists are currently being preloaded",
                    color=discord.Color.blue()
                )
                await ctx.send(embed=embed)
                return
            
            embed = discord.Embed(
                title="📊 Active Preloads",
                description=f"Found {len(player.preloading)} preload(s)",
                color=discord.Color.blue()
            )
            
            for name, status in player.preloading.items():
                if status['status'] == 'completed':
                    status_text = f"✅ Completed: {status['cached']}/{status['total']} cached"
                else:
                    status_text = f"🔄 Preloading: {status['progress']}/{status['total']} ({status['progress']/status['total']*100:.1f}%)"
                
                embed.add_field(
                    name=name,
                    value=status_text,
                    inline=False
                )
            
            await ctx.send(embed=embed)
    
    # ========== HELPER METHODS ==========
    
    async def search_tracks(self, query: str, limit: int = 25) -> List[Dict]:
        """Search for multiple tracks in index"""
        try:
            # Load index
            index_file = "data/music_index.json"
            if not Path(index_file).exists():
                # Create initial index from database
                await self._create_initial_index()
                return []
            
            with open(index_file, 'r', encoding='utf-8') as f:
                index = json.load(f)
            
            # Clean query
            query = query.lower().strip()
            
            if not query:
                # Return some random tracks for empty query
                return random.sample(index, min(10, len(index))) if index else []
            
            # Score each track
            scored_tracks = []
            
            for track in index:
                score = 0
                
                # Exact filename match
                if track.get('filename', '').lower() == query:
                    score += 100
                
                # Exact title match
                if track.get('title', '').lower() == query:
                    score += 50
                
                # Partial matches
                if query in track.get('title', '').lower():
                    score += 30
                
                if track.get('artist') and query in track.get('artist', '').lower():
                    score += 20
                
                if track.get('filename') and query in track.get('filename', '').lower():
                    score += 10
                
                if track.get('genre') and query in track.get('genre', '').lower():
                    score += 5
                
                if score > 0:
                    scored_tracks.append((score, track))
            
            # Sort by score
            scored_tracks.sort(key=lambda x: x[0], reverse=True)
            
            # Return top results
            return [track for score, track in scored_tracks[:limit]]
            
        except Exception as e:
            logger.error(f"Search error: {e}")
            return []
    
    async def _create_initial_index(self):
        """Create initial index from database"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                cursor = await db.execute('''
                    SELECT filename, title, artist, genre, direct_link, service, added_date
                    FROM track_stats
                ''')
                
                rows = await cursor.fetchall()
                
                index = []
                for row in rows:
                    index.append({
                        'filename': row[0],
                        'title': row[1] or "Unknown Title",
                        'artist': row[2] or "Unknown Artist",
                        'genre': row[3] or "Unknown",
                        'direct_link': row[4] or '',
                        'service': row[5] or 'unknown',
                        'added_date': row[6] or datetime.now().isoformat(),
                        'source': 'database'
                    })
                
                # Save to file
                with open("data/music_index.json", 'w', encoding='utf-8') as f:
                    json.dump(index, f, indent=2, ensure_ascii=False)
                
                logger.info(f"Created initial index with {len(index)} tracks")
                
        except Exception as e:
            logger.error(f"Failed to create initial index: {e}")
    
    async def get_track_by_filename(self, filename: str) -> Optional[Dict]:
        """Get track by filename from database"""
        try:
            async with aiosqlite.connect("data/music_bot.db") as db:
                cursor = await db.execute('''
                    SELECT filename, title, artist, genre, direct_link, service
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
                        'service': row[5] or 'unknown'
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
                    SELECT ts.filename, ts.title, ts.artist, ts.direct_link, ts.genre, ts.service
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
                for filename, title, artist, direct_link, genre, service in rows:
                    tracks.append({
                        'filename': filename,
                        'title': title or "Unknown Title",
                        'artist': artist or "Unknown Artist",
                        'direct_link': direct_link or '',
                        'genre': genre or "Unknown",
                        'service': service or 'unknown'
                    })
                
                return tracks
                
        except Exception as e:
            logger.error(f"Failed to get playlist tracks: {e}")
            return []
    
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
    
    # ========== BACKGROUND TASKS ==========
    
    @tasks.loop(hours=6)
    async def cache_cleanup_task(self):
        """Automatically clean up cache when over limit"""
        try:
            await self.cleanup_cache()
        except Exception as e:
            logger.error(f"Cache cleanup task failed: {e}")
    
    async def cleanup_cache(self):
        """Clean up cache based on track scores"""
        try:
            db_path = "data/music_bot.db"
            async with aiosqlite.connect(db_path) as db:
                # Get tracks with cache info, ordered by score (plays - skips) and last played
                cursor = await db.execute('''
                    SELECT filename, cache_path, plays, skips, last_played, 
                           (COALESCE(plays, 0) - COALESCE(skips, 0)) as score,
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
    
    # ========== EVENT LISTENERS ==========
    
    @commands.Cog.listener()
    async def on_voice_state_update(self, member, before, after):
        """Handle voice state updates for auto-disconnect"""
        if member == self.bot.user and before.channel and not after.channel:
            # Bot was disconnected
            guild_id = before.channel.guild.id
            if guild_id in self.players:
                player = self.players[guild_id]
                player.is_playing = False
                player.is_paused = False
                
                # Clear messages
                if player.now_playing_message:
                    try:
                        await player.now_playing_message.delete()
                    except:
                        pass
                    player.now_playing_message = None
    
    async def cog_unload(self):
        """Cleanup on cog unload"""
        # Stop background tasks
        self.cache_cleanup_task.cancel()
        
        # Disconnect all voice clients
        for player in self.players.values():
            if player.voice_client and player.voice_client.is_connected():
                await player.voice_client.disconnect()
        
        logger.info("Music cog unloaded")

async def setup(bot):
    """Setup function for the cog"""
    await bot.add_cog(Music(bot))
    logger.info("✅ Complete Music cog loaded successfully with universal cloud storage support and autocomplete")

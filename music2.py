import discord
from discord.ext import commands, tasks
from discord import app_commands
import asyncio
import aiohttp
import aiosqlite
import json
import os
import re
import time
import hashlib
import subprocess
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple, Set, Union
import logging
from pathlib import Path
import yarl
from dataclasses import dataclass
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
MAX_CACHE_SIZE = int(os.getenv('MAX_CACHE_SIZE', 10 * 1024 * 1024 * 1024))  # 10GB default
DOWNLOAD_SPEED = int(os.getenv('DOWNLOAD_SPEED', 2 * 1024 * 1024))  # 2MB/s default
DATA_DIR = Path("data")
MUSIC_DIR = DATA_DIR / "musics"
CACHE_DIR = MUSIC_DIR / "cache"
INDEX_FILE = DATA_DIR / "music_index.json"
DB_FILE = DATA_DIR / "music.db"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
MUSIC_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

# Supported audio formats
SUPPORTED_FORMATS = {
    '.mp3', '.mp4', '.m4a', '.flac', '.wav', '.ogg', '.aac', '.webm',
    '.wma', '.opus', '.mka', '.mkv', '.mov', '.avi', '.mpg', '.mpeg',
    '.wmv', '.3gp', '.aiff', '.alac', '.amr', '.au', '.mid', '.midi',
    '.ra', '.rm', '.swf'
}

# Cloud storage regex patterns
CLOUD_PATTERNS = {
    'dropbox': r'https?://(?:www\.)?dropbox\.com/[sh]/',
    'google_drive': r'https?://drive\.google\.com/',
    'mediafire': r'https?://(?:www\.)?mediafire\.com/',
    'mega': r'https?://mega\.nz/',
    'onedrive': r'https?://(?:onedrive\.live\.com|1drv\.ms)/',
    'terabox': r'https?://(?:www\.)?terabox\.com/',
    'pixeldrain': r'https?://(?:www\.)?pixeldrain\.com/',
    'github': r'https?://(?:raw\.)?github(?:usercontent)?\.com/',
    'zippyshare': r'https?://(?:www\.\d+\.)?zippyshare\.com/',
}

# Emojis for UI
EMOJIS = {
    'music': '🎵',
    'queue': '📋',
    'pause': '⏸️',
    'play': '▶️',
    'skip': '⏭️',
    'previous': '⏮️',
    'shuffle': '🔀',
    'loop': '🔁',
    'volume': '🔊',
    'success': '✅',
    'loading': '⏳',
    'error': '❌',
    'warning': '⚠️',
    'info': 'ℹ️',
    'stop': '⏹️',
    'check': '✓',
    'cross': '✗',
}

# Custom Exceptions
class MusicError(Exception):
    """Base exception for music bot errors"""
    pass

class LinkResolveError(MusicError):
    """Failed to resolve download link"""
    pass

class DownloadError(MusicError):
    """Failed to download audio file"""
    pass

class CacheFullError(MusicError):
    """Cache is full and cannot accommodate more files"""
    pass

class FormatNotSupported(MusicError):
    """Audio format not supported"""
    pass

# Data Classes
@dataclass
class TrackInfo:
    """Information about a music track"""
    filename: str
    title: str
    artist: str
    genre: Optional[str] = None
    description: Optional[str] = None
    direct_link: Optional[str] = None
    service: Optional[str] = None
    plays: int = 0
    skips: int = 0
    is_cached: bool = False
    cache_path: Optional[str] = None
    last_cached: Optional[str] = None
    last_played: Optional[str] = None
    added_date: Optional[str] = None
    
    @property
    def display_name(self) -> str:
        """Get display name for the track"""
        return f"{self.title} - {self.artist}"
    
    @property
    def score(self) -> int:
        """Calculate track score for cache prioritization"""
        # Base score: plays - (skips * 2) + recency bonus
        score = self.plays - (self.skips * 2)
        
        # Add recency bonus (more recent = higher score)
        if self.last_played:
            try:
                last_played_dt = datetime.fromisoformat(self.last_played)
                days_ago = (datetime.now() - last_played_dt).days
                recency_bonus = max(0, 30 - days_ago)  # Bonus up to 30 days
                score += recency_bonus
            except:
                pass
        
        # Add cache bonus if currently cached
        if self.is_cached:
            score += 10
        
        return score

@dataclass
class Playlist:
    """Playlist information"""
    id: int
    name: str
    user_id: int
    description: Optional[str] = None
    created_at: Optional[str] = None
    tracks: List[TrackInfo] = None
    
    def __post_init__(self):
        if self.tracks is None:
            self.tracks = []

@dataclass
class PlayerState:
    """State of a music player in a guild"""
    guild_id: int
    voice_channel: Optional[discord.VoiceChannel] = None
    text_channel: Optional[discord.TextChannel] = None
    voice_client: Optional[discord.VoiceClient] = None
    current_track: Optional[TrackInfo] = None
    queue: List[TrackInfo] = []
    history: List[TrackInfo] = []
    volume: float = 0.5
    loop_mode: str = 'off'  # off, track, queue
    is_playing: bool = False
    is_paused: bool = False
    now_playing_message: Optional[discord.Message] = None
    control_view: Optional[discord.ui.View] = None
    last_activity: datetime = None
    
    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = datetime.now()
    
    @property
    def queue_size(self) -> int:
        """Get queue size"""
        return len(self.queue)
    
    def add_to_queue(self, track: TrackInfo):
        """Add track to queue"""
        self.queue.append(track)
        self.update_activity()
    
    def remove_from_queue(self, position: int) -> Optional[TrackInfo]:
        """Remove track from queue by position (1-indexed)"""
        if 1 <= position <= len(self.queue):
            return self.queue.pop(position - 1)
        return None
    
    def clear_queue(self):
        """Clear all tracks from queue"""
        self.queue.clear()
        self.update_activity()
    
    def shuffle_queue(self):
        """Shuffle the queue"""
        random.shuffle(self.queue)
        self.update_activity()
    
    def get_next_track(self) -> Optional[TrackInfo]:
        """Get next track from queue based on loop mode"""
        if self.loop_mode == 'track' and self.current_track:
            return self.current_track
        
        if self.queue:
            track = self.queue.pop(0)
            if self.loop_mode == 'queue':
                self.queue.append(track)  # Add to end for queue loop
            return track
        
        return None

# Link Resolver Base Class
class LinkResolver:
    """Base class for resolving cloud storage links"""
    
    def __init__(self):
        self.session = None
        self.cache: Dict[str, Tuple[str, float]] = {}  # url -> (resolved_url, expiry)
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def resolve(self, url: str) -> Optional[str]:
        """Resolve a URL to direct download link"""
        # Check cache first
        if url in self.cache:
            resolved_url, expiry = self.cache[url]
            if time.time() < expiry:
                return resolved_url
        
        # Detect service
        service = self.detect_service(url)
        if not service:
            return None
        
        # Resolve based on service
        resolved_url = await self._resolve_service(url, service)
        
        if resolved_url:
            # Cache for 1 hour
            self.cache[url] = (resolved_url, time.time() + 3600)
        
        return resolved_url
    
    def detect_service(self, url: str) -> Optional[str]:
        """Detect cloud storage service from URL"""
        for service, pattern in CLOUD_PATTERNS.items():
            if re.match(pattern, url, re.IGNORECASE):
                return service
        return None
    
    async def _resolve_service(self, url: str, service: str) -> Optional[str]:
        """Resolve URL for specific service"""
        try:
            if service == 'dropbox':
                return await self._resolve_dropbox(url)
            elif service == 'google_drive':
                return await self._resolve_google_drive(url)
            elif service == 'mediafire':
                return await self._resolve_mediafire(url)
            elif service == 'mega':
                return await self._resolve_mega(url)
            # Add other services as needed
            else:
                # For services without specific resolver, try to get direct link
                return await self._get_direct_link(url)
        except Exception as e:
            logger.error(f"Error resolving {service} link {url}: {e}")
            return None
    
    async def _resolve_dropbox(self, url: str) -> Optional[str]:
        """Resolve Dropbox share link to direct download"""
        # Dropbox: change ?dl=0 to ?dl=1 or ?raw=1
        if '?dl=0' in url:
            return url.replace('?dl=0', '?dl=1')
        elif '?raw=0' in url:
            return url.replace('?raw=0', '?raw=1')
        elif 'dropbox.com/s/' in url and '?' not in url:
            return f"{url}?dl=1"
        return url
    
    async def _resolve_google_drive(self, url: str) -> Optional[str]:
        """Resolve Google Drive link to direct download"""
        # Pattern: https://drive.google.com/file/d/FILE_ID/view
        file_id_match = re.search(r'/d/([a-zA-Z0-9_-]+)', url)
        if file_id_match:
            file_id = file_id_match.group(1)
            return f"https://drive.google.com/uc?export=download&id={file_id}"
        return url
    
    async def _resolve_mediafire(self, url: str) -> Optional[str]:
        """Resolve MediaFire link"""
        session = await self.get_session()
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    # Look for direct download link
                    direct_match = re.search(r'href="(https?://download[^"]+)"', html)
                    if direct_match:
                        return direct_match.group(1)
        except:
            pass
        return url
    
    async def _resolve_mega(self, url: str) -> Optional[str]:
        """Resolve MEGA link"""
        # MEGA links are already direct for downloading
        return url.replace('/#!', '/file/').replace('!', '#')
    
    async def _get_direct_link(self, url: str) -> Optional[str]:
        """Try to get direct link by following redirects"""
        session = await self.get_session()
        try:
            async with session.head(url, allow_redirects=True) as response:
                return str(response.url)
        except:
            return url
    
    async def test_link(self, url: str) -> Dict[str, Any]:
        """Test a link and return information about it"""
        result = {
            'url': url,
            'service': None,
            'status': 'unknown',
            'direct_url': None,
            'file_size': 0,
            'content_type': None,
            'is_supported': False
        }
        
        try:
            # Detect service
            result['service'] = self.detect_service(url)
            
            # Resolve direct URL
            direct_url = await self.resolve(url)
            result['direct_url'] = direct_url
            
            if not direct_url:
                result['status'] = 'unresolved'
                return result
            
            # Test the direct URL
            session = await self.get_session()
            async with session.head(direct_url) as response:
                result['status'] = 'success' if response.status == 200 else 'failed'
                
                if response.status == 200:
                    result['file_size'] = int(response.headers.get('Content-Length', 0))
                    result['content_type'] = response.headers.get('Content-Type', '')
                    
                    # Check if it's a supported audio format
                    for fmt in SUPPORTED_FORMATS:
                        if fmt in direct_url.lower() or fmt.replace('.', '') in result['content_type'].lower():
                            result['is_supported'] = True
                            break
            
        except Exception as e:
            result['status'] = f'error: {str(e)}'
        
        return result
    
    async def close(self):
        """Close the session"""
        if self.session and not self.session.closed:
            await self.session.close()

# Database Manager
class DatabaseManager:
    """Manages SQLite database operations"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = None
    
    async def connect(self):
        """Connect to database and create tables if needed"""
        self.conn = await aiosqlite.connect(self.db_path)
        await self.create_tables()
    
    async def create_tables(self):
        """Create necessary tables"""
        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS track_stats (
                filename TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                artist TEXT NOT NULL,
                genre TEXT,
                description TEXT,
                direct_link TEXT,
                service TEXT,
                plays INTEGER DEFAULT 0,
                skips INTEGER DEFAULT 0,
                is_cached INTEGER DEFAULT 0,
                cache_path TEXT,
                last_cached TEXT,
                last_played TEXT,
                added_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS playlists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                description TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(name, user_id)
            )
        ''')
        
        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS playlist_tracks (
                playlist_id INTEGER,
                track_filename TEXT,
                position INTEGER,
                added_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (playlist_id) REFERENCES playlists(id) ON DELETE CASCADE,
                FOREIGN KEY (track_filename) REFERENCES track_stats(filename) ON DELETE CASCADE,
                PRIMARY KEY (playlist_id, track_filename)
            )
        ''')
        
        # Create indices for faster searches
        await self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_track_search 
            ON track_stats(title, artist, genre)
        ''')
        
        await self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_playlist_user 
            ON playlists(user_id)
        ''')
        
        await self.conn.commit()
    
    async def add_track(self, track: TrackInfo) -> bool:
        """Add a track to database"""
        try:
            await self.conn.execute('''
                INSERT OR REPLACE INTO track_stats 
                (filename, title, artist, genre, description, direct_link, service, 
                 plays, skips, is_cached, cache_path, last_cached, last_played, added_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                track.filename, track.title, track.artist, track.genre, track.description,
                track.direct_link, track.service, track.plays, track.skips,
                1 if track.is_cached else 0, track.cache_path, track.last_cached,
                track.last_played, track.added_date
            ))
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding track to database: {e}")
            return False
    
    async def get_track(self, filename: str) -> Optional[TrackInfo]:
        """Get track by filename"""
        cursor = await self.conn.execute('''
            SELECT * FROM track_stats WHERE filename = ?
        ''', (filename,))
        
        row = await cursor.fetchone()
        await cursor.close()
        
        if row:
            return TrackInfo(
                filename=row[0],
                title=row[1],
                artist=row[2],
                genre=row[3],
                description=row[4],
                direct_link=row[5],
                service=row[6],
                plays=row[7],
                skips=row[8],
                is_cached=bool(row[9]),
                cache_path=row[10],
                last_cached=row[11],
                last_played=row[12],
                added_date=row[13]
            )
        return None
    
    async def search_tracks(self, query: str, limit: int = 25) -> List[TrackInfo]:
        """Search tracks by query"""
        query = f"%{query}%"
        cursor = await self.conn.execute('''
            SELECT * FROM track_stats 
            WHERE title LIKE ? OR artist LIKE ? OR filename LIKE ? OR genre LIKE ?
            ORDER BY plays DESC
            LIMIT ?
        ''', (query, query, query, query, limit))
        
        rows = await cursor.fetchall()
        await cursor.close()
        
        tracks = []
        for row in rows:
            tracks.append(TrackInfo(
                filename=row[0],
                title=row[1],
                artist=row[2],
                genre=row[3],
                description=row[4],
                direct_link=row[5],
                service=row[6],
                plays=row[7],
                skips=row[8],
                is_cached=bool(row[9]),
                cache_path=row[10],
                last_cached=row[11],
                last_played=row[12],
                added_date=row[13]
            ))
        
        return tracks
    
    async def increment_play(self, filename: str):
        """Increment play count for track"""
        await self.conn.execute('''
            UPDATE track_stats 
            SET plays = plays + 1, last_played = datetime('now')
            WHERE filename = ?
        ''', (filename,))
        await self.conn.commit()
    
    async def increment_skip(self, filename: str):
        """Increment skip count for track"""
        await self.conn.execute('''
            UPDATE track_stats 
            SET skips = skips + 1
            WHERE filename = ?
        ''', (filename,))
        await self.conn.commit()
    
    async def create_playlist(self, name: str, user_id: int, description: str = None) -> Optional[int]:
        """Create a new playlist"""
        try:
            cursor = await self.conn.execute('''
                INSERT INTO playlists (name, user_id, description)
                VALUES (?, ?, ?)
            ''', (name, user_id, description))
            
            await self.conn.commit()
            playlist_id = cursor.lastrowid
            await cursor.close()
            
            return playlist_id
        except Exception as e:
            logger.error(f"Error creating playlist: {e}")
            return None
    
    async def get_playlist(self, playlist_id: int) -> Optional[Playlist]:
        """Get playlist by ID"""
        cursor = await self.conn.execute('''
            SELECT * FROM playlists WHERE id = ?
        ''', (playlist_id,))
        
        row = await cursor.fetchone()
        await cursor.close()
        
        if row:
            playlist = Playlist(
                id=row[0],
                name=row[1],
                user_id=row[2],
                description=row[3],
                created_at=row[4]
            )
            
            # Get tracks for this playlist
            cursor = await self.conn.execute('''
                SELECT ts.* FROM track_stats ts
                JOIN playlist_tracks pt ON ts.filename = pt.track_filename
                WHERE pt.playlist_id = ?
                ORDER BY pt.position
            ''', (playlist_id,))
            
            track_rows = await cursor.fetchall()
            await cursor.close()
            
            for trow in track_rows:
                playlist.tracks.append(TrackInfo(
                    filename=trow[0],
                    title=trow[1],
                    artist=trow[2],
                    genre=trow[3],
                    description=trow[4],
                    direct_link=trow[5],
                    service=trow[6],
                    plays=trow[7],
                    skips=trow[8],
                    is_cached=bool(trow[9]),
                    cache_path=trow[10],
                    last_cached=trow[11],
                    last_played=trow[12],
                    added_date=trow[13]
                ))
            
            return playlist
        
        return None
    
    async def get_user_playlists(self, user_id: int) -> List[Playlist]:
        """Get all playlists for a user"""
        cursor = await self.conn.execute('''
            SELECT * FROM playlists WHERE user_id = ? ORDER BY created_at DESC
        ''', (user_id,))
        
        rows = await cursor.fetchall()
        await cursor.close()
        
        playlists = []
        for row in rows:
            playlist = Playlist(
                id=row[0],
                name=row[1],
                user_id=row[2],
                description=row[3],
                created_at=row[4]
            )
            playlists.append(playlist)
        
        return playlists
    
    async def add_to_playlist(self, playlist_id: int, track_filename: str):
        """Add track to playlist"""
        try:
            # Get current max position
            cursor = await self.conn.execute('''
                SELECT MAX(position) FROM playlist_tracks WHERE playlist_id = ?
            ''', (playlist_id,))
            
            result = await cursor.fetchone()
            await cursor.close()
            
            position = (result[0] or 0) + 1
            
            await self.conn.execute('''
                INSERT OR IGNORE INTO playlist_tracks (playlist_id, track_filename, position)
                VALUES (?, ?, ?)
            ''', (playlist_id, track_filename, position))
            
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding to playlist: {e}")
            return False
    
    async def remove_from_playlist(self, playlist_id: int, track_filename: str):
        """Remove track from playlist"""
        try:
            await self.conn.execute('''
                DELETE FROM playlist_tracks 
                WHERE playlist_id = ? AND track_filename = ?
            ''', (playlist_id, track_filename))
            
            # Update positions
            await self.conn.execute('''
                UPDATE playlist_tracks 
                SET position = position - 1
                WHERE playlist_id = ? AND position > (
                    SELECT position FROM playlist_tracks 
                    WHERE playlist_id = ? AND track_filename = ?
                )
            ''', (playlist_id, playlist_id, track_filename))
            
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error removing from playlist: {e}")
            return False
    
    async def delete_playlist(self, playlist_id: int):
        """Delete a playlist"""
        try:
            await self.conn.execute('DELETE FROM playlists WHERE id = ?', (playlist_id,))
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting playlist: {e}")
            return False
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get library statistics"""
        stats = {}
        
        # Total tracks
        cursor = await self.conn.execute('SELECT COUNT(*) FROM track_stats')
        stats['total_tracks'] = (await cursor.fetchone())[0]
        await cursor.close()
        
        # Cached tracks
        cursor = await self.conn.execute('SELECT COUNT(*) FROM track_stats WHERE is_cached = 1')
        stats['cached_tracks'] = (await cursor.fetchone())[0]
        await cursor.close()
        
        # Total playlists
        cursor = await self.conn.execute('SELECT COUNT(*) FROM playlists')
        stats['total_playlists'] = (await cursor.fetchone())[0]
        await cursor.close()
        
        # Top played tracks
        cursor = await self.conn.execute('''
            SELECT title, artist, plays FROM track_stats 
            ORDER BY plays DESC LIMIT 5
        ''')
        stats['top_tracks'] = await cursor.fetchall()
        await cursor.close()
        
        return stats
    
    async def close(self):
        """Close database connection"""
        if self.conn:
            await self.conn.close()

# Cache Manager
class CacheManager:
    """Manages audio file caching"""
    
    def __init__(self, cache_dir: Path, max_size: int):
        self.cache_dir = cache_dir
        self.max_size = max_size
        self.current_size = 0
        self.download_queue = asyncio.Queue()
        self.active_downloads: Set[str] = set()
        self.download_speed = DOWNLOAD_SPEED
        
    async def initialize(self):
        """Initialize cache manager"""
        await self._calculate_cache_size()
    
    async def _calculate_cache_size(self):
        """Calculate current cache size"""
        self.current_size = 0
        for file_path in self.cache_dir.glob('**/*'):
            if file_path.is_file():
                self.current_size += file_path.stat().st_size
    
    async def get_cache_path(self, filename: str) -> Path:
        """Get cache path for a filename"""
        # Use hash of filename for directory structure
        file_hash = hashlib.md5(filename.encode()).hexdigest()
        subdir = self.cache_dir / file_hash[:2]
        subdir.mkdir(exist_ok=True)
        return subdir / f"{file_hash}.cache"
    
    async def is_cached(self, filename: str) -> bool:
        """Check if file is cached"""
        cache_path = await self.get_cache_path(filename)
        return cache_path.exists()
    
    async def cache_file(self, url: str, filename: str) -> Optional[Path]:
        """Download and cache a file"""
        cache_path = await self.get_cache_path(filename)
        
        if cache_path.exists():
            return cache_path
        
        # Check if we have space
        if not await self._ensure_space():
            raise CacheFullError("Cache is full")
        
        # Download the file
        session = aiohttp.ClientSession()
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    raise DownloadError(f"Failed to download: {response.status}")
                
                file_size = int(response.headers.get('Content-Length', 0))
                
                # Check if file will fit
                if file_size > 0 and self.current_size + file_size > self.max_size:
                    if not await self._make_space(file_size):
                        raise CacheFullError("Not enough space in cache")
                
                # Download with speed limit
                downloaded = 0
                start_time = time.time()
                
                with open(cache_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Speed limiting
                        elapsed = time.time() - start_time
                        expected_time = downloaded / self.download_speed
                        
                        if elapsed < expected_time:
                            await asyncio.sleep(expected_time - elapsed)
                
                # Update cache size
                self.current_size += cache_path.stat().st_size
                
                return cache_path
                
        except Exception as e:
            # Clean up partial download
            if cache_path.exists():
                cache_path.unlink()
            raise DownloadError(f"Failed to cache file: {e}")
        
        finally:
            await session.close()
    
    async def _ensure_space(self) -> bool:
        """Ensure there's space in cache"""
        if self.current_size < self.max_size * 0.8:  # Under 80% full
            return True
        
        return await self._cleanup_cache()
    
    async def _make_space(self, required_size: int) -> bool:
        """Make space for required size"""
        target_size = self.max_size * 0.7  # Target 70% full after cleanup
        
        while self.current_size + required_size > target_size:
            if not await self._remove_lowest_score():
                return False  # Can't make enough space
        
        return True
    
    async def _cleanup_cache(self, target_percent: float = 0.7) -> bool:
        """Cleanup cache to target percentage"""
        target_size = self.max_size * target_percent
        
        while self.current_size > target_size:
            if not await self._remove_lowest_score():
                return False  # Can't cleanup enough
        
        return True
    
    async def _remove_lowest_score(self) -> bool:
        """Remove lowest score track from cache"""
        # This should query database for track scores
        # For now, remove oldest file
        cache_files = list(self.cache_dir.glob('**/*.cache'))
        if not cache_files:
            return False
        
        # Find oldest file
        oldest_file = min(cache_files, key=lambda p: p.stat().st_mtime)
        
        # Remove file
        file_size = oldest_file.stat().st_size
        oldest_file.unlink()
        self.current_size -= file_size
        
        # Update database
        # TODO: Update is_cached flag in database
        
        return True
    
    async def remove_from_cache(self, filename: str) -> bool:
        """Remove file from cache"""
        cache_path = await self.get_cache_path(filename)
        
        if cache_path.exists():
            file_size = cache_path.stat().st_size
            cache_path.unlink()
            self.current_size -= file_size
            return True
        
        return False
    
    async def preload_track(self, track: TrackInfo):
        """Preload a track in background"""
        if not track.direct_link or await self.is_cached(track.filename):
            return
        
        # Add to download queue
        await self.download_queue.put((track.direct_link, track.filename))
    
    async def start_download_worker(self):
        """Start background download worker"""
        while True:
            try:
                url, filename = await self.download_queue.get()
                
                if filename not in self.active_downloads:
                    self.active_downloads.add(filename)
                    
                    try:
                        await self.cache_file(url, filename)
                        logger.info(f"Preloaded: {filename}")
                    except Exception as e:
                        logger.error(f"Failed to preload {filename}: {e}")
                    finally:
                        self.active_downloads.remove(filename)
                
                self.download_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Download worker error: {e}")
                await asyncio.sleep(1)

# Search Index
class SearchIndex:
    """JSON-based search index for fast lookups"""
    
    def __init__(self, index_file: Path):
        self.index_file = index_file
        self.index: Dict[str, Dict[str, Any]] = {}
        self.loaded = False
    
    def load(self):
        """Load index from file"""
        try:
            if self.index_file.exists():
                with open(self.index_file, 'r', encoding='utf-8') as f:
                    self.index = json.load(f)
            else:
                self.index = {}
            self.loaded = True
        except Exception as e:
            logger.error(f"Error loading search index: {e}")
            self.index = {}
            self.loaded = True
    
    def save(self):
        """Save index to file"""
        try:
            with open(self.index_file, 'w', encoding='utf-8') as f:
                json.dump(self.index, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving search index: {e}")
    
    def add_track(self, track: TrackInfo):
        """Add track to index"""
        self.index[track.filename] = {
            'title': track.title,
            'artist': track.artist,
            'genre': track.genre or '',
            'description': track.description or '',
            'plays': track.plays,
            'skips': track.skips,
            'is_cached': track.is_cached
        }
    
    def remove_track(self, filename: str):
        """Remove track from index"""
        if filename in self.index:
            del self.index[filename]
    
    def search(self, query: str, limit: int = 25) -> List[Tuple[str, int]]:
        """Fuzzy search tracks"""
        if not self.loaded:
            self.load()
        
        if not query:
            # Return all tracks sorted by plays
            results = [(filename, data['plays']) for filename, data in self.index.items()]
            results.sort(key=lambda x: x[1], reverse=True)
            return results[:limit]
        
        query = query.lower()
        results = []
        
        for filename, data in self.index.items():
            score = 0
            
            # Exact match check
            if query == filename.lower():
                score += 1000
            
            # Title match
            title = data['title'].lower()
            if query == title:
                score += 800
            elif query in title:
                score += 500
                # Bonus for early match
                position = title.find(query)
                if position >= 0:
                    score += (len(title) - position) * 10
            
            # Artist match
            artist = data['artist'].lower()
            if query == artist:
                score += 600
            elif query in artist:
                score += 300
                position = artist.find(query)
                if position >= 0:
                    score += (len(artist) - position) * 5
            
            # Partial matches
            if score == 0:
                # Check for partial matches in title
                if any(word in title for word in query.split()):
                    score += 200
                
                # Check for partial matches in artist
                if any(word in artist for word in query.split()):
                    score += 100
            
            # Add play count as tiebreaker
            score += data['plays']
            
            # Subtract skips
            score -= data['skips'] * 2
            
            if score > 0:
                results.append((filename, score))
        
        # Sort by score
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:limit]
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

# Music Cog - Main Class
class Music(commands.Cog):
    """Complete music system with cloud storage, caching, and premium UI"""
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.players: Dict[int, PlayerState] = {}
        self.link_resolver = LinkResolver()
        self.db = DatabaseManager(DB_FILE)
        self.cache = CacheManager(CACHE_DIR, MAX_CACHE_SIZE)
        self.search_index = SearchIndex(INDEX_FILE)
        self.background_tasks: List[asyncio.Task] = []
        self.now_playing_updates: Dict[int, asyncio.Task] = {}
        
        # Load search index
        self.search_index.load()
    
    async def cog_load(self):
        """Called when cog is loaded"""
        logger.info("Loading Music cog...")
        
        # Initialize database
        await self.db.connect()
        
        # Initialize cache
        await self.cache.initialize()
        
        # Start background tasks
        self.background_tasks.append(
            self.bot.loop.create_task(self.cache.start_download_worker())
        )
        self.background_tasks.append(
            self.bot.loop.create_task(self.background_cache_cleanup())
        )
        self.background_tasks.append(
            self.bot.loop.create_task(self.background_index_update())
        )
        self.background_tasks.append(
            self.bot.loop.create_task(self.background_stats_cleanup())
        )
        self.background_tasks.append(
            self.bot.loop.create_task(self.background_auto_disconnect())
        )
        
        # Update search index from database
        await self.update_search_index()
        
        logger.info("Music cog loaded successfully")
    
    async def cog_unload(self):
        """Called when cog is unloaded"""
        logger.info("Unloading Music cog...")
        
        # Stop background tasks
        for task in self.background_tasks:
            task.cancel()
        
        # Stop now playing updates
        for task in self.now_playing_updates.values():
            task.cancel()
        
        # Stop all players
        for guild_id, player in list(self.players.items()):
            await self.cleanup_player(guild_id)
        
        # Close connections
        await self.db.close()
        await self.link_resolver.close()
        
        logger.info("Music cog unloaded")
    
    # Background Tasks
    @tasks.loop(hours=6)
    async def background_cache_cleanup(self):
        """Clean up cache every 6 hours"""
        try:
            logger.info("Running cache cleanup...")
            
            # Get all cached tracks from database
            cursor = await self.db.conn.execute('''
                SELECT filename, plays, skips, last_played FROM track_stats 
                WHERE is_cached = 1
            ''')
            
            tracks = await cursor.fetchall()
            await cursor.close()
            
            # Calculate scores and sort
            track_scores = []
            for filename, plays, skips, last_played in tracks:
                score = plays - (skips * 2)
                
                # Add recency bonus
                if last_played:
                    try:
                        last_played_dt = datetime.fromisoformat(last_played)
                        days_ago = (datetime.now() - last_played_dt).days
                        recency_bonus = max(0, 30 - days_ago)
                        score += recency_bonus
                    except:
                        pass
                
                track_scores.append((filename, score))
            
            # Sort by score (lowest first)
            track_scores.sort(key=lambda x: x[1])
            
            # Check if cache is over 80% full
            cache_percent = (self.cache.current_size / self.cache.max_size) * 100
            
            if cache_percent > 80:
                # Remove low-score tracks until under 70%
                target_percent = 70
                target_size = self.cache.max_size * (target_percent / 100)
                
                removed_count = 0
                for filename, score in track_scores:
                    if self.cache.current_size <= target_size:
                        break
                    
                    if await self.cache.remove_from_cache(filename):
                        # Update database
                        await self.db.conn.execute(
                            'UPDATE track_stats SET is_cached = 0, cache_path = NULL WHERE filename = ?',
                            (filename,)
                        )
                        removed_count += 1
                
                await self.db.conn.commit()
                logger.info(f"Cache cleanup removed {removed_count} tracks")
            
        except Exception as e:
            logger.error(f"Error in cache cleanup: {e}")
    
    @tasks.loop(hours=1)
    async def background_index_update(self):
        """Update search index every hour"""
        try:
            logger.info("Updating search index...")
            
            # Get all tracks from database
            cursor = await self.db.conn.execute('SELECT * FROM track_stats')
            rows = await cursor.fetchall()
            await cursor.close()
            
            # Update index
            for row in rows:
                track = TrackInfo(
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
                self.search_index.add_track(track)
            
            self.search_index.save()
            logger.info(f"Search index updated with {len(rows)} tracks")
            
        except Exception as e:
            logger.error(f"Error updating search index: {e}")
    
    @tasks.loop(hours=24)
    async def background_stats_cleanup(self):
        """Clean up old statistics every 24 hours"""
        try:
            logger.info("Running stats cleanup...")
            
            # Remove tracks not played in 90 days and not cached
            cutoff_date = (datetime.now() - timedelta(days=90)).isoformat()
            
            cursor = await self.db.conn.execute('''
                DELETE FROM track_stats 
                WHERE last_played < ? AND is_cached = 0
            ''', (cutoff_date,))
            
            deleted_count = cursor.rowcount
            await cursor.close()
            await self.db.conn.commit()
            
            # Vacuum database
            await self.db.conn.execute('VACUUM')
            await self.db.conn.commit()
            
            logger.info(f"Stats cleanup removed {deleted_count} old tracks")
            
        except Exception as e:
            logger.error(f"Error in stats cleanup: {e}")
    
    @tasks.loop(minutes=1)
    async def background_auto_disconnect(self):
        """Auto-disconnect from empty voice channels"""
        try:
            current_time = datetime.now()
            
            for guild_id, player in list(self.players.items()):
                # Check if player is inactive for 5 minutes
                if player.last_activity and (current_time - player.last_activity).total_seconds() > 300:
                    # Check if voice client is connected
                    if player.voice_client and player.voice_client.is_connected():
                        # Check if channel is empty (except bot)
                        if len(player.voice_client.channel.members) <= 1:
                            await self.cleanup_player(guild_id)
                            logger.info(f"Auto-disconnected from guild {guild_id} due to inactivity")
                    
                    # If no voice client but player exists, clean up
                    elif not player.voice_client:
                        await self.cleanup_player(guild_id)
                
        except Exception as e:
            logger.error(f"Error in auto-disconnect: {e}")
    
    # Utility Methods
    async def update_search_index(self):
        """Update search index from database"""
        cursor = await self.db.conn.execute('SELECT * FROM track_stats')
        rows = await cursor.fetchall()
        await cursor.close()
        
        for row in rows:
            track = TrackInfo(
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
            self.search_index.add_track(track)
        
        self.search_index.save()
        logger.info(f"Search index loaded with {len(rows)} tracks")
    
    async def get_player(self, guild_id: int) -> PlayerState:
        """Get or create player state for guild"""
        if guild_id not in self.players:
            self.players[guild_id] = PlayerState(guild_id=guild_id)
        return self.players[guild_id]
    
    async def cleanup_player(self, guild_id: int):
        """Clean up player state for guild"""
        player = self.players.get(guild_id)
        if player:
            # Stop now playing update
            if guild_id in self.now_playing_updates:
                self.now_playing_updates[guild_id].cancel()
                del self.now_playing_updates[guild_id]
            
            # Disconnect voice
            if player.voice_client:
                await player.voice_client.disconnect()
            
            # Clear queue and state
            player.queue.clear()
            player.history.clear()
            player.current_track = None
            player.is_playing = False
            player.is_paused = False
            
            # Remove from players dict
            del self.players[guild_id]
    
    async def ensure_voice(self, interaction: discord.Interaction) -> bool:
        """Ensure bot is in voice channel and user is in same channel"""
        player = await self.get_player(interaction.guild.id)
        
        # Check if user is in voice channel
        if not interaction.user.voice or not interaction.user.voice.channel:
            await interaction.response.send_message(
                "❌ You need to be in a voice channel to use this command.",
                ephemeral=True
            )
            return False
        
        # Check if bot is already in voice
        if player.voice_client:
            # Check if in same channel
            if player.voice_client.channel != interaction.user.voice.channel:
                await interaction.response.send_message(
                    "❌ I'm already in another voice channel.",
                    ephemeral=True
                )
                return False
        else:
            # Connect to voice
            try:
                player.voice_channel = interaction.user.voice.channel
                player.voice_client = await player.voice_channel.connect()
                player.text_channel = interaction.channel
                player.update_activity()
            except Exception as e:
                await interaction.response.send_message(
                    f"❌ Failed to connect to voice channel: {e}",
                    ephemeral=True
                )
                return False
        
        return True
    
    async def play_next(self, guild_id: int, error: Exception = None):
        """Play next track in queue"""
        player = self.players.get(guild_id)
        if not player or not player.voice_client:
            return
        
        if error:
            logger.error(f"Playback error in guild {guild_id}: {error}")
        
        # Get next track
        next_track = player.get_next_track()
        
        if next_track:
            player.current_track = next_track
            player.is_playing = True
            player.is_paused = False
            player.update_activity()
            
            # Update play count
            await self.db.increment_play(next_track.filename)
            
            # Get audio source
            try:
                audio_source = await self.get_audio_source(next_track)
                
                # Start playback
                player.voice_client.play(
                    audio_source,
                    after=lambda e: self.bot.loop.create_task(self.play_next(guild_id, e))
                )
                
                # Set volume
                player.voice_client.source.volume = player.volume
                
                # Update now playing display
                await self.update_now_playing(guild_id)
                
                # Preload next tracks in background
                await self.preload_queue_tracks(guild_id)
                
            except Exception as e:
                logger.error(f"Error playing track {next_track.filename}: {e}")
                # Try next track
                await self.play_next(guild_id, e)
        
        else:
            # No more tracks, stop playback
            player.is_playing = False
            player.current_track = None
            
            # Update now playing to show empty
            await self.update_now_playing(guild_id)
    
    async def get_audio_source(self, track: TrackInfo) -> discord.AudioSource:
        """Get audio source for track"""
        if not track.is_cached and track.direct_link:
            # Download and cache
            try:
                cache_path = await self.cache.cache_file(track.direct_link, track.filename)
                
                # Update database
                await self.db.conn.execute('''
                    UPDATE track_stats 
                    SET is_cached = 1, cache_path = ?, last_cached = datetime('now')
                    WHERE filename = ?
                ''', (str(cache_path), track.filename))
                await self.db.conn.commit()
                
                track.is_cached = True
                track.cache_path = str(cache_path)
                
            except Exception as e:
                logger.error(f"Failed to cache track {track.filename}: {e}")
                # Try to play directly from URL
                if track.direct_link:
                    return discord.FFmpegPCMAudio(track.direct_link, options='-vn')
                else:
                    raise DownloadError(f"No cached or direct source for {track.filename}")
        
        if track.is_cached and track.cache_path:
            # Play from cache
            return discord.FFmpegPCMAudio(track.cache_path)
        
        elif track.direct_link:
            # Play directly from URL
            return discord.FFmpegPCMAudio(track.direct_link, options='-vn')
        
        else:
            raise DownloadError(f"No audio source available for {track.filename}")
    
    async def preload_queue_tracks(self, guild_id: int):
        """Preload next few tracks in queue"""
        player = self.players.get(guild_id)
        if not player:
            return
        
        # Preload next 3 tracks
        for i in range(min(3, len(player.queue))):
            track = player.queue[i]
            if not track.is_cached and track.direct_link:
                await self.cache.preload_track(track)
    
    async def update_now_playing(self, guild_id: int):
        """Update or create now playing display"""
        player = self.players.get(guild_id)
        if not player:
            return
        
        # Stop existing update task
        if guild_id in self.now_playing_updates:
            self.now_playing_updates[guild_id].cancel()
        
        # Create new now playing message if needed
        if not player.now_playing_message:
            player.now_playing_message = await player.text_channel.send(
                embed=await self.create_now_playing_embed(player),
                view=MusicControls(self, guild_id)
            )
        else:
            # Update existing message
            try:
                await player.now_playing_message.edit(
                    embed=await self.create_now_playing_embed(player),
                    view=MusicControls(self, guild_id)
                )
            except discord.NotFound:
                # Message was deleted, create new one
                player.now_playing_message = await player.text_channel.send(
                    embed=await self.create_now_playing_embed(player),
                    view=MusicControls(self, guild_id)
                )
        
        # Start auto-update task if playing
        if player.is_playing and not player.is_paused:
            self.now_playing_updates[guild_id] = self.bot.loop.create_task(
                self.auto_update_now_playing(guild_id)
            )
    
    async def create_now_playing_embed(self, player: PlayerState) -> discord.Embed:
        """Create now playing embed"""
        embed = discord.Embed(color=discord.Color.green())
        
        if player.current_track:
            # Playing track
            track = player.current_track
            
            embed.title = f"{EMOJIS['music']} Now Playing"
            embed.description = f"**{track.title}**\nby {track.artist}"
            
            # Add metadata
            if track.genre:
                embed.add_field(name="Genre", value=track.genre, inline=True)
            
            embed.add_field(name="Plays", value=str(track.plays + 1), inline=True)
            
            # Cache status
            cache_status = f"{EMOJIS['success']} Cached" if track.is_cached else f"{EMOJIS['warning']} Streaming"
            embed.add_field(name="Status", value=cache_status, inline=True)
            
            # Queue info
            if player.queue:
                next_track = player.queue[0] if player.queue else None
                queue_info = f"{len(player.queue)} track{'s' if len(player.queue) != 1 else ''} in queue"
                
                if next_track:
                    queue_info += f"\nNext: **{next_track.title}**"
                
                embed.add_field(name="Queue", value=queue_info, inline=False)
            
            # Playback info
            if player.voice_client and player.voice_client.is_playing():
                # Get position (simplified - in real implementation, track FFmpeg position)
                status = f"{EMOJIS['play']} Playing"
                if player.is_paused:
                    status = f"{EMOJIS['pause']} Paused"
                
                embed.add_field(name="Status", value=status, inline=True)
                embed.add_field(name="Volume", value=f"{int(player.volume * 100)}%", inline=True)
                embed.add_field(name="Loop", value=player.loop_mode.capitalize(), inline=True)
            
            # Progress bar (simplified)
            if player.voice_client and not player.is_paused:
                # Note: For actual progress bar, need to track playback position
                embed.set_footer(text="🎵 ┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈▶️")
        
        else:
            # No track playing
            embed.title = f"{EMOJIS['music']} Music Player"
            embed.description = "No track is currently playing."
            
            if player.queue:
                embed.add_field(
                    name="Queue",
                    value=f"{len(player.queue)} track{'s' if len(player.queue) != 1 else ''} in queue",
                    inline=False
                )
            
            embed.add_field(name="Status", value="⏹️ Stopped", inline=True)
            embed.add_field(name="Volume", value=f"{int(player.volume * 100)}%", inline=True)
            embed.add_field(name="Loop", value=player.loop_mode.capitalize(), inline=True)
        
        embed.set_footer(text=f"Use e!play or /play to add songs | {player.guild_id}")
        return embed
    
    async def auto_update_now_playing(self, guild_id: int):
        """Auto-update now playing display every 5 seconds"""
        player = self.players.get(guild_id)
        if not player:
            return
        
        try:
            while player.is_playing and not player.is_paused:
                await asyncio.sleep(5)
                
                if not player.now_playing_message:
                    break
                
                try:
                    await player.now_playing_message.edit(
                        embed=await self.create_now_playing_embed(player),
                        view=MusicControls(self, guild_id)
                    )
                except discord.NotFound:
                    break
                except Exception as e:
                    logger.error(f"Error updating now playing: {e}")
                    break
        
        except asyncio.CancelledError:
            pass
    
    # UI Components
    class TrackSelectView(discord.ui.View):
        """View for selecting tracks"""
        
        def __init__(self, music_cog, tracks: List[TrackInfo], page: int = 0):
            super().__init__(timeout=60)
            self.music_cog = music_cog
            self.tracks = tracks
            self.page = page
            self.items_per_page = 20
            
            # Add track select dropdown
            self.add_item(self.TrackSelectDropdown(music_cog, tracks, page))
            
            # Add navigation buttons if needed
            if len(tracks) > self.items_per_page:
                self.add_item(NavigationButton("Previous", "previous", disabled=page == 0))
                self.add_item(NavigationButton("Next", "next", 
                    disabled=(page + 1) * self.items_per_page >= len(tracks)))
        
        class TrackSelectDropdown(discord.ui.Select):
            """Dropdown for selecting tracks"""
            
            def __init__(self, music_cog, tracks: List[TrackInfo], page: int):
                self.music_cog = music_cog
                self.all_tracks = tracks
                self.page = page
                self.items_per_page = 20
                
                # Get tracks for this page
                start_idx = page * self.items_per_page
                end_idx = min(start_idx + self.items_per_page, len(tracks))
                page_tracks = tracks[start_idx:end_idx]
                
                # Create options
                options = []
                for i, track in enumerate(page_tracks):
                    display_name = track.display_name
                    if len(display_name) > 90:
                        display_name = display_name[:87] + "..."
                    
                    cache_indicator = " ✅" if track.is_cached else " ⏳"
                    
                    options.append(discord.SelectOption(
                        label=f"{start_idx + i + 1}. {track.title[:90]}",
                        description=f"{track.artist[:45]}{cache_indicator}",
                        value=track.filename,
                        emoji=EMOJIS['music']
                    ))
                
                super().__init__(
                    placeholder=f"Select tracks (Page {page + 1})",
                    min_values=1,
                    max_values=len(options),
                    options=options
                )
            
            async def callback(self, interaction: discord.Interaction):
                # This will be overridden by parent
                pass
    
    class PlaylistSelectView(discord.ui.View):
        """View for selecting playlists"""
        
        def __init__(self, music_cog, playlists: List[Playlist], page: int = 0):
            super().__init__(timeout=60)
            self.music_cog = music_cog
            self.playlists = playlists
            self.page = page
            self.items_per_page = 20
            
            # Add playlist select dropdown
            self.add_item(self.PlaylistSelectDropdown(music_cog, playlists, page))
            
            # Add navigation buttons if needed
            if len(playlists) > self.items_per_page:
                self.add_item(NavigationButton("Previous", "previous", disabled=page == 0))
                self.add_item(NavigationButton("Next", "next", 
                    disabled=(page + 1) * self.items_per_page >= len(playlists)))
        
        class PlaylistSelectDropdown(discord.ui.Select):
            """Dropdown for selecting playlists"""
            
            def __init__(self, music_cog, playlists: List[Playlist], page: int):
                self.music_cog = music_cog
                self.all_playlists = playlists
                self.page = page
                self.items_per_page = 20
                
                # Get playlists for this page
                start_idx = page * self.items_per_page
                end_idx = min(start_idx + self.items_per_page, len(playlists))
                page_playlists = playlists[start_idx:end_idx]
                
                # Create options
                options = []
                for i, playlist in enumerate(page_playlists):
                    description = playlist.description or "No description"
                    if len(description) > 45:
                        description = description[:42] + "..."
                    
                    options.append(discord.SelectOption(
                        label=f"{playlist.name[:90]}",
                        description=f"{description} | {len(playlist.tracks)} tracks",
                        value=str(playlist.id),
                        emoji="📋"
                    ))
                
                super().__init__(
                    placeholder=f"Select playlists (Page {page + 1})",
                    min_values=1,
                    max_values=len(options),
                    options=options
                )
            
            async def callback(self, interaction: discord.Interaction):
                # This will be overridden by parent
                pass
    
    # Management Panel Views
    class ManageMusicView(discord.ui.View):
        """Main management panel view"""
        
        def __init__(self, music_cog, user_id: int):
            super().__init__(timeout=300)  # 5 minute timeout
            self.music_cog = music_cog
            self.user_id = user_id
            self.current_section = None
        
        @discord.ui.button(label="Add Content", style=discord.ButtonStyle.green, emoji="➕")
        async def add_content(self, interaction: discord.Interaction, button: discord.ui.Button):
            """Add content section"""
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="📥 Add Content",
                    description="Choose what you want to add:",
                    color=discord.Color.green()
                ).add_field(
                    name="Options",
                    value="1. **Add Music** - Add a new track to library\n"
                          "2. **Create Playlist** - Create a new playlist\n"
                          "3. **Add to Playlist** - Add tracks to existing playlist",
                    inline=False
                ),
                view=AddContentView(self.music_cog, self.user_id),
                ephemeral=True
            )
        
        @discord.ui.button(label="Remove Content", style=discord.ButtonStyle.red, emoji="🗑️")
        async def remove_content(self, interaction: discord.Interaction, button: discord.ui.Button):
            """Remove content section"""
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="🗑️ Remove Content",
                    description="Choose what you want to remove:",
                    color=discord.Color.red()
                ).add_field(
                    name="Options",
                    value="1. **Remove Music** - Remove tracks from library\n"
                          "2. **Delete Playlist** - Delete entire playlists\n"
                          "3. **Remove from Playlist** - Remove tracks from specific playlist",
                    inline=False
                ),
                view=RemoveContentView(self.music_cog, self.user_id),
                ephemeral=True
            )
        
        @discord.ui.button(label="Manage", style=discord.ButtonStyle.blurple, emoji="⚙️")
        async def manage(self, interaction: discord.Interaction, button: discord.ui.Button):
            """Manage section"""
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="⚙️ Manage",
                    description="Choose management action:",
                    color=discord.Color.blue()
                ).add_field(
                    name="Options",
                    value="1. **Preload** - Download tracks to cache\n"
                          "2. **Unload** - Remove tracks from cache\n"
                          "3. **Edit** - Edit track or playlist metadata",
                    inline=False
                ),
                view=ManageContentView(self.music_cog, self.user_id),
                ephemeral=True
            )
        
        @discord.ui.button(label="Statistics", style=discord.ButtonStyle.grey, emoji="📊")
        async def statistics(self, interaction: discord.Interaction, button: discord.ui.Button):
            """Statistics section"""
            try:
                stats = await self.music_cog.db.get_stats()
                
                embed = discord.Embed(
                    title="📊 Library Statistics",
                    color=discord.Color.greyple()
                )
                
                # Basic stats
                embed.add_field(name="Total Tracks", value=str(stats['total_tracks']), inline=True)
                embed.add_field(name="Cached Tracks", value=str(stats['cached_tracks']), inline=True)
                embed.add_field(name="Total Playlists", value=str(stats['total_playlists']), inline=True)
                
                # Cache size
                cache_size_mb = self.music_cog.cache.current_size / (1024 * 1024)
                max_cache_mb = self.music_cog.cache.max_size / (1024 * 1024)
                cache_percent = (cache_size_mb / max_cache_mb) * 100
                
                embed.add_field(
                    name="Cache Usage", 
                    value=f"{cache_size_mb:.1f} MB / {max_cache_mb:.1f} MB ({cache_percent:.1f}%)",
                    inline=False
                )
                
                # Top tracks
                if stats['top_tracks']:
                    top_tracks_text = ""
                    for i, (title, artist, plays) in enumerate(stats['top_tracks'], 1):
                        top_tracks_text += f"{i}. **{title[:30]}** - {artist[:20]} ({plays} plays)\n"
                    
                    embed.add_field(name="Top 5 Tracks", value=top_tracks_text, inline=False)
                
                await interaction.response.send_message(embed=embed, ephemeral=True)
                
            except Exception as e:
                await interaction.response.send_message(
                    f"❌ Error getting statistics: {e}",
                    ephemeral=True
                )
        
        @discord.ui.button(label="Help", style=discord.ButtonStyle.grey, emoji="❓")
        async def help(self, interaction: discord.Interaction, button: discord.ui.Button):
            """Help section"""
            embed = discord.Embed(
                title="❓ Music Bot Help",
                description="Complete guide for managing your music library",
                color=discord.Color.greyple()
            )
            
            # Add section
            embed.add_field(
                name="📥 Add Content",
                value="• **Add Music**: Upload new tracks with metadata\n"
                      "• **Create Playlist**: Make new playlists\n"
                      "• **Add to Playlist**: Add tracks to existing playlists",
                inline=False
            )
            
            # Remove section
            embed.add_field(
                name="🗑️ Remove Content",
                value="• **Remove Music**: Delete tracks from library\n"
                      "• **Delete Playlist**: Remove entire playlists\n"
                      "• **Remove from Playlist**: Remove tracks from playlists",
                inline=False
            )
            
            # Manage section
            embed.add_field(
                name="⚙️ Manage",
                value="• **Preload**: Download tracks to cache for faster playback\n"
                      "• **Unload**: Remove tracks from cache to free space\n"
                      "• **Edit**: Update track or playlist information",
                inline=False
            )
            
            # Playback commands
            embed.add_field(
                name="🎵 Playback Commands",
                value="• `e!play` or `/play` - Play a track\n"
                      "• `e!skip` - Skip current track\n"
                      "• `e!pause`/`e!resume` - Pause/Resume\n"
                      "• `e!stop` - Stop playback\n"
                      "• `e!volume` - Adjust volume\n"
                      "• `e!queue` - Show queue\n"
                      "• `e!nowplaying` - Current track info",
                inline=False
            )
            
            embed.set_footer(text="Use the buttons above to navigate management panel")
            
            await interaction.response.send_message(embed=embed, ephemeral=True)

# Sub-views for management panel
class AddContentView(discord.ui.View):
    """View for add content section"""
    
    def __init__(self, music_cog, user_id: int):
        super().__init__(timeout=60)
        self.music_cog = music_cog
        self.user_id = user_id
    
    @discord.ui.button(label="Add Music", style=discord.ButtonStyle.green, emoji="🎵")
    async def add_music(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Open add music modal"""
        await interaction.response.send_modal(AddMusicModal(self.music_cog))
    
    @discord.ui.button(label="Create Playlist", style=discord.ButtonStyle.blurple, emoji="📋")
    async def create_playlist(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Open create playlist modal"""
        await interaction.response.send_modal(CreatePlaylistModal(self.music_cog, self.user_id))
    
    @discord.ui.button(label="Add to Playlist", style=discord.ButtonStyle.grey, emoji="➕")
    async def add_to_playlist(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Add tracks to playlist"""
        # First get user's playlists
        playlists = await self.music_cog.db.get_user_playlists(self.user_id)
        
        if not playlists:
            await interaction.response.send_message(
                "❌ You don't have any playlists yet. Create one first!",
                ephemeral=True
            )
            return
        
        # Send playlist selector
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Select Playlist",
                description="Choose a playlist to add tracks to:",
                color=discord.Color.blue()
            ),
            view=PlaylistSelectorView(self.music_cog, playlists, self.user_id, "add_tracks"),
            ephemeral=True
        )

class RemoveContentView(discord.ui.View):
    """View for remove content section"""
    
    def __init__(self, music_cog, user_id: int):
        super().__init__(timeout=60)
        self.music_cog = music_cog
        self.user_id = user_id
    
    @discord.ui.button(label="Remove Music", style=discord.ButtonStyle.red, emoji="🎵")
    async def remove_music(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Remove tracks from library"""
        # Get user's tracks (tracks they added)
        cursor = await self.music_cog.db.conn.execute(
            'SELECT * FROM track_stats WHERE added_by = ? OR ? IN (SELECT user_id FROM bot_admins)',
            (self.user_id, self.user_id)
        )
        
        tracks_data = await cursor.fetchall()
        await cursor.close()
        
        if not tracks_data:
            await interaction.response.send_message(
                "❌ No tracks found that you can remove.",
                ephemeral=True
            )
            return
        
        # Convert to TrackInfo objects
        tracks = []
        for row in tracks_data:
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
        
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Remove Tracks",
                description="Select tracks to remove from library:",
                color=discord.Color.red()
            ),
            view=TrackSelectorView(self.music_cog, tracks, self.user_id, "remove_tracks"),
            ephemeral=True
        )
    
    @discord.ui.button(label="Delete Playlist", style=discord.ButtonStyle.red, emoji="📋")
    async def delete_playlist(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Delete playlists"""
        # Get user's playlists
        playlists = await self.music_cog.db.get_user_playlists(self.user_id)
        
        if not playlists:
            await interaction.response.send_message(
                "❌ You don't have any playlists to delete.",
                ephemeral=True
            )
            return
        
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Delete Playlists",
                description="Select playlists to delete:",
                color=discord.Color.red()
            ),
            view=PlaylistSelectorView(self.music_cog, playlists, self.user_id, "delete_playlists"),
            ephemeral=True
        )
    
    @discord.ui.button(label="Remove from Playlist", style=discord.ButtonStyle.grey, emoji="➖")
    async def remove_from_playlist(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Remove tracks from playlist"""
        # First get user's playlists
        playlists = await self.music_cog.db.get_user_playlists(self.user_id)
        
        if not playlists:
            await interaction.response.send_message(
                "❌ You don't have any playlists yet.",
                ephemeral=True
            )
            return
        
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Select Playlist",
                description="Choose a playlist to remove tracks from:",
                color=discord.Color.blue()
            ),
            view=PlaylistSelectorView(self.music_cog, playlists, self.user_id, "remove_from_playlist"),
            ephemeral=True
        )

class ManageContentView(discord.ui.View):
    """View for manage section"""
    
    def __init__(self, music_cog, user_id: int):
        super().__init__(timeout=60)
        self.music_cog = music_cog
        self.user_id = user_id
    
    @discord.ui.button(label="Preload", style=discord.ButtonStyle.green, emoji="⬇️")
    async def preload(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Preload tracks to cache"""
        await interaction.response.send_message(
            embed=discord.Embed(
                title="⬇️ Preload",
                description="Choose preload option:",
                color=discord.Color.green()
            ).add_field(
                name="Options",
                value="1. **Preload Music** - Cache individual tracks\n"
                      "2. **Preload Playlist** - Cache all tracks in playlist",
                inline=False
            ),
            view=PreloadView(self.music_cog, self.user_id),
            ephemeral=True
        )
    
    @discord.ui.button(label="Unload", style=discord.ButtonStyle.red, emoji="⬆️")
    async def unload(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Unload tracks from cache"""
        await interaction.response.send_message(
            embed=discord.Embed(
                title="⬆️ Unload",
                description="Choose unload option:",
                color=discord.Color.red()
            ).add_field(
                name="Options",
                value="1. **Unload Music** - Remove tracks from cache\n"
                      "2. **Unload Playlist** - Remove all playlist tracks from cache",
                inline=False
            ),
            view=UnloadView(self.music_cog, self.user_id),
            ephemeral=True
        )
    
    @discord.ui.button(label="Edit", style=discord.ButtonStyle.blurple, emoji="✏️")
    async def edit(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Edit content"""
        await interaction.response.send_message(
            embed=discord.Embed(
                title="✏️ Edit",
                description="Choose edit option:",
                color=discord.Color.blue()
            ).add_field(
                name="Options",
                value="1. **Edit Music** - Update track metadata\n"
                      "2. **Edit Playlist** - Update playlist info",
                inline=False
            ),
            view=EditView(self.music_cog, self.user_id),
            ephemeral=True
        )

# Navigation button for paginated views
class NavigationButton(discord.ui.Button):
    """Navigation button for paginated views"""
    
    def __init__(self, label: str, action: str, disabled: bool = False):
        super().__init__(
            label=label,
            style=discord.ButtonStyle.grey,
            disabled=disabled
        )
        self.action = action
    
    async def callback(self, interaction: discord.Interaction):
        # This will be overridden by parent view
        pass

# Modals for data input
class AddMusicModal(discord.ui.Modal, title="Add Music to Library"):
    """Modal for adding new music"""
    
    def __init__(self, music_cog):
        super().__init__()
        self.music_cog = music_cog
    
    title_input = discord.ui.TextInput(
        label="Track Title",
        placeholder="Enter the track title...",
        max_length=100,
        required=True
    )
    
    artist_input = discord.ui.TextInput(
        label="Artist",
        placeholder="Enter the artist name...",
        max_length=100,
        required=True
    )
    
    link_input = discord.ui.TextInput(
        label="Download Link",
        placeholder="Paste Dropbox, Google Drive, etc. link...",
        max_length=500,
        required=True
    )
    
    genre_input = discord.ui.TextInput(
        label="Genre (Optional)",
        placeholder="Rock, Pop, Classical, etc.",
        max_length=50,
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True, ephemeral=True)
        
        try:
            # Test the link
            test_result = await self.music_cog.link_resolver.test_link(self.link_input.value)
            
            if test_result['status'] != 'success':
                await interaction.followup.send(
                    f"❌ Link test failed: {test_result['status']}",
                    ephemeral=True
                )
                return
            
            if not test_result['is_supported']:
                await interaction.followup.send(
                    f"⚠️ Warning: This may not be a supported audio format.\n"
                    f"Content-Type: {test_result['content_type']}\n"
                    f"Continue anyway?",
                    view=ConfirmAddView(self.music_cog, self, test_result),
                    ephemeral=True
                )
                return
            
            # Create filename from title and artist
            safe_title = "".join(c for c in self.title_input.value if c.isalnum() or c in " -_")
            safe_artist = "".join(c for c in self.artist_input.value if c.isalnum() or c in " -_")
            filename = f"{safe_title}_{safe_artist}.{self._get_extension(test_result['direct_url'])}"
            
            # Create track info
            track = TrackInfo(
                filename=filename,
                title=self.title_input.value,
                artist=self.artist_input.value,
                genre=self.genre_input.value or None,
                direct_link=test_result['direct_url'],
                service=test_result['service']
            )
            
            # Add to database
            success = await self.music_cog.db.add_track(track)
            
            if success:
                # Update search index
                self.music_cog.search_index.add_track(track)
                self.music_cog.search_index.save()
                
                await interaction.followup.send(
                    f"✅ Successfully added **{track.title}** by **{track.artist}** to library!\n"
                    f"Filename: `{filename}`\n"
                    f"Service: {track.service}",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "❌ Failed to add track to database.",
                    ephemeral=True
                )
                
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error adding track: {e}",
                ephemeral=True
            )
    
    def _get_extension(self, url: str) -> str:
        """Extract file extension from URL"""
        try:
            # Try to get from URL path
            path = yarl.URL(url).path
            if '.' in path:
                return path.split('.')[-1].lower()
            
            # Default to mp3
            return "mp3"
        except:
            return "mp3"

class ConfirmAddView(discord.ui.View):
    """View for confirming addition of unsupported format"""
    
    def __init__(self, music_cog, modal: AddMusicModal, test_result: Dict):
        super().__init__(timeout=60)
        self.music_cog = music_cog
        self.modal = modal
        self.test_result = test_result
    
    @discord.ui.button(label="Yes, Add Anyway", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(thinking=True, ephemeral=True)
        
        try:
            # Create filename
            safe_title = "".join(c for c in self.modal.title_input.value if c.isalnum() or c in " -_")
            safe_artist = "".join(c for c in self.modal.artist_input.value if c.isalnum() or c in " -_")
            filename = f"{safe_title}_{safe_artist}.{self._get_extension(self.test_result['direct_url'])}"
            
            # Create track info
            track = TrackInfo(
                filename=filename,
                title=self.modal.title_input.value,
                artist=self.modal.artist_input.value,
                genre=self.modal.genre_input.value or None,
                direct_link=self.test_result['direct_url'],
                service=self.test_result['service']
            )
            
            # Add to database
            success = await self.music_cog.db.add_track(track)
            
            if success:
                self.music_cog.search_index.add_track(track)
                self.music_cog.search_index.save()
                
                await interaction.followup.send(
                    f"✅ Added **{track.title}** (unsupported format - may not play correctly)",
                    ephemeral=True
                )
            else:
                await interaction.followup.send("❌ Failed to add track.", ephemeral=True)
                
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            content="❌ Track addition cancelled.",
            view=None
        )
    
    def _get_extension(self, url: str) -> str:
        """Extract file extension from URL"""
        try:
            path = yarl.URL(url).path
            if '.' in path:
                return path.split('.')[-1].lower()
            return "mp3"
        except:
            return "mp3"

class CreatePlaylistModal(discord.ui.Modal, title="Create New Playlist"):
    """Modal for creating new playlist"""
    
    def __init__(self, music_cog, user_id: int):
        super().__init__()
        self.music_cog = music_cog
        self.user_id = user_id
    
    name_input = discord.ui.TextInput(
        label="Playlist Name",
        placeholder="My Awesome Playlist",
        max_length=100,
        required=True
    )
    
    description_input = discord.ui.TextInput(
        label="Description (Optional)",
        placeholder="What's this playlist about?",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True, ephemeral=True)
        
        try:
            playlist_id = await self.music_cog.db.create_playlist(
                name=self.name_input.value,
                user_id=self.user_id,
                description=self.description_input.value or None
            )
            
            if playlist_id:
                await interaction.followup.send(
                    f"✅ Created playlist **{self.name_input.value}**!",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "❌ Failed to create playlist (maybe name already exists?)",
                    ephemeral=True
                )
                
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)

# Music Controls View (Auto-updating Now Playing)
class MusicControls(discord.ui.View):
    """Interactive music controls for now playing display"""
    
    def __init__(self, music_cog, guild_id: int):
        super().__init__(timeout=None)  # No timeout - permanent view
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    @discord.ui.button(emoji=EMOJIS['previous'], style=discord.ButtonStyle.grey)
    async def previous(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Previous track"""
        player = self.music_cog.players.get(self.guild_id)
        if not player:
            await interaction.response.send_message("❌ No player active.", ephemeral=True)
            return
        
        # Add current track to beginning of queue and play previous from history
        if player.current_track and player.history:
            if player.current_track:
                player.queue.insert(0, player.current_track)
            
            previous_track = player.history.pop()
            player.queue.insert(0, previous_track)
            
            # Skip current
            if player.voice_client and player.voice_client.is_playing():
                player.voice_client.stop()
            
            await interaction.response.defer()
        else:
            await interaction.response.send_message("❌ No previous track.", ephemeral=True)
    
    @discord.ui.button(emoji=EMOJIS['pause'], style=discord.ButtonStyle.grey)
    async def pause_resume(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Pause/Resume playback"""
        player = self.music_cog.players.get(self.guild_id)
        if not player or not player.voice_client:
            await interaction.response.send_message("❌ Nothing is playing.", ephemeral=True)
            return
        
        if player.is_paused:
            player.voice_client.resume()
            player.is_paused = False
            button.emoji = EMOJIS['pause']
        else:
            player.voice_client.pause()
            player.is_paused = True
            button.emoji = EMOJIS['play']
        
        player.update_activity()
        await self.music_cog.update_now_playing(self.guild_id)
        await interaction.response.defer()
    
    @discord.ui.button(emoji=EMOJIS['skip'], style=discord.ButtonStyle.grey)
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Skip current track"""
        player = self.music_cog.players.get(self.guild_id)
        if not player or not player.voice_client:
            await interaction.response.send_message("❌ Nothing is playing.", ephemeral=True)
            return
        
        # Add to history
        if player.current_track:
            player.history.append(player.current_track)
            # Keep history size limited
            if len(player.history) > 50:
                player.history.pop(0)
            
            # Increment skip count
            await self.music_cog.db.increment_skip(player.current_track.filename)
        
        # Stop current playback
        if player.voice_client.is_playing():
            player.voice_client.stop()
        
        player.update_activity()
        await interaction.response.defer()
    
    @discord.ui.button(emoji=EMOJIS['loop'], style=discord.ButtonStyle.grey)
    async def loop(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Toggle loop mode"""
        player = self.music_cog.players.get(self.guild_id)
        if not player:
            await interaction.response.send_message("❌ No player active.", ephemeral=True)
            return
        
        # Cycle through loop modes
        if player.loop_mode == 'off':
            player.loop_mode = 'track'
            button.style = discord.ButtonStyle.green
        elif player.loop_mode == 'track':
            player.loop_mode = 'queue'
            button.style = discord.ButtonStyle.blurple
        else:
            player.loop_mode = 'off'
            button.style = discord.ButtonStyle.grey
        
        player.update_activity()
        await self.music_cog.update_now_playing(self.guild_id)
        await interaction.response.defer()
    
    @discord.ui.button(emoji=EMOJIS['shuffle'], style=discord.ButtonStyle.grey)
    async def shuffle(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Shuffle queue"""
        player = self.music_cog.players.get(self.guild_id)
        if not player:
            await interaction.response.send_message("❌ No player active.", ephemeral=True)
            return
        
        if player.queue:
            player.shuffle_queue()
            button.style = discord.ButtonStyle.green
            await interaction.response.send_message("🔀 Queue shuffled!", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Queue is empty.", ephemeral=True)
    
    @discord.ui.button(emoji=EMOJIS['queue'], style=discord.ButtonStyle.grey)
    async def show_queue(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show queue"""
        player = self.music_cog.players.get(self.guild_id)
        if not player:
            await interaction.response.send_message("❌ No player active.", ephemeral=True)
            return
        
        if not player.queue:
            await interaction.response.send_message("❌ Queue is empty.", ephemeral=True)
            return
        
        # Create queue embed
        embed = discord.Embed(
            title=f"{EMOJIS['queue']} Queue ({len(player.queue)} tracks)",
            color=discord.Color.blue()
        )
        
        # Show first 10 tracks
        queue_text = ""
        for i, track in enumerate(player.queue[:10], 1):
            cache_indicator = " ✅" if track.is_cached else " ⏳"
            queue_text += f"{i}. **{track.title}** - {track.artist}{cache_indicator}\n"
        
        if len(player.queue) > 10:
            queue_text += f"\n...and {len(player.queue) - 10} more tracks"
        
        embed.description = queue_text
        
        if player.current_track:
            embed.set_footer(text=f"Now Playing: {player.current_track.title}")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    @discord.ui.button(emoji=EMOJIS['volume'], style=discord.ButtonStyle.grey)
    async def volume(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Adjust volume"""
        player = self.music_cog.players.get(self.guild_id)
        if not player:
            await interaction.response.send_message("❌ No player active.", ephemeral=True)
            return
        
        # Send volume adjustment modal
        await interaction.response.send_modal(VolumeModal(self.music_cog, self.guild_id))
    
    @discord.ui.button(emoji=EMOJIS['stop'], style=discord.ButtonStyle.red)
    async def stop(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Stop playback"""
        player = self.music_cog.players.get(self.guild_id)
        if not player:
            await interaction.response.send_message("❌ No player active.", ephemeral=True)
            return
        
        # Stop playback
        if player.voice_client:
            player.voice_client.stop()
        
        # Clear queue
        player.clear_queue()
        player.current_track = None
        player.is_playing = False
        player.is_paused = False
        
        # Update display
        await self.music_cog.update_now_playing(self.guild_id)
        await interaction.response.send_message("⏹️ Playback stopped and queue cleared.", ephemeral=True)

class VolumeModal(discord.ui.Modal, title="Adjust Volume"):
    """Modal for adjusting volume"""
    
    def __init__(self, music_cog, guild_id: int):
        super().__init__()
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    volume_input = discord.ui.TextInput(
        label="Volume (1-100)",
        placeholder="Enter volume level...",
        default="50",
        max_length=3,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            volume = int(self.volume_input.value)
            
            if volume < 1 or volume > 100:
                await interaction.response.send_message(
                    "❌ Volume must be between 1 and 100.",
                    ephemeral=True
                )
                return
            
            player = self.music_cog.players.get(self.guild_id)
            if player and player.voice_client:
                player.volume = volume / 100
                if player.voice_client.source:
                    player.voice_client.source.volume = player.volume
                
                player.update_activity()
                await self.music_cog.update_now_playing(self.guild_id)
                
                await interaction.response.send_message(
                    f"🔊 Volume set to {volume}%",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    "❌ No player active.",
                    ephemeral=True
                )
                
        except ValueError:
            await interaction.response.send_message(
                "❌ Please enter a valid number.",
                ephemeral=True
            )

# Continuation of Music cog class

# Additional UI Views from Part 2
class PreloadView(discord.ui.View):
    """View for preload section"""
    
    def __init__(self, music_cog, user_id: int):
        super().__init__(timeout=60)
        self.music_cog = music_cog
        self.user_id = user_id
    
    @discord.ui.button(label="Preload Music", style=discord.ButtonStyle.green, emoji="🎵")
    async def preload_music(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Preload individual tracks"""
        # Get all uncached tracks
        cursor = await self.music_cog.db.conn.execute(
            'SELECT * FROM track_stats WHERE is_cached = 0 AND direct_link IS NOT NULL'
        )
        
        tracks_data = await cursor.fetchall()
        await cursor.close()
        
        if not tracks_data:
            await interaction.response.send_message(
                "✅ All tracks are already cached!",
                ephemeral=True
            )
            return
        
        # Convert to TrackInfo objects
        tracks = []
        for row in tracks_data:
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
        
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Preload Tracks",
                description=f"Select uncached tracks to download ({len(tracks)} available):",
                color=discord.Color.green()
            ),
            view=TrackSelectorView(self.music_cog, tracks, self.user_id, "preload_tracks"),
            ephemeral=True
        )
    
    @discord.ui.button(label="Preload Playlist", style=discord.ButtonStyle.blurple, emoji="📋")
    async def preload_playlist(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Preload entire playlist"""
        # Get user's playlists
        playlists = await self.music_cog.db.get_user_playlists(self.user_id)
        
        if not playlists:
            await interaction.response.send_message(
                "❌ You don't have any playlists yet.",
                ephemeral=True
            )
            return
        
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Preload Playlist",
                description="Select a playlist to cache all tracks:",
                color=discord.Color.green()
            ),
            view=PlaylistSelectorView(self.music_cog, playlists, self.user_id, "preload_playlist"),
            ephemeral=True
        )

class UnloadView(discord.ui.View):
    """View for unload section"""
    
    def __init__(self, music_cog, user_id: int):
        super().__init__(timeout=60)
        self.music_cog = music_cog
        self.user_id = user_id
    
    @discord.ui.button(label="Unload Music", style=discord.ButtonStyle.red, emoji="🎵")
    async def unload_music(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Unload tracks from cache"""
        # Get all cached tracks
        cursor = await self.music_cog.db.conn.execute(
            'SELECT * FROM track_stats WHERE is_cached = 1'
        )
        
        tracks_data = await cursor.fetchall()
        await cursor.close()
        
        if not tracks_data:
            await interaction.response.send_message(
                "❌ No tracks are currently cached.",
                ephemeral=True
            )
            return
        
        # Convert to TrackInfo objects
        tracks = []
        for row in tracks_data:
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
        
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Unload Tracks",
                description=f"Select cached tracks to remove from cache ({len(tracks)} available):",
                color=discord.Color.red()
            ),
            view=TrackSelectorView(self.music_cog, tracks, self.user_id, "unload_tracks"),
            ephemeral=True
        )
    
    @discord.ui.button(label="Unload Playlist", style=discord.ButtonStyle.red, emoji="📋")
    async def unload_playlist(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Unload entire playlist from cache"""
        # Get user's playlists
        playlists = await self.music_cog.db.get_user_playlists(self.user_id)
        
        if not playlists:
            await interaction.response.send_message(
                "❌ You don't have any playlists yet.",
                ephemeral=True
            )
            return
        
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Unload Playlist",
                description="Select a playlist to remove all tracks from cache:",
                color=discord.Color.red()
            ),
            view=PlaylistSelectorView(self.music_cog, playlists, self.user_id, "unload_playlist"),
            ephemeral=True
        )

class EditView(discord.ui.View):
    """View for edit section"""
    
    def __init__(self, music_cog, user_id: int):
        super().__init__(timeout=60)
        self.music_cog = music_cog
        self.user_id = user_id
    
    @discord.ui.button(label="Edit Music", style=discord.ButtonStyle.blurple, emoji="🎵")
    async def edit_music(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Edit track metadata"""
        # Get all tracks (could be limited to user's tracks)
        cursor = await self.music_cog.db.conn.execute(
            'SELECT * FROM track_stats ORDER BY title'
        )
        
        tracks_data = await cursor.fetchall()
        await cursor.close()
        
        if not tracks_data:
            await interaction.response.send_message(
                "❌ No tracks in library.",
                ephemeral=True
            )
            return
        
        # Convert to TrackInfo objects
        tracks = []
        for row in tracks_data:
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
        
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Edit Track",
                description="Select a track to edit:",
                color=discord.Color.blue()
            ),
            view=TrackSelectorView(self.music_cog, tracks, self.user_id, "edit_track", single_select=True),
            ephemeral=True
        )
    
    @discord.ui.button(label="Edit Playlist", style=discord.ButtonStyle.blurple, emoji="📋")
    async def edit_playlist(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Edit playlist info"""
        # Get user's playlists
        playlists = await self.music_cog.db.get_user_playlists(self.user_id)
        
        if not playlists:
            await interaction.response.send_message(
                "❌ You don't have any playlists yet.",
                ephemeral=True
            )
            return
        
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Edit Playlist",
                description="Select a playlist to edit:",
                color=discord.Color.blue()
            ),
            view=PlaylistSelectorView(self.music_cog, playlists, self.user_id, "edit_playlist", single_select=True),
            ephemeral=True
        )

class PlaylistSelectorView(discord.ui.View):
    """View for selecting playlists with action"""
    
    def __init__(self, music_cog, playlists: List[Playlist], user_id: int, action: str, single_select: bool = False):
        super().__init__(timeout=60)
        self.music_cog = music_cog
        self.playlists = playlists
        self.user_id = user_id
        self.action = action
        self.single_select = single_select
        self.page = 0
        self.items_per_page = 20
        
        # Add dropdown
        self.add_item(self.PlaylistDropdown(self))
    
    class PlaylistDropdown(discord.ui.Select):
        """Dropdown for playlist selection"""
        
        def __init__(self, parent_view):
            self.parent = parent_view
            
            # Get playlists for current page
            start_idx = parent_view.page * parent_view.items_per_page
            end_idx = min(start_idx + parent_view.items_per_page, len(parent_view.playlists))
            page_playlists = parent_view.playlists[start_idx:end_idx]
            
            # Create options
            options = []
            for i, playlist in enumerate(page_playlists):
                description = playlist.description or "No description"
                if len(description) > 45:
                    description = description[:42] + "..."
                
                options.append(discord.SelectOption(
                    label=f"{playlist.name[:90]}",
                    description=f"{description} | {len(playlist.tracks)} tracks",
                    value=str(playlist.id),
                    emoji="📋"
                ))
            
            max_values = 1 if parent_view.single_select else len(options)
            
            super().__init__(
                placeholder=f"Select playlist{'s' if max_values > 1 else ''} (Page {parent_view.page + 1})",
                min_values=1,
                max_values=max_values,
                options=options
            )
        
        async def callback(self, interaction: discord.Interaction):
            selected_ids = [int(val) for val in self.values]
            
            if self.parent.action == "delete_playlists":
                # Delete selected playlists
                await interaction.response.defer(thinking=True, ephemeral=True)
                
                deleted_count = 0
                for playlist_id in selected_ids:
                    success = await self.parent.music_cog.db.delete_playlist(playlist_id)
                    if success:
                        deleted_count += 1
                
                await interaction.followup.send(
                    f"✅ Deleted {deleted_count} playlist{'s' if deleted_count != 1 else ''}.",
                    ephemeral=True
                )
            
            elif self.parent.action == "preload_playlist":
                # Preload all tracks in selected playlists
                await interaction.response.defer(thinking=True, ephemeral=True)
                
                total_tracks = 0
                preloaded = 0
                failed = 0
                
                for playlist_id in selected_ids:
                    playlist = await self.parent.music_cog.db.get_playlist(playlist_id)
                    if playlist:
                        total_tracks += len(playlist.tracks)
                        
                        for track in playlist.tracks:
                            if not track.is_cached and track.direct_link:
                                try:
                                    await self.parent.music_cog.cache.preload_track(track)
                                    preloaded += 1
                                except:
                                    failed += 1
                
                await interaction.followup.send(
                    f"⏳ Preloading {preloaded} tracks from {len(selected_ids)} playlist{'s' if len(selected_ids) != 1 else ''}...\n"
                    f"✅ {preloaded} added to download queue\n"
                    f"❌ {failed} failed\n"
                    f"Total tracks in playlists: {total_tracks}",
                    ephemeral=True
                )
            
            elif self.parent.action == "unload_playlist":
                # Unload all tracks in selected playlists from cache
                await interaction.response.defer(thinking=True, ephemeral=True)
                
                unloaded_count = 0
                failed_count = 0
                
                for playlist_id in selected_ids:
                    playlist = await self.parent.music_cog.db.get_playlist(playlist_id)
                    if playlist:
                        for track in playlist.tracks:
                            if track.is_cached:
                                success = await self.parent.music_cog.cache.remove_from_cache(track.filename)
                                if success:
                                    # Update database
                                    await self.parent.music_cog.db.conn.execute(
                                        'UPDATE track_stats SET is_cached = 0, cache_path = NULL WHERE filename = ?',
                                        (track.filename,)
                                    )
                                    unloaded_count += 1
                                else:
                                    failed_count += 1
                
                await self.parent.music_cog.db.conn.commit()
                
                # Calculate freed space
                freed_mb = (unloaded_count * 5)  # Approximation
                
                await interaction.followup.send(
                    f"✅ Unloaded {unloaded_count} tracks from cache\n"
                    f"❌ {failed_count} failed\n"
                    f"📊 Approximately {freed_mb} MB freed",
                    ephemeral=True
                )
            
            elif self.parent.action == "edit_playlist":
                # Edit single playlist
                playlist_id = selected_ids[0]
                playlist = await self.parent.music_cog.db.get_playlist(playlist_id)
                
                if playlist:
                    await interaction.response.send_modal(
                        EditPlaylistModal(self.parent.music_cog, playlist)
                    )
            
            elif self.parent.action in ["add_tracks", "remove_from_playlist"]:
                # Store selected playlist for next step
                self.parent.selected_playlist_id = selected_ids[0]
                
                # Get tracks for the next step
                if self.parent.action == "add_tracks":
                    # Get all tracks not in playlist
                    cursor = await self.parent.music_cog.db.conn.execute('''
                        SELECT ts.* FROM track_stats ts
                        WHERE ts.filename NOT IN (
                            SELECT track_filename FROM playlist_tracks 
                            WHERE playlist_id = ?
                        )
                        ORDER BY ts.title
                    ''', (selected_ids[0],))
                else:  # remove_from_playlist
                    # Get tracks in playlist
                    cursor = await self.parent.music_cog.db.conn.execute('''
                        SELECT ts.* FROM track_stats ts
                        JOIN playlist_tracks pt ON ts.filename = pt.track_filename
                        WHERE pt.playlist_id = ?
                        ORDER BY pt.position
                    ''', (selected_ids[0],))
                
                tracks_data = await cursor.fetchall()
                await cursor.close()
                
                # Convert to TrackInfo objects
                tracks = []
                for row in tracks_data:
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
                
                action_text = "add to" if self.parent.action == "add_tracks" else "remove from"
                
                await interaction.response.send_message(
                    embed=discord.Embed(
                        title=f"Select Tracks to {action_text.replace('_', ' ').title()}",
                        description=f"Select tracks to {action_text} **{playlist.name}**:",
                        color=discord.Color.blue()
                    ),
                    view=TrackSelectorView(
                        self.parent.music_cog, 
                        tracks, 
                        self.parent.user_id, 
                        self.parent.action,
                        playlist_id=selected_ids[0]
                    ),
                    ephemeral=True
                )
    
    @discord.ui.button(label="Previous", style=discord.ButtonStyle.grey)
    async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page"""
        if self.page > 0:
            self.page -= 1
            await self.update_view(interaction)
        else:
            await interaction.response.defer()
    
    @discord.ui.button(label="Next", style=discord.ButtonStyle.grey)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page"""
        if (self.page + 1) * self.items_per_page < len(self.playlists):
            self.page += 1
            await self.update_view(interaction)
        else:
            await interaction.response.defer()
    
    async def update_view(self, interaction: discord.Interaction):
        """Update the view with current page"""
        # Clear existing items
        self.clear_items()
        
        # Add dropdown for current page
        self.add_item(self.PlaylistDropdown(self))
        
        # Add navigation buttons
        if len(self.playlists) > self.items_per_page:
            self.add_item(NavigationButton("Previous", "previous", disabled=self.page == 0))
            self.add_item(NavigationButton("Next", "next", 
                disabled=(self.page + 1) * self.items_per_page >= len(self.playlists)))
        
        await interaction.response.edit_message(view=self)

class TrackSelectorView(discord.ui.View):
    """View for selecting tracks with action"""
    
    def __init__(self, music_cog, tracks: List[TrackInfo], user_id: int, action: str, 
                 playlist_id: int = None, single_select: bool = False):
        super().__init__(timeout=60)
        self.music_cog = music_cog
        self.tracks = tracks
        self.user_id = user_id
        self.action = action
        self.playlist_id = playlist_id
        self.single_select = single_select
        self.page = 0
        self.items_per_page = 20
        
        # Add dropdown
        self.add_item(self.TrackDropdown(self))
    
    class TrackDropdown(discord.ui.Select):
        """Dropdown for track selection"""
        
        def __init__(self, parent_view):
            self.parent = parent_view
            
            # Get tracks for current page
            start_idx = parent_view.page * parent_view.items_per_page
            end_idx = min(start_idx + parent_view.items_per_page, len(parent_view.tracks))
            page_tracks = parent_view.tracks[start_idx:end_idx]
            
            # Create options
            options = []
            for i, track in enumerate(page_tracks):
                display_name = track.display_name
                if len(display_name) > 90:
                    display_name = display_name[:87] + "..."
                
                cache_indicator = " ✅" if track.is_cached else " ⏳"
                plays_indicator = f" | {track.plays} plays" if track.plays > 0 else ""
                
                options.append(discord.SelectOption(
                    label=f"{track.title[:90]}",
                    description=f"{track.artist[:45]}{cache_indicator}{plays_indicator}",
                    value=track.filename,
                    emoji=EMOJIS['music']
                ))
            
            max_values = 1 if parent_view.single_select else len(options)
            
            super().__init__(
                placeholder=f"Select track{'s' if max_values > 1 else ''} (Page {parent_view.page + 1})",
                min_values=1,
                max_values=max_values,
                options=options
            )
        
        async def callback(self, interaction: discord.Interaction):
            selected_filenames = self.values
            
            if self.parent.action == "remove_tracks":
                # Remove tracks from library
                await interaction.response.defer(thinking=True, ephemeral=True)
                
                removed_count = 0
                failed_count = 0
                
                for filename in selected_filenames:
                    try:
                        # Remove from database
                        await self.parent.music_cog.db.conn.execute(
                            'DELETE FROM track_stats WHERE filename = ?',
                            (filename,)
                        )
                        
                        # Remove from cache if exists
                        if await self.parent.music_cog.cache.remove_from_cache(filename):
                            # Update cache size
                            pass
                        
                        # Remove from search index
                        self.parent.music_cog.search_index.remove_track(filename)
                        
                        removed_count += 1
                    except:
                        failed_count += 1
                
                await self.parent.music_cog.db.conn.commit()
                self.parent.music_cog.search_index.save()
                
                await interaction.followup.send(
                    f"✅ Removed {removed_count} track{'s' if removed_count != 1 else ''} from library\n"
                    f"❌ {failed_count} failed",
                    ephemeral=True
                )
            
            elif self.parent.action == "preload_tracks":
                # Preload selected tracks
                await interaction.response.defer(thinking=True, ephemeral=True)
                
                preloaded_count = 0
                already_cached = 0
                failed_count = 0
                
                for filename in selected_filenames:
                    track = next((t for t in self.parent.tracks if t.filename == filename), None)
                    if track:
                        if track.is_cached:
                            already_cached += 1
                        elif track.direct_link:
                            try:
                                await self.parent.music_cog.cache.preload_track(track)
                                preloaded_count += 1
                            except:
                                failed_count += 1
                
                await interaction.followup.send(
                    f"⏳ Added {preloaded_count} track{'s' if preloaded_count != 1 else ''} to download queue\n"
                    f"✅ {already_cached} already cached\n"
                    f"❌ {failed_count} failed",
                    ephemeral=True
                )
            
            elif self.parent.action == "unload_tracks":
                # Unload selected tracks from cache
                await interaction.response.defer(thinking=True, ephemeral=True)
                
                unloaded_count = 0
                failed_count = 0
                freed_mb = 0
                
                for filename in selected_filenames:
                    track = next((t for t in self.parent.tracks if t.filename == filename), None)
                    if track and track.is_cached:
                        success = await self.parent.music_cog.cache.remove_from_cache(filename)
                        if success:
                            # Update database
                            await self.parent.music_cog.db.conn.execute(
                                'UPDATE track_stats SET is_cached = 0, cache_path = NULL WHERE filename = ?',
                                (filename,)
                            )
                            unloaded_count += 1
                            freed_mb += 5  # Approximation
                        else:
                            failed_count += 1
                
                await self.parent.music_cog.db.conn.commit()
                
                await interaction.followup.send(
                    f"✅ Unloaded {unloaded_count} track{'s' if unloaded_count != 1 else ''} from cache\n"
                    f"❌ {failed_count} failed\n"
                    f"📊 Approximately {freed_mb} MB freed",
                    ephemeral=True
                )
            
            elif self.parent.action == "edit_track":
                # Edit single track
                filename = selected_filenames[0]
                track = next((t for t in self.parent.tracks if t.filename == filename), None)
                
                if track:
                    await interaction.response.send_modal(
                        EditTrackModal(self.parent.music_cog, track)
                    )
            
            elif self.parent.action == "add_tracks" and self.parent.playlist_id:
                # Add tracks to playlist
                await interaction.response.defer(thinking=True, ephemeral=True)
                
                added_count = 0
                already_in_playlist = 0
                
                for filename in selected_filenames:
                    success = await self.parent.music_cog.db.add_to_playlist(
                        self.parent.playlist_id, 
                        filename
                    )
                    if success:
                        added_count += 1
                    else:
                        already_in_playlist += 1
                
                playlist = await self.parent.music_cog.db.get_playlist(self.parent.playlist_id)
                
                await interaction.followup.send(
                    f"✅ Added {added_count} track{'s' if added_count != 1 else ''} to **{playlist.name}**\n"
                    f"⚠️ {already_in_playlist} already in playlist",
                    ephemeral=True
                )
            
            elif self.parent.action == "remove_from_playlist" and self.parent.playlist_id:
                # Remove tracks from playlist
                await interaction.response.defer(thinking=True, ephemeral=True)
                
                removed_count = 0
                
                for filename in selected_filenames:
                    success = await self.parent.music_cog.db.remove_from_playlist(
                        self.parent.playlist_id, 
                        filename
                    )
                    if success:
                        removed_count += 1
                
                playlist = await self.parent.music_cog.db.get_playlist(self.parent.playlist_id)
                
                await interaction.followup.send(
                    f"✅ Removed {removed_count} track{'s' if removed_count != 1 else ''} from **{playlist.name}**",
                    ephemeral=True
                )
    
    @discord.ui.button(label="Previous", style=discord.ButtonStyle.grey)
    async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page"""
        if self.page > 0:
            self.page -= 1
            await self.update_view(interaction)
        else:
            await interaction.response.defer()
    
    @discord.ui.button(label="Next", style=discord.ButtonStyle.grey)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page"""
        if (self.page + 1) * self.items_per_page < len(self.tracks):
            self.page += 1
            await self.update_view(interaction)
        else:
            await interaction.response.defer()
    
    async def update_view(self, interaction: discord.Interaction):
        """Update the view with current page"""
        # Clear existing items
        self.clear_items()
        
        # Add dropdown for current page
        self.add_item(self.TrackDropdown(self))
        
        # Add navigation buttons
        if len(self.tracks) > self.items_per_page:
            self.add_item(NavigationButton("Previous", "previous", disabled=self.page == 0))
            self.add_item(NavigationButton("Next", "next", 
                disabled=(self.page + 1) * self.items_per_page >= len(self.tracks)))
        
        await interaction.response.edit_message(view=self)

class EditTrackModal(discord.ui.Modal, title="Edit Track"):
    """Modal for editing track metadata"""
    
    def __init__(self, music_cog, track: TrackInfo):
        super().__init__()
        self.music_cog = music_cog
        self.track = track
        
        # Pre-fill with current values
        self.title_input = discord.ui.TextInput(
            label="Track Title",
            default=track.title,
            max_length=100,
            required=True
        )
        
        self.artist_input = discord.ui.TextInput(
            label="Artist",
            default=track.artist,
            max_length=100,
            required=True
        )
        
        self.genre_input = discord.ui.TextInput(
            label="Genre",
            default=track.genre or "",
            max_length=50,
            required=False
        )
        
        self.add_item(self.title_input)
        self.add_item(self.artist_input)
        self.add_item(self.genre_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True, ephemeral=True)
        
        try:
            # Update database
            await self.music_cog.db.conn.execute('''
                UPDATE track_stats 
                SET title = ?, artist = ?, genre = ?
                WHERE filename = ?
            ''', (
                self.title_input.value,
                self.artist_input.value,
                self.genre_input.value or None,
                self.track.filename
            ))
            
            await self.music_cog.db.conn.commit()
            
            # Update search index
            self.track.title = self.title_input.value
            self.track.artist = self.artist_input.value
            self.track.genre = self.genre_input.value or None
            self.music_cog.search_index.add_track(self.track)
            self.music_cog.search_index.save()
            
            await interaction.followup.send(
                f"✅ Updated **{self.track.title}** by **{self.track.artist}**",
                ephemeral=True
            )
            
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error updating track: {e}",
                ephemeral=True
            )

class EditPlaylistModal(discord.ui.Modal, title="Edit Playlist"):
    """Modal for editing playlist info"""
    
    def __init__(self, music_cog, playlist: Playlist):
        super().__init__()
        self.music_cog = music_cog
        self.playlist = playlist
        
        # Pre-fill with current values
        self.name_input = discord.ui.TextInput(
            label="Playlist Name",
            default=playlist.name,
            max_length=100,
            required=True
        )
        
        self.description_input = discord.ui.TextInput(
            label="Description",
            default=playlist.description or "",
            style=discord.TextStyle.paragraph,
            max_length=500,
            required=False
        )
        
        self.add_item(self.name_input)
        self.add_item(self.description_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True, ephemeral=True)
        
        try:
            # Update database
            await self.music_cog.db.conn.execute('''
                UPDATE playlists 
                SET name = ?, description = ?
                WHERE id = ?
            ''', (
                self.name_input.value,
                self.description_input.value or None,
                self.playlist.id
            ))
            
            await self.music_cog.db.conn.commit()
            
            await interaction.followup.send(
                f"✅ Updated playlist **{self.name_input.value}**",
                ephemeral=True
            )
            
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error updating playlist: {e}",
                ephemeral=True
            )

# Autocomplete for /play command
async def play_autocomplete(
    interaction: discord.Interaction,
    current: str
) -> List[app_commands.Choice[str]]:
    """Autocomplete for /play command"""
    if not interaction.guild:
        return []
    
    music_cog = interaction.client.get_cog('Music')
    if not music_cog:
        return []
    
    # Search for tracks
    search_results = music_cog.search_index.search(current, limit=25)
    
    choices = []
    for filename, score in search_results:
        track = await music_cog.db.get_track(filename)
        if track:
            display_text = f"{track.title} - {track.artist}"
            if len(display_text) > 95:
                display_text = display_text[:92] + "..."
            
            cache_indicator = " ✅" if track.is_cached else ""
            choices.append(
                app_commands.Choice(
                    name=f"{display_text}{cache_indicator}",
                    value=filename
                )
            )
    
    return choices[:25]

# Slash Commands
@app_commands.command(name="play", description="Play a track from the library")
@app_commands.describe(
    query="Search for a track to play",
    playlist="Optional: Play a playlist instead"
)
@app_commands.autocomplete(query=play_autocomplete)
async def play_command(interaction: discord.Interaction, query: str = None, playlist: str = None):
    """Play a track or playlist"""
    music_cog = interaction.client.get_cog('Music')
    
    if not await music_cog.ensure_voice(interaction):
        return
    
    if not query and not playlist:
        await interaction.response.send_message(
            "❌ Please provide a track to play or select a playlist.",
            ephemeral=True
        )
        return
    
    await interaction.response.defer()
    
    player = await music_cog.get_player(interaction.guild.id)
    
    try:
        if playlist:
            # Play a playlist
            try:
                playlist_id = int(playlist)
                playlist_obj = await music_cog.db.get_playlist(playlist_id)
                
                if not playlist_obj:
                    await interaction.followup.send("❌ Playlist not found.")
                    return
                
                # Add all tracks from playlist to queue
                for track in playlist_obj.tracks:
                    player.add_to_queue(track)
                
                # Start playback if not already playing
                if not player.is_playing:
                    await music_cog.play_next(interaction.guild.id)
                
                await interaction.followup.send(
                    f"📋 Added **{playlist_obj.name}** ({len(playlist_obj.tracks)} tracks) to queue!"
                )
                
            except ValueError:
                await interaction.followup.send("❌ Invalid playlist ID.")
        
        else:
            # Play a single track
            track = await music_cog.db.get_track(query)
            
            if not track:
                # Try to search for track
                search_results = music_cog.search_index.search(query, limit=1)
                if search_results:
                    track = await music_cog.db.get_track(search_results[0][0])
            
            if not track:
                await interaction.followup.send("❌ Track not found.")
                return
            
            # Add track to queue
            player.add_to_queue(track)
            
            # Start playback if not already playing
            if not player.is_playing:
                await music_cog.play_next(interaction.guild.id)
                await interaction.followup.send(f"🎵 Now playing **{track.title}** by **{track.artist}**!")
            else:
                await interaction.followup.send(f"🎵 Added **{track.title}** to queue!")
        
        # Preload next tracks
        await music_cog.preload_queue_tracks(interaction.guild.id)
        
    except Exception as e:
        logger.error(f"Error in play command: {e}")
        await interaction.followup.send(f"❌ Error playing track: {e}")

@app_commands.command(name="managemusic", description="Open music management panel")
async def managemusic_command(interaction: discord.Interaction):
    """Open management panel"""
    music_cog = interaction.client.get_cog('Music')
    
    embed = discord.Embed(
        title="🎵 Music Management Panel",
        description="Welcome to the music management interface!",
        color=discord.Color.blue()
    ).add_field(
        name="Sections",
        value="• **Add Content**: Add tracks or create playlists\n"
              "• **Remove Content**: Remove tracks or delete playlists\n"
              "• **Manage**: Preload, unload, or edit content\n"
              "• **Statistics**: View library stats\n"
              "• **Help**: Get help with all features",
        inline=False
    ).set_footer(text="Click the buttons below to navigate")
    
    await interaction.response.send_message(
        embed=embed,
        view=music_cog.ManageMusicView(music_cog, interaction.user.id),
        ephemeral=True
    )

@app_commands.command(name="testlink", description="Test a cloud storage link")
@app_commands.describe(link="The link to test")
async def testlink_command(interaction: discord.Interaction, link: str):
    """Test a cloud storage link"""
    music_cog = interaction.client.get_cog('Music')
    
    await interaction.response.defer(thinking=True, ephemeral=True)
    
    try:
        result = await music_cog.link_resolver.test_link(link)
        
        embed = discord.Embed(
            title="🔗 Link Test Results",
            color=discord.Color.blue()
        )
        
        embed.add_field(name="Original URL", value=link[:500], inline=False)
        embed.add_field(name="Service", value=result['service'] or "Unknown", inline=True)
        embed.add_field(name="Status", value=result['status'], inline=True)
        
        if result['direct_url']:
            embed.add_field(name="Direct URL", value=result['direct_url'][:500], inline=False)
        
        if result['file_size'] > 0:
            size_mb = result['file_size'] / (1024 * 1024)
            embed.add_field(name="File Size", value=f"{size_mb:.2f} MB", inline=True)
        
        if result['content_type']:
            embed.add_field(name="Content Type", value=result['content_type'], inline=True)
        
        if result['is_supported']:
            embed.add_field(name="Supported", value="✅ Yes", inline=True)
        else:
            embed.add_field(name="Supported", value="⚠️ May not be audio", inline=True)
        
        await interaction.followup.send(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.followup.send(f"❌ Error testing link: {e}", ephemeral=True)

# Text Commands (Jockey-style)
@commands.command(name="play", aliases=["p"])
async def text_play(self, ctx: commands.Context, *, query: str = None):
    """Play a track (text command)"""
    # Convert to slash command style
    interaction = await self.bot.get_context(ctx)
    interaction.command = self.play_command
    interaction.data = {"options": [{"name": "query", "value": query}]} if query else {}
    
    await self.play_command.callback(self, interaction)

@commands.command(name="skip", aliases=["s"])
async def text_skip(self, ctx: commands.Context):
    """Skip current track"""
    player = self.players.get(ctx.guild.id)
    
    if not player or not player.voice_client or not player.is_playing:
        await ctx.send("❌ Nothing is playing.")
        return
    
    # Add to history
    if player.current_track:
        player.history.append(player.current_track)
        if len(player.history) > 50:
            player.history.pop(0)
        
        # Increment skip count
        await self.db.increment_skip(player.current_track.filename)
    
    # Stop current playback
    if player.voice_client.is_playing():
        player.voice_client.stop()
    
    player.update_activity()
    await ctx.send("⏭️ Skipped!")

@commands.command(name="pause")
async def text_pause(self, ctx: commands.Context):
    """Pause playback"""
    player = self.players.get(ctx.guild.id)
    
    if not player or not player.voice_client or not player.is_playing:
        await ctx.send("❌ Nothing is playing.")
        return
    
    if player.is_paused:
        await ctx.send("⚠️ Already paused.")
        return
    
    player.voice_client.pause()
    player.is_paused = True
    player.update_activity()
    
    await self.update_now_playing(ctx.guild.id)
    await ctx.send("⏸️ Paused!")

@commands.command(name="resume", aliases=["r"])
async def text_resume(self, ctx: commands.Context):
    """Resume playback"""
    player = self.players.get(ctx.guild.id)
    
    if not player or not player.voice_client:
        await ctx.send("❌ Nothing is playing.")
        return
    
    if not player.is_paused:
        await ctx.send("⚠️ Already playing.")
        return
    
    player.voice_client.resume()
    player.is_paused = False
    player.update_activity()
    
    await self.update_now_playing(ctx.guild.id)
    await ctx.send("▶️ Resumed!")

@commands.command(name="stop")
async def text_stop(self, ctx: commands.Context):
    """Stop playback and clear queue"""
    player = self.players.get(ctx.guild.id)
    
    if not player:
        await ctx.send("❌ No player active.")
        return
    
    # Stop playback
    if player.voice_client:
        player.voice_client.stop()
    
    # Clear queue
    player.clear_queue()
    player.current_track = None
    player.is_playing = False
    player.is_paused = False
    
    # Update display
    await self.update_now_playing(ctx.guild.id)
    await ctx.send("⏹️ Playback stopped and queue cleared!")

@commands.command(name="queue", aliases=["q"])
async def text_queue(self, ctx: commands.Context):
    """Show current queue"""
    player = self.players.get(ctx.guild.id)
    
    if not player:
        await ctx.send("❌ No player active.")
        return
    
    if not player.queue and not player.current_track:
        await ctx.send("❌ Queue is empty.")
        return
    
    embed = discord.Embed(
        title=f"{EMOJIS['queue']} Queue",
        color=discord.Color.blue()
    )
    
    # Show current track
    if player.current_track:
        embed.add_field(
            name="Now Playing",
            value=f"**{player.current_track.title}** - {player.current_track.artist}",
            inline=False
        )
    
    # Show queue
    if player.queue:
        queue_text = ""
        for i, track in enumerate(player.queue[:15], 1):
            cache_indicator = " ✅" if track.is_cached else " ⏳"
            queue_text += f"{i}. **{track.title}** - {track.artist}{cache_indicator}\n"
        
        if len(player.queue) > 15:
            queue_text += f"\n...and {len(player.queue) - 15} more tracks"
        
        embed.add_field(
            name=f"Up Next ({len(player.queue)} tracks)",
            value=queue_text,
            inline=False
        )
    else:
        embed.add_field(
            name="Up Next",
            value="Queue is empty",
            inline=False
        )
    
    # Add playback info
    if player.current_track:
        embed.add_field(name="Loop Mode", value=player.loop_mode.capitalize(), inline=True)
        embed.add_field(name="Volume", value=f"{int(player.volume * 100)}%", inline=True)
    
    await ctx.send(embed=embed)

@commands.command(name="nowplaying", aliases=["np"])
async def text_nowplaying(self, ctx: commands.Context):
    """Show current track info"""
    player = self.players.get(ctx.guild.id)
    
    if not player or not player.current_track:
        await ctx.send("❌ Nothing is playing.")
        return
    
    track = player.current_track
    
    embed = discord.Embed(
        title=f"{EMOJIS['music']} Now Playing",
        color=discord.Color.green()
    )
    
    embed.description = f"**{track.title}**\nby {track.artist}"
    
    if track.genre:
        embed.add_field(name="Genre", value=track.genre, inline=True)
    
    embed.add_field(name="Plays", value=str(track.plays + 1), inline=True)
    
    # Cache status
    cache_status = f"{EMOJIS['success']} Cached" if track.is_cached else f"{EMOJIS['warning']} Streaming"
    embed.add_field(name="Status", value=cache_status, inline=True)
    
    # Playback info
    status = f"{EMOJIS['play']} Playing" if not player.is_paused else f"{EMOJIS['pause']} Paused"
    embed.add_field(name="Status", value=status, inline=True)
    embed.add_field(name="Volume", value=f"{int(player.volume * 100)}%", inline=True)
    embed.add_field(name="Loop", value=player.loop_mode.capitalize(), inline=True)
    
    # Queue info
    if player.queue:
        next_track = player.queue[0]
        embed.add_field(
            name="Next Up",
            value=f"**{next_track.title}** - {next_track.artist}",
            inline=False
        )
    
    await ctx.send(embed=embed)

@commands.command(name="previous", aliases=["prev"])
async def text_previous(self, ctx: commands.Context):
    """Play previous track"""
    player = self.players.get(ctx.guild.id)
    
    if not player:
        await ctx.send("❌ No player active.")
        return
    
    # Add current track to beginning of queue and play previous from history
    if player.current_track and player.history:
        if player.current_track:
            player.queue.insert(0, player.current_track)
        
        previous_track = player.history.pop()
        player.queue.insert(0, previous_track)
        
        # Skip current
        if player.voice_client and player.voice_client.is_playing():
            player.voice_client.stop()
        
        await ctx.send("⏮️ Playing previous track!")
    else:
        await ctx.send("❌ No previous track.")

@commands.command(name="volume", aliases=["vol"])
async def text_volume(self, ctx: commands.Context, volume: int = None):
    """Set volume (1-100)"""
    player = self.players.get(ctx.guild.id)
    
    if not player or not player.voice_client:
        await ctx.send("❌ No player active.")
        return
    
    if volume is None:
        await ctx.send(f"🔊 Current volume: {int(player.volume * 100)}%")
        return
    
    if volume < 1 or volume > 100:
        await ctx.send("❌ Volume must be between 1 and 100.")
        return
    
    player.volume = volume / 100
    if player.voice_client.source:
        player.voice_client.source.volume = player.volume
    
    player.update_activity()
    await self.update_now_playing(ctx.guild.id)
    await ctx.send(f"🔊 Volume set to {volume}%")

@commands.command(name="loop")
async def text_loop(self, ctx: commands.Context, mode: str = None):
    """Set loop mode (off, track, queue)"""
    player = self.players.get(ctx.guild.id)
    
    if not player:
        await ctx.send("❌ No player active.")
        return
    
    if mode is None:
        await ctx.send(f"🔁 Current loop mode: {player.loop_mode.capitalize()}")
        return
    
    mode = mode.lower()
    if mode not in ['off', 'track', 'queue']:
        await ctx.send("❌ Invalid mode. Use: off, track, or queue")
        return
    
    player.loop_mode = mode
    player.update_activity()
    
    await self.update_now_playing(ctx.guild.id)
    await ctx.send(f"🔁 Loop mode set to: {mode.capitalize()}")

@commands.command(name="shuffle")
async def text_shuffle(self, ctx: commands.Context):
    """Shuffle the queue"""
    player = self.players.get(ctx.guild.id)
    
    if not player:
        await ctx.send("❌ No player active.")
        return
    
    if player.queue:
        player.shuffle_queue()
        await ctx.send("🔀 Queue shuffled!")
    else:
        await ctx.send("❌ Queue is empty.")

@commands.command(name="remove")
async def text_remove(self, ctx: commands.Context, *, positions: str):
    """Remove tracks from queue (e.g., "1,3,5-7" or "all")"""
    player = self.players.get(ctx.guild.id)
    
    if not player:
        await ctx.send("❌ No player active.")
        return
    
    if not player.queue:
        await ctx.send("❌ Queue is empty.")
        return
    
    if positions.lower() == 'all':
        removed_count = len(player.queue)
        player.clear_queue()
        await ctx.send(f"✅ Removed all {removed_count} tracks from queue.")
        return
    
    # Parse positions
    positions_to_remove = set()
    
    for part in positions.split(','):
        part = part.strip()
        if '-' in part:
            # Range
            try:
                start, end = map(int, part.split('-'))
                for pos in range(start, end + 1):
                    if 1 <= pos <= len(player.queue):
                        positions_to_remove.add(pos - 1)  # Convert to 0-indexed
            except:
                continue
        else:
            # Single position
            try:
                pos = int(part)
                if 1 <= pos <= len(player.queue):
                    positions_to_remove.add(pos - 1)  # Convert to 0-indexed
            except:
                continue
    
    if not positions_to_remove:
        await ctx.send("❌ No valid positions provided.")
        return
    
    # Remove tracks (from highest to lowest to maintain indices)
    removed_tracks = []
    for pos in sorted(positions_to_remove, reverse=True):
        if 0 <= pos < len(player.queue):
            removed_tracks.append(player.queue.pop(pos))
    
    player.update_activity()
    
    if removed_tracks:
        track_list = "\n".join([f"• {track.title}" for track in removed_tracks[:5]])
        if len(removed_tracks) > 5:
            track_list += f"\n...and {len(removed_tracks) - 5} more"
        
        await ctx.send(f"✅ Removed {len(removed_tracks)} track{'s' if len(removed_tracks) != 1 else ''}:\n{track_list}")
    else:
        await ctx.send("❌ No tracks were removed.")

# Add commands to cog
async def setup(bot: commands.Bot):
    """Setup function for the cog"""
    music_cog = Music(bot)
    
    # Add slash commands
    music_cog.play_command = play_command
    music_cog.managemusic_command = managemusic_command
    music_cog.testlink_command = testlink_command
    
    # Add text commands
    music_cog.text_play = text_play
    music_cog.text_skip = text_skip
    music_cog.text_pause = text_pause
    music_cog.text_resume = text_resume
    music_cog.text_stop = text_stop
    music_cog.text_queue = text_queue
    music_cog.text_nowplaying = text_nowplaying
    music_cog.text_previous = text_previous
    music_cog.text_volume = text_volume
    music_cog.text_loop = text_loop
    music_cog.text_shuffle = text_shuffle
    music_cog.text_remove = text_remove
    
    # Add commands to cog
    bot.tree.add_command(play_command)
    bot.tree.add_command(managemusic_command)
    bot.tree.add_command(testlink_command)
    
    await bot.add_cog(music_cog)
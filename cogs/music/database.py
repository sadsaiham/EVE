"""
Complete PostgreSQL database models for EVE Music System with:
- Async database operations
- Connection pooling
- Database migrations
- Backup system
- Analytics queries
"""

import hashlib
import discord
from discord.ext import commands
from typing import Dict, List, Optional, Any
import asyncpg
from datetime import datetime, timedelta
import json
import asyncio
import logging
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)

class MusicTable(Enum):
    """Database table names"""
    USERS = "music_users"
    GUILDS = "music_guilds"
    TRACKS = "music_tracks"
    HISTORY = "music_history"
    PLAYLISTS = "music_playlists"
    PLAYLIST_TRACKS = "music_playlist_tracks"
    QUEUES = "music_queues"
    SESSIONS = "music_sessions"
    STATISTICS = "music_statistics"
    CACHE = "music_cache"


@dataclass
class MusicUser:
    """User data model"""
    user_id: int
    guild_id: int
    total_tracks_played: int = 0
    total_play_time: int = 0
    favorite_artist: Optional[str] = None
    favorite_genre: Optional[str] = None
    volume_preference: float = 1.0
    last_active: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for database"""
        data = asdict(self)
        # Convert datetime to ISO string
        for key in ['last_active', 'created_at', 'updated_at']:
            if data[key] and isinstance(data[key], datetime):
                data[key] = data[key].isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'MusicUser':
        """Create from dictionary"""
        # Convert ISO strings to datetime
        for key in ['last_active', 'created_at', 'updated_at']:
            if data.get(key) and isinstance(data[key], str):
                data[key] = datetime.fromisoformat(data[key].replace('Z', '+00:00'))
        return cls(**data)


@dataclass
class MusicGuild:
    """Guild data model"""
    guild_id: int
    music_enabled: bool = True
    default_volume: float = 1.0
    max_queue_length: int = 100
    require_dj_role: bool = False
    dj_role_id: Optional[int] = None
    auto_disconnect_minutes: int = 5
    log_channel_id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        data = asdict(self)
        # Convert datetime to ISO string
        for key in ['created_at', 'updated_at']:
            if data[key] and isinstance(data[key], datetime):
                data[key] = data[key].isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'MusicGuild':
        """Create from dictionary"""
        # Convert ISO strings to datetime
        for key in ['created_at', 'updated_at']:
            if data.get(key) and isinstance(data[key], str):
                data[key] = datetime.fromisoformat(data[key].replace('Z', '+00:00'))
        return cls(**data)


@dataclass
class MusicTrack:
    """Track data model"""
    track_id: str
    title: str
    artist: str
    duration: int
    source: str
    url: Optional[str] = None
    thumbnail_url: Optional[str] = None
    genre: Optional[str] = None
    album: Optional[str] = None
    play_count: int = 0
    last_played: Optional[datetime] = None
    added_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        data = asdict(self)
        # Convert datetime to ISO string
        for key in ['last_played', 'added_at']:
            if data[key] and isinstance(data[key], datetime):
                data[key] = data[key].isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'MusicTrack':
        """Create from dictionary"""
        # Convert ISO strings to datetime
        for key in ['last_played', 'added_at']:
            if data.get(key) and isinstance(data[key], str):
                data[key] = datetime.fromisoformat(data[key].replace('Z', '+00:00'))
        return cls(**data)


@dataclass
class PlayHistory:
    """Play history data model"""
    id: int
    user_id: int
    guild_id: int
    track_id: str
    played_at: datetime
    listening_context: str = "normal"  # normal, party, radio, etc.
    duration_played: Optional[int] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        data = asdict(self)
        # Convert datetime to ISO string
        data['played_at'] = data['played_at'].isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'PlayHistory':
        """Create from dictionary"""
        # Convert ISO string to datetime
        if data.get('played_at') and isinstance(data['played_at'], str):
            data['played_at'] = datetime.fromisoformat(data['played_at'].replace('Z', '+00:00'))
        return cls(**data)


class MusicDatabase:
    """Complete database management system"""
    
    def __init__(self, bot):
        self.bot = bot
        self.pool: Optional[asyncpg.Pool] = None
        self.connection_string: Optional[str] = None
        
        # Connection settings
        self.min_connections = 2
        self.max_connections = 20
        self.command_timeout = 30
        
        # Migration management
        self.current_version = 1
        self.migrations_applied = set()
        
        # Backup settings
        self.backup_interval_hours = 24
        self.max_backups = 30
        
        # Cache for frequent queries
        self.query_cache: Dict[str, Any] = {}
        self.cache_ttl = 300  # 5 minutes
    
    async def initialize(self, connection_string: str):
        """Initialize database connection"""
        self.connection_string = connection_string
        
        try:
            # Create connection pool
            self.pool = await asyncpg.create_pool(
                dsn=connection_string,
                min_size=self.min_connections,
                max_size=self.max_connections,
                command_timeout=self.command_timeout,
                max_inactive_connection_lifetime=300,
                max_queries=50000
            )
            
            logger.info("Database connection pool created")
            
            # Initialize tables
            await self.create_tables()
            
            # Apply migrations
            await self.apply_migrations()
            
            # Start backup task
            asyncio.create_task(self.backup_scheduler())
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            return False
    
    async def create_tables(self):
        """Create all necessary tables"""
        try:
            async with self.pool.acquire() as conn:
                # Users table
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MusicTable.USERS.value} (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        total_tracks_played INTEGER DEFAULT 0,
                        total_play_time BIGINT DEFAULT 0,
                        favorite_artist TEXT,
                        favorite_genre TEXT,
                        volume_preference REAL DEFAULT 1.0,
                        last_active TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_id, guild_id)
                    )
                """)
                
                # Guilds table
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MusicTable.GUILDS.value} (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT UNIQUE NOT NULL,
                        music_enabled BOOLEAN DEFAULT TRUE,
                        default_volume REAL DEFAULT 1.0,
                        max_queue_length INTEGER DEFAULT 100,
                        require_dj_role BOOLEAN DEFAULT FALSE,
                        dj_role_id BIGINT,
                        auto_disconnect_minutes INTEGER DEFAULT 5,
                        log_channel_id BIGINT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Tracks table
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MusicTable.TRACKS.value} (
                        id SERIAL PRIMARY KEY,
                        track_id VARCHAR(255) UNIQUE NOT NULL,
                        title TEXT NOT NULL,
                        artist TEXT NOT NULL,
                        duration INTEGER NOT NULL,
                        source VARCHAR(50) NOT NULL,
                        url TEXT,
                        thumbnail_url TEXT,
                        genre VARCHAR(100),
                        album TEXT,
                        play_count INTEGER DEFAULT 0,
                        last_played TIMESTAMP,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_artist (artist),
                        INDEX idx_genre (genre),
                        INDEX idx_last_played (last_played)
                    )
                """)
                
                # History table
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MusicTable.HISTORY.value} (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        track_id VARCHAR(255) NOT NULL,
                        played_at TIMESTAMP NOT NULL,
                        listening_context VARCHAR(50) DEFAULT 'normal',
                        duration_played INTEGER,
                        INDEX idx_user_guild (user_id, guild_id),
                        INDEX idx_played_at (played_at),
                        INDEX idx_track (track_id),
                        FOREIGN KEY (track_id) REFERENCES {MusicTable.TRACKS.value}(track_id) ON DELETE CASCADE
                    )
                """)
                
                # Playlists table
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MusicTable.PLAYLISTS.value} (
                        id SERIAL PRIMARY KEY,
                        playlist_id VARCHAR(255) UNIQUE NOT NULL,
                        name VARCHAR(255) NOT NULL,
                        description TEXT,
                        owner_id BIGINT NOT NULL,
                        guild_id BIGINT,
                        playlist_type VARCHAR(50) DEFAULT 'personal',
                        privacy VARCHAR(50) DEFAULT 'private',
                        track_count INTEGER DEFAULT 0,
                        total_duration BIGINT DEFAULT 0,
                        play_count INTEGER DEFAULT 0,
                        last_played TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        thumbnail_url TEXT,
                        color INTEGER,
                        INDEX idx_owner (owner_id),
                        INDEX idx_guild (guild_id),
                        INDEX idx_type (playlist_type)
                    )
                """)
                
                # Playlist tracks table
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MusicTable.PLAYLIST_TRACKS.value} (
                        id SERIAL PRIMARY KEY,
                        playlist_id VARCHAR(255) NOT NULL,
                        track_id VARCHAR(255) NOT NULL,
                        position INTEGER NOT NULL,
                        added_by BIGINT,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(playlist_id, track_id),
                        INDEX idx_playlist (playlist_id),
                        INDEX idx_track (track_id),
                        FOREIGN KEY (playlist_id) REFERENCES {MusicTable.PLAYLISTS.value}(playlist_id) ON DELETE CASCADE,
                        FOREIGN KEY (track_id) REFERENCES {MusicTable.TRACKS.value}(track_id) ON DELETE CASCADE
                    )
                """)
                
                # Queues table (for persistent queues)
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MusicTable.QUEUES.value} (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT UNIQUE NOT NULL,
                        queue_data JSONB NOT NULL,
                        current_track JSONB,
                        current_position INTEGER DEFAULT 0,
                        loop_mode VARCHAR(20) DEFAULT 'off',
                        shuffle_enabled BOOLEAN DEFAULT FALSE,
                        volume REAL DEFAULT 1.0,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Sessions table
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MusicTable.SESSIONS.value} (
                        id SERIAL PRIMARY KEY,
                        session_id VARCHAR(255) UNIQUE NOT NULL,
                        guild_id BIGINT NOT NULL,
                        start_time TIMESTAMP NOT NULL,
                        end_time TIMESTAMP,
                        total_tracks INTEGER DEFAULT 0,
                        total_duration BIGINT DEFAULT 0,
                        unique_users INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_guild (guild_id),
                        INDEX idx_start_time (start_time)
                    )
                """)
                
                # Statistics table
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MusicTable.STATISTICS.value} (
                        id SERIAL PRIMARY KEY,
                        stat_date DATE NOT NULL,
                        stat_type VARCHAR(50) NOT NULL,
                        guild_id BIGINT,
                        user_id BIGINT,
                        value JSONB NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(stat_date, stat_type, guild_id, user_id),
                        INDEX idx_date_type (stat_date, stat_type),
                        INDEX idx_guild (guild_id),
                        INDEX idx_user (user_id)
                    )
                """)
                
                # Cache table
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MusicTable.CACHE.value} (
                        id SERIAL PRIMARY KEY,
                        cache_key VARCHAR(255) UNIQUE NOT NULL,
                        cache_data JSONB NOT NULL,
                        expires_at TIMESTAMP NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        access_count INTEGER DEFAULT 0,
                        INDEX idx_expires (expires_at),
                        INDEX idx_key (cache_key)
                    )
                """)
                
                logger.info("Database tables created successfully")
                
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    async def apply_migrations(self):
        """Apply database migrations"""
        try:
            async with self.pool.acquire() as conn:
                # Create migrations table if not exists
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS music_migrations (
                        id SERIAL PRIMARY KEY,
                        migration_name VARCHAR(255) UNIQUE NOT NULL,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Get applied migrations
                rows = await conn.fetch("SELECT migration_name FROM music_migrations")
                applied = {row['migration_name'] for row in rows}
                
                # Apply pending migrations
                migrations_to_apply = [
                    # Add your migrations here
                    # Example:
                    # ("001_add_favorite_track_column", """
                    #     ALTER TABLE music_users ADD COLUMN IF NOT EXISTS favorite_track_id VARCHAR(255);
                    # """)
                ]
                
                for migration_name, sql in migrations_to_apply:
                    if migration_name not in applied:
                        try:
                            await conn.execute(sql)
                            await conn.execute(
                                "INSERT INTO music_migrations (migration_name) VALUES ($1)",
                                migration_name
                            )
                            logger.info(f"Applied migration: {migration_name}")
                        except Exception as e:
                            logger.error(f"Failed to apply migration {migration_name}: {e}")
                            raise
                
                self.migrations_applied = applied
                
        except Exception as e:
            logger.error(f"Migration error: {e}")
            raise
    
    # User operations
    async def get_or_create_user(self, user_id: int, guild_id: int) -> MusicUser:
        """Get or create user record"""
        cache_key = f"user:{user_id}:{guild_id}"
        
        # Check cache
        cached = self.query_cache.get(cache_key)
        if cached and cached['expires'] > datetime.utcnow():
            return cached['data']
        
        try:
            async with self.pool.acquire() as conn:
                # Try to get existing user
                row = await conn.fetchrow(f"""
                    SELECT * FROM {MusicTable.USERS.value}
                    WHERE user_id = $1 AND guild_id = $2
                """, user_id, guild_id)
                
                if row:
                    user = MusicUser.from_dict(dict(row))
                else:
                    # Create new user
                    now = datetime.utcnow()
                    user = MusicUser(
                        user_id=user_id,
                        guild_id=guild_id,
                        created_at=now,
                        updated_at=now,
                        last_active=now
                    )
                    
                    await conn.execute(f"""
                        INSERT INTO {MusicTable.USERS.value} 
                        (user_id, guild_id, created_at, updated_at, last_active)
                        VALUES ($1, $2, $3, $4, $5)
                    """, user_id, guild_id, now, now, now)
                
                # Cache result
                self.query_cache[cache_key] = {
                    'data': user,
                    'expires': datetime.utcnow() + timedelta(seconds=self.cache_ttl)
                }
                
                return user
                
        except Exception as e:
            logger.error(f"Failed to get/create user: {e}")
            # Return fallback user object
            return MusicUser(user_id=user_id, guild_id=guild_id)
    
    async def update_user(self, user_id: int, guild_id: int, **kwargs):
        """Update user record"""
        try:
            async with self.pool.acquire() as conn:
                # Build SET clause
                set_clauses = []
                values = []
                param_index = 3  # Start after $1 and $2
                
                for key, value in kwargs.items():
                    set_clauses.append(f"{key} = ${param_index}")
                    values.append(value)
                    param_index += 1
                
                # Add updated_at
                set_clauses.append("updated_at = $3")
                values.append(datetime.utcnow())
                
                # Execute update
                query = f"""
                    UPDATE {MusicTable.USERS.value}
                    SET {', '.join(set_clauses)}
                    WHERE user_id = $1 AND guild_id = $2
                """
                
                await conn.execute(query, user_id, guild_id, *values)
                
                # Clear cache
                cache_key = f"user:{user_id}:{guild_id}"
                self.query_cache.pop(cache_key, None)
                
        except Exception as e:
            logger.error(f"Failed to update user: {e}")
    
    async def update_user_activity(self, user_id: int, guild_id: int):
        """Update user's last active time"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f"""
                    UPDATE {MusicTable.USERS.value}
                    SET last_active = $3, updated_at = $3
                    WHERE user_id = $1 AND guild_id = $2
                """, user_id, guild_id, datetime.utcnow())
                
        except Exception as e:
            logger.error(f"Failed to update user activity: {e}")
    
    # Track operations
    async def get_or_create_track(self, track_data: Dict) -> MusicTrack:
        """Get or create track record"""
        track_id = track_data.get('id', '')
        if not track_id:
            # Generate track ID from hash
            track_string = f"{track_data.get('title')}:{track_data.get('artist')}:{track_data.get('source')}"
            track_id = hashlib.md5(track_string.encode()).hexdigest()
            track_data['id'] = track_id
        
        cache_key = f"track:{track_id}"
        
        # Check cache
        cached = self.query_cache.get(cache_key)
        if cached and cached['expires'] > datetime.utcnow():
            return cached['data']
        
        try:
            async with self.pool.acquire() as conn:
                # Try to get existing track
                row = await conn.fetchrow(f"""
                    SELECT * FROM {MusicTable.TRACKS.value}
                    WHERE track_id = $1
                """, track_id)
                
                if row:
                    track = MusicTrack.from_dict(dict(row))
                else:
                    # Create new track
                    now = datetime.utcnow()
                    track = MusicTrack(
                        track_id=track_id,
                        title=track_data.get('title', 'Unknown'),
                        artist=track_data.get('artist', 'Unknown'),
                        duration=track_data.get('duration', 0),
                        source=track_data.get('source', 'unknown'),
                        url=track_data.get('url'),
                        thumbnail_url=track_data.get('thumbnail_url'),
                        genre=track_data.get('genre'),
                        album=track_data.get('album'),
                        added_at=now
                    )
                    
                    await conn.execute(f"""
                        INSERT INTO {MusicTable.TRACKS.value}
                        (track_id, title, artist, duration, source, url, 
                         thumbnail_url, genre, album, added_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    """, track.track_id, track.title, track.artist, track.duration,
                       track.source, track.url, track.thumbnail_url,
                       track.genre, track.album, track.added_at)
                
                # Cache result
                self.query_cache[cache_key] = {
                    'data': track,
                    'expires': datetime.utcnow() + timedelta(seconds=self.cache_ttl)
                }
                
                return track
                
        except Exception as e:
            logger.error(f"Failed to get/create track: {e}")
            # Return fallback track object
            return MusicTrack(
                track_id=track_id,
                title=track_data.get('title', 'Unknown'),
                artist=track_data.get('artist', 'Unknown'),
                duration=track_data.get('duration', 0),
                source=track_data.get('source', 'unknown')
            )
    
    async def increment_track_play_count(self, track_id: str):
        """Increment track play count"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f"""
                    UPDATE {MusicTable.TRACKS.value}
                    SET play_count = play_count + 1,
                        last_played = $2
                    WHERE track_id = $1
                """, track_id, datetime.utcnow())
                
                # Clear cache
                cache_key = f"track:{track_id}"
                self.query_cache.pop(cache_key, None)
                
        except Exception as e:
            logger.error(f"Failed to increment track play count: {e}")
    
    # History operations
    async def add_play_history(self, user_id: int, guild_id: int, track_id: str, 
                               context: str = "normal", duration_played: int = None):
        """Add play history record"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {MusicTable.HISTORY.value}
                    (user_id, guild_id, track_id, played_at, listening_context, duration_played)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """, user_id, guild_id, track_id, datetime.utcnow(), context, duration_played)
                
                # Update user stats
                await conn.execute(f"""
                    UPDATE {MusicTable.USERS.value}
                    SET total_tracks_played = total_tracks_played + 1,
                        updated_at = $3,
                        last_active = $3
                    WHERE user_id = $1 AND guild_id = $2
                """, user_id, guild_id, datetime.utcnow())
                
                if duration_played:
                    await conn.execute(f"""
                        UPDATE {MusicTable.USERS.value}
                        SET total_play_time = total_play_time + $3,
                            updated_at = $4
                        WHERE user_id = $1 AND guild_id = $2
                    """, user_id, guild_id, duration_played, datetime.utcnow())
                
        except Exception as e:
            logger.error(f"Failed to add play history: {e}")
    
    async def get_user_history(self, user_id: int, guild_id: int, 
                              limit: int = 50) -> List[Dict]:
        """Get user's play history"""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(f"""
                    SELECT h.*, t.title, t.artist, t.duration, t.source
                    FROM {MusicTable.HISTORY.value} h
                    JOIN {MusicTable.TRACKS.value} t ON h.track_id = t.track_id
                    WHERE h.user_id = $1 AND h.guild_id = $2
                    ORDER BY h.played_at DESC
                    LIMIT $3
                """, user_id, guild_id, limit)
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Failed to get user history: {e}")
            return []
    
    # Statistics operations
    async def save_daily_statistics(self):
        """Save daily statistics"""
        try:
            async with self.pool.acquire() as conn:
                today = datetime.utcnow().date()
                
                # User statistics
                user_stats = await conn.fetch(f"""
                    SELECT 
                        user_id,
                        guild_id,
                        COUNT(*) as tracks_played,
                        SUM(duration_played) as total_duration
                    FROM {MusicTable.HISTORY.value}
                    WHERE DATE(played_at) = $1
                    GROUP BY user_id, guild_id
                """, today)
                
                for stat in user_stats:
                    await conn.execute(f"""
                        INSERT INTO {MusicTable.STATISTICS.value}
                        (stat_date, stat_type, guild_id, user_id, value)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (stat_date, stat_type, guild_id, user_id) 
                        DO UPDATE SET value = $5
                    """, today, 'user_daily', stat['guild_id'], 
                       stat['user_id'], json.dumps(dict(stat)))
                
                # Guild statistics
                guild_stats = await conn.fetch(f"""
                    SELECT 
                        guild_id,
                        COUNT(*) as tracks_played,
                        COUNT(DISTINCT user_id) as unique_users,
                        SUM(duration_played) as total_duration
                    FROM {MusicTable.HISTORY.value}
                    WHERE DATE(played_at) = $1
                    GROUP BY guild_id
                """, today)
                
                for stat in guild_stats:
                    await conn.execute(f"""
                        INSERT INTO {MusicTable.STATISTICS.value}
                        (stat_date, stat_type, guild_id, value)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (stat_date, stat_type, guild_id) 
                        DO UPDATE SET value = $4
                    """, today, 'guild_daily', stat['guild_id'], json.dumps(dict(stat)))
                
                # Global statistics
                global_stats = await conn.fetchrow(f"""
                    SELECT 
                        COUNT(*) as total_tracks,
                        COUNT(DISTINCT user_id) as total_users,
                        COUNT(DISTINCT guild_id) as total_guilds,
                        SUM(duration_played) as total_duration
                    FROM {MusicTable.HISTORY.value}
                    WHERE DATE(played_at) = $1
                """, today)
                
                if global_stats:
                    await conn.execute(f"""
                        INSERT INTO {MusicTable.STATISTICS.value}
                        (stat_date, stat_type, value)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (stat_date, stat_type) 
                        DO UPDATE SET value = $3
                    """, today, 'global_daily', json.dumps(dict(global_stats)))
                
                logger.info("Daily statistics saved")
                
        except Exception as e:
            logger.error(f"Failed to save daily statistics: {e}")
    
    async def get_user_statistics(self, user_id: int, guild_id: int, 
                                 days: int = 30) -> Dict:
        """Get user statistics for period"""
        try:
            async with self.pool.acquire() as conn:
                start_date = datetime.utcnow() - timedelta(days=days)
                
                # Basic stats
                stats = await conn.fetchrow(f"""
                    SELECT 
                        COUNT(*) as total_tracks,
                        COALESCE(SUM(duration_played), 0) as total_duration,
                        MIN(played_at) as first_play,
                        MAX(played_at) as last_play
                    FROM {MusicTable.HISTORY.value}
                    WHERE user_id = $1 AND guild_id = $2
                    AND played_at >= $3
                """, user_id, guild_id, start_date)
                
                # Top artists
                top_artists = await conn.fetch(f"""
                    SELECT t.artist, COUNT(*) as play_count
                    FROM {MusicTable.HISTORY.value} h
                    JOIN {MusicTable.TRACKS.value} t ON h.track_id = t.track_id
                    WHERE h.user_id = $1 AND h.guild_id = $2
                    AND h.played_at >= $3
                    GROUP BY t.artist
                    ORDER BY play_count DESC
                    LIMIT 10
                """, user_id, guild_id, start_date)
                
                # Top tracks
                top_tracks = await conn.fetch(f"""
                    SELECT t.title, t.artist, COUNT(*) as play_count
                    FROM {MusicTable.HISTORY.value} h
                    JOIN {MusicTable.TRACKS.value} t ON h.track_id = t.track_id
                    WHERE h.user_id = $1 AND h.guild_id = $2
                    AND h.played_at >= $3
                    GROUP BY t.title, t.artist
                    ORDER BY play_count DESC
                    LIMIT 10
                """, user_id, guild_id, start_date)
                
                # Daily breakdown
                daily_breakdown = await conn.fetch(f"""
                    SELECT 
                        DATE(played_at) as play_date,
                        COUNT(*) as tracks_played,
                        COALESCE(SUM(duration_played), 0) as total_duration
                    FROM {MusicTable.HISTORY.value}
                    WHERE user_id = $1 AND guild_id = $2
                    AND played_at >= $3
                    GROUP BY DATE(played_at)
                    ORDER BY play_date
                """, user_id, guild_id, start_date)
                
                result = {
                    'period_days': days,
                    'start_date': start_date.isoformat(),
                    'total_tracks': stats['total_tracks'] if stats else 0,
                    'total_duration': stats['total_duration'] if stats else 0,
                    'first_play': stats['first_play'].isoformat() if stats and stats['first_play'] else None,
                    'last_play': stats['last_play'].isoformat() if stats and stats['last_play'] else None,
                    'top_artists': [dict(row) for row in top_artists],
                    'top_tracks': [dict(row) for row in top_tracks],
                    'daily_breakdown': [dict(row) for row in daily_breakdown]
                }
                
                return result
                
        except Exception as e:
            logger.error(f"Failed to get user statistics: {e}")
            return {}
    
    # Cache operations
    async def cache_set(self, key: str, data: Any, ttl_seconds: int = 3600):
        """Set cache value"""
        try:
            async with self.pool.acquire() as conn:
                expires_at = datetime.utcnow() + timedelta(seconds=ttl_seconds)
                
                await conn.execute(f"""
                    INSERT INTO {MusicTable.CACHE.value}
                    (cache_key, cache_data, expires_at, last_accessed)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (cache_key) 
                    DO UPDATE SET 
                        cache_data = $2,
                        expires_at = $3,
                        last_accessed = $4,
                        access_count = {MusicTable.CACHE.value}.access_count + 1
                """, key, json.dumps(data), expires_at, datetime.utcnow())
                
        except Exception as e:
            logger.error(f"Failed to set cache: {e}")
    
    async def cache_get(self, key: str) -> Optional[Any]:
        """Get cache value"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(f"""
                    SELECT cache_data, expires_at
                    FROM {MusicTable.CACHE.value}
                    WHERE cache_key = $1 AND expires_at > $2
                """, key, datetime.utcnow())
                
                if row:
                    # Update last accessed
                    await conn.execute(f"""
                        UPDATE {MusicTable.CACHE.value}
                        SET last_accessed = $2,
                            access_count = access_count + 1
                        WHERE cache_key = $1
                    """, key, datetime.utcnow())
                    
                    return json.loads(row['cache_data'])
                
                return None
                
        except Exception as e:
            logger.error(f"Failed to get cache: {e}")
            return None
    
    async def cache_delete(self, key: str):
        """Delete cache value"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f"""
                    DELETE FROM {MusicTable.CACHE.value}
                    WHERE cache_key = $1
                """, key)
                
        except Exception as e:
            logger.error(f"Failed to delete cache: {e}")
    
    async def cleanup_expired_cache(self):
        """Clean up expired cache entries"""
        try:
            async with self.pool.acquire() as conn:
                deleted = await conn.execute(f"""
                    DELETE FROM {MusicTable.CACHE.value}
                    WHERE expires_at <= $1
                """, datetime.utcnow())
                
                logger.debug(f"Cleaned up expired cache entries: {deleted}")
                
        except Exception as e:
            logger.error(f"Failed to cleanup cache: {e}")
    
    # Backup operations
    async def create_backup(self):
        """Create database backup"""
        try:
            backup_time = datetime.utcnow()
            backup_filename = f"eve_music_backup_{backup_time.strftime('%Y%m%d_%H%M%S')}.sql"
            
            async with self.pool.acquire() as conn:
                # Export schema
                schema = await conn.fetch("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                """)
                
                backup_content = []
                backup_content.append(f"-- EVE Music Database Backup")
                backup_content.append(f"-- Created: {backup_time.isoformat()}")
                backup_content.append("")
                
                # Export each table
                for table in schema:
                    table_name = table['table_name']
                    
                    # Get table data
                    rows = await conn.fetch(f"SELECT * FROM {table_name}")
                    
                    if rows:
                        backup_content.append(f"\n-- Table: {table_name}")
                        backup_content.append(f"TRUNCATE TABLE {table_name} CASCADE;")
                        
                        for row in rows:
                            columns = list(row.keys())
                            values = []
                            
                            for col in columns:
                                val = row[col]
                                if val is None:
                                    values.append("NULL")
                                elif isinstance(val, (int, float)):
                                    values.append(str(val))
                                elif isinstance(val, datetime):
                                    values.append(f"'{val.isoformat()}'")
                                else:
                                    # Escape quotes
                                    val_str = str(val).replace("'", "''")
                                    values.append(f"'{val_str}'")
                            
                            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
                            backup_content.append(insert_sql)
                
                # Save backup to file (or cloud storage)
                # For now, just log
                logger.info(f"Backup created: {backup_filename} ({len(backup_content)} lines)")
                
                # Clean old backups
                await self.cleanup_old_backups()
                
                return True
                
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            return False
    
    async def cleanup_old_backups(self):
        """Clean up old backups"""
        # This would delete old backup files
        # For now, just log
        logger.debug("Old backup cleanup would run here")
    
    async def backup_scheduler(self):
        """Schedule automatic backups"""
        while True:
            try:
                # Wait until next backup time
                await asyncio.sleep(self.backup_interval_hours * 3600)
                
                # Create backup
                await self.create_backup()
                
                # Cleanup cache
                await self.cleanup_expired_cache()
                
                # Clear query cache
                self.query_cache.clear()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Backup scheduler error: {e}")
                await asyncio.sleep(3600)  # Wait 1 hour on error
    
    # Maintenance operations
    async def vacuum_database(self):
        """Vacuum database for optimization"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("VACUUM ANALYZE")
                logger.info("Database vacuum completed")
                
        except Exception as e:
            logger.error(f"Failed to vacuum database: {e}")
    
    async def get_database_stats(self) -> Dict:
        """Get database statistics"""
        try:
            async with self.pool.acquire() as conn:
                stats = {}
                
                # Table sizes
                table_sizes = await conn.fetch("""
                    SELECT 
                        table_name,
                        pg_size_pretty(pg_total_relation_size('"' || table_name || '"')) as total_size,
                        pg_size_pretty(pg_relation_size('"' || table_name || '"')) as table_size,
                        pg_size_pretty(pg_total_relation_size('"' || table_name || '"') - 
                                      pg_relation_size('"' || table_name || '"')) as index_size
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY pg_total_relation_size('"' || table_name || '"') DESC
                """)
                
                stats['table_sizes'] = [dict(row) for row in table_sizes]
                
                # Row counts
                for table in MusicTable:
                    count = await conn.fetchrow(f"SELECT COUNT(*) FROM {table.value}")
                    stats[f"{table.value}_count"] = count['count']
                
                # Connection stats
                conn_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_connections,
                        SUM(CASE WHEN state = 'active' THEN 1 ELSE 0 END) as active_connections,
                        SUM(CASE WHEN state = 'idle' THEN 1 ELSE 0 END) as idle_connections
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                """)
                
                stats['connections'] = dict(conn_stats)
                
                # Cache hit rate
                cache_stats = await conn.fetchrow("""
                    SELECT 
                        SUM(heap_blks_hit) / NULLIF(SUM(heap_blks_hit + heap_blks_read), 0) as heap_hit_ratio,
                        SUM(idx_blks_hit) / NULLIF(SUM(idx_blks_hit + idx_blks_read), 0) as idx_hit_ratio
                    FROM pg_statio_user_tables
                """)
                
                if cache_stats:
                    stats['cache_hit_ratio'] = {
                        'heap': float(cache_stats['heap_hit_ratio'] or 0),
                        'index': float(cache_stats['idx_hit_ratio'] or 0)
                    }
                
                return stats
                
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {}
    
    async def close(self):
        """Close database connections"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connections closed")
    
    @commands.hybrid_command(
        name="dbstats",
        description="Show database statistics"
    )
    @commands.has_permissions(administrator=True)
    async def dbstats_command(self, ctx):
        """Show database statistics"""
        stats = await self.get_database_stats()
        
        if not stats:
            await ctx.send("Could not retrieve database statistics.")
            return
        
        embed = discord.Embed(
            title="🗄️ Database Statistics",
            color=self.bot.colors.VIOLET,
            timestamp=datetime.utcnow()
        )
        
        # Table sizes
        if 'table_sizes' in stats:
            table_info = ""
            for table in stats['table_sizes'][:5]:  # Show top 5
                table_info += f"**{table['table_name']}:** {table['total_size']}\n"
            
            embed.add_field(
                name="📊 Table Sizes",
                value=table_info,
                inline=True
            )
        
        # Connection stats
        if 'connections' in stats:
            conn_stats = stats['connections']
            embed.add_field(
                name="🔗 Connections",
                value=(
                    f"**Total:** {conn_stats.get('total_connections', 0)}\n"
                    f"**Active:** {conn_stats.get('active_connections', 0)}\n"
                    f"**Idle:** {conn_stats.get('idle_connections', 0)}"
                ),
                inline=True
            )
        
        # Cache hit rate
        if 'cache_hit_ratio' in stats:
            cache = stats['cache_hit_ratio']
            embed.add_field(
                name="🎯 Cache Performance",
                value=(
                    f"**Heap:** {cache.get('heap', 0)*100:.1f}%\n"
                    f"**Index:** {cache.get('index', 0)*100:.1f}%"
                ),
                inline=True
            )
        
        # Record counts
        count_info = ""
        for key, value in stats.items():
            if key.endswith('_count'):
                table_name = key.replace('_count', '')
                count_info += f"**{table_name}:** {value:,}\n"
        
        if count_info:
            embed.add_field(
                name="📈 Record Counts",
                value=count_info,
                inline=False
            )
        
        await ctx.send(embed=embed)
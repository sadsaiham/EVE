"""
Complete utility system with:
- Track formatting and display
- Duration conversion
- URL validation and parsing
- Search query parsing
- Error handling and user feedback
- Rate limiting system
"""

import discord
from discord.ext import commands
from typing import Dict, List, Optional, Tuple, Union, Any
from datetime import datetime, timedelta
import re
import urllib.parse
import time
import hashlib
import asyncio
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

class TrackFormatter:
    """Professional track formatting utilities"""
    
    @staticmethod
    def format_track_display(track, include_duration: bool = True) -> str:
        """Format track for display"""
        if not track:
            return "Unknown Track"
        
        title = getattr(track, 'title', 'Unknown Title')
        artist = getattr(track, 'author', 'Unknown Artist')
        
        if include_duration and hasattr(track, 'length'):
            duration = DurationConverter.format_milliseconds(track.length)
            return f"**{title}** - {artist} `[{duration}]`"
        
        return f"**{title}** - {artist}"
    
    @staticmethod
    def format_queue_entry(track, position: int) -> str:
        """Format track for queue display"""
        if not track:
            return f"{position}. Unknown Track"
        
        title = getattr(track, 'title', 'Unknown Title')[:40]
        artist = getattr(track, 'author', 'Unknown Artist')[:30]
        
        if hasattr(track, 'length'):
            duration = DurationConverter.format_milliseconds(track.length)
            return f"**{position}.** {title} - {artist} `[{duration}]`"
        
        return f"**{position}.** {title} - {artist}"
    
    @staticmethod
    def truncate_text(text: str, max_length: int = 50, ellipsis: str = "...") -> str:
        """Truncate text with ellipsis"""
        if not text:
            return ""
        
        if len(text) <= max_length:
            return text
        
        return text[:max_length - len(ellipsis)] + ellipsis
    
    @staticmethod
    def format_large_number(number: int) -> str:
        """Format large numbers with K/M suffixes"""
        if number >= 1_000_000:
            return f"{number/1_000_000:.1f}M"
        elif number >= 1_000:
            return f"{number/1_000:.1f}K"
        return str(number)
    
    @staticmethod
    def create_progress_bar(position: int, duration: int, width: int = 20) -> str:
        """Create a visual progress bar"""
        if duration == 0:
            return "▬" * width
        
        progress = min(position / duration, 1.0)
        filled = int(width * progress)
        
        bar = "▬" * filled + "🔘" + "▬" * (width - filled - 1)
        return bar
    
    @staticmethod
    def format_track_source(source) -> str:
        """Format track source for display"""
        if not source:
            return "Unknown"
        
        source_str = str(source)
        
        source_map = {
            'youtube': 'YouTube',
            'youtubemusic': 'YouTube Music',
            'soundcloud': 'SoundCloud',
            'spotify': 'Spotify',
            'bandcamp': 'Bandcamp',
            'twitch': 'Twitch',
            'http': 'Direct URL'
        }
        
        for key, display in source_map.items():
            if key in source_str.lower():
                return display
        
        return source_str.split('.')[-1].title()


class DurationConverter:
    """Duration conversion and formatting utilities"""
    
    @staticmethod
    def format_milliseconds(milliseconds: int) -> str:
        """Format milliseconds to MM:SS or HH:MM:SS"""
        seconds = milliseconds // 1000
        
        if seconds >= 3600:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            secs = seconds % 60
            return f"{hours}:{minutes:02d}:{secs:02d}"
        else:
            minutes = seconds // 60
            secs = seconds % 60
            return f"{minutes}:{secs:02d}"
    
    @staticmethod
    def format_seconds(seconds: int) -> str:
        """Format seconds to human readable string"""
        if seconds >= 86400:  # More than a day
            days = seconds // 86400
            hours = (seconds % 86400) // 3600
            return f"{days}d {hours}h"
        elif seconds >= 3600:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}h {minutes}m"
        elif seconds >= 60:
            minutes = seconds // 60
            return f"{minutes}m"
        else:
            return f"{seconds}s"
    
    @staticmethod
    def parse_duration_string(duration_str: str) -> Optional[int]:
        """Parse duration string to milliseconds"""
        try:
            # Handle MM:SS or HH:MM:SS format
            parts = duration_str.split(':')
            
            if len(parts) == 2:  # MM:SS
                minutes, seconds = parts
                total_ms = (int(minutes) * 60 + int(seconds)) * 1000
            elif len(parts) == 3:  # HH:MM:SS
                hours, minutes, seconds = parts
                total_ms = (int(hours) * 3600 + int(minutes) * 60 + int(seconds)) * 1000
            else:
                return None
            
            return total_ms
        except:
            return None
    
    @staticmethod
    def calculate_remaining_time(position: int, duration: int) -> str:
        """Calculate remaining time from position"""
        if duration <= position:
            return "0:00"
        
        remaining_ms = duration - position
        return DurationConverter.format_milliseconds(remaining_ms)
    
    @staticmethod
    def calculate_time_ago(timestamp: datetime) -> str:
        """Calculate time ago from timestamp"""
        now = datetime.utcnow()
        diff = now - timestamp
        
        if diff.days > 365:
            years = diff.days // 365
            return f"{years} year{'s' if years != 1 else ''} ago"
        elif diff.days > 30:
            months = diff.days // 30
            return f"{months} month{'s' if months != 1 else ''} ago"
        elif diff.days > 0:
            return f"{diff.days} day{'s' if diff.days != 1 else ''} ago"
        elif diff.seconds > 3600:
            hours = diff.seconds // 3600
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif diff.seconds > 60:
            minutes = diff.seconds // 60
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        else:
            return "just now"


class URLValidator:
    """URL validation and parsing utilities"""
    
    # Platform patterns
    PLATFORM_PATTERNS = {
        'youtube': [
            r'(https?://)?(www\.)?(youtube\.com|youtu\.be)/',
            r'(https?://)?(www\.)?youtube\.com/watch\?v=',
            r'(https?://)?(www\.)?youtube\.com/playlist\?list='
        ],
        'spotify': [
            r'(https?://)?(open\.)?spotify\.com/',
            r'spotify:(track|album|playlist|artist):'
        ],
        'soundcloud': [
            r'(https?://)?(www\.)?soundcloud\.com/'
        ],
        'bandcamp': [
            r'(https?://)?(.*\.)?bandcamp\.com/'
        ],
        'twitch': [
            r'(https?://)?(www\.)?twitch\.tv/'
        ]
    }
    
    @staticmethod
    def validate_url(url: str) -> bool:
        """Validate if string is a URL"""
        try:
            result = urllib.parse.urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False
    
    @staticmethod
    def detect_platform(url: str) -> Optional[str]:
        """Detect which platform a URL belongs to"""
        url_lower = url.lower()
        
        for platform, patterns in URLValidator.PLATFORM_PATTERNS.items():
            for pattern in patterns:
                if re.match(pattern, url_lower):
                    return platform
        
        return None
    
    @staticmethod
    def extract_youtube_id(url: str) -> Optional[str]:
        """Extract YouTube video ID from URL"""
        patterns = [
            r'(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})',
            r'youtube\.com/embed/([a-zA-Z0-9_-]{11})',
            r'youtube\.com/v/([a-zA-Z0-9_-]{11})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        return None
    
    @staticmethod
    def extract_spotify_id(url: str) -> Optional[Tuple[str, str]]:
        """Extract Spotify ID and type from URL"""
        patterns = {
            'track': r'spotify\.com/track/([a-zA-Z0-9]{22})',
            'album': r'spotify\.com/album/([a-zA-Z0-9]{22})',
            'playlist': r'spotify\.com/playlist/([a-zA-Z0-9]{22})',
            'artist': r'spotify\.com/artist/([a-zA-Z0-9]{22})'
        }
        
        for type_name, pattern in patterns.items():
            match = re.search(pattern, url)
            if match:
                return type_name, match.group(1)
        
        return None
    
    @staticmethod
    def is_playlist_url(url: str) -> bool:
        """Check if URL is a playlist"""
        playlist_patterns = [
            r'playlist\?list=',
            r'spotify\.com/playlist/',
            r'soundcloud\.com/.*/sets/',
            r'youtube\.com/playlist\?list='
        ]
        
        for pattern in playlist_patterns:
            if re.search(pattern, url.lower()):
                return True
        
        return False
    
    @staticmethod
    def clean_url(url: str) -> str:
        """Clean URL by removing tracking parameters"""
        try:
            parsed = urllib.parse.urlparse(url)
            
            # Remove common tracking parameters
            query_params = urllib.parse.parse_qs(parsed.query)
            filtered_params = {}
            
            for key, values in query_params.items():
                # Keep essential parameters, remove tracking
                if key in ['v', 'list', 't', 'start']:
                    filtered_params[key] = values
            
            # Rebuild URL
            filtered_query = urllib.parse.urlencode(filtered_params, doseq=True)
            cleaned = urllib.parse.urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                filtered_query,
                parsed.fragment
            ))
            
            return cleaned
        except:
            return url


class SearchParser:
    """Search query parsing and processing"""
    
    @staticmethod
    def parse_search_query(query: str) -> Dict[str, Any]:
        """Parse search query into components"""
        result = {
            'original': query,
            'keywords': [],
            'filters': {},
            'is_url': False,
            'platform': None
        }
        
        # Check if it's a URL
        if URLValidator.validate_url(query):
            result['is_url'] = True
            result['platform'] = URLValidator.detect_platform(query)
            return result
        
        # Parse keywords and filters
        words = query.split()
        keywords = []
        filters = {}
        
        for word in words:
            if ':' in word and len(word) > 2:  # Could be a filter
                parts = word.split(':', 1)
                if len(parts) == 2:
                    filter_key = parts[0].lower()
                    filter_value = parts[1]
                    
                    # Known filters
                    if filter_key in ['artist', 'band', 'singer']:
                        filters['artist'] = filter_value
                    elif filter_key in ['album', 'record']:
                        filters['album'] = filter_value
                    elif filter_key in ['year', 'released']:
                        filters['year'] = filter_value
                    elif filter_key in ['genre', 'style']:
                        filters['genre'] = filter_value
                    elif filter_key in ['duration', 'length']:
                        filters['duration'] = filter_value
                    else:
                        keywords.append(word)
                else:
                    keywords.append(word)
            else:
                keywords.append(word)
        
        result['keywords'] = keywords
        result['filters'] = filters
        
        # Check for platform prefixes
        platform_prefixes = {
            'yt': 'youtube',
            'sc': 'soundcloud',
            'sp': 'spotify',
            'bc': 'bandcamp'
        }
        
        if keywords and keywords[0].lower() in platform_prefixes:
            result['platform'] = platform_prefixes[keywords[0].lower()]
            result['keywords'] = keywords[1:]  # Remove platform prefix
        
        return result
    
    @staticmethod
    def build_search_query(parsed_query: Dict) -> str:
        """Build search query from parsed components"""
        if parsed_query['is_url']:
            return parsed_query['original']
        
        # Combine keywords
        query_parts = parsed_query['keywords']
        
        # Add filters
        for key, value in parsed_query['filters'].items():
            if key == 'artist':
                query_parts.append(f"artist:{value}")
            elif key == 'album':
                query_parts.append(f"album:{value}")
            elif key == 'year':
                query_parts.append(f"year:{value}")
            elif key == 'genre':
                query_parts.append(f"genre:{value}")
        
        # Add platform prefix if specified
        if parsed_query['platform']:
            platform_map = {
                'youtube': 'ytsearch:',
                'soundcloud': 'scsearch:',
                'spotify': 'spsearch:'
            }
            
            if parsed_query['platform'] in platform_map:
                return platform_map[parsed_query['platform']] + ' '.join(query_parts)
        
        return ' '.join(query_parts)
    
    @staticmethod
    def extract_artist_and_title(query: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract artist and title from query string"""
        # Common patterns
        patterns = [
            r'^(.*?)\s*[-\u2013\u2014]\s*(.*)$',  # Artist - Title
            r'^(.*?)\s*:\s*(.*)$',  # Artist: Title
            r'^(.*?)\s*"\s*(.*?)\s*"$',  # Artist "Title"
            r'^(.*?)\s*\|\s*(.*)$',  # Artist | Title
        ]
        
        for pattern in patterns:
            match = re.match(pattern, query)
            if match:
                artist = match.group(1).strip()
                title = match.group(2).strip()
                return artist, title
        
        # No pattern matched, assume it's just a title
        return None, query.strip()


class ErrorHandler:
    """Graceful error handling and user feedback"""
    
    def __init__(self, bot):
        self.bot = bot
        
        # Error statistics
        self.error_stats = defaultdict(int)
        self.recent_errors = []
        
        # User-friendly error messages
        self.error_messages = {
            'voice_channel': {
                'elegant': "Please join a voice channel first... The music awaits your presence.",
                'seductive': "I need you in a voice channel, darling... Where should I meet you? 🌹",
                'playful': "Hey! Join a voice channel first! I can't play to an empty room! 🎵",
                'supportive': "Let's start by joining a voice channel together, shall we? 💫",
                'teacher': "Step one: Join a voice channel. Then we may proceed.",
                'obedient': "Error: User not in voice channel. Please join a voice channel."
            },
            'no_permission': {
                'elegant': "I lack the necessary permissions... A gentle reminder to grant them.",
                'seductive': "I can't reach you without the right permissions... Help me get closer? 💋",
                'playful': "Oops! I need permissions to do that! Can you help me out? 🔧",
                'supportive': "I need a little more access to help you properly...",
                'teacher': "Insufficient permissions. Please grant the required access.",
                'obedient': "Error: Insufficient permissions. Please grant access."
            },
            'player_not_found': {
                'elegant': "No active player found... Shall we begin a new symphony?",
                'seductive': "I'm not playing anything right now... What would you like to hear? 🌹",
                'playful': "No music playing! Time to start the party! 🎉",
                'supportive': "Let's start some music together... What would you like to hear?",
                'teacher': "No active playback session. Use play command to begin.",
                'obedient': "Error: No active player. Use play command."
            },
            'track_not_found': {
                'elegant': "That melody eludes me... Perhaps try different words?",
                'seductive': "I couldn't find that song... Maybe we can discover something else together? 💫",
                'playful': "No results found! Try different search words! 🔍",
                'supportive': "I couldn't find that track... Let's try something different?",
                'teacher': "Search returned no results. Please refine your query.",
                'obedient': "Error: No tracks found. Please refine search."
            },
            'connection_error': {
                'elegant': "The connection falters... Let me try to restore harmony.",
                'seductive': "Our connection stumbled... Let me try to reconnect with you. 🔄",
                'playful': "Whoops! Connection problem! Trying again... ⚡",
                'supportive': "There seems to be a connection issue... Let me try again.",
                'teacher': "Connection error occurred. Attempting to reconnect.",
                'obedient': "Error: Connection failed. Attempting recovery."
            },
            'rate_limited': {
                'elegant': "Patience, please... The system requires a moment to breathe.",
                'seductive': "Slow down, darling... Let's savor each moment. 🌹",
                'playful': "Whoa there! Too fast! Let's slow down a bit! 🐢",
                'supportive': "Let's take it slowly... The system needs a moment.",
                'teacher': "Rate limit exceeded. Please wait before trying again.",
                'obedient': "Error: Rate limited. Please wait."
            }
        }
    
    async def handle_error(self, ctx, error_type: str, error: Exception = None) -> str:
        """Handle error and return user-friendly message"""
        # Log error
        self.error_stats[error_type] += 1
        
        error_record = {
            'type': error_type,
            'user_id': ctx.author.id if ctx else 0,
            'guild_id': ctx.guild.id if ctx and ctx.guild else 0,
            'timestamp': datetime.utcnow(),
            'error': str(error) if error else None
        }
        
        self.recent_errors.append(error_record)
        
        # Keep only recent errors
        if len(self.recent_errors) > 100:
            self.recent_errors = self.recent_errors[-100:]
        
        # Get personality-based message
        personality = getattr(self.bot, 'personality_mode', 'elegant')
        
        if error_type in self.error_messages:
            messages = self.error_messages[error_type]
            if personality in messages:
                return messages[personality]
            return messages.get('elegant', "An error occurred.")
        
        # Generic error message
        generic_messages = {
            'elegant': "An unexpected complication arose... Please try again.",
            'seductive': "Something went wrong... Let's try that again, shall we? 💋",
            'playful': "Oopsie! Something broke! Let's try again! 🔧",
            'supportive': "There was a problem... Let's try again together.",
            'teacher': "An error occurred. Please retry the operation.",
            'obedient': "Error occurred. Please retry."
        }
        
        return generic_messages.get(personality, "An error occurred.")
    
    async def log_error(self, error: Exception, context: str = ""):
        """Log error with context"""
        logger.error(f"{context}: {error}", exc_info=True)
        
        error_record = {
            'error': str(error),
            'type': type(error).__name__,
            'context': context,
            'timestamp': datetime.utcnow()
        }
        
        self.recent_errors.append(error_record)
        
        # Log to file if needed
        # await self._write_error_log(error_record)
    
    async def get_error_stats(self) -> Dict:
        """Get error statistics"""
        # Calculate error rates
        total_errors = sum(self.error_stats.values())
        
        stats = {
            'total_errors': total_errors,
            'error_types': dict(self.error_stats),
            'recent_error_count': len(self.recent_errors),
            'error_rate_per_hour': self._calculate_error_rate()
        }
        
        return stats
    
    def _calculate_error_rate(self) -> float:
        """Calculate error rate per hour"""
        if not self.recent_errors:
            return 0.0
        
        # Get errors from last hour
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        recent_count = sum(
            1 for e in self.recent_errors
            if e['timestamp'] > one_hour_ago
        )
        
        return recent_count  # Errors per hour
    
    async def send_error_embed(self, ctx, error_type: str, error: Exception = None):
        """Send error as embed"""
        error_message = await self.handle_error(ctx, error_type, error)
        
        embed = discord.Embed(
            title="⚠️ Error",
            description=error_message,
            color=0xFF6B6B  # Red color for errors
        )
        
        # Add error details for developers
        if ctx.author.id in self.bot.config.DEVELOPER_IDS and error:
            embed.add_field(
                name="Technical Details",
                value=f"```{type(error).__name__}: {str(error)[:200]}```",
                inline=False
            )
        
        await ctx.send(embed=embed)


class RateLimiter:
    """Advanced rate limiting system"""
    
    def __init__(self):
        self.user_limits: Dict[int, Dict[str, List[datetime]]] = {}
        self.guild_limits: Dict[int, Dict[str, List[datetime]]] = {}
        self.command_limits: Dict[str, Dict[int, List[datetime]]] = {}
        
        # Default limits
        self.default_limits = {
            'play': {'per_minute': 10, 'per_hour': 50},
            'search': {'per_minute': 15, 'per_hour': 100},
            'skip': {'per_minute': 20, 'per_hour': 200},
            'queue': {'per_minute': 30, 'per_hour': 300}
        }
        
        # VIP/Donor multipliers
        self.vip_multipliers = {
            'vip': 2.0,  # Double limits
            'premium': 3.0,  # Triple limits
            'developer': 10.0  # 10x limits for developers
        }
    
    def check_rate_limit(
        self,
        user_id: int,
        guild_id: int,
        command: str,
        user_tier: str = 'default'
    ) -> Tuple[bool, Optional[int]]:
        """Check rate limit for user/guild/command"""
        now = datetime.utcnow()
        
        # Get limits for command
        limits = self.default_limits.get(command, {'per_minute': 10, 'per_hour': 100})
        
        # Apply tier multiplier
        multiplier = self.vip_multipliers.get(user_tier, 1.0)
        per_minute = int(limits['per_minute'] * multiplier)
        per_hour = int(limits['per_hour'] * multiplier)
        
        # Check user limits
        user_key = f"{command}:{guild_id}"
        
        if user_id not in self.user_limits:
            self.user_limits[user_id] = {}
        
        if user_key not in self.user_limits[user_id]:
            self.user_limits[user_id][user_key] = []
        
        user_times = self.user_limits[user_id][user_key]
        
        # Clean old entries
        one_minute_ago = now - timedelta(minutes=1)
        one_hour_ago = now - timedelta(hours=1)
        
        user_times = [t for t in user_times if t > one_hour_ago]
        
        # Check minute limit
        recent_minute = [t for t in user_times if t > one_minute_ago]
        if len(recent_minute) >= per_minute:
            # Calculate wait time
            oldest = min(recent_minute)
            wait_seconds = 60 - (now - oldest).total_seconds()
            return False, int(wait_seconds)
        
        # Check hour limit
        recent_hour = user_times
        if len(recent_hour) >= per_hour:
            # Calculate wait time
            oldest = min(recent_hour)
            wait_seconds = 3600 - (now - oldest).total_seconds()
            return False, int(wait_seconds)
        
        # Check guild limits
        guild_key = command
        
        if guild_id not in self.guild_limits:
            self.guild_limits[guild_id] = {}
        
        if guild_key not in self.guild_limits[guild_id]:
            self.guild_limits[guild_id][guild_key] = []
        
        guild_times = self.guild_limits[guild_id][guild_key]
        
        # Clean old entries
        guild_times = [t for t in guild_times if t > one_hour_ago]
        
        # Guild limits (5x user limits)
        guild_per_minute = per_minute * 5
        guild_per_hour = per_hour * 5
        
        # Check guild minute limit
        guild_recent_minute = [t for t in guild_times if t > one_minute_ago]
        if len(guild_recent_minute) >= guild_per_minute:
            return False, 60  # Wait 1 minute
        
        # Check guild hour limit
        guild_recent_hour = guild_times
        if len(guild_recent_hour) >= guild_per_hour:
            return False, 3600  # Wait 1 hour
        
        # All checks passed, record the command
        user_times.append(now)
        guild_times.append(now)
        
        # Update stored times
        self.user_limits[user_id][user_key] = user_times
        self.guild_limits[guild_id][guild_key] = guild_times
        
        return True, None
    
    def record_command(self, user_id: int, guild_id: int, command: str):
        """Record command usage (for statistics)"""
        now = datetime.utcnow()
        
        # Command-level tracking
        if command not in self.command_limits:
            self.command_limits[command] = {}
        
        if user_id not in self.command_limits[command]:
            self.command_limits[command][user_id] = []
        
        self.command_limits[command][user_id].append(now)
        
        # Keep only last 1000 entries per user per command
        if len(self.command_limits[command][user_id]) > 1000:
            self.command_limits[command][user_id] = self.command_limits[command][user_id][-1000:]
    
    def get_user_stats(self, user_id: int) -> Dict:
        """Get rate limiting statistics for user"""
        stats = {
            'total_commands': 0,
            'commands_last_hour': 0,
            'commands_last_day': 0,
            'most_used_command': None,
            'limit_status': {}
        }
        
        now = datetime.utcnow()
        one_hour_ago = now - timedelta(hours=1)
        one_day_ago = now - timedelta(days=1)
        
        # Count commands
        command_counts = defaultdict(int)
        hourly_counts = defaultdict(int)
        daily_counts = defaultdict(int)
        
        for command, users in self.command_limits.items():
            if user_id in users:
                times = users[user_id]
                stats['total_commands'] += len(times)
                
                # Count by command
                command_counts[command] += len(times)
                
                # Count recent
                hourly = [t for t in times if t > one_hour_ago]
                daily = [t for t in times if t > one_day_ago]
                
                hourly_counts[command] += len(hourly)
                daily_counts[command] += len(daily)
        
        # Get most used command
        if command_counts:
            stats['most_used_command'] = max(command_counts.items(), key=lambda x: x[1])[0]
        
        # Get hourly and daily totals
        stats['commands_last_hour'] = sum(hourly_counts.values())
        stats['commands_last_day'] = sum(daily_counts.values())
        
        # Check current limits
        for command in self.default_limits:
            allowed, wait_time = self.check_rate_limit(user_id, 0, command, 'default')
            stats['limit_status'][command] = {
                'allowed': allowed,
                'wait_time': wait_time
            }
        
        return stats
    
    def cleanup_old_entries(self):
        """Clean up old rate limiting entries"""
        now = datetime.utcnow()
        cutoff = now - timedelta(days=7)  # Keep 7 days
        
        # Clean user limits
        for user_id in list(self.user_limits.keys()):
            for key in list(self.user_limits[user_id].keys()):
                self.user_limits[user_id][key] = [
                    t for t in self.user_limits[user_id][key]
                    if t > cutoff
                ]
                
                if not self.user_limits[user_id][key]:
                    self.user_limits[user_id].pop(key, None)
            
            if not self.user_limits[user_id]:
                self.user_limits.pop(user_id, None)
        
        # Clean guild limits
        for guild_id in list(self.guild_limits.keys()):
            for key in list(self.guild_limits[guild_id].keys()):
                self.guild_limits[guild_id][key] = [
                    t for t in self.guild_limits[guild_id][key]
                    if t > cutoff
                ]
                
                if not self.guild_limits[guild_id][key]:
                    self.guild_limits[guild_id].pop(key, None)
            
            if not self.guild_limits[guild_id]:
                self.guild_limits.pop(guild_id, None)
        
        # Clean command limits
        for command in list(self.command_limits.keys()):
            for user_id in list(self.command_limits[command].keys()):
                self.command_limits[command][user_id] = [
                    t for t in self.command_limits[command][user_id]
                    if t > cutoff
                ]
                
                if not self.command_limits[command][user_id]:
                    self.command_limits[command].pop(user_id, None)
            
            if not self.command_limits[command]:
                self.command_limits.pop(command, None)


class MusicUtils(
    TrackFormatter,
    DurationConverter,
    URLValidator,
    SearchParser,
    ErrorHandler,
    RateLimiter
):
    """Complete utilities system combining all utility classes"""
    
    def __init__(self, bot):
        TrackFormatter.__init__()
        DurationConverter.__init__()
        URLValidator.__init__()
        SearchParser.__init__()
        ErrorHandler.__init__(bot)
        RateLimiter.__init__()
        
        self.bot = bot
        
        # Start cleanup task
        self.cleanup_task = asyncio.create_task(self.periodic_cleanup())
    
    async def periodic_cleanup(self):
        """Periodic cleanup of old data"""
        while True:
            try:
                # Clean rate limiter
                self.cleanup_old_entries()
                
                # Clean error logs
                one_week_ago = datetime.utcnow() - timedelta(days=7)
                self.recent_errors = [
                    e for e in self.recent_errors
                    if e['timestamp'] > one_week_ago
                ]
                
                await asyncio.sleep(3600)  # Run every hour
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
                await asyncio.sleep(300)
    
    @staticmethod
    def calculate_similarity(str1: str, str2: str) -> float:
        """Calculate string similarity (0-1)"""
        if not str1 or not str2:
            return 0.0
        
        str1_lower = str1.lower()
        str2_lower = str2.lower()
        
        # Exact match
        if str1_lower == str2_lower:
            return 1.0
        
        # Contains match
        if str1_lower in str2_lower or str2_lower in str1_lower:
            return 0.8
        
        # Word overlap
        words1 = set(str1_lower.split())
        words2 = set(str2_lower.split())
        
        if words1 and words2:
            overlap = len(words1 & words2)
            total = len(words1 | words2)
            return overlap / total
        
        return 0.0
    
    @staticmethod
    def generate_session_id() -> str:
        """Generate unique session ID"""
        timestamp = int(time.time() * 1000)
        random_part = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
        return f"{timestamp}_{random_part}"
    
    @staticmethod
    async def safe_async_operation(coro, fallback=None, max_retries=3):
        """Safely execute async operation with retries"""
        for attempt in range(max_retries):
            try:
                return await coro
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Operation failed after {max_retries} attempts: {e}")
                    return fallback
                await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
        return fallback
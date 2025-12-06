"""
Complete lyrics system with synchronization, karaoke mode,
multiple providers (Genius, AZLyrics, Musixmatch), and translation
"""

import discord
from discord.ext import commands, tasks
import aiohttp
import asyncio
from typing import Dict, List, Optional, Tuple
import re
from datetime import datetime, timedelta
import json
from html import unescape
import logging

logger = logging.getLogger(__name__)

class LyricsProvider:
    """Base class for lyrics providers"""
    
    def __init__(self, name: str, api_key: str = None):
        self.name = name
        self.api_key = api_key
        self.session: Optional[aiohttp.ClientSession] = None
        self.cache: Dict[str, Dict] = {}
        self.rate_limits: Dict[str, datetime] = {}
    
    async def create_session(self):
        """Create aiohttp session"""
        if not self.session:
            self.session = aiohttp.ClientSession(
                headers={
                    'User-Agent': 'EVE-Music-Bot/1.0'
                }
            )
    
    async def search_lyrics(self, track: str, artist: str) -> Optional[Dict]:
        """Search for lyrics"""
        raise NotImplementedError
    
    async def get_lyrics(self, url: str) -> Optional[str]:
        """Get lyrics from URL"""
        raise NotImplementedError
    
    async def get_synced_lyrics(self, track: str, artist: str) -> Optional[List[Dict]]:
        """Get synchronized lyrics (timestamped)"""
        raise NotImplementedError
    
    def parse_duration(self, time_str: str) -> int:
        """Parse duration string to milliseconds"""
        try:
            if ':' in time_str:
                parts = time_str.split(':')
                if len(parts) == 3:  # HH:MM:SS
                    hours, minutes, seconds = parts
                    seconds = seconds.split('.')[0]  # Remove milliseconds
                    total_ms = (int(hours) * 3600 + int(minutes) * 60 + int(seconds)) * 1000
                else:  # MM:SS
                    minutes, seconds = parts
                    seconds = seconds.split('.')[0]
                    total_ms = (int(minutes) * 60 + int(seconds)) * 1000
                
                # Add milliseconds if present
                if '.' in time_str:
                    ms_part = time_str.split('.')[1][:3]
                    total_ms += int(ms_part.ljust(3, '0'))
                
                return total_ms
        except:
            pass
        return 0
    
    def clean_lyrics(self, lyrics: str) -> str:
        """Clean and format lyrics"""
        if not lyrics:
            return ""
        
        # Remove HTML entities
        lyrics = unescape(lyrics)
        
        # Remove [Verse], [Chorus], etc.
        lyrics = re.sub(r'\[.*?\]', '', lyrics)
        
        # Remove multiple newlines
        lyrics = re.sub(r'\n\s*\n', '\n\n', lyrics)
        
        # Remove trailing whitespace
        lyrics = lyrics.strip()
        
        return lyrics
    
    def cache_lyrics(self, cache_key: str, lyrics_data: Dict):
        """Cache lyrics with expiration"""
        self.cache[cache_key] = {
            'data': lyrics_data,
            'expires': datetime.utcnow() + timedelta(days=7)
        }
    
    def get_cached_lyrics(self, cache_key: str) -> Optional[Dict]:
        """Get cached lyrics if not expired"""
        cached = self.cache.get(cache_key)
        if cached and cached['expires'] > datetime.utcnow():
            return cached['data']
        return None
    
    async def cleanup_cache(self):
        """Clean up expired cache entries"""
        now = datetime.utcnow()
        expired = [
            key for key, data in self.cache.items()
            if data['expires'] < now
        ]
        for key in expired:
            self.cache.pop(key, None)


class GeniusProvider(LyricsProvider):
    """Genius.com lyrics provider"""
    
    def __init__(self, api_key: str):
        super().__init__("Genius", api_key)
        self.base_url = "https://api.genius.com"
        
    async def search_lyrics(self, track: str, artist: str) -> Optional[Dict]:
        """Search for lyrics on Genius"""
        await self.create_session()
        
        cache_key = f"genius:{artist}:{track}".lower()
        cached = self.get_cached_lyrics(cache_key)
        if cached:
            return cached
        
        try:
            # Search for song
            search_url = f"{self.base_url}/search"
            params = {
                'q': f"{track} {artist}",
                'access_token': self.api_key
            }
            
            async with self.session.get(search_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Find best match
                    if data.get('response', {}).get('hits'):
                        for hit in data['response']['hits']:
                            result = hit.get('result', {})
                            if result.get('primary_artist', {}).get('name', '').lower() in artist.lower():
                                lyrics_data = {
                                    'title': result.get('title', ''),
                                    'artist': result.get('primary_artist', {}).get('name', ''),
                                    'url': result.get('url', ''),
                                    'id': result.get('id'),
                                    'thumbnail': result.get('song_art_image_url')
                                }
                                
                                # Cache the result
                                self.cache_lyrics(cache_key, lyrics_data)
                                return lyrics_data
                
                return None
                
        except Exception as e:
            logger.error(f"Genius search error: {e}")
            return None
    
    async def get_lyrics(self, url: str) -> Optional[str]:
        """Get lyrics from Genius URL"""
        await self.create_session()
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Extract lyrics from HTML
                    # Genius stores lyrics in a div with class "lyrics"
                    lyrics_match = re.search(r'<div[^>]*class="[^"]*lyrics[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)
                    if lyrics_match:
                        lyrics_html = lyrics_match.group(1)
                        
                        # Remove HTML tags
                        lyrics = re.sub(r'<[^>]*>', '', lyrics_html)
                        
                        # Clean up
                        lyrics = self.clean_lyrics(lyrics)
                        return lyrics
                    
                    # Try new Genius format
                    lyrics_match = re.search(r'<div[^>]*data-lyrics-container="true"[^>]*>(.*?)</div>', html, re.DOTALL)
                    if lyrics_match:
                        lyrics_html = lyrics_match.group(1)
                        
                        # Remove inner tags but keep line breaks
                        lyrics = re.sub(r'<br\s*/?>', '\n', lyrics_html)
                        lyrics = re.sub(r'<[^>]*>', '', lyrics)
                        
                        lyrics = self.clean_lyrics(lyrics)
                        return lyrics
                
                return None
                
        except Exception as e:
            logger.error(f"Genius lyrics fetch error: {e}")
            return None


class AZLyricsProvider(LyricsProvider):
    """AZLyrics.com provider (no API needed)"""
    
    def __init__(self):
        super().__init__("AZLyrics")
    
    async def search_lyrics(self, track: str, artist: str) -> Optional[Dict]:
        """Search for lyrics on AZLyrics"""
        await self.create_session()
        
        cache_key = f"azlyrics:{artist}:{track}".lower()
        cached = self.get_cached_lyrics(cache_key)
        if cached:
            return cached
        
        try:
            # AZLyrics URL pattern
            artist_part = artist.lower().replace(' ', '').replace('&', 'and')
            track_part = track.lower().replace(' ', '').replace('&', 'and')
            
            # Remove special characters
            artist_part = re.sub(r'[^a-z0-9]', '', artist_part)
            track_part = re.sub(r'[^a-z0-9]', '', track_part)
            
            url = f"https://www.azlyrics.com/lyrics/{artist_part}/{track_part}.html"
            
            # Try to fetch
            async with self.session.get(url) as response:
                if response.status == 200:
                    lyrics_data = {
                        'title': track,
                        'artist': artist,
                        'url': url,
                        'id': f"{artist_part}:{track_part}",
                        'thumbnail': None
                    }
                    
                    self.cache_lyrics(cache_key, lyrics_data)
                    return lyrics_data
            
            return None
            
        except Exception as e:
            logger.error(f"AZLyrics search error: {e}")
            return None
    
    async def get_lyrics(self, url: str) -> Optional[str]:
        """Get lyrics from AZLyrics URL"""
        await self.create_session()
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # AZLyrics stores lyrics in a specific div
                    # They have a comment "<!-- Usage of azlyrics.com content by any third-party lyrics provider is prohibited by our licensing agreement. Sorry about that. -->"
                    # Lyrics are between that comment and the next comment
                    
                    # Find the start
                    start_marker = "<!-- Usage of azlyrics.com content"
                    end_marker = "<!-- MxM banner -->"
                    
                    start_idx = html.find(start_marker)
                    if start_idx != -1:
                        # Find lyrics div after start marker
                        lyrics_start = html.find('<div>', start_idx)
                        if lyrics_start != -1:
                            # Find closing div before end marker
                            lyrics_end = html.find('</div>', lyrics_start)
                            if lyrics_end != -1:
                                lyrics_html = html[lyrics_start + 5:lyrics_end]
                                
                                # Clean up
                                lyrics = lyrics_html.strip()
                                lyrics = self.clean_lyrics(lyrics)
                                return lyrics
                
                return None
                
        except Exception as e:
            logger.error(f"AZLyrics lyrics fetch error: {e}")
            return None


class MusixmatchProvider(LyricsProvider):
    """Musixmatch provider (would need API)"""
    
    def __init__(self, api_key: str = None):
        super().__init__("Musixmatch", api_key)
        self.base_url = "https://api.musixmatch.com/ws/1.1"
    
    async def search_lyrics(self, track: str, artist: str) -> Optional[Dict]:
        """Search for lyrics on Musixmatch"""
        if not self.api_key:
            return None
        
        await self.create_session()
        
        cache_key = f"musixmatch:{artist}:{track}".lower()
        cached = self.get_cached_lyrics(cache_key)
        if cached:
            return cached
        
        try:
            search_url = f"{self.base_url}/track.search"
            params = {
                'q_track': track,
                'q_artist': artist,
                'apikey': self.api_key,
                'page_size': 1,
                's_track_rating': 'desc'
            }
            
            async with self.session.get(search_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    tracks = data.get('message', {}).get('body', {}).get('track_list', [])
                    if tracks:
                        track_data = tracks[0].get('track', {})
                        
                        lyrics_data = {
                            'title': track_data.get('track_name', ''),
                            'artist': track_data.get('artist_name', ''),
                            'url': f"https://www.musixmatch.com/lyrics/{track_data.get('artist_name', '').replace(' ', '-')}/{track_data.get('track_name', '').replace(' ', '-')}",
                            'id': track_data.get('track_id'),
                            'thumbnail': track_data.get('album_coverart_100x100')
                        }
                        
                        self.cache_lyrics(cache_key, lyrics_data)
                        return lyrics_data
                
                return None
                
        except Exception as e:
            logger.error(f"Musixmatch search error: {e}")
            return None
    
    async def get_lyrics(self, track_id: str) -> Optional[str]:
        """Get lyrics from Musixmatch"""
        if not self.api_key:
            return None
        
        await self.create_session()
        
        try:
            lyrics_url = f"{self.base_url}/track.lyrics.get"
            params = {
                'track_id': track_id,
                'apikey': self.api_key
            }
            
            async with self.session.get(lyrics_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    lyrics_body = data.get('message', {}).get('body', {}).get('lyrics', {}).get('lyrics_body', '')
                    if lyrics_body:
                        lyrics = self.clean_lyrics(lyrics_body)
                        return lyrics
                
                return None
                
        except Exception as e:
            logger.error(f"Musixmatch lyrics fetch error: {e}")
            return None
    
    async def get_synced_lyrics(self, track: str, artist: str) -> Optional[List[Dict]]:
        """Get synchronized lyrics from Musixmatch"""
        if not self.api_key:
            return None
        
        await self.create_session()
        
        try:
            # First search for track
            search_url = f"{self.base_url}/track.search"
            params = {
                'q_track': track,
                'q_artist': artist,
                'apikey': self.api_key,
                'page_size': 1
            }
            
            async with self.session.get(search_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    tracks = data.get('message', {}).get('body', {}).get('track_list', [])
                    
                    if tracks:
                        track_id = tracks[0].get('track', {}).get('track_id')
                        
                        # Get synced lyrics
                        sync_url = f"{self.base_url}/track.subtitle.get"
                        params = {
                            'track_id': track_id,
                            'apikey': self.api_key
                        }
                        
                        async with self.session.get(sync_url, params=params) as sync_response:
                            if sync_response.status == 200:
                                sync_data = await sync_response.json()
                                subtitle = sync_data.get('message', {}).get('body', {}).get('subtitle', {}).get('subtitle_body', '')
                                
                                if subtitle:
                                    # Parse LRC format
                                    lines = []
                                    for line in subtitle.split('\n'):
                                        # Match [MM:SS.xx] text
                                        match = re.match(r'\[(\d+:\d+\.\d+)\](.*)', line)
                                        if match:
                                            timestamp = match.group(1)
                                            text = match.group(2).strip()
                                            
                                            if text:
                                                ms = self.parse_duration(timestamp)
                                                lines.append({
                                                    'timestamp': ms,
                                                    'text': text
                                                })
                                    
                                    return lines
                
                return None
                
        except Exception as e:
            logger.error(f"Musixmatch synced lyrics error: {e}")
            return None


class LyricsKaraoke:
    """Complete lyrics and karaoke system"""
    
    def __init__(self, bot):
        self.bot = bot
        
        # Initialize providers
        self.providers: List[LyricsProvider] = []
        self.active_provider: Optional[LyricsProvider] = None
        
        # Karaoke sessions
        self.karaoke_sessions: Dict[int, Dict] = {}  # guild_id: session_data
        
        # Lyrics cache
        self.lyrics_cache: Dict[str, Dict] = {}
        self.synced_cache: Dict[str, List] = {}
        
        # Translation service (would need API)
        self.translation_enabled = False
        
        # Background tasks
        self.cache_cleanup.start()
    
    async def initialize_providers(self):
        """Initialize lyrics providers"""
        # Add Genius if API key available
        if hasattr(self.bot.config, 'GENIUS_API_KEY') and self.bot.config.GENIUS_API_KEY:
            genius = GeniusProvider(self.bot.config.GENIUS_API_KEY)
            self.providers.append(genius)
            self.active_provider = genius
            logger.info("Genius lyrics provider initialized")
        
        # Add AZLyrics (always available)
        azlyrics = AZLyricsProvider()
        self.providers.append(azlyrics)
        if not self.active_provider:
            self.active_provider = azlyrics
            logger.info("AZLyrics provider initialized")
        
        # Add Musixmatch if API key available
        if hasattr(self.bot.config, 'MUSIXMATCH_API_KEY') and self.bot.config.MUSIXMATCH_API_KEY:
            musixmatch = MusixmatchProvider(self.bot.config.MUSIXMATCH_API_KEY)
            self.providers.append(musixmatch)
            logger.info("Musixmatch provider initialized")
    
    async def get_lyrics(self, track: str, artist: str, force_search: bool = False) -> Optional[Dict]:
        """Get lyrics for a track"""
        cache_key = f"lyrics:{artist}:{track}".lower()
        
        # Check cache
        if not force_search and cache_key in self.lyrics_cache:
            cached = self.lyrics_cache[cache_key]
            if cached['expires'] > datetime.utcnow():
                return cached['data']
        
        # Search through providers
        lyrics_data = None
        lyrics_text = None
        
        for provider in self.providers:
            try:
                # Search for track
                search_result = await provider.search_lyrics(track, artist)
                if search_result:
                    # Get lyrics text
                    if provider.name == "Musixmatch":
                        lyrics_text = await provider.get_lyrics(search_result.get('id', ''))
                    else:
                        lyrics_text = await provider.get_lyrics(search_result.get('url', ''))
                    
                    if lyrics_text:
                        lyrics_data = {
                            'provider': provider.name,
                            'title': search_result.get('title', track),
                            'artist': search_result.get('artist', artist),
                            'lyrics': lyrics_text,
                            'url': search_result.get('url', ''),
                            'thumbnail': search_result.get('thumbnail'),
                            'synced': False
                        }
                        break
            except Exception as e:
                logger.error(f"Provider {provider.name} error: {e}")
                continue
        
        # Cache result
        if lyrics_data:
            self.lyrics_cache[cache_key] = {
                'data': lyrics_data,
                'expires': datetime.utcnow() + timedelta(days=30)
            }
        
        return lyrics_data
    
    async def get_synced_lyrics(self, track: str, artist: str) -> Optional[List[Dict]]:
        """Get synchronized lyrics"""
        cache_key = f"synced:{artist}:{track}".lower()
        
        # Check cache
        if cache_key in self.synced_cache:
            cached = self.synced_cache[cache_key]
            if cached['expires'] > datetime.utcnow():
                return cached['data']
        
        # Try Musixmatch first (best for synced)
        musixmatch = next((p for p in self.providers if p.name == "Musixmatch"), None)
        synced_lyrics = None
        
        if musixmatch:
            synced_lyrics = await musixmatch.get_synced_lyrics(track, artist)
        
        # Cache result
        if synced_lyrics:
            self.synced_cache[cache_key] = {
                'data': synced_lyrics,
                'expires': datetime.utcnow() + timedelta(days=30)
            }
        
        return synced_lyrics
    
    async def format_lyrics_embed(self, lyrics_data: Dict, current_line: int = 0) -> discord.Embed:
        """Format lyrics into Discord embed"""
        color = self.bot.colors.VIOLET
        
        embed = discord.Embed(
            title=f"📝 Lyrics: {lyrics_data['title']}",
            color=color,
            timestamp=datetime.utcnow()
        )
        
        embed.set_author(name=lyrics_data['artist'])
        
        # Add provider attribution
        embed.set_footer(text=f"Source: {lyrics_data['provider']}")
        
        # Handle lyrics text (limit to 4096 characters)
        lyrics_text = lyrics_data['lyrics']
        
        if len(lyrics_text) > 4000:
            # For very long lyrics, show portion around current line if synced
            if lyrics_data.get('synced') and current_line > 0:
                lines = lyrics_text.split('\n')
                start = max(0, current_line - 10)
                end = min(len(lines), current_line + 20)
                
                displayed_lines = lines[start:end]
                for i, line in enumerate(displayed_lines):
                    if start + i == current_line:
                        displayed_lines[i] = f"**{line}**"
                
                lyrics_display = '\n'.join(displayed_lines)
                
                # Add navigation info
                embed.description = lyrics_display
                embed.add_field(
                    name="Position",
                    value=f"Line {current_line + 1}/{len(lines)}",
                    inline=True
                )
                
                if start > 0:
                    embed.add_field(
                        name="View",
                        value=f"Showing lines {start + 1}-{end}",
                        inline=True
                    )
            else:
                # Truncate with link
                embed.description = lyrics_text[:2000] + "...\n\n*Lyrics truncated*"
                embed.add_field(
                    name="Full Lyrics",
                    value=f"[View on {lyrics_data['provider']}]({lyrics_data['url']})",
                    inline=False
                )
        else:
            embed.description = lyrics_text
        
        # Add thumbnail if available
        if lyrics_data.get('thumbnail'):
            embed.set_thumbnail(url=lyrics_data['thumbnail'])
        
        return embed
    
    async def start_karaoke_session(self, guild_id: int, track_data: Dict, lyrics: List[Dict]):
        """Start a karaoke session"""
        self.karaoke_sessions[guild_id] = {
            'track': track_data,
            'lyrics': lyrics,
            'started_at': datetime.utcnow(),
            'current_line': 0,
            'participants': {},
            'scores': {},
            'is_active': True
        }
        
        # Start karaoke task
        asyncio.create_task(self.karaoke_ticker(guild_id))
    
    async def karaoke_ticker(self, guild_id: int):
        """Karaoke ticker that updates current line"""
        while guild_id in self.karaoke_sessions:
            session = self.karaoke_sessions[guild_id]
            
            if not session['is_active']:
                break
            
            # Get current playback position from music player
            music_cog = self.bot.get_cog('MusicSystem')
            if music_cog:
                player = music_cog.get_player(guild_id)
                if player and player.current and player.is_playing():
                    current_pos = player.position
                    
                    # Find current line
                    lyrics = session['lyrics']
                    current_line = session['current_line']
                    
                    # Find line at or just past current position
                    new_line = current_line
                    for i in range(current_line, len(lyrics)):
                        if lyrics[i]['timestamp'] > current_pos:
                            break
                        new_line = i
                    
                    if new_line != current_line:
                        session['current_line'] = new_line
                        
                        # Update display if channel available
                        # This would send updates to a karaoke channel
                        pass
            
            await asyncio.sleep(0.5)  # Update twice per second
    
    async def score_karaoke_participant(self, guild_id: int, user_id: int, accuracy: float):
        """Score a karaoke participant"""
        if guild_id not in self.karaoke_sessions:
            return
        
        session = self.karaoke_sessions[guild_id]
        
        if user_id not in session['scores']:
            session['scores'][user_id] = {
                'total_score': 0,
                'lines_sung': 0,
                'avg_accuracy': 0
            }
        
        score_data = session['scores'][user_id]
        score_data['lines_sung'] += 1
        score_data['total_score'] += int(accuracy * 100)
        score_data['avg_accuracy'] = score_data['total_score'] / score_data['lines_sung']
        
        # Add to participants if not already
        if user_id not in session['participants']:
            session['participants'][user_id] = {
                'joined_at': datetime.utcnow(),
                'lines_sung': 0
            }
        
        session['participants'][user_id]['lines_sung'] += 1
    
    @commands.hybrid_command(
        name="lyrics",
        description="Get lyrics for current or specified song"
    )
    async def lyrics_command(
        self,
        ctx,
        song_query: Optional[str] = None,
        synced: bool = False
    ):
        """Get lyrics for a song"""
        # Determine track info
        track_title = ""
        artist = ""
        
        if song_query:
            # Parse query (could be "artist - song" or just song)
            if ' - ' in song_query:
                artist, track_title = song_query.split(' - ', 1)
            else:
                track_title = song_query
                # Try to get from current playing
                music_cog = self.bot.get_cog('MusicSystem')
                if music_cog:
                    player = music_cog.get_player(ctx.guild.id)
                    if player and player.current:
                        artist = getattr(player.current, 'author', '')
        else:
            # Get current playing track
            music_cog = self.bot.get_cog('MusicSystem')
            if not music_cog:
                await ctx.send("Music system not available!")
                return
            
            player = music_cog.get_player(ctx.guild.id)
            if not player or not player.current:
                await ctx.send("No song is currently playing!")
                return
            
            track_title = player.current.title
            artist = getattr(player.current, 'author', 'Unknown Artist')
        
        if not track_title:
            await ctx.send("Please specify a song!")
            return
        
        # Show searching message
        search_msg = await ctx.send(f"Searching lyrics for **{track_title}**... ✨")
        
        try:
            if synced:
                # Get synced lyrics
                synced_lyrics = await self.get_synced_lyrics(track_title, artist or '')
                
                if synced_lyrics:
                    # Convert to display format
                    lyrics_text = "\n".join(
                        f"[{self.format_ms_to_time(line['timestamp'])}] {line['text']}"
                        for line in synced_lyrics[:50]  # Show first 50 lines
                    )
                    
                    lyrics_data = {
                        'title': track_title,
                        'artist': artist or 'Unknown',
                        'lyrics': lyrics_text,
                        'provider': 'Musixmatch',
                        'synced': True,
                        'url': '',
                        'thumbnail': None
                    }
                else:
                    await search_msg.edit(content="No synced lyrics found. Trying regular lyrics...")
                    synced = False
            
            if not synced:
                # Get regular lyrics
                lyrics_data = await self.get_lyrics(track_title, artist or '')
            
            if not lyrics_data:
                await search_msg.edit(content=f"Could not find lyrics for **{track_title}**")
                return
            
            # Create embed
            embed = await self.format_lyrics_embed(lyrics_data)
            
            # Add view with controls
            view = LyricsView(self, lyrics_data)
            
            await search_msg.edit(content=None, embed=embed, view=view)
            
        except Exception as e:
            logger.error(f"Lyrics error: {e}")
            await search_msg.edit(content=f"Error fetching lyrics: {str(e)}")
    
    @commands.hybrid_command(
        name="karaoke",
        description="Start karaoke mode for current song"
    )
    async def karaoke_command(self, ctx):
        """Start karaoke mode"""
        # Get current track
        music_cog = self.bot.get_cog('MusicSystem')
        if not music_cog:
            await ctx.send("Music system not available!")
            return
        
        player = music_cog.get_player(ctx.guild.id)
        if not player or not player.current:
            await ctx.send("No song is currently playing!")
            return
        
        track_title = player.current.title
        artist = getattr(player.current, 'author', 'Unknown Artist')
        
        # Get synced lyrics
        search_msg = await ctx.send(f"Setting up karaoke for **{track_title}**... 🎤")
        
        synced_lyrics = await self.get_synced_lyrics(track_title, artist)
        
        if not synced_lyrics:
            await search_msg.edit(content="No synchronized lyrics available for karaoke mode.")
            return
        
        # Start karaoke session
        await self.start_karaoke_session(
            ctx.guild.id,
            {
                'title': track_title,
                'artist': artist,
                'duration': player.current.length
            },
            synced_lyrics
        )
        
        # Create karaoke embed
        embed = discord.Embed(
            title="🎤 Karaoke Mode Started!",
            description=f"**{track_title}** - {artist}",
            color=self.bot.colors.ROSE_GOLD
        )
        
        # Show first few lines
        preview_lines = "\n".join(
            f"{self.format_ms_to_time(line['timestamp'])} {line['text']}"
            for line in synced_lyrics[:5]
        )
        
        embed.add_field(
            name="Lyrics Preview",
            value=preview_lines,
            inline=False
        )
        
        embed.add_field(
            name="How to Play",
            value="Sing along! Your voice will be scored based on timing and accuracy.",
            inline=False
        )
        
        embed.set_footer(text="Karaoke session started")
        
        # Add karaoke controls
        view = KaraokeView(self, ctx.guild.id)
        
        await search_msg.edit(content=None, embed=embed, view=view)
    
    @commands.hybrid_command(
        name="translate_lyrics",
        description="Translate lyrics to another language"
    )
    async def translate_lyrics_command(
        self,
        ctx,
        language: str,
        song_query: Optional[str] = None
    ):
        """Translate lyrics to another language"""
        if not self.translation_enabled:
            await ctx.send("Translation service is not configured.")
            return
        
        # Similar to lyrics command but with translation
        await ctx.send(f"Translation to {language} coming soon! 🌍")
    
    def format_ms_to_time(self, milliseconds: int) -> str:
        """Format milliseconds to MM:SS"""
        seconds = milliseconds // 1000
        minutes = seconds // 60
        seconds = seconds % 60
        return f"{minutes:02d}:{seconds:02d}"
    
    @tasks.loop(hours=6)
    async def cache_cleanup(self):
        """Clean up old cache entries"""
        now = datetime.utcnow()
        
        # Clean lyrics cache
        expired_lyrics = [
            key for key, data in self.lyrics_cache.items()
            if data['expires'] < now
        ]
        for key in expired_lyrics:
            self.lyrics_cache.pop(key, None)
        
        # Clean synced cache
        expired_synced = [
            key for key, data in self.synced_cache.items()
            if data['expires'] < now
        ]
        for key in expired_synced:
            self.synced_cache.pop(key, None)
        
        logger.info(f"Cleaned cache: {len(expired_lyrics)} lyrics, {len(expired_synced)} synced")


class LyricsView(discord.ui.View):
    """Lyrics controls view"""
    
    def __init__(self, lyrics_cog, lyrics_data: Dict):
        super().__init__(timeout=300)
        self.lyrics_cog = lyrics_cog
        self.lyrics_data = lyrics_data
        self.current_page = 0
        
        # Calculate total pages if lyrics are long
        self.lyrics_text = lyrics_data['lyrics']
        self.lines = self.lyrics_text.split('\n')
        self.total_pages = max(1, (len(self.lines) + 19) // 20)  # 20 lines per page
    
    @discord.ui.button(label="◀️", style=discord.ButtonStyle.secondary)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Previous page of lyrics"""
        if self.current_page > 0:
            self.current_page -= 1
            await self.update_lyrics_embed(interaction)
    
    @discord.ui.button(label="▶️", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Next page of lyrics"""
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            await self.update_lyrics_embed(interaction)
    
    @discord.ui.button(label="🔗 Open Source", style=discord.ButtonStyle.link)
    async def source_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Open lyrics source"""
        if self.lyrics_data.get('url'):
            await interaction.response.send_message(
                f"Opening lyrics: {self.lyrics_data['url']}",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "No source URL available",
                ephemeral=True
            )
    
    async def update_lyrics_embed(self, interaction: discord.Interaction):
        """Update lyrics embed with current page"""
        # Calculate lines for current page
        start_line = self.current_page * 20
        end_line = min(start_line + 20, len(self.lines))
        
        page_text = "\n".join(self.lines[start_line:end_line])
        
        # Create new embed
        embed = discord.Embed(
            title=f"📝 Lyrics: {self.lyrics_data['title']} (Page {self.current_page + 1}/{self.total_pages})",
            description=page_text,
            color=self.lyrics_cog.bot.colors.VIOLET
        )
        
        embed.set_author(name=self.lyrics_data['artist'])
        embed.set_footer(text=f"Source: {self.lyrics_data['provider']}")
        
        await interaction.response.edit_message(embed=embed, view=self)


class KaraokeView(discord.ui.View):
    """Karaoke controls view"""
    
    def __init__(self, lyrics_cog, guild_id: int):
        super().__init__(timeout=600)
        self.lyrics_cog = lyrics_cog
        self.guild_id = guild_id
    
    @discord.ui.button(label="🎤 Join Karaoke", style=discord.ButtonStyle.success)
    async def join_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Join karaoke session"""
        if self.guild_id not in self.lyrics_cog.karaoke_sessions:
            await interaction.response.send_message("Karaoke session not found!", ephemeral=True)
            return
        
        session = self.lyrics_cog.karaoke_sessions[self.guild_id]
        
        if interaction.user.id in session['participants']:
            await interaction.response.send_message("You're already in the karaoke session!", ephemeral=True)
            return
        
        # Add participant
        session['participants'][interaction.user.id] = {
            'joined_at': datetime.utcnow(),
            'lines_sung': 0
        }
        
        await interaction.response.send_message(
            f"Welcome to karaoke, {interaction.user.mention}! 🎤",
            ephemeral=True
        )
    
    @discord.ui.button(label="📊 Scores", style=discord.ButtonStyle.primary)
    async def scores_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show karaoke scores"""
        if self.guild_id not in self.lyrics_cog.karaoke_sessions:
            await interaction.response.send_message("No active karaoke session!", ephemeral=True)
            return
        
        session = self.lyrics_cog.karaoke_sessions[self.guild_id]
        
        if not session['scores']:
            await interaction.response.send_message("No scores yet! Start singing!", ephemeral=True)
            return
        
        # Create scores embed
        embed = discord.Embed(
            title="🎤 Karaoke Scores",
            color=self.lyrics_cog.bot.colors.PLUM,
            timestamp=datetime.utcnow()
        )
        
        # Sort scores
        sorted_scores = sorted(
            session['scores'].items(),
            key=lambda x: x[1]['total_score'],
            reverse=True
        )[:10]  # Top 10
        
        for i, (user_id, score_data) in enumerate(sorted_scores, 1):
            user = self.lyrics_cog.bot.get_user(user_id)
            username = user.name if user else f"User {user_id}"
            
            embed.add_field(
                name=f"{i}. {username}",
                value=f"Score: {score_data['total_score']}\nAccuracy: {score_data['avg_accuracy']:.1f}%",
                inline=True
            )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="⏹️ End Karaoke", style=discord.ButtonStyle.danger)
    async def end_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """End karaoke session"""
        if self.guild_id not in self.lyrics_cog.karaoke_sessions:
            await interaction.response.send_message("No active karaoke session!", ephemeral=True)
            return
        
        # End session
        session = self.lyrics_cog.karaoke_sessions.pop(self.guild_id)
        session['is_active'] = False
        
        # Show final scores
        embed = discord.Embed(
            title="🎤 Karaoke Ended!",
            description="Final Scores:",
            color=self.lyrics_cog.bot.colors.ROSE_GOLD
        )
        
        if session['scores']:
            sorted_scores = sorted(
                session['scores'].items(),
                key=lambda x: x[1]['total_score'],
                reverse=True
            )
            
            # Add winner
            if sorted_scores:
                winner_id, winner_score = sorted_scores[0]
                winner = self.lyrics_cog.bot.get_user(winner_id)
                winner_name = winner.name if winner else f"User {winner_id}"
                
                embed.add_field(
                    name="🏆 Winner",
                    value=f"{winner_name}\nScore: {winner_score['total_score']}",
                    inline=False
                )
            
            # Add participant count
            embed.add_field(
                name="Participants",
                value=str(len(session['participants'])),
                inline=True
            )
            
            embed.add_field(
                name="Duration",
                value=f"{(datetime.utcnow() - session['started_at']).total_seconds() / 60:.1f} minutes",
                inline=True
            )
        
        await interaction.response.send_message(embed=embed)
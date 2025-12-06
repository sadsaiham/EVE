"""
Complete core music system with professional player, queue management,
and all advanced features
"""

import wavelink
import discord
from discord.ext import commands
import asyncio
from typing import Dict, List, Optional, Union, Deque, Tuple
from collections import deque
from datetime import datetime, timedelta
import random
import json
import logging
from datetime import timezone

# Fix for wavelink 3.x imports
try:
    from wavelink import Track
    from wavelink import Node, Player, Playlist
except ImportError:
    # For wavelink 3.x
    from wavelink.tracks import Playable as Track
    from wavelink import Node, Player, Playlist

logger = logging.getLogger(__name__)

class EvePlayer(wavelink.Player):
    """Advanced player with all features"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Queue system
        self.queue: Deque[Track] = deque()
        self.history: Deque[Track] = deque(maxlen=100)
        self.original_queue: List[Track] = []  # For shuffle restore
        
        # Playback modes
        self.loop_mode: str = "off"  # off, track, queue
        self.shuffle: bool = False
        self.auto_play: bool = False
        self.radio_mode: bool = False
        self.radio_seed: Optional[str] = None
        
        # Voting system
        self.skip_votes: set = set()
        self.required_skip_votes: int = 1
        self.volume_votes: set = set()
        
        # Player state
        self.text_channel: Optional[int] = None
        self.last_activity: datetime = datetime.now(timezone.utc)
        self.play_start_time: Optional[datetime] = None
        self.total_play_time: int = 0
        
        # Statistics
        self.tracks_played: int = 0
        self.unique_listeners: set = set()
        self.session_start: datetime = datetime.now(timezone.utc)
        
        # Equalizer
        self.equalizer: Optional[wavelink.Equalizer] = None
        self.equalizer_preset: str = "flat"
        
        # Effects
        self.effects: Dict = {
            'nightcore': False,
            'bassboost': False,
            'slow': False,
            'vaporwave': False
        }
        
        # Auto-disconnect timer
        self.alone_timer: Optional[asyncio.Task] = None
        
        # Queue metadata
        self.queue_metadata: List[Dict] = []
    
    async def play(self, track: Track, **kwargs):
        """Override play to add metadata"""
        if not hasattr(track, 'requester'):
            track.requester = getattr(self, 'last_requester', None)
        
        if not hasattr(track, 'added_at'):
            track.added_at = datetime.now(timezone.utc)
        
        self.last_activity = datetime.now(timezone.utc)
        self.last_requester = track.requester
        
        # Add to unique listeners
        if track.requester:
            self.unique_listeners.add(track.requester.id)
        
        return await super().play(track, **kwargs)
    
    async def play_next(self) -> bool:
        """Play next track with all mode handling"""
        if self.loop_mode == "track" and self.current:
            await self.play(self.current)
            return True
        
        if self.queue:
            # Get next track based on shuffle
            if self.shuffle and len(self.queue) > 1:
                # Don't shuffle if only one track left
                track = random.choice(list(self.queue))
                self.queue.remove(track)
            else:
                track = self.queue.popleft()
            
            # Add current to history
            if self.current:
                self.history.append(self.current)
                self.tracks_played += 1
                
                # Calculate play time
                if self.play_start_time:
                    play_duration = (datetime.now(timezone.utc) - self.play_start_time).total_seconds()
                    self.total_play_time += int(play_duration)
            
            # Play next track
            await self.play(track)
            self.play_start_time = datetime.now(timezone.utc)
            return True
        
        elif self.auto_play and self.radio_mode and self.current:
            # Auto-play radio
            try:
                next_track = await self.get_radio_track()
                if next_track:
                    self.queue.append(next_track)
                    return await self.play_next()
            except Exception as e:
                logger.error(f"Radio auto-play error: {e}")
        
        return False
    
    async def get_radio_track(self) -> Optional[Track]:
        """Get next radio track"""
        if not self.current:
            return None
        
        # This would interface with RadioDiscovery
        # For now, search for similar tracks
        query = f"Similar to: {self.current.title} {self.current.author}"
        
        try:
            tracks = await self.node.get_tracks(query=query)
            if tracks and not isinstance(tracks, wavelink.Playlist):
                # Filter out current track
                filtered = [t for t in tracks[:5] if t.identifier != self.current.identifier]
                if filtered:
                    return random.choice(filtered)
        except:
            pass
        
        return None
    
    def add_to_queue(self, track: Track, requester: discord.User, position: int = None):
        """Add track to queue with metadata"""
        track.requester = requester
        track.added_at = datetime.now(timezone.utc)
        
        if position is not None and 0 <= position < len(self.queue):
            # Convert deque to list for insertion
            queue_list = list(self.queue)
            queue_list.insert(position, track)
            self.queue = deque(queue_list)
        else:
            self.queue.append(track)
        
        # Store metadata
        self.queue_metadata.append({
            'track_id': track.identifier,
            'title': track.title,
            'requester_id': requester.id,
            'added_at': track.added_at.isoformat(),
            'position': position if position is not None else len(self.queue) - 1
        })
        
        return len(self.queue)
    
    def remove_from_queue(self, position: int) -> Optional[Track]:
        """Remove track from queue by position"""
        if 0 <= position < len(self.queue):
            # Convert to list for removal
            queue_list = list(self.queue)
            removed = queue_list.pop(position)
            self.queue = deque(queue_list)
            
            # Remove metadata
            self.queue_metadata = [m for m in self.queue_metadata if m['position'] != position]
            
            # Update positions
            for meta in self.queue_metadata:
                if meta['position'] > position:
                    meta['position'] -= 1
            
            return removed
        return None
    
    def move_in_queue(self, from_pos: int, to_pos: int) -> bool:
        """Move track in queue"""
        if (0 <= from_pos < len(self.queue) and 
            0 <= to_pos < len(self.queue) and 
            from_pos != to_pos):
            
            queue_list = list(self.queue)
            track = queue_list.pop(from_pos)
            queue_list.insert(to_pos, track)
            self.queue = deque(queue_list)
            
            # Update metadata
            for meta in self.queue_metadata:
                if meta['position'] == from_pos:
                    meta['position'] = to_pos
                elif from_pos < to_pos:
                    if from_pos < meta['position'] <= to_pos:
                        meta['position'] -= 1
                else:
                    if to_pos <= meta['position'] < from_pos:
                        meta['position'] += 1
            
            return True
        return False
    
    def shuffle_queue(self):
        """Shuffle the queue"""
        if len(self.queue) > 1:
            # Store original order
            if not self.original_queue:
                self.original_queue = list(self.queue)
            
            # Shuffle
            queue_list = list(self.queue)
            random.shuffle(queue_list)
            self.queue = deque(queue_list)
            
            # Update metadata positions
            for i, track in enumerate(self.queue):
                track_id = getattr(track, 'identifier', None)
                if track_id:
                    for meta in self.queue_metadata:
                        if meta['track_id'] == track_id:
                            meta['position'] = i
                            break
    
    def restore_queue_order(self):
        """Restore original queue order"""
        if self.original_queue:
            self.queue = deque(self.original_queue)
            self.original_queue = []
            self.shuffle = False
            
            # Update metadata positions
            for i, track in enumerate(self.queue):
                track_id = getattr(track, 'identifier', None)
                if track_id:
                    for meta in self.queue_metadata:
                        if meta['track_id'] == track_id:
                            meta['position'] = i
                            break
    
    def update_skip_requirements(self):
        """Update required skip votes based on listeners"""
        if not self.channel:
            self.required_skip_votes = 1
            return
        
        listeners = len([m for m in self.channel.members if not m.bot])
        self.required_skip_votes = max(1, (listeners + 1) // 2)  # Majority
    
    def add_skip_vote(self, user_id: int) -> Tuple[bool, int]:
        """Add skip vote, return (success, votes_needed)"""
        self.skip_votes.add(user_id)
        votes_needed = max(0, self.required_skip_votes - len(self.skip_votes))
        
        if len(self.skip_votes) >= self.required_skip_votes:
            self.skip_votes.clear()
            return True, 0
        
        return False, votes_needed
    
    def clear_skip_votes(self):
        """Clear all skip votes"""
        self.skip_votes.clear()
    
    def get_queue_info(self) -> Dict:
        """Get comprehensive queue info"""
        total_duration = sum(t.length for t in self.queue) if self.queue else 0
        if self.current:
            total_duration += self.current.length - self.position
        
        return {
            'current': self.current.title if self.current else None,
            'queue_length': len(self.queue),
            'total_duration': total_duration,
            'loop_mode': self.loop_mode,
            'shuffle': self.shuffle,
            'auto_play': self.auto_play,
            'radio_mode': self.radio_mode,
            'unique_listeners': len(self.unique_listeners),
            'tracks_played': self.tracks_played
        }
    
    async def apply_equalizer(self, preset: str):
        """Apply equalizer preset"""
        try:
            # For wavelink 3.x, equalizers might work differently
            if preset == 'flat':
                eq = wavelink.Equalizer.flat()
            elif preset == 'bassboost':
                eq = wavelink.Equalizer.bass()
            elif preset == 'metal':
                eq = wavelink.Equalizer.metal()
            elif preset == 'rock':
                eq = wavelink.Equalizer.rock()
            else:
                # Create custom equalizer
                bands = [(i, 0.0) for i in range(15)]  # Flat EQ
                eq = wavelink.Equalizer(bands=bands)
            
            self.equalizer = eq
            self.equalizer_preset = preset
            await self.set_eq(eq)
            return True
        except Exception as e:
            logger.error(f"Error applying equalizer: {e}")
            return False
    
    def create_lofi_eq(self):
        """Create lofi equalizer preset"""
        # Custom EQ bands for lofi
        bands = [
            (0, 0.2),   # Boost bass
            (1, 0.1),
            (2, 0.0),
            (3, -0.1),  # Reduce mids
            (4, -0.2),
            (5, -0.1),
            (6, 0.0),
            (7, 0.1),   # Slight treble
            (8, 0.15),
            (9, 0.1),
            (10, 0.05)
        ]
        # Add remaining bands if needed
        bands.extend([(i, 0.0) for i in range(11, 15)])
        return wavelink.Equalizer(bands=bands)
    
    def create_nightcore_eq(self):
        """Create nightcore equalizer preset"""
        bands = [
            (0, 0.3),   # Strong bass
            (1, 0.25),
            (2, 0.2),
            (3, 0.15),
            (4, 0.1),
            (5, 0.05),
            (6, 0.0),
            (7, 0.1),   # Boost highs
            (8, 0.2),
            (9, 0.25),
            (10, 0.3),
            (11, 0.25),
            (12, 0.2),
            (13, 0.15),
            (14, 0.1)
        ]
        return wavelink.Equalizer(bands=bands)
    
    async def apply_effects(self, effect: str, enabled: bool):
        """Apply audio effects"""
        if effect not in self.effects:
            return False
        
        self.effects[effect] = enabled
        
        try:
            # Apply effect filters
            filters = wavelink.Filters()
            
            if self.effects['nightcore']:
                filters.timescale = wavelink.Timescale(speed=1.2, pitch=1.3, rate=1.0)
            
            if self.effects['slow']:
                filters.timescale = wavelink.Timescale(speed=0.8, pitch=0.9, rate=1.0)
            
            if self.effects['vaporwave']:
                filters.timescale = wavelink.Timescale(speed=0.8, pitch=0.9, rate=1.0)
                filters.rotation = wavelink.Rotation(rotation_hz=0.2)
            
            await self.set_filters(filters)
            return True
        except Exception as e:
            logger.error(f"Error applying effects: {e}")
            return False
    
    def start_alone_timer(self):
        """Start auto-disconnect timer when alone"""
        if self.alone_timer and not self.alone_timer.done():
            self.alone_timer.cancel()
        
        self.alone_timer = asyncio.create_task(self.alone_check())
    
    async def alone_check(self):
        """Check if bot is alone in voice"""
        await asyncio.sleep(300)  # 5 minutes
        
        if self.is_connected() and self.channel:
            listeners = len([m for m in self.channel.members if not m.bot])
            if listeners == 0:
                await self.disconnect()
    
    def cancel_alone_timer(self):
        """Cancel auto-disconnect timer"""
        if self.alone_timer and not self.alone_timer.done():
            self.alone_timer.cancel()
    
    async def disconnect(self, *args, **kwargs):
        """Override disconnect to cleanup"""
        self.cancel_alone_timer()
        return await super().disconnect(*args, **kwargs)


class MusicCore:
    """Core music functionality with all features"""
    
    def __init__(self, bot):
        self.bot = bot
        self.players: Dict[int, EvePlayer] = {}
        
        # Command cooldowns
        self.user_cooldowns: Dict[int, List[datetime]] = {}
        self.command_stats: Dict[str, int] = {}
        
        # Session tracking
        self.sessions: Dict[int, Dict] = {}  # guild_id: session_data
        
        # Auto-play settings
        self.auto_play_sources = {
            'youtube': True,
            'spotify': True,
            'soundcloud': True
        }
        
        # Performance optimization
        self.track_cache: Dict[str, Track] = {}
        self.search_cache: Dict[str, List[Track]] = {}
        
        # Background tasks
        self.cleanup_task = None
    
    def get_player(self, guild_id: int) -> Optional[EvePlayer]:
        """Get player for guild"""
        return self.players.get(guild_id)
    
    async def create_player(self, guild_id: int, channel: discord.VoiceChannel) -> EvePlayer:
        """Create new player for guild"""
        player = EvePlayer(channel)
        self.players[guild_id] = player
        return player
    
    async def delete_player(self, guild_id: int):
        """Delete player for guild"""
        player = self.players.pop(guild_id, None)
        if player:
            await player.disconnect()
    
    async def create_play_session(self, ctx, player: EvePlayer) -> Dict:
        """Create a new play session"""
        session_id = f"{ctx.guild.id}_{int(datetime.now(timezone.utc).timestamp())}"
        
        session = {
            'id': session_id,
            'guild_id': ctx.guild.id,
            'channel_id': ctx.channel.id,
            'start_time': datetime.now(timezone.utc),
            'player': player,
            'tracks_played': 0,
            'unique_users': set(),
            'total_duration': 0
        }
        
        self.sessions[ctx.guild.id] = session
        return session
    
    async def end_play_session(self, guild_id: int):
        """End a play session and save stats"""
        if guild_id in self.sessions:
            session = self.sessions.pop(guild_id)
            
            # Calculate session duration
            duration = (datetime.now(timezone.utc) - session['start_time']).total_seconds()
            
            # Save session stats
            session_data = {
                'session_id': session['id'],
                'guild_id': guild_id,
                'duration': duration,
                'tracks_played': session['tracks_played'],
                'unique_users': len(session['unique_users']),
                'end_time': datetime.now(timezone.utc)
            }
            
            return session_data
        return None
    
    async def add_track_to_session(self, guild_id: int, track: Track):
        """Add track to current session"""
        if guild_id in self.sessions:
            session = self.sessions[guild_id]
            session['tracks_played'] += 1
            session['total_duration'] += track.length
            
            if hasattr(track, 'requester') and track.requester:
                session['unique_users'].add(track.requester.id)
    
    # Voting system
    async def handle_vote_skip(self, ctx, player: EvePlayer) -> str:
        """Handle vote skipping with personality responses"""
        player.update_skip_requirements()
        success, votes_needed = player.add_skip_vote(ctx.author.id)
        
        if success:
            await player.stop()
            responses = {
                "elegant": "The consensus has spoken... Moving forward.",
                "seductive": "The vote passes... On to the next pleasure. 💋",
                "playful": "VOTE PASSED! Skipping! 🗳️🎉",
                "teacher": "Majority decision made. Progressing.",
                "supportive": "Everyone agreed... Let's move on together.",
                "obedient": "Vote threshold reached. Skipping."
            }
            return responses.get(self.bot.personality_mode, "Vote passed! Skipping...")
        else:
            responses = {
                "elegant": f"Vote registered. {votes_needed} more required.",
                "seductive": f"Your vote is noted, darling... {votes_needed} more needed. 💋",
                "playful": f"Vote added! Need {votes_needed} more to skip! 🎵",
                "teacher": f"Vote recorded. {votes_needed} additional votes needed.",
                "supportive": f"Your voice is heard... {votes_needed} more needed.",
                "obedient": f"Vote added. {votes_needed} more required."
            }
            return responses.get(self.bot.personality_mode, f"{votes_needed} more votes needed.")
    
    # Effects and filters
    async def apply_player_effects(self, player: EvePlayer, effects: Dict):
        """Apply multiple effects to player"""
        try:
            filters = wavelink.Filters()
            
            if effects.get('nightcore'):
                filters.timescale = wavelink.Timescale(speed=1.2, pitch=1.3, rate=1.0)
            
            if effects.get('slow'):
                filters.timescale = wavelink.Timescale(speed=0.8, pitch=0.9, rate=1.0)
            
            if effects.get('vaporwave'):
                filters.timescale = wavelink.Timescale(speed=0.8, pitch=0.9, rate=1.0)
                filters.rotation = wavelink.Rotation(rotation_hz=0.2)
            
            await player.set_filters(filters)
            return True
        except Exception as e:
            logger.error(f"Error applying player effects: {e}")
            return False
    
    # Statistics and analytics
    async def get_player_statistics(self, guild_id: int) -> Dict:
        """Get comprehensive player statistics"""
        player = self.get_player(guild_id)
        if not player:
            return {}
        
        stats = {
            'session_duration': (datetime.now(timezone.utc) - player.session_start).total_seconds(),
            'tracks_played': player.tracks_played,
            'unique_listeners': len(player.unique_listeners),
            'total_play_time': player.total_play_time,
            'queue_length': len(player.queue),
            'current_track': player.current.title if player.current else None,
            'current_position': player.position if player.current else 0,
            'current_duration': player.current.length if player.current else 0,
            'loop_mode': player.loop_mode,
            'shuffle_enabled': player.shuffle,
            'auto_play': player.auto_play,
            'radio_mode': player.radio_mode
        }
        
        # Add queue statistics
        if player.queue:
            queue_stats = {
                'queue_duration': sum(t.length for t in player.queue),
                'average_track_length': sum(t.length for t in player.queue) / len(player.queue),
                'most_requested': self.get_most_requested(player),
                'oldest_in_queue': min(t.added_at for t in player.queue if hasattr(t, 'added_at')) if player.queue else None
            }
            stats.update(queue_stats)
        
        return stats
    
    def get_most_requested(self, player: EvePlayer) -> Dict:
        """Get most requested artist in queue"""
        artists = {}
        tracks = list(player.queue) + ([player.current] if player.current else [])
        
        for track in tracks:
            if hasattr(track, 'author') and track.author:
                artists[track.author] = artists.get(track.author, 0) + 1
        
        if artists:
            most_requested = max(artists.items(), key=lambda x: x[1])
            return {'artist': most_requested[0], 'count': most_requested[1]}
        
        return {'artist': None, 'count': 0}
    
    # Cache management
    async def cache_track(self, track: Track):
        """Cache track for faster future access"""
        cache_key = f"track:{track.source}:{track.identifier}"
        self.track_cache[cache_key] = track
        
        # Limit cache size
        if len(self.track_cache) > 1000:
            # Remove oldest entries
            keys = list(self.track_cache.keys())
            for key in keys[:100]:
                self.track_cache.pop(key, None)
    
    async def get_cached_track(self, identifier: str, source: str) -> Optional[Track]:
        """Get track from cache"""
        cache_key = f"track:{source}:{identifier}"
        return self.track_cache.get(cache_key)
    
    async def cache_search_results(self, query: str, tracks: List[Track]):
        """Cache search results"""
        cache_key = f"search:{query.lower()}"
        self.search_cache[cache_key] = tracks
        
        # Set expiration (1 hour)
        asyncio.get_event_loop().call_later(
            3600, 
            lambda: self.search_cache.pop(cache_key, None)
        )
    
    async def get_cached_search(self, query: str) -> Optional[List[Track]]:
        """Get cached search results"""
        cache_key = f"search:{query.lower()}"
        return self.search_cache.get(cache_key)
    
    # Background cleanup
    async def start_cleanup_task(self):
        """Start background cleanup task"""
        if self.cleanup_task and not self.cleanup_task.done():
            self.cleanup_task.cancel()
        
        self.cleanup_task = asyncio.create_task(self.periodic_cleanup())
    
    async def periodic_cleanup(self):
        """Periodic cleanup of old data"""
        while True:
            try:
                # Clean old cooldowns
                now = datetime.now(timezone.utc)
                for user_id in list(self.user_cooldowns.keys()):
                    self.user_cooldowns[user_id] = [
                        ts for ts in self.user_cooldowns[user_id]
                        if (now - ts).total_seconds() < 3600  # 1 hour
                    ]
                    if not self.user_cooldowns[user_id]:
                        self.user_cooldowns.pop(user_id, None)
                
                # Clean old sessions
                for guild_id in list(self.sessions.keys()):
                    session = self.sessions[guild_id]
                    session_age = (now - session['start_time']).total_seconds()
                    if session_age > 86400:  # 24 hours
                        await self.end_play_session(guild_id)
                
                # Clean disconnected players
                for guild_id in list(self.players.keys()):
                    player = self.players[guild_id]
                    if not player.is_connected():
                        # Wait 5 minutes before cleanup
                        last_activity = getattr(player, 'last_activity', now)
                        if (now - last_activity).total_seconds() > 300:
                            self.players.pop(guild_id, None)
                
                await asyncio.sleep(300)  # Run every 5 minutes
                
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
                await asyncio.sleep(60)
    
    async def stop_cleanup_task(self):
        """Stop cleanup task"""
        if self.cleanup_task and not self.cleanup_task.done():
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
    
    # Utility methods
    def format_duration(self, milliseconds: int) -> str:
        """Format duration to readable string"""
        seconds = milliseconds // 1000
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        
        if hours > 0:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        return f"{minutes}:{seconds:02d}"
    
    def format_time_ago(self, dt: datetime) -> str:
        """Format time ago string"""
        now = datetime.now(timezone.utc)
        diff = now - dt
        
        if diff.days > 0:
            return f"{diff.days} day{'s' if diff.days != 1 else ''} ago"
        elif diff.seconds > 3600:
            hours = diff.seconds // 3600
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif diff.seconds > 60:
            minutes = diff.seconds // 60
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        else:
            return "just now"
    
    def truncate_text(self, text: str, length: int = 50) -> str:
        """Truncate text with ellipsis"""
        if len(text) <= length:
            return text
        return text[:length - 3] + "..."
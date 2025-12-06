"""
EVE Music System - Complete Professional Music Bot
Combines all music modules into a single cog with full features
"""

import discord
from discord.ext import commands, tasks
import asyncio
import logging
from discord.ext import tasks 
import random
import hashlib
from typing import Tuple, Dict, Optional, List, Any, Set

logger = logging.getLogger(__name__)

# Import all modules
from .lavalink_client import OptimizedLavalinkClient
from .core import MusicCore, EvePlayer
from .commands import MusicCommands
from .player_ui import PlayerUI, NowPlayingView, QueueView
from .playlist_manager import PlaylistManager
from .radio_discovery import RadioDiscovery
from .lyrics_karaoke import LyricsKaraoke
from .social_features import SocialFeatures
from .history_stats import HistoryStats
from .cache_manager import CacheManager
from .utils import MusicUtils, TrackFormatter, DurationConverter
from .database import MusicDatabase

class MusicSystem(
    commands.Cog,
    OptimizedLavalinkClient,
    MusicCore,
    MusicCommands,
    PlayerUI,
    PlaylistManager,
    RadioDiscovery,
    LyricsKaraoke,
    SocialFeatures,
    HistoryStats,
    CacheManager,
    MusicUtils
):
    """Complete professional music system for EVE Discord Bot"""
    
    def __init__(self, bot):
        self.bot = bot
        self.color = bot.colors.VIOLET
        self.personality = bot.personality_mode
        
        # Initialize database
        self.db = MusicDatabase(bot)
        self.db_initilized = False
        
        # Initialize all parent classes
        OptimizedLavalinkClient.__init__(self, bot)
        MusicCore.__init__(self, bot)
        MusicCommands.__init__(self, bot)
        PlayerUI.__init__(self, bot)
        PlaylistManager.__init__(self, bot)
        RadioDiscovery.__init__(self, bot)
        LyricsKaraoke.__init__(self, bot)
        SocialFeatures.__init__(self, bot)
        HistoryStats.__init__(self, bot)
        CacheManager.__init__(self, bot)
        MusicUtils.__init__(self, bot)
        
        # Active players
        self.players: Dict[int, EvePlayer] = {}
        
        # User sessions
        self.user_sessions = {}
        
        # Statistics
        self.global_stats = {
            'tracks_played': 0,
            'total_play_time': 0,
            'active_players': 0,
            'unique_users': set()
        }
        
        # Start background tasks
        self.bot.loop.create_task(self.initialize_system())
    
    async def initialize_system(self):
        """Initialize the complete music system"""
        await self.bot.wait_until_ready()
        
        # Initialize Lavalink
        logger.info("Initializing Lavalink client...")
        await self.create_session()
        await self.connect_lavalink()
        
        # Initialize database
        await self.db.initialize()
        
        # Load caches
        await self.load_caches()
        
        # Start background tasks
        self.update_presence.start()
        self.cleanup_players.start()
        self.save_stats.start()
        
        logger.info("EVE Music System initialized successfully!")
    
    @commands.Cog.listener()
    async def on_voice_state_update(self, member, before, after):
        """Handle voice state changes for auto-disconnect"""
        if member.id == self.bot.user.id:
            return
        
        # Auto-disconnect if bot is alone
        for guild_id, player in self.players.items():
            if player.is_connected():
                channel = player.channel
                if channel and len(channel.members) == 1:  # Only bot
                    await asyncio.sleep(60)  # Wait 60 seconds
                    if len(channel.members) == 1:  # Still alone
                        await self.handle_disconnect(player)
                        guild = self.bot.get_guild(guild_id)
                        if guild:
                            await guild.system_channel.send(
                                self.get_personality_response("disconnect_alone")
                            )
    
    @commands.Cog.listener()
    async def on_wavelink_node_ready(self, node):
        """Handle Lavalink node ready"""
        logger.info(f"Lavalink node {node.identifier} is ready!")
        
        # Update presence
        await self.update_presence()
    
    @commands.Cog.listener()
    async def on_wavelink_track_start(self, player, track):
        """Handle track start"""
        guild_id = player.guild.id
        
        # Update statistics
        self.global_stats['tracks_played'] += 1
        self.global_stats['unique_users'].add(track.requester.id)
        
        # Save to history
        await self.add_to_history(guild_id, track)
        
        # Update player session
        if guild_id in self.players:
            self.players[guild_id].tracks_played += 1
            self.players[guild_id].play_start_time = asyncio.get_event_loop().time()
        
        # Send now playing if configured
        if hasattr(player, 'send_now_playing') and player.send_now_playing:
            channel = self.bot.get_channel(player.text_channel)
            if channel:
                embed = await self.create_now_playing_embed(track, player)
                view = NowPlayingView(self, guild_id)
                await channel.send(embed=embed, view=view)
    
    @commands.Cog.listener()
    async def on_wavelink_track_end(self, player, track, reason):
        """Handle track end"""
        if reason == "FINISHED":
            # Auto-play next if enabled
            if player.auto_play or player.radio_mode:
                await player.play_next()
            elif player.loop_mode == "queue":
                player.queue.append(track)
                await player.play_next()
            elif player.loop_mode == "track":
                await player.play(track)
            else:
                await player.play_next()
    
    @commands.Cog.listener()
    async def on_wavelink_track_exception(self, player, track, error):
        """Handle track exception"""
        logger.error(f"Track exception: {error}")
        
        # Skip to next track
        await player.play_next()
        
        # Notify users
        channel = self.bot.get_channel(player.text_channel)
        if channel:
            await channel.send(
                f"⏭️ Skipped due to playback error: {error}"
            )
    
    @commands.Cog.listener()
    async def on_wavelink_track_stuck(self, player, track, threshold):
        """Handle stuck track"""
        logger.warning(f"Track stuck for {threshold}ms")
        
        # Skip stuck track
        await player.play_next()
        
        # Notify users
        channel = self.bot.get_channel(player.text_channel)
        if channel:
            await channel.send(
                f"⏭️ Skipped stuck track after {threshold}ms"
            )

    @commands.Cog.listener()
    async def on_wavelink_node_disconnected(self, node, reason):
        """Handle node disconnection"""
        logger.warning(f"Node {node.identifier} disconnected: {reason}")
        
    @commands.Cog.listener() 
    async def on_wavelink_websocket_closed(self, player, reason, code):
        """Handle websocket closure"""
        logger.warning(f"Websocket closed: {reason} ({code})")
    
    # Background tasks
    @tasks.loop(minutes=5)
    async def update_presence(self):
        """Update bot presence with music stats"""
        active_players = sum(1 for p in self.players.values() if p.is_playing())
        
        activities = [
            discord.Activity(type=discord.ActivityType.listening, name=f"music in {active_players} servers"),
            discord.Activity(type=discord.ActivityType.watching, name=f"{self.global_stats['tracks_played']} tracks played"),
            discord.Activity(type=discord.ActivityType.playing, name="with EVE's music system")
        ]
        
        activity = random.choice(activities)
        await self.bot.change_presence(activity=activity)
    
    @tasks.loop(minutes=10)
    async def cleanup_players(self):
        """Clean up inactive players"""
        for guild_id in list(self.players.keys()):
            player = self.players[guild_id]
            
            # Check if player is idle for too long
            if player.is_connected() and not player.is_playing():
                idle_time = asyncio.get_event_loop().time() - getattr(player, 'last_activity', 0)
                if idle_time > 300:  # 5 minutes
                    await self.handle_disconnect(player)
    
    @tasks.loop(minutes=15)
    async def save_stats(self):
        """Save statistics to database"""
        await self.db.save_global_stats(self.global_stats)
        
        # Save player stats
        for guild_id, player in self.players.items():
            await self.db.save_player_stats(guild_id, player)
    
    # Utility methods
    def get_player(self, guild_id: int) -> Optional[EvePlayer]:
        """Get player for guild"""
        return self.players.get(guild_id)
    
    def create_player(self, guild_id: int) -> EvePlayer:
        """Create new player for guild"""
        player = EvePlayer(bot=self.bot, guild_id=guild_id)
        self.players[guild_id] = player
        return player
    
    async def ensure_voice(self, ctx) -> EvePlayer:
        """Ensure bot is in voice channel"""
        # Implementation from core.py
        pass
    
    def get_personality_response(self, key: str, **kwargs) -> str:
        """Get personality-based response"""
        responses = self.personality_responses.get(self.bot.personality_mode, {})
        response = responses.get(key, "")
        
        if response and kwargs:
            response = response.format(**kwargs)
        
        return response
    
    @property
    def personality_responses(self) -> Dict:
        """Personality response templates"""
        return {
            "elegant": {
                "play": "The symphony begins... **{title}** graces our ears.",
                "pause": "The music pauses... A moment of reflection.",
                "skip": "Onward to the next movement...",
                "queue_add": "Added to our musical journey: **{title}**",
                "error": "An orchestral misstep... Please try again.",
                "empty_queue": "The concert hall awaits your selection...",
                "disconnect_alone": "Playing to an empty room... I shall take my leave."
            },
            "seductive": {
                "play": "Mmm, this track... **{title}** Let it move through you. 🌹",
                "pause": "Paused... Just like my breath when you're near.",
                "skip": "Moving on... Some pleasures are brief but memorable. 💋",
                "queue_add": "I'll save this one for later... **{title}** added.",
                "error": "Something went wrong... Let's try that again, slower.",
                "empty_queue": "The silence is intimate... What shall we fill it with?",
                "disconnect_alone": "All alone... I'll be waiting for your call."
            },
            "playful": {
                "play": "LET'S GOOOOO! 🎵 **{title}** is playing!",
                "pause": "Paused! ⏸️ Time for a dance break!",
                "skip": "SKIPPITY SKIP! Next track! 🎉",
                "queue_add": "Added to the party! **{title}** coming up!",
                "error": "Oopsie! Something broke! Let's try again! 😅",
                "empty_queue": "Queue's empty! Time to add some JAMS! 🎶",
                "disconnect_alone": "Aww, everyone left! Bye-bye! 👋"
            },
            "supportive": {
                "play": "Here's something beautiful... **{title}**",
                "pause": "Taking a break... That's okay.",
                "skip": "Moving forward... Growth comes from change.",
                "queue_add": "I've added **{title}** for you, dear.",
                "error": "It's okay to stumble... Let's try again.",
                "empty_queue": "The playlist is empty... Ready for new beginnings.",
                "disconnect_alone": "Time for quiet reflection... I'll be here when you return."
            },
            "teacher": {
                "play": "Let's analyze **{title}**... A fine selection.",
                "pause": "Pausing for study... Take notes if needed.",
                "skip": "Progressing to the next piece...",
                "queue_add": "Added to the curriculum: **{title}**",
                "error": "An educational setback... Let's correct it.",
                "empty_queue": "The lesson plan is empty... What shall we learn?",
                "disconnect_alone": "Class dismissed due to absence."
            },
            "obedient": {
                "play": "Playing **{title}** as commanded.",
                "pause": "Paused at your command.",
                "skip": "Skipping immediately.",
                "queue_add": "Added to queue: **{title}**",
                "error": "Command failed. Please retry.",
                "empty_queue": "Queue empty. Awaiting commands.",
                "disconnect_alone": "Disconnecting due to inactivity."
            }
        }
    
    async def cog_unload(self):
        """Cleanup on cog unload"""
        # Disconnect all players
        for player in self.players.values():
            try:
                await player.disconnect()
            except:
                pass
        
        # Close database
        await self.db.close()
        
        # Cancel tasks
        self.update_presence.cancel()
        self.cleanup_players.cancel()
        self.save_stats.cancel()
        
        logger.info("Music system unloaded successfully!")

async def setup(bot):
    await bot.add_cog(MusicSystem(bot))
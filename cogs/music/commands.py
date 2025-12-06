"""
All text commands implementation with personality responses
"""

import discord
from discord.ext import commands
import wavelink
import asyncio
from typing import Optional, Union, List
import datetime
import re
import random
import logging

logger = logging.getLogger(__name__)

class MusicCommands:
    """Implementation of all text commands"""
    
    def __init__(self, bot):
        self.bot = bot
        
        # Search cache for interactive results
        self.search_cache = {}
        
    @commands.hybrid_command(
        name="play",
        aliases=["p"],
        description="Play a song or add to queue"
    )
    async def play_command(self, ctx, *, query: str):
        """Play music from various sources"""
        
        # Check rate limit
        if not self.check_rate_limit(ctx.author.id, "play"):
            responses = {
                "elegant": "Patience, dear... Let the current melody settle.",
                "playful": "Whoa there! Let the music breathe! 🎵",
                "seductive": "So eager... But let's not rush the pleasure. 🌹"
            }
            await ctx.send(responses.get(self.bot.personality_mode, "Please wait a moment."))
            return
        
        # Ensure in voice
        try:
            player = await self.ensure_voice(ctx)
        except commands.CommandError as e:
            await ctx.send(str(e))
            return
        
        # Show searching message
        search_msg = await ctx.send("Searching for your melody... ✨")
        
        # Search for tracks
        tracks = await self.search_tracks(query)
        
        if not tracks:
            await search_msg.edit(content="No songs found... Perhaps try different words?")
            return
        
        # Handle different result types
        if isinstance(tracks, wavelink.YouTubePlaylist):
            # Playlist
            await self.handle_playlist(ctx, tracks, player, search_msg)
        elif len(tracks) == 1:
            # Single track
            await self.play_track(ctx, tracks[0], player)
            await search_msg.delete()
        else:
            # Multiple results - show interactive selector
            await self.show_search_results(ctx, tracks, player, search_msg)
    
    async def show_search_results(self, ctx, tracks: List[wavelink.Track], player, search_msg):
        """Show interactive search results like Maki"""
        embed = discord.Embed(
            title="🎶 Search Results",
            description="Click a button to select a track:",
            color=self.bot.colors.ROSE_GOLD
        )
        
        # Add top 10 results
        for i, track in enumerate(tracks[:10], 1):
            duration = str(datetime.timedelta(milliseconds=track.length))
            embed.add_field(
                name=f"{i}. {track.title[:50]}",
                value=f"`{duration}` • {track.author or 'Unknown'}",
                inline=False
            )
        
        # Cache for button callbacks
        cache_key = f"{ctx.guild.id}_{ctx.author.id}"
        self.search_cache[cache_key] = tracks[:10]
        
        # Create buttons for selection
        view = SearchResultsView(self, cache_key)
        await search_msg.edit(embed=embed, view=view)
    
    @commands.hybrid_command(
        name="pause",
        description="Pause current playback"
    )
    async def pause_command(self, ctx):
        """Pause the music"""
        player = self.get_player(ctx.guild.id)
        
        if not player or not player.is_playing():
            await ctx.send("No music is playing... The silence speaks volumes.")
            return
        
        if player.is_paused():
            await ctx.send("Already paused... The music awaits your command.")
            return
        
        await player.pause()
        
        responses = {
            "elegant": "The music pauses... A moment of reflection.",
            "seductive": "Paused... Just like my breath when you're near. 🌹",
            "playful": "Paused! ⏸️ The beat is on break!"
        }
        
        await ctx.send(responses.get(self.bot.personality_mode, "Paused the music."))
    
    @commands.hybrid_command(
        name="resume",
        description="Resume paused playback"
    )
    async def resume_command(self, ctx):
        """Resume paused music"""
        player = self.get_player(ctx.guild.id)
        
        if not player:
            await ctx.send("No player found... Start with `e!play` first.")
            return
        
        if not player.is_paused():
            await ctx.send("Already playing... The music flows uninterrupted.")
            return
        
        await player.resume()
        
        responses = {
            "elegant": "The symphony resumes... As if it never stopped.",
            "seductive": "Resumed... Let the rhythm move through you again. 💃",
            "playful": "And we're back! 🎵 Let's gooo!"
        }
        
        await ctx.send(responses.get(self.bot.personality_mode, "Resumed playback."))
    
    @commands.hybrid_command(
        name="skip",
        aliases=["next", "s"],
        description="Skip current song"
    )
    async def skip_command(self, ctx):
        """Skip current track"""
        player = self.get_player(ctx.guild.id)
        
        if not player or not player.is_playing():
            await ctx.send("Nothing to skip... The silence continues.")
            return
        
        # Update skip requirements
        player.update_skip_requirements()
        
        # Check if requester has permission
        if ctx.author.guild_permissions.manage_channels or ctx.author == player.current.requester:
            # Instant skip for mods or requester
            await player.stop()
            
            responses = {
                "elegant": "Skipped at your command.",
                "obedient": "Immediate skip executed, Master.",
                "seductive": "As you wish... Moving to the next pleasure. 🌹"
            }
            
            await ctx.send(responses.get(self.bot.personality_mode, "Skipped!"))
            return
        
        # Vote skipping for regular users
        if player.add_vote(ctx.author.id):
            # Vote passed
            await player.stop()
            
            responses = {
                "elegant": "The consensus has spoken... Skipping.",
                "playful": "Vote passed! Skipping to the next track! 🗳️",
                "teacher": "Democratic decision made. Skipping track."
            }
            
            await ctx.send(responses.get(self.bot.personality_mode, "Vote passed! Skipping..."))
        else:
            # Vote in progress
            votes_needed = player.required_skip_votes - len(player.skip_votes)
            
            responses = {
                "elegant": f"Vote registered. {votes_needed} more votes needed to skip.",
                "playful": f"Vote added! Need {votes_needed} more to skip! 🎵",
                "seductive": f"Your vote is counted, darling... {votes_needed} more needed. 💋"
            }
            
            await ctx.send(responses.get(self.bot.personality_mode, 
                f"{votes_needed} more votes needed to skip."))
    
    @commands.hybrid_command(
        name="stop",
        aliases=["disconnect", "leave", "dc"],
        description="Stop playback and disconnect"
    )
    async def stop_command(self, ctx):
        """Stop music and disconnect"""
        player = self.get_player(ctx.guild.id)
        
        if not player or not player.is_connected():
            await ctx.send("Not in a voice channel... Already silent.")
            return
        
        # Clear queue
        player.queue.clear()
        player.auto_play = False
        player.radio_mode = False
        
        # Disconnect
        await self.handle_disconnect(player)
        
        responses = {
            "elegant": "The music fades... Until next time.",
            "seductive": "Disconnecting... But I'll be here when you call again. 💋",
            "playful": "Stopping! 🛑 Queue cleared! See you next time!",
            "supportive": "Music stopped... Rest your ears, my dear."
        }
        
        await ctx.send(responses.get(self.bot.personality_mode, "Stopped and disconnected."))
    
    @commands.hybrid_command(
        name="queue",
        aliases=["q"],
        description="Show current queue"
    )
    async def queue_command(self, ctx, page: int = 1):
        """Display the current queue"""
        player = self.get_player(ctx.guild.id)
        
        if not player or (not player.current and not player.queue):
            await ctx.send("The queue is empty... Waiting for your melody.")
            return
        
        # Calculate pages
        items_per_page = 10
        total_tracks = len(player.queue) + (1 if player.current else 0)
        total_pages = (total_tracks + items_per_page - 1) // items_per_page
        
        # Clamp page number
        page = max(1, min(page, total_pages))
        
        # Create queue embed
        embed = self.create_queue_embed(player, page, total_pages)
        
        # Add queue controls if multiple pages
        if total_pages > 1:
            view = QueuePaginationView(self, ctx.guild.id, page, total_pages)
            await ctx.send(embed=embed, view=view)
        else:
            await ctx.send(embed=embed)
    
    def create_queue_embed(self, player, page: int, total_pages: int):
        """Create beautiful queue embed"""
        embed = discord.Embed(
            title="🎼 Music Queue",
            color=self.bot.colors.PLUM
        )
        
        # Now playing
        if player.current:
            duration = str(datetime.timedelta(milliseconds=player.current.length))
            current_time = str(datetime.timedelta(milliseconds=player.position))
            
            progress_bar = self.create_progress_bar(
                player.position, 
                player.current.length
            )
            
            embed.add_field(
                name="Now Playing",
                value=(
                    f"[{player.current.title}]({player.current.uri})\n"
                    f"`{current_time} {progress_bar} {duration}`\n"
                    f"*Requested by {getattr(player.current, 'requester', 'Unknown')}*"
                ),
                inline=False
            )
        
        # Queue items
        start_idx = (page - 1) * 10
        end_idx = min(start_idx + 10, len(player.queue))
        
        if player.queue[start_idx:end_idx]:
            queue_text = ""
            for i, track in enumerate(player.queue[start_idx:end_idx], start_idx + 1):
                duration = str(datetime.timedelta(milliseconds=track.length))
                queue_text += f"**{i}.** {track.title[:50]}\n`{duration}` • {track.author or 'Unknown'}\n\n"
            
            embed.add_field(
                name=f"Up Next (Page {page}/{total_pages})",
                value=queue_text or "Nothing queued...",
                inline=False
            )
        
        # Queue stats
        total_duration = sum(t.length for t in player.queue) + (
            player.current.length - player.position if player.current else 0
        )
        total_duration_str = str(datetime.timedelta(milliseconds=total_duration))
        
        embed.set_footer(
            text=(
                f"Loop: {player.loop_mode.title()} | "
                f"{len(player.queue)} tracks in queue | "
                f"Total: {total_duration_str}"
            )
        )
        
        return embed
    
    def create_progress_bar(self, position: int, length: int, size: int = 20):
        """Create a progress bar for now playing"""
        if length == 0:
            return "▬" * size
        
        progress = position / length
        filled = int(size * progress)
        
        bar = "▬" * filled + "🔘" + "▬" * (size - filled - 1)
        return bar
    
    @commands.hybrid_command(
        name="nowplaying",
        aliases=["np", "current"],
        description="Show currently playing song"
    )
    async def nowplaying_command(self, ctx):
        """Show detailed now playing info"""
        player = self.get_player(ctx.guild.id)
        
        if not player or not player.current:
            await ctx.send("No music playing... The air is still.")
            return
        
        await self.send_now_playing(ctx, player.current, player)
    
    @commands.hybrid_command(
        name="loop",
        description="Set loop mode"
    )
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def loop_command(self, ctx, mode: str = None):
        """Set loop mode: off, track, queue"""
        player = self.get_player(ctx.guild.id)
        
        if not player or not player.is_connected():
            await ctx.send("Not playing anything... Can't loop silence.")
            return
        
        valid_modes = ["off", "track", "queue", "song", "none", "all"]
        
        if not mode:
            # Show current mode
            responses = {
                "elegant": f"Current loop mode: **{player.loop_mode.title()}**",
                "playful": f"Loop mode is: **{player.loop_mode.upper()}**! 🔄",
                "seductive": f"The rhythm currently loops in **{player.loop_mode}** mode... 🌹"
            }
            
            await ctx.send(responses.get(self.bot.personality_mode, 
                f"Loop mode: {player.loop_mode}"))
            return
        
        mode = mode.lower()
        if mode in ["off", "none"]:
            player.loop_mode = "off"
            response = "Loop disabled."
        elif mode in ["track", "song"]:
            player.loop_mode = "track"
            response = "Looping current track."
        elif mode in ["queue", "all"]:
            player.loop_mode = "queue"
            response = "Looping entire queue."
        else:
            await ctx.send("Invalid mode. Use: off, track, or queue")
            return
        
        # Personality responses
        responses = {
            "off": {
                "elegant": "Loop released... Each note shall pass only once.",
                "seductive": "The loop breaks... Moving forward, always forward. 🌹",
                "playful": "Loop disabled! One-time playthrough mode! 🎵"
            },
            "track": {
                "elegant": "This melody shall repeat... Until you command otherwise.",
                "seductive": "Looping this track... Some pleasures bear repeating. 💋",
                "playful": "Looping this song! 🔁 Get ready to hear it again and again!"
            },
            "queue": {
                "elegant": "The entire queue shall cycle... An endless symphony.",
                "seductive": "All tracks will loop... An eternal dance. 💃",
                "teacher": "Queue loop activated. The musical journey continues indefinitely."
            }
        }
        
        await ctx.send(responses.get(mode, {}).get(
            self.bot.personality_mode, response))
    
    @commands.hybrid_command(
        name="lyrics",
        description="Show lyrics for current or specific song"
    )
    async def lyrics_command(self, ctx, *, song_name: str = None):
        """Fetch and display lyrics"""
        # Will be implemented in lyrics_karaoke.py
        await ctx.send("Lyrics feature coming soon... 🎶")
    
    @commands.hybrid_command(
        name="playlist",
        description="Manage your playlists"
    )
    async def playlist_command(self, ctx, action: str = None, *, args: str = None):
        """Playlist management"""
        # Will be implemented in playlist_manager.py
        await ctx.send("Playlist manager loading... 📁")
    
    @commands.hybrid_command(
        name="radio",
        description="Start a radio station"
    )
    async def radio_command(self, ctx, genre: str = None):
        """Start a radio station based on genre or current track"""
        # Will be implemented in radio_discovery.py
        await ctx.send("Radio feature tuning in... 📻")
    
    @commands.hybrid_command(
        name="history",
        description="Show recently played tracks"
    )
    async def history_command(self, ctx):
        """Show playback history"""
        player = self.get_player(ctx.guild.id)
        
        if not player or not player.history:
            await ctx.send("No history yet... Start playing to create memories.")
            return
        
        # Show last 10 tracks
        embed = discord.Embed(
            title="⏮️ Recently Played",
            color=self.bot.colors.BLUSH
        )
        
        for i, track in enumerate(reversed(list(player.history)[-10:]), 1):
            duration = str(datetime.timedelta(milliseconds=track.length))
            embed.add_field(
                name=f"{i}. {track.title[:40]}",
                value=f"`{duration}` • {track.author or 'Unknown'}",
                inline=False
            )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(
        name="playrandom",
        description="Play a random song from queue or playlist"
    )
    async def playrandom_command(self, ctx, source: str = "queue"):
        """Play a random track"""
        player = self.get_player(ctx.guild.id)
        
        if not player:
            await ctx.send("No player active... Start with `e!play` first.")
            return
        
        if source == "queue" and player.queue:
            # Shuffle queue and play first
            random.shuffle(player.queue)
            await ctx.send("Queue shuffled! 🎲 Playing random track...")
            await player.play_next()
        else:
            await ctx.send("Nothing to randomize... Add songs to queue first.")
    
    # Helper methods
    def check_rate_limit(self, user_id: int, command: str) -> bool:
        """Check if user is rate limited"""
        # Implement rate limiting
        return True
    
    # In commands.py, add actual implementations
    async def search_tracks(self, query: str) -> List[wavelink.Track]:
        """Actual search implementation"""
        try:
            # Use the lavalink client
            tracks = await self.bot.get_cog('MusicSystem').search_tracks(query)
            return tracks
        except Exception as e:
            logger.error(f"Search error: {e}")
            return []

# Interactive views
class SearchResultsView(discord.ui.View):
    """Interactive search results view"""
    
    def __init__(self, music_cog, cache_key):
        super().__init__(timeout=60)
        self.music_cog = music_cog
        self.cache_key = cache_key
        
        # Create buttons 1-10
        for i in range(1, 11):
            button = discord.ui.Button(
                label=str(i),
                style=discord.ButtonStyle.secondary,
                custom_id=f"search_{i}"
            )
            button.callback = self.create_callback(i)
            self.add_item(button)
    
    def create_callback(self, index):
        async def callback(interaction):
            tracks = self.music_cog.search_cache.get(self.cache_key)
            if not tracks or index - 1 >= len(tracks):
                await interaction.response.send_message("Selection expired.", ephemeral=True)
                return
            
            track = tracks[index - 1]
            # Play the selected track
            # Implementation depends on context storage
            await interaction.response.send_message(f"Selected: {track.title}")
        return callback


class QueuePaginationView(discord.ui.View):
    """Queue pagination view"""
    
    def __init__(self, music_cog, guild_id, current_page, total_pages):
        super().__init__(timeout=120)
        self.music_cog = music_cog
        self.guild_id = guild_id
        self.current_page = current_page
        self.total_pages = total_pages
    
    @discord.ui.button(label="◀️", style=discord.ButtonStyle.primary)
    async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 1:
            self.current_page -= 1
            await self.update_queue_embed(interaction)
    
    @discord.ui.button(label="▶️", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < self.total_pages:
            self.current_page += 1
            await self.update_queue_embed(interaction)
    
    async def update_queue_embed(self, interaction):
        player = self.music_cog.get_player(self.guild_id)
        if player:
            embed = self.music_cog.create_queue_embed(player, self.current_page, self.total_pages)
            await interaction.response.edit_message(embed=embed, view=self)
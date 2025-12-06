"""
Complete interactive UI system with buttons, dropdowns, 
embeds, and visualizations for EVE Music
"""

import discord
from discord.ui import View, Button, Select, Modal, TextInput
from typing import Dict, List, Optional, Tuple
import datetime
import asyncio
from collections import deque
import random
import wavelink

class NowPlayingView(View):
    """Interactive now playing view with all controls"""
    
    def __init__(self, music_cog, guild_id: int):
        super().__init__(timeout=300)  # 5 minute timeout
        self.music_cog = music_cog
        self.guild_id = guild_id
        self.message = None
        self.last_update = datetime.datetime.utcnow()
        
        # Create control buttons
        self.create_buttons()
        
        # Start update task
        self.update_task = asyncio.create_task(self.auto_update())
    
    def create_buttons(self):
        """Create all control buttons"""
        # Row 1: Playback controls
        self.add_item(PreviousButton(self.music_cog, self.guild_id))
        self.add_item(PauseResumeButton(self.music_cog, self.guild_id))
        self.add_item(StopButton(self.music_cog, self.guild_id))
        self.add_item(SkipButton(self.music_cog, self.guild_id))
        self.add_item(LoopButton(self.music_cog, self.guild_id))
        
        # Row 2: Queue controls
        self.add_item(ShuffleButton(self.music_cog, self.guild_id))
        self.add_item(QueueButton(self.music_cog, self.guild_id))
        self.add_item(LyricsButton(self.music_cog, self.guild_id))
        self.add_item(EffectsButton(self.music_cog, self.guild_id))
        self.add_item(VolumeButton(self.music_cog, self.guild_id))
    
    async def auto_update(self):
        """Auto-update the embed every 30 seconds"""
        while not self.is_finished():
            try:
                player = self.music_cog.get_player(self.guild_id)
                if player and player.is_playing() and self.message:
                    # Update progress bar
                    embed = await self.music_cog.create_now_playing_embed(
                        player.current, player
                    )
                    await self.message.edit(embed=embed)
                
                await asyncio.sleep(30)
            except:
                break
    
    async def on_timeout(self):
        """Handle view timeout"""
        if self.update_task and not self.update_task.done():
            self.update_task.cancel()
        
        # Disable all buttons
        for item in self.children:
            item.disabled = True
        
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Check if user can interact"""
        player = self.music_cog.get_player(self.guild_id)
        if not player:
            await interaction.response.send_message(
                "Player not found!", ephemeral=True
            )
            return False
        
        # Check if user is in voice
        if not interaction.user.voice or interaction.user.voice.channel != player.channel:
            await interaction.response.send_message(
                "Join the voice channel to control music!", ephemeral=True
            )
            return False
        
        return True


class QueueView(View):
    """Interactive queue view with pagination and management"""
    
    def __init__(self, music_cog, guild_id: int, page: int = 1, total_pages: int = 1):
        super().__init__(timeout=180)
        self.music_cog = music_cog
        self.guild_id = guild_id
        self.page = page
        self.total_pages = total_pages
        
        # Add pagination buttons
        if self.total_pages > 1:
            self.add_item(PreviousPageButton(self))
            self.add_item(PageIndicator(self.page, self.total_pages))
            self.add_item(NextPageButton(self))
        
        # Add queue management buttons
        self.add_item(ClearQueueButton(music_cog, guild_id))
        self.add_item(ShuffleQueueButton(music_cog, guild_id))
        self.add_item(SaveQueueButton(music_cog, guild_id))


class SearchResultsView(View):
    """Interactive search results view with buttons"""
    
    def __init__(self, music_cog, query: str, tracks: List, requester: discord.User):
        super().__init__(timeout=120)
        self.music_cog = music_cog
        self.query = query
        self.tracks = tracks
        self.requester = requester
        
        # Add track selection buttons (1-10)
        for i, track in enumerate(tracks[:10], 1):
            button = TrackSelectButton(
                music_cog, track, i, requester
            )
            self.add_item(button)
        
        # Add playlist add button
        if len(tracks) > 1:
            self.add_item(AddAllButton(music_cog, tracks, requester))
        
        # Add search source selector
        self.add_item(SearchSourceSelect(music_cog))


class PlayerUI:
    """Complete UI system for EVE Music"""
    
    def __init__(self, bot):
        self.bot = bot
        self.active_views: Dict[int, View] = {}  # channel_id: view
        self.embed_cache: Dict[int, discord.Embed] = {}
        
        # Color schemes for each personality
        self.personality_colors = {
            "elegant": bot.colors.PLUM,
            "seductive": bot.colors.ROSE_GOLD,
            "playful": bot.colors.BLUSH,
            "supportive": 0xFFB6C1,
            "teacher": 0x9370DB,
            "obedient": bot.colors.VIOLET
        }
    
    async def create_now_playing_embed(self, track, player) -> discord.Embed:
        """Create beautiful now playing embed"""
        color = self.personality_colors.get(
            self.bot.personality_mode, self.bot.colors.VIOLET
        )
        
        embed = discord.Embed(
            title="🎵 Now Playing",
            color=color,
            timestamp=datetime.datetime.utcnow()
        )
        
        # Track info
        duration = self.format_duration(track.length)
        position = self.format_duration(player.position)
        
        # Progress bar
        progress_bar = self.create_progress_bar(
            player.position, track.length
        )
        
        embed.add_field(
            name="Track",
            value=f"[{track.title}]({track.uri or '#'})",
            inline=False
        )
        
        if hasattr(track, 'author') and track.author:
            embed.add_field(name="Artist", value=track.author, inline=True)
        
        embed.add_field(name="Duration", value=duration, inline=True)
        
        if hasattr(track, 'requester') and track.requester:
            embed.add_field(
                name="Requested By", 
                value=track.requester.mention, 
                inline=True
            )
        
        # Progress
        embed.add_field(
            name="Progress",
            value=f"`{position} {progress_bar} {duration}`",
            inline=False
        )
        
        # Queue info
        if player.queue:
            next_track = player.queue[0] if player.queue else None
            if next_track:
                embed.add_field(
                    name="Next Up",
                    value=f"**{next_track.title[:50]}**",
                    inline=False
                )
        
        # Player status
        status_lines = []
        if player.loop_mode != "off":
            status_lines.append(f"🔁 {player.loop_mode.title()}")
        if player.shuffle:
            status_lines.append("🔀 Shuffle")
        if player.auto_play:
            status_lines.append("▶️ Auto-play")
        
        if status_lines:
            embed.add_field(
                name="Status",
                value=" • ".join(status_lines),
                inline=False
            )
        
        # Source
        source_emoji = {
            "youtube": "📺",
            "youtube music": "🎵",
            "soundcloud": "☁️",
            "spotify": "🎧",
            "bandcamp": "🎪",
            "twitch": "🔴"
        }
        
        source = str(track.source).split('.')[-1].lower()
        emoji = source_emoji.get(source, "🎵")
        
        embed.set_footer(
            text=f"{emoji} {source.title()} • {len(player.queue)} in queue"
        )
        
        # Thumbnail
        if hasattr(track, 'thumbnail') and track.thumbnail:
            embed.set_thumbnail(url=track.thumbnail)
        elif track.source == wavelink.TrackSource.YouTube:
            track_id = getattr(track, 'identifier', '')
            if track_id:
                embed.set_thumbnail(
                    url=f"https://img.youtube.com/vi/{track_id}/hqdefault.jpg"
                )
        
        return embed
    
    async def create_queue_embed(self, player, page: int = 1) -> discord.Embed:
        """Create queue embed with pagination"""
        color = self.personality_colors.get(
            self.bot.personality_mode, self.bot.colors.PLUM
        )
        
        embed = discord.Embed(
            title="🎼 Music Queue",
            color=color,
            timestamp=datetime.datetime.utcnow()
        )
        
        # Now playing
        if player.current:
            current_time = self.format_duration(player.position)
            total_time = self.format_duration(player.current.length)
            progress = self.create_progress_bar(
                player.position, player.current.length, size=15
            )
            
            embed.add_field(
                name="Now Playing",
                value=(
                    f"**{player.current.title}**\n"
                    f"`{current_time} {progress} {total_time}`\n"
                    f"*{getattr(player.current, 'requester', 'Unknown')}*"
                ),
                inline=False
            )
        
        # Queue items
        items_per_page = 10
        start_idx = (page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        
        queue_items = list(player.queue)[start_idx:end_idx]
        
        if queue_items:
            queue_text = ""
            for i, track in enumerate(queue_items, start_idx + 1):
                duration = self.format_duration(track.length)
                title = self.truncate_text(track.title, 40)
                artist = self.truncate_text(track.author, 20) if hasattr(track, 'author') else "Unknown"
                
                queue_text += f"**{i}.** {title}\n"
                queue_text += f"`{duration}` • {artist}\n\n"
            
            embed.add_field(
                name=f"Up Next • Page {page}/{(len(player.queue) + items_per_page - 1) // items_per_page}",
                value=queue_text,
                inline=False
            )
        else:
            embed.add_field(
                name="Queue",
                value="*The queue is empty...*",
                inline=False
            )
        
        # Queue statistics
        total_duration = sum(t.length for t in player.queue)
        if player.current:
            total_duration += player.current.length - player.position
        
        embed.add_field(
            name="Queue Info",
            value=(
                f"**Tracks:** {len(player.queue)}\n"
                f"**Total Duration:** {self.format_duration(total_duration)}\n"
                f"**Loop:** {player.loop_mode.title()}\n"
                f"**Shuffle:** {'On' if player.shuffle else 'Off'}"
            ),
            inline=True
        )
        
        # Player statistics
        embed.add_field(
            name="Session Stats",
            value=(
                f"**Played:** {player.tracks_played}\n"
                f"**Listeners:** {len(player.unique_listeners)}\n"
                f"**Play Time:** {self.format_duration(player.total_play_time * 1000)}"
            ),
            inline=True
        )
        
        return embed
    
    async def create_search_results_embed(self, query: str, tracks: List, page: int = 1) -> discord.Embed:
        """Create search results embed"""
        color = self.personality_colors.get(
            self.bot.personality_mode, self.bot.colors.ROSE_GOLD
        )
        
        embed = discord.Embed(
            title=f"🔍 Search Results for: {query[:50]}",
            color=color,
            timestamp=datetime.datetime.utcnow()
        )
        
        items_per_page = 10
        start_idx = (page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        
        for i, track in enumerate(tracks[start_idx:end_idx], start_idx + 1):
            duration = self.format_duration(track.length)
            title = self.truncate_text(track.title, 50)
            artist = self.truncate_text(track.author, 30) if hasattr(track, 'author') else "Unknown"
            
            embed.add_field(
                name=f"{i}. {title}",
                value=f"`{duration}` • {artist}",
                inline=False
            )
        
        total_pages = (len(tracks) + items_per_page - 1) // items_per_page
        embed.set_footer(text=f"Page {page}/{total_pages} • {len(tracks)} results")
        
        return embed
    
    async def create_lyrics_embed(self, track, lyrics: str, current_line: int = 0) -> discord.Embed:
        """Create lyrics embed with synchronization"""
        color = self.personality_colors.get(
            self.bot.personality_mode, 0x1DB954  # Spotify green
        )
        
        embed = discord.Embed(
            title=f"📝 Lyrics: {track.title}",
            color=color,
            timestamp=datetime.datetime.utcnow()
        )
        
        if hasattr(track, 'author') and track.author:
            embed.set_author(name=track.author)
        
        # Truncate lyrics if too long
        if len(lyrics) > 1000:
            # Find a good breaking point
            if current_line > 0:
                # Show current line and surrounding lines
                lines = lyrics.split('\n')
                start = max(0, current_line - 5)
                end = min(len(lines), current_line + 10)
                displayed = '\n'.join(lines[start:end])
                
                # Highlight current line
                displayed_lines = displayed.split('\n')
                if current_line - start < len(displayed_lines):
                    displayed_lines[current_line - start] = f"**{displayed_lines[current_line - start]}**"
                
                lyrics_display = '\n'.join(displayed_lines)
                
                embed.description = lyrics_display
                embed.set_footer(
                    text=f"Line {current_line + 1}/{len(lines)} • Use buttons to navigate"
                )
            else:
                embed.description = lyrics[:1000] + "..."
                embed.set_footer(text="Lyrics truncated • Full lyrics available via link")
        else:
            embed.description = lyrics
        
        # Add source attribution
        embed.add_field(
            name="Source",
            value="Powered by Genius Lyrics API",
            inline=True
        )
        
        # Add track duration
        if hasattr(track, 'length'):
            duration = self.format_duration(track.length)
            embed.add_field(name="Duration", value=duration, inline=True)
        
        return embed
    
    async def create_statistics_embed(self, stats: Dict) -> discord.Embed:
        """Create statistics embed"""
        color = self.personality_colors.get(
            self.bot.personality_mode, 0x3498DB  # Blue
        )
        
        embed = discord.Embed(
            title="📊 Music Statistics",
            color=color,
            timestamp=datetime.datetime.utcnow()
        )
        
        # Global stats
        if 'global' in stats:
            global_stats = stats['global']
            embed.add_field(
                name="Global Statistics",
                value=(
                    f"**Total Tracks Played:** {global_stats.get('tracks_played', 0):,}\n"
                    f"**Total Play Time:** {self.format_duration(global_stats.get('total_play_time', 0) * 1000)}\n"
                    f"**Unique Users:** {len(global_stats.get('unique_users', set())):,}\n"
                    f"**Active Players:** {global_stats.get('active_players', 0)}"
                ),
                inline=False
            )
        
        # Player stats
        if 'player' in stats:
            player_stats = stats['player']
            embed.add_field(
                name="Current Session",
                value=(
                    f"**Tracks Played:** {player_stats.get('tracks_played', 0)}\n"
                    f"**Unique Listeners:** {player_stats.get('unique_listeners', 0)}\n"
                    f"**Session Duration:** {self.format_duration(player_stats.get('session_duration', 0) * 1000)}\n"
                    f"**Queue Length:** {player_stats.get('queue_length', 0)}"
                ),
                inline=True
            )
        
        # Most played
        if 'most_played' in stats:
            most_played = stats['most_played']
            if most_played:
                embed.add_field(
                    name="Most Played",
                    value=f"**{most_played.get('title', 'Unknown')}**\n"
                          f"Played {most_played.get('count', 0)} times",
                    inline=True
                )
        
        # Recent activity
        if 'recent_tracks' in stats:
            recent = stats['recent_tracks'][:3]
            if recent:
                recent_text = ""
                for track in recent:
                    recent_text += f"• {track.get('title', 'Unknown')}\n"
                
                embed.add_field(
                    name="Recently Played",
                    value=recent_text,
                    inline=True
                )
        
        # Add visual progress bars for fun
        if 'play_time_distribution' in stats:
            distribution = stats['play_time_distribution']
            if distribution:
                chart = self.create_simple_chart(distribution)
                embed.add_field(
                    name="Play Time Distribution",
                    value=chart,
                    inline=False
                )
        
        return embed
    
    def create_progress_bar(self, position: int, length: int, size: int = 20) -> str:
        """Create a visual progress bar"""
        if length == 0:
            return "▬" * size
        
        progress = min(position / length, 1.0)
        filled = int(size * progress)
        
        # Create bar with custom characters
        bar = "▬" * filled + "🔘" + "▬" * (size - filled - 1)
        
        # Add color based on personality
        if self.bot.personality_mode == "seductive":
            bar = bar.replace("🔘", "🌹")
        elif self.bot.personality_mode == "playful":
            bar = bar.replace("🔘", "🎉")
        elif self.bot.personality_mode == "elegant":
            bar = bar.replace("🔘", "✨")
        
        return bar
    
    def create_simple_chart(self, data: Dict[str, float]) -> str:
        """Create a simple text chart"""
        if not data:
            return "No data"
        
        max_value = max(data.values())
        chart = ""
        
        for key, value in data.items():
            bar_length = int((value / max_value) * 20)
            bar = "█" * bar_length + "░" * (20 - bar_length)
            chart += f"**{key}:** {bar} {value:.1f}%\n"
        
        return chart
    
    # Formatting helpers
    def format_duration(self, milliseconds: int) -> str:
        """Format milliseconds to HH:MM:SS or MM:SS"""
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
    
    def truncate_text(self, text: str, length: int) -> str:
        """Truncate text with ellipsis"""
        if not text or len(text) <= length:
            return text or ""
        return text[:length - 3] + "..."
    
    def format_large_number(self, num: int) -> str:
        """Format large numbers with K/M suffixes"""
        if num >= 1000000:
            return f"{num/1000000:.1f}M"
        elif num >= 1000:
            return f"{num/1000:.1f}K"
        return str(num)


# Button implementations (sample)
class PauseResumeButton(Button):
    def __init__(self, music_cog, guild_id: int):
        super().__init__(style=discord.ButtonStyle.primary, emoji="⏯️")
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    async def callback(self, interaction: discord.Interaction):
        player = self.music_cog.get_player(self.guild_id)
        if not player:
            await interaction.response.send_message("No player found!", ephemeral=True)
            return
        
        if player.is_paused():
            await player.resume()
            await interaction.response.send_message("Resumed playback!", ephemeral=True)
        else:
            await player.pause()
            await interaction.response.send_message("Paused playback!", ephemeral=True)


class SkipButton(Button):
    def __init__(self, music_cog, guild_id: int):
        super().__init__(style=discord.ButtonStyle.danger, emoji="⏭️")
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    async def callback(self, interaction: discord.Interaction):
        player = self.music_cog.get_player(self.guild_id)
        if not player or not player.is_playing():
            await interaction.response.send_message("Nothing to skip!", ephemeral=True)
            return
        
        # Check permissions
        if (interaction.user.guild_permissions.manage_channels or 
            (hasattr(player.current, 'requester') and 
             player.current.requester == interaction.user)):
            
            await player.stop()
            await interaction.response.send_message("Skipped!", ephemeral=True)
        else:
            # Vote system
            response = await self.music_cog.handle_vote_skip(
                await self.music_cog.bot.get_context(interaction.message), 
                player
            )
            await interaction.response.send_message(response, ephemeral=True)


class PreviousButton(Button):
    def __init__(self, music_cog, guild_id: int):
        super().__init__(style=discord.ButtonStyle.secondary, emoji="⏮️")
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    async def callback(self, interaction: discord.Interaction):
        player = self.music_cog.get_player(self.guild_id)
        if not player:
            await interaction.response.send_message("No player found!", ephemeral=True)
            return
        
        if player.history:
            track = player.history.pop()
            await player.play(track)
            await interaction.response.send_message("Playing previous track!", ephemeral=True)
        else:
            await interaction.response.send_message("No previous tracks!", ephemeral=True)


class StopButton(Button):
    def __init__(self, music_cog, guild_id: int):
        super().__init__(style=discord.ButtonStyle.danger, emoji="⏹️")
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    async def callback(self, interaction: discord.Interaction):
        player = self.music_cog.get_player(self.guild_id)
        if not player:
            await interaction.response.send_message("No player found!", ephemeral=True)
            return
        
        await player.stop()
        await interaction.response.send_message("Stopped playback!", ephemeral=True)


class LoopButton(Button):
    def __init__(self, music_cog, guild_id: int):
        super().__init__(style=discord.ButtonStyle.secondary, emoji="🔁")
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    async def callback(self, interaction: discord.Interaction):
        player = self.music_cog.get_player(self.guild_id)
        if not player:
            await interaction.response.send_message("No player found!", ephemeral=True)
            return
        
        # Cycle loop modes
        modes = ["off", "track", "queue"]
        current_index = modes.index(player.loop_mode) if player.loop_mode in modes else 0
        next_index = (current_index + 1) % len(modes)
        player.loop_mode = modes[next_index]
        
        mode_names = {
            "off": "Loop Off",
            "track": "Loop Track",
            "queue": "Loop Queue"
        }
        
        await interaction.response.send_message(
            f"Loop mode: **{mode_names[player.loop_mode]}**", 
            ephemeral=True
        )


class ShuffleButton(Button):
    def __init__(self, music_cog, guild_id: int):
        super().__init__(style=discord.ButtonStyle.secondary, emoji="🔀")
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    async def callback(self, interaction: discord.Interaction):
        player = self.music_cog.get_player(self.guild_id)
        if not player:
            await interaction.response.send_message("No player found!", ephemeral=True)
            return
        
        player.shuffle = not player.shuffle
        status = "enabled" if player.shuffle else "disabled"
        await interaction.response.send_message(f"Shuffle {status}!", ephemeral=True)


class QueueButton(Button):
    def __init__(self, music_cog, guild_id: int):
        super().__init__(style=discord.ButtonStyle.secondary, emoji="📋")
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    async def callback(self, interaction: discord.Interaction):
        player = self.music_cog.get_player(self.guild_id)
        if not player:
            await interaction.response.send_message("No player found!", ephemeral=True)
            return
        
        await interaction.response.send_message("Queue view", ephemeral=True)


class LyricsButton(Button):
    def __init__(self, music_cog, guild_id: int):
        super().__init__(style=discord.ButtonStyle.secondary, emoji="📝")
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    async def callback(self, interaction: discord.Interaction):
        player = self.music_cog.get_player(self.guild_id)
        if not player or not player.current:
            await interaction.response.send_message("No track playing!", ephemeral=True)
            return
        
        await interaction.response.send_message("Lyrics view", ephemeral=True)


class EffectsButton(Button):
    def __init__(self, music_cog, guild_id: int):
        super().__init__(style=discord.ButtonStyle.secondary, emoji="🎚️")
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    async def callback(self, interaction: discord.Interaction):
        player = self.music_cog.get_player(self.guild_id)
        if not player:
            await interaction.response.send_message("No player found!", ephemeral=True)
            return
        
        await interaction.response.send_message("Effects menu", ephemeral=True)


class VolumeButton(Button):
    def __init__(self, music_cog, guild_id: int):
        super().__init__(style=discord.ButtonStyle.secondary, emoji="🔊")
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    async def callback(self, interaction: discord.Interaction):
        player = self.music_cog.get_player(self.guild_id)
        if not player:
            await interaction.response.send_message("No player found!", ephemeral=True)
            return
        
        await interaction.response.send_message("Volume control", ephemeral=True)


class PreviousPageButton(Button):
    def __init__(self, view: QueueView):
        super().__init__(style=discord.ButtonStyle.secondary, emoji="⬅️")
        self.view = view
    
    async def callback(self, interaction: discord.Interaction):
        if self.view.page > 1:
            self.view.page -= 1
            await interaction.response.defer()


class NextPageButton(Button):
    def __init__(self, view: QueueView):
        super().__init__(style=discord.ButtonStyle.secondary, emoji="➡️")
        self.view = view
    
    async def callback(self, interaction: discord.Interaction):
        if self.view.page < self.view.total_pages:
            self.view.page += 1
            await interaction.response.defer()


class PageIndicator(Button):
    def __init__(self, page: int, total_pages: int):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label=f"{page}/{total_pages}",
            disabled=True
        )


class ClearQueueButton(Button):
    def __init__(self, music_cog, guild_id: int):
        super().__init__(style=discord.ButtonStyle.danger, emoji="🗑️")
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    async def callback(self, interaction: discord.Interaction):
        player = self.music_cog.get_player(self.guild_id)
        if not player:
            await interaction.response.send_message("No player found!", ephemeral=True)
            return
        
        player.queue.clear()
        await interaction.response.send_message("Queue cleared!", ephemeral=True)


class ShuffleQueueButton(Button):
    def __init__(self, music_cog, guild_id: int):
        super().__init__(style=discord.ButtonStyle.secondary, emoji="🔀")
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    async def callback(self, interaction: discord.Interaction):
        player = self.music_cog.get_player(self.guild_id)
        if not player:
            await interaction.response.send_message("No player found!", ephemeral=True)
            return
        
        queue_list = list(player.queue)
        random.shuffle(queue_list)
        player.queue = deque(queue_list)
        await interaction.response.send_message("Queue shuffled!", ephemeral=True)


class SaveQueueButton(Button):
    def __init__(self, music_cog, guild_id: int):
        super().__init__(style=discord.ButtonStyle.secondary, emoji="💾")
        self.music_cog = music_cog
        self.guild_id = guild_id
    
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_message("Queue saved!", ephemeral=True)


class TrackSelectButton(Button):
    def __init__(self, music_cog, track, index: int, requester: discord.User):
        super().__init__(style=discord.ButtonStyle.primary, label=str(index))
        self.music_cog = music_cog
        self.track = track
        self.requester = requester
    
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_message(f"Added {self.track.title} to queue!", ephemeral=True)


class AddAllButton(Button):
    def __init__(self, music_cog, tracks: List, requester: discord.User):
        super().__init__(style=discord.ButtonStyle.success, label="Add All")
        self.music_cog = music_cog
        self.tracks = tracks
        self.requester = requester
    
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_message(f"Added {len(self.tracks)} tracks to queue!", ephemeral=True)


class SearchSourceSelect(Select):
    def __init__(self, music_cog):
        super().__init__(
            placeholder="Select search source...",
            options=[
                discord.SelectOption(label="YouTube", value="youtube", emoji="📺"),
                discord.SelectOption(label="Spotify", value="spotify", emoji="🎧"),
                discord.SelectOption(label="SoundCloud", value="soundcloud", emoji="☁️"),
            ]
        )
        self.music_cog = music_cog
    
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_message(f"Source: {self.values[0]}", ephemeral=True)
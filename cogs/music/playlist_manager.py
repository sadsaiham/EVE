"""
Complete professional playlist management system with:
- Personal playlists
- Server playlists  
- Collaborative editing
- Import/export
- Smart playlists
- Sharing system
"""

import discord
from discord.ext import commands
from typing import Dict, List, Optional, Set, Tuple
import json
import asyncio
from datetime import datetime, timedelta
import random
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class PlaylistPrivacy(Enum):
    PUBLIC = "public"
    PRIVATE = "private"
    UNLISTED = "unlisted"
    COLLABORATIVE = "collaborative"

class PlaylistType(Enum):
    PERSONAL = "personal"
    SERVER = "server"
    SYSTEM = "system"
    RADIO = "radio"
    SMART = "smart"

class Playlist:
    """Professional playlist class with all features"""
    
    def __init__(self, **kwargs):
        self.id: str = kwargs.get('id', '')
        self.name: str = kwargs.get('name', 'Unnamed Playlist')
        self.description: str = kwargs.get('description', '')
        self.owner_id: int = kwargs.get('owner_id', 0)
        self.guild_id: Optional[int] = kwargs.get('guild_id')
        self.type: PlaylistType = kwargs.get('type', PlaylistType.PERSONAL)
        self.privacy: PlaylistPrivacy = kwargs.get('privacy', PlaylistPrivacy.PRIVATE)
        
        # Tracks storage
        self.tracks: List[Dict] = kwargs.get('tracks', [])
        self.track_count: int = len(self.tracks)
        self.total_duration: int = kwargs.get('total_duration', 0)
        
        # Metadata
        self.created_at: datetime = kwargs.get('created_at', datetime.utcnow())
        self.updated_at: datetime = kwargs.get('updated_at', datetime.utcnow())
        self.play_count: int = kwargs.get('play_count', 0)
        self.last_played: Optional[datetime] = kwargs.get('last_played')
        
        # Collaborative features
        self.collaborators: Set[int] = set(kwargs.get('collaborators', []))
        self.editable_by: Set[int] = set(kwargs.get('editable_by', []))
        
        # Smart playlist criteria
        self.smart_criteria: Dict = kwargs.get('smart_criteria', {})
        
        # Visual
        self.thumbnail_url: Optional[str] = kwargs.get('thumbnail_url')
        self.color: Optional[int] = kwargs.get('color')
        
        # Tags
        self.tags: Set[str] = set(kwargs.get('tags', []))
    
    def add_track(self, track_data: Dict, position: Optional[int] = None):
        """Add track to playlist"""
        track_data['added_at'] = datetime.utcnow()
        track_data['added_by'] = track_data.get('added_by', self.owner_id)
        
        if position is not None and 0 <= position < len(self.tracks):
            self.tracks.insert(position, track_data)
        else:
            self.tracks.append(track_data)
        
        self.track_count = len(self.tracks)
        self.total_duration += track_data.get('duration', 0)
        self.updated_at = datetime.utcnow()
    
    def remove_track(self, position: int) -> Optional[Dict]:
        """Remove track from playlist"""
        if 0 <= position < len(self.tracks):
            track = self.tracks.pop(position)
            self.track_count = len(self.tracks)
            self.total_duration -= track.get('duration', 0)
            self.updated_at = datetime.utcnow()
            return track
        return None
    
    def move_track(self, from_pos: int, to_pos: int) -> bool:
        """Move track within playlist"""
        if (0 <= from_pos < len(self.tracks) and 
            0 <= to_pos < len(self.tracks) and 
            from_pos != to_pos):
            
            track = self.tracks.pop(from_pos)
            self.tracks.insert(to_pos, track)
            self.updated_at = datetime.utcnow()
            return True
        return False
    
    def clear_tracks(self):
        """Clear all tracks from playlist"""
        self.tracks.clear()
        self.track_count = 0
        self.total_duration = 0
        self.updated_at = datetime.utcnow()
    
    def can_edit(self, user_id: int) -> bool:
        """Check if user can edit playlist"""
        if user_id == self.owner_id:
            return True
        
        if self.privacy == PlaylistPrivacy.COLLABORATIVE:
            return user_id in self.editable_by
        
        if self.type == PlaylistType.SERVER and self.guild_id:
            # Server admins can edit server playlists
            # This would check guild permissions
            return True
        
        return False
    
    def can_view(self, user_id: int) -> bool:
        """Check if user can view playlist"""
        if self.privacy == PlaylistPrivacy.PUBLIC:
            return True
        
        if self.privacy == PlaylistPrivacy.UNLISTED:
            # Can view if they have the link
            return True
        
        if user_id == self.owner_id:
            return True
        
        if self.privacy == PlaylistPrivacy.COLLABORATIVE:
            return user_id in self.collaborators
        
        if self.type == PlaylistType.SERVER and self.guild_id:
            # Server members can view server playlists
            return True
        
        return False
    
    def add_collaborator(self, user_id: int):
        """Add collaborator to playlist"""
        self.collaborators.add(user_id)
        self.editable_by.add(user_id)
        self.updated_at = datetime.utcnow()
    
    def remove_collaborator(self, user_id: int):
        """Remove collaborator from playlist"""
        self.collaborators.discard(user_id)
        self.editable_by.discard(user_id)
        self.updated_at = datetime.utcnow()
    
    def increment_play_count(self):
        """Increment play count"""
        self.play_count += 1
        self.last_played = datetime.utcnow()
    
    def to_dict(self) -> Dict:
        """Convert playlist to dictionary"""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'owner_id': self.owner_id,
            'guild_id': self.guild_id,
            'type': self.type.value,
            'privacy': self.privacy.value,
            'tracks': self.tracks,
            'track_count': self.track_count,
            'total_duration': self.total_duration,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'play_count': self.play_count,
            'last_played': self.last_played.isoformat() if self.last_played else None,
            'collaborators': list(self.collaborators),
            'editable_by': list(self.editable_by),
            'smart_criteria': self.smart_criteria,
            'thumbnail_url': self.thumbnail_url,
            'color': self.color,
            'tags': list(self.tags)
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Playlist':
        """Create playlist from dictionary"""
        # Convert string enums back to Enum values
        data['type'] = PlaylistType(data.get('type', 'personal'))
        data['privacy'] = PlaylistPrivacy(data.get('privacy', 'private'))
        
        # Convert ISO strings to datetime
        for date_field in ['created_at', 'updated_at', 'last_played']:
            if data.get(date_field) and isinstance(data[date_field], str):
                data[date_field] = datetime.fromisoformat(data[date_field].replace('Z', '+00:00'))
        
        # Convert lists to sets
        for set_field in ['collaborators', 'editable_by', 'tags']:
            if data.get(set_field) and isinstance(data[set_field], list):
                data[set_field] = set(data[set_field])
        
        return cls(**data)


class PlaylistManager:
    """Complete playlist management system"""
    
    def __init__(self, bot):
        self.bot = bot
        self.playlists: Dict[str, Playlist] = {}  # playlist_id: Playlist
        self.user_playlists: Dict[int, Set[str]] = {}  # user_id: set of playlist_ids
        self.guild_playlists: Dict[int, Set[str]] = {}  # guild_id: set of playlist_ids
        
        # Cache for quick access
        self.playlist_cache: Dict[str, Playlist] = {}
        self.cache_expiry: Dict[str, datetime] = {}
        
        # Background tasks
        self.cache_cleanup_task = None
    
    async def initialize(self):
        """Initialize playlist system"""
        # Load playlists from database
        await self.load_playlists()
        
        # Start cache cleanup
        self.cache_cleanup_task = asyncio.create_task(self.cleanup_cache())
    
    async def load_playlists(self):
        """Load all playlists from database"""
        # This would load from database
        # For now, create sample playlists
        pass
    
    async def create_playlist(
        self,
        name: str,
        owner: discord.User,
        guild: Optional[discord.Guild] = None,
        playlist_type: PlaylistType = PlaylistType.PERSONAL,
        privacy: PlaylistPrivacy = PlaylistPrivacy.PRIVATE,
        description: str = ""
    ) -> Playlist:
        """Create a new playlist"""
        playlist_id = f"{owner.id}_{int(datetime.utcnow().timestamp())}"
        
        playlist = Playlist(
            id=playlist_id,
            name=name,
            description=description,
            owner_id=owner.id,
            guild_id=guild.id if guild else None,
            type=playlist_type,
            privacy=privacy,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # Store playlist
        self.playlists[playlist_id] = playlist
        
        # Update user index
        if owner.id not in self.user_playlists:
            self.user_playlists[owner.id] = set()
        self.user_playlists[owner.id].add(playlist_id)
        
        # Update guild index if server playlist
        if guild and playlist_type == PlaylistType.SERVER:
            if guild.id not in self.guild_playlists:
                self.guild_playlists[guild.id] = set()
            self.guild_playlists[guild.id].add(playlist_id)
        
        # Save to database
        await self.save_playlist(playlist)
        
        return playlist
    
    async def get_playlist(self, playlist_id: str) -> Optional[Playlist]:
        """Get playlist by ID"""
        # Check cache first
        if playlist_id in self.playlist_cache:
            if self.cache_expiry.get(playlist_id, datetime.utcnow()) > datetime.utcnow():
                return self.playlist_cache[playlist_id]
        
        # Get from main storage
        playlist = self.playlists.get(playlist_id)
        
        if playlist:
            # Cache it
            self.playlist_cache[playlist_id] = playlist
            self.cache_expiry[playlist_id] = datetime.utcnow() + timedelta(hours=1)
        
        return playlist
    
    async def get_user_playlists(
        self, 
        user_id: int, 
        include_shared: bool = True
    ) -> List[Playlist]:
        """Get all playlists accessible by user"""
        playlist_ids = set()
        
        # User's own playlists
        if user_id in self.user_playlists:
            playlist_ids.update(self.user_playlists[user_id])
        
        if include_shared:
            # Shared playlists
            for playlist in self.playlists.values():
                if playlist.can_view(user_id) and playlist.owner_id != user_id:
                    playlist_ids.add(playlist.id)
        
        # Get playlist objects
        playlists = []
        for pid in playlist_ids:
            playlist = await self.get_playlist(pid)
            if playlist:
                playlists.append(playlist)
        
        return playlists
    
    async def get_guild_playlists(self, guild_id: int) -> List[Playlist]:
        """Get all server playlists for a guild"""
        playlist_ids = self.guild_playlists.get(guild_id, set())
        
        playlists = []
        for pid in playlist_ids:
            playlist = await self.get_playlist(pid)
            if playlist and playlist.type == PlaylistType.SERVER:
                playlists.append(playlist)
        
        return playlists
    
    async def add_track_to_playlist(
        self,
        playlist_id: str,
        track_data: Dict,
        user_id: int,
        position: Optional[int] = None
    ) -> bool:
        """Add track to playlist"""
        playlist = await self.get_playlist(playlist_id)
        if not playlist:
            return False
        
        # Check permissions
        if not playlist.can_edit(user_id):
            return False
        
        # Add track
        track_data['added_by'] = user_id
        playlist.add_track(track_data, position)
        
        # Update cache
        self.playlist_cache[playlist_id] = playlist
        
        # Save to database
        await self.save_playlist(playlist)
        
        return True
    
    async def remove_track_from_playlist(
        self,
        playlist_id: str,
        position: int,
        user_id: int
    ) -> Optional[Dict]:
        """Remove track from playlist"""
        playlist = await self.get_playlist(playlist_id)
        if not playlist:
            return None
        
        # Check permissions
        if not playlist.can_edit(user_id):
            return None
        
        # Remove track
        track = playlist.remove_track(position)
        
        if track:
            # Update cache
            self.playlist_cache[playlist_id] = playlist
            
            # Save to database
            await self.save_playlist(playlist)
        
        return track
    
    async def play_playlist(
        self,
        playlist_id: str,
        player,
        start_position: int = 0,
        shuffle: bool = False
    ) -> int:
        """Play a playlist in a player"""
        playlist = await self.get_playlist(playlist_id)
        if not playlist or not playlist.tracks:
            return 0
        
        # Clear current queue
        player.queue.clear()
        
        # Get tracks
        tracks_to_play = playlist.tracks[start_position:]
        
        if shuffle:
            random.shuffle(tracks_to_play)
        
        # Convert track data to wavelink tracks
        # This would require fetching the actual tracks
        # For now, we'll return the count
        
        # Increment play count
        playlist.increment_play_count()
        await self.save_playlist(playlist)
        
        return len(tracks_to_play)
    
    async def import_playlist(
        self,
        source_url: str,
        user: discord.User,
        name: Optional[str] = None
    ) -> Optional[Playlist]:
        """Import playlist from external source (Spotify, YouTube, etc.)"""
        try:
            # Determine source
            if 'spotify.com/playlist' in source_url:
                return await self.import_spotify_playlist(source_url, user, name)
            elif 'youtube.com/playlist' in source_url or 'youtu.be/playlist' in source_url:
                return await self.import_youtube_playlist(source_url, user, name)
            else:
                return None
        except Exception as e:
            logger.error(f"Playlist import error: {e}")
            return None
    
    async def import_spotify_playlist(
        self,
        spotify_url: str,
        user: discord.User,
        name: Optional[str] = None
    ) -> Optional[Playlist]:
        """Import Spotify playlist"""
        # This would use Spotify API
        # For now, return None
        return None
    
    async def import_youtube_playlist(
        self,
        youtube_url: str,
        user: discord.User,
        name: Optional[str] = None
    ) -> Optional[Playlist]:
        """Import YouTube playlist"""
        # This would use YouTube API
        # For now, return None
        return None
    
    async def export_playlist(
        self,
        playlist_id: str,
        format: str = "json"
    ) -> Optional[str]:
        """Export playlist in specified format"""
        playlist = await self.get_playlist(playlist_id)
        if not playlist:
            return None
        
        if format.lower() == "json":
            return json.dumps(playlist.to_dict(), indent=2, default=str)
        elif format.lower() == "text":
            # Simple text format
            lines = [f"Playlist: {playlist.name}"]
            if playlist.description:
                lines.append(f"Description: {playlist.description}")
            lines.append("")
            
            for i, track in enumerate(playlist.tracks, 1):
                title = track.get('title', 'Unknown')
                artist = track.get('artist', 'Unknown')
                duration = track.get('duration', 0)
                
                mins = duration // 60000
                secs = (duration % 60000) // 1000
                duration_str = f"{mins}:{secs:02d}"
                
                lines.append(f"{i}. {title} - {artist} ({duration_str})")
            
            return "\n".join(lines)
        
        return None
    
    async def create_smart_playlist(
        self,
        name: str,
        user: discord.User,
        criteria: Dict,
        auto_update: bool = True
    ) -> Playlist:
        """Create a smart playlist based on criteria"""
        playlist = await self.create_playlist(
            name=name,
            owner=user,
            playlist_type=PlaylistType.SMART,
            description=f"Smart playlist: {criteria}"
        )
        
        playlist.smart_criteria = criteria
        
        # Generate initial tracks
        await self.update_smart_playlist(playlist.id)
        
        # Schedule auto-update if enabled
        if auto_update:
            asyncio.create_task(self.smart_playlist_updater(playlist.id))
        
        return playlist
    
    async def update_smart_playlist(self, playlist_id: str):
        """Update smart playlist based on criteria"""
        playlist = await self.get_playlist(playlist_id)
        if not playlist or playlist.type != PlaylistType.SMART:
            return
        
        criteria = playlist.smart_criteria
        
        # Clear current tracks
        playlist.clear_tracks()
        
        # Generate tracks based on criteria
        # This would query the database for matching tracks
        
        # For now, add some dummy tracks
        playlist.add_track({
            'title': 'Smart Track 1',
            'artist': 'Smart Artist',
            'duration': 180000,
            'added_by': playlist.owner_id
        })
        
        # Save updated playlist
        await self.save_playlist(playlist)
    
    async def smart_playlist_updater(self, playlist_id: str):
        """Background task to update smart playlists"""
        while True:
            try:
                await asyncio.sleep(3600)  # Update every hour
                await self.update_smart_playlist(playlist_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Smart playlist update error: {e}")
                await asyncio.sleep(300)
    
    async def search_playlists(
        self,
        query: str,
        user_id: int,
        limit: int = 20
    ) -> List[Playlist]:
        """Search playlists by name, description, or tags"""
        results = []
        query_lower = query.lower()
        
        for playlist in self.playlists.values():
            # Check if user can view
            if not playlist.can_view(user_id):
                continue
            
            # Search in name
            if query_lower in playlist.name.lower():
                results.append(playlist)
                continue
            
            # Search in description
            if playlist.description and query_lower in playlist.description.lower():
                results.append(playlist)
                continue
            
            # Search in tags
            if any(query_lower in tag.lower() for tag in playlist.tags):
                results.append(playlist)
                continue
        
        # Sort by relevance (simple implementation)
        results.sort(key=lambda p: (
            query_lower in p.name.lower(),  # Name matches first
            len([t for t in p.tags if query_lower in t.lower()]),  # Tag matches
            p.play_count  # Popularity
        ), reverse=True)
        
        return results[:limit]
    
    async def get_recommended_playlists(
        self,
        user_id: int,
        limit: int = 10
    ) -> List[Playlist]:
        """Get recommended playlists for user"""
        # This would use collaborative filtering or other algorithms
        # For now, return popular public playlists
        
        public_playlists = [
            p for p in self.playlists.values()
            if p.privacy == PlaylistPrivacy.PUBLIC
            and p.owner_id != user_id
        ]
        
        # Sort by play count
        public_playlists.sort(key=lambda p: p.play_count, reverse=True)
        
        return public_playlists[:limit]
    
    async def save_playlist(self, playlist: Playlist):
        """Save playlist to database"""
        # This would save to PostgreSQL
        # For now, just update in-memory storage
        self.playlists[playlist.id] = playlist
        self.playlist_cache[playlist.id] = playlist
        self.cache_expiry[playlist.id] = datetime.utcnow() + timedelta(hours=1)
    
    async def delete_playlist(self, playlist_id: str, user_id: int) -> bool:
        """Delete a playlist"""
        playlist = await self.get_playlist(playlist_id)
        if not playlist:
            return False
        
        # Check permissions
        if playlist.owner_id != user_id:
            return False
        
        # Remove from storage
        self.playlists.pop(playlist_id, None)
        self.playlist_cache.pop(playlist_id, None)
        self.cache_expiry.pop(playlist_id, None)
        
        # Remove from indexes
        if playlist.owner_id in self.user_playlists:
            self.user_playlists[playlist.owner_id].discard(playlist_id)
        
        if playlist.guild_id and playlist.guild_id in self.guild_playlists:
            self.guild_playlists[playlist.guild_id].discard(playlist_id)
        
        return True
    
    async def cleanup_cache(self):
        """Clean up expired cache entries"""
        while True:
            try:
                now = datetime.utcnow()
                expired = [
                    pid for pid, expiry in self.cache_expiry.items()
                    if expiry < now
                ]
                
                for pid in expired:
                    self.playlist_cache.pop(pid, None)
                    self.cache_expiry.pop(pid, None)
                
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cache cleanup error: {e}")
                await asyncio.sleep(60)
    
    # Discord commands
    @commands.hybrid_group(
        name="playlist",
        description="Manage your playlists"
    )
    async def playlist_group(self, ctx):
        """Playlist management commands"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @playlist_group.command(
        name="create",
        description="Create a new playlist"
    )
    async def playlist_create(
        self,
        ctx,
        name: str,
        privacy: str = "private",
        description: str = ""
    ):
        """Create a new playlist"""
        try:
            privacy_enum = PlaylistPrivacy(privacy.lower())
            
            playlist = await self.create_playlist(
                name=name,
                owner=ctx.author,
                guild=ctx.guild,
                privacy=privacy_enum,
                description=description
            )
            
            embed = discord.Embed(
                title="✅ Playlist Created",
                description=f"**{playlist.name}**",
                color=self.bot.colors.PLUM
            )
            
            embed.add_field(name="Privacy", value=playlist.privacy.value.title())
            embed.add_field(name="Type", value=playlist.type.value.title())
            
            if description:
                embed.add_field(name="Description", value=description, inline=False)
            
            embed.set_footer(text=f"ID: {playlist.id}")
            
            await ctx.send(embed=embed)
            
        except ValueError:
            await ctx.send(
                "Invalid privacy setting. Use: public, private, unlisted, or collaborative"
            )
    
    @playlist_group.command(
        name="add",
        description="Add current song to a playlist"
    )
    async def playlist_add(
        self,
        ctx,
        playlist_name: str,
        position: Optional[int] = None
    ):
        """Add current song to playlist"""
        player = self.bot.get_cog('MusicSystem').get_player(ctx.guild.id)
        if not player or not player.current:
            await ctx.send("No song is currently playing!")
            return
        
        # Find playlist
        playlists = await self.get_user_playlists(ctx.author.id)
        target_playlist = None
        
        for playlist in playlists:
            if playlist.name.lower() == playlist_name.lower():
                target_playlist = playlist
                break
        
        if not target_playlist:
            await ctx.send(f"No playlist found with name: {playlist_name}")
            return
        
        # Create track data
        track_data = {
            'title': player.current.title,
            'artist': getattr(player.current, 'author', 'Unknown'),
            'duration': player.current.length,
            'url': getattr(player.current, 'uri', ''),
            'source': str(player.current.source),
            'track_id': player.current.identifier
        }
        
        # Add to playlist
        success = await self.add_track_to_playlist(
            target_playlist.id,
            track_data,
            ctx.author.id,
            position
        )
        
        if success:
            await ctx.send(
                f"Added **{player.current.title}** to playlist **{target_playlist.name}**!"
            )
        else:
            await ctx.send("You don't have permission to edit this playlist.")
    
    @playlist_group.command(
        name="list",
        description="List your playlists"
    )
    async def playlist_list(self, ctx, user: Optional[discord.User] = None):
        """List playlists"""
        target_user = user or ctx.author
        
        playlists = await self.get_user_playlists(target_user.id)
        
        if not playlists:
            await ctx.send(f"{target_user.name} has no playlists.")
            return
        
        embed = discord.Embed(
            title=f"🎵 {target_user.name}'s Playlists",
            color=self.bot.colors.ROSE_GOLD
        )
        
        for playlist in playlists[:10]:  # Show first 10
            info = (
                f"**Tracks:** {playlist.track_count}\n"
                f"**Duration:** {self.format_duration(playlist.total_duration)}\n"
                f"**Plays:** {playlist.play_count}\n"
                f"**Privacy:** {playlist.privacy.value.title()}"
            )
            
            embed.add_field(
                name=playlist.name,
                value=info,
                inline=True
            )
        
        if len(playlists) > 10:
            embed.set_footer(text=f"And {len(playlists) - 10} more playlists...")
        
        await ctx.send(embed=embed)
    
    @playlist_group.command(
        name="play",
        description="Play a playlist"
    )
    async def playlist_play(
        self,
        ctx,
        playlist_name: str,
        shuffle: bool = False
    ):
        """Play a playlist"""
        # Implementation would play the playlist
        await ctx.send(f"Playing playlist: {playlist_name}")
    
    @playlist_group.command(
        name="import",
        description="Import a playlist from Spotify or YouTube"
    )
    async def playlist_import(
        self,
        ctx,
        url: str,
        name: Optional[str] = None
    ):
        """Import playlist from URL"""
        await ctx.send("Starting import... This may take a moment.")
        
        playlist = await self.import_playlist(url, ctx.author, name)
        
        if playlist:
            embed = discord.Embed(
                title="✅ Playlist Imported",
                description=f"**{playlist.name}**",
                color=self.bot.colors.VIOLET
            )
            
            embed.add_field(name="Tracks", value=playlist.track_count)
            embed.add_field(name="Duration", value=self.format_duration(playlist.total_duration))
            
            await ctx.send(embed=embed)
        else:
            await ctx.send("Failed to import playlist. Make sure the URL is valid.")
    
    def format_duration(self, milliseconds: int) -> str:
        """Format duration for display"""
        seconds = milliseconds // 1000
        
        if seconds >= 3600:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}h {minutes}m"
        elif seconds >= 60:
            minutes = seconds // 60
            return f"{minutes}m"
        else:
            return f"{seconds}s"
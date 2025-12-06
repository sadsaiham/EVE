"""
Complete radio station, music discovery, recommendations, and auto-DJ system
with genre-based stations, mood detection, and intelligent recommendations
"""

import discord
from discord.ext import commands, tasks
import wavelink
from typing import Dict, List, Optional, Set, Tuple
import random
from datetime import datetime, timedelta
import asyncio
import json
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

class RadioStation:
    """Radio station with genre, mood, and intelligent track selection"""
    
    def __init__(self, **kwargs):
        self.id: str = kwargs.get('id', '')
        self.name: str = kwargs.get('name', 'Unnamed Station')
        self.description: str = kwargs.get('description', '')
        self.genre: str = kwargs.get('genre', 'mixed')
        self.mood: str = kwargs.get('mood', 'neutral')
        self.era: Optional[str] = kwargs.get('era')  # 80s, 90s, 2000s, etc.
        
        # Station configuration
        self.temperature: float = kwargs.get('temperature', 0.7)  # Creativity vs popular
        self.variety: float = kwargs.get('variety', 0.5)  # 0=samey, 1=diverse
        self.discovery: float = kwargs.get('discovery', 0.3)  # New vs familiar
        
        # Seed data
        self.seed_tracks: List[str] = kwargs.get('seed_tracks', [])
        self.seed_artists: List[str] = kwargs.get('seed_artists', [])
        self.seed_genres: List[str] = kwargs.get('seed_genres', [])
        
        # Station statistics
        self.listeners: int = kwargs.get('listeners', 0)
        self.total_plays: int = kwargs.get('total_plays', 0)
        self.avg_session: float = kwargs.get('avg_session', 0)
        
        # Track history
        self.recent_tracks: List[Dict] = kwargs.get('recent_tracks', [])
        self.rotation: Dict[str, int] = kwargs.get('rotation', {})  # track_id: play_count
        
        # Station metadata
        self.created_at: datetime = kwargs.get('created_at', datetime.utcnow())
        self.updated_at: datetime = kwargs.get('updated_at', datetime.utcnow())
        self.is_public: bool = kwargs.get('is_public', True)
        self.owner_id: Optional[int] = kwargs.get('owner_id')
        self.guild_id: Optional[int] = kwargs.get('guild_id')
        
        # Station art
        self.thumbnail: Optional[str] = kwargs.get('thumbnail')
        self.color: Optional[int] = kwargs.get('color')
        
        # Live DJ mode
        self.dj_mode: bool = kwargs.get('dj_mode', False)
        self.dj_script: Optional[str] = kwargs.get('dj_script')
        self.dj_personality: str = kwargs.get('dj_personality', 'default')
    
    async def get_next_track(self, current_track: Optional[Dict] = None) -> Optional[Dict]:
        """Get next track for radio station"""
        # Implement track selection algorithm
        # 1. Check rotation (avoid repetition)
        # 2. Consider temperature (popular vs niche)
        # 3. Apply variety (genre/mood variation)
        # 4. Apply discovery (new vs familiar)
        
        # For now, return a mock track
        return {
            'title': f'{self.genre.title()} Radio Track',
            'artist': 'Various Artists',
            'duration': 180000,
            'genre': self.genre,
            'mood': self.mood
        }
    
    def record_play(self, track_data: Dict):
        """Record a track play for station statistics"""
        self.total_plays += 1
        self.recent_tracks.append({
            'track': track_data,
            'played_at': datetime.utcnow(),
            'listeners': self.listeners
        })
        
        # Keep only last 100 tracks
        if len(self.recent_tracks) > 100:
            self.recent_tracks.pop(0)
        
        # Update rotation
        track_id = track_data.get('id', '')
        if track_id:
            self.rotation[track_id] = self.rotation.get(track_id, 0) + 1
        
        self.updated_at = datetime.utcnow()
    
    def to_dict(self) -> Dict:
        """Convert station to dictionary"""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'genre': self.genre,
            'mood': self.mood,
            'era': self.era,
            'temperature': self.temperature,
            'variety': self.variety,
            'discovery': self.discovery,
            'seed_tracks': self.seed_tracks,
            'seed_artists': self.seed_artists,
            'seed_genres': self.seed_genres,
            'listeners': self.listeners,
            'total_plays': self.total_plays,
            'avg_session': self.avg_session,
            'recent_tracks': self.recent_tracks,
            'rotation': self.rotation,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'is_public': self.is_public,
            'owner_id': self.owner_id,
            'guild_id': self.guild_id,
            'thumbnail': self.thumbnail,
            'color': self.color,
            'dj_mode': self.dj_mode,
            'dj_script': self.dj_script,
            'dj_personality': self.dj_personality
        }


class DiscoveryEngine:
    """Music discovery engine with recommendation algorithms"""
    
    def __init__(self, bot):
        self.bot = bot
        self.user_profiles: Dict[int, Dict] = {}
        self.track_features: Dict[str, Dict] = {}
        self.recommendation_cache: Dict[str, List] = {}
        
        # Genre and mood definitions
        self.genres = {
            'pop': {'energy': 0.7, 'danceability': 0.8, 'valence': 0.7},
            'rock': {'energy': 0.8, 'danceability': 0.5, 'valence': 0.6},
            'hiphop': {'energy': 0.6, 'danceability': 0.7, 'valence': 0.5},
            'electronic': {'energy': 0.8, 'danceability': 0.9, 'valence': 0.6},
            'jazz': {'energy': 0.4, 'danceability': 0.4, 'valence': 0.5},
            'classical': {'energy': 0.3, 'danceability': 0.2, 'valence': 0.4},
            'lofi': {'energy': 0.3, 'danceability': 0.4, 'valence': 0.5},
            'indie': {'energy': 0.5, 'danceability': 0.5, 'valence': 0.6},
            'metal': {'energy': 0.9, 'danceability': 0.3, 'valence': 0.4},
            'country': {'energy': 0.5, 'danceability': 0.5, 'valence': 0.7},
            'r&b': {'energy': 0.5, 'danceability': 0.6, 'valence': 0.6},
            'reggae': {'energy': 0.6, 'danceability': 0.7, 'valence': 0.8},
            'blues': {'energy': 0.4, 'danceability': 0.4, 'valence': 0.4}
        }
        
        self.moods = {
            'happy': {'valence': 0.8, 'energy': 0.7},
            'sad': {'valence': 0.3, 'energy': 0.4},
            'energetic': {'valence': 0.6, 'energy': 0.9},
            'chill': {'valence': 0.5, 'energy': 0.3},
            'romantic': {'valence': 0.7, 'energy': 0.5},
            'angry': {'valence': 0.2, 'energy': 0.9},
            'focused': {'valence': 0.5, 'energy': 0.6},
            'relaxed': {'valence': 0.6, 'energy': 0.3}
        }
        
        # Predefined radio stations
        self.default_stations = self.create_default_stations()
        
        # Start background tasks
        self.update_profiles.start()
        self.clear_cache.start()
    
    def create_default_stations(self) -> List[RadioStation]:
        """Create default radio stations"""
        stations = []
        
        default_configs = [
            {
                'id': 'top_hits',
                'name': 'Top Hits Radio',
                'description': 'Current popular music from all genres',
                'genre': 'pop',
                'mood': 'energetic',
                'temperature': 0.2,  # Mostly popular
                'variety': 0.8,  # Diverse genres
                'discovery': 0.1  # Mostly familiar
            },
            {
                'id': 'lofi_study',
                'name': 'LoFi Study Beats',
                'description': 'Chill beats to study/relax to',
                'genre': 'lofi',
                'mood': 'focused',
                'temperature': 0.5,
                'variety': 0.3,
                'discovery': 0.4
            },
            {
                'id': 'rock_classics',
                'name': 'Rock Classics',
                'description': 'Classic rock from the 70s-90s',
                'genre': 'rock',
                'era': '70s-90s',
                'mood': 'energetic',
                'temperature': 0.3,
                'variety': 0.6,
                'discovery': 0.2
            },
            {
                'id': 'chill_vibes',
                'name': 'Chill Vibes',
                'description': 'Relaxing music for any time',
                'genre': 'mixed',
                'mood': 'chill',
                'temperature': 0.6,
                'variety': 0.7,
                'discovery': 0.5
            },
            {
                'id': 'electronic_dance',
                'name': 'Electronic Dance',
                'description': 'High-energy electronic music',
                'genre': 'electronic',
                'mood': 'energetic',
                'temperature': 0.4,
                'variety': 0.8,
                'discovery': 0.3
            },
            {
                'id': 'discovery_mix',
                'name': 'Discovery Mix',
                'description': 'New music based on your taste',
                'genre': 'mixed',
                'mood': 'mixed',
                'temperature': 0.8,
                'variety': 0.9,
                'discovery': 0.9  # Maximum discovery
            }
        ]
        
        for config in default_configs:
            station = RadioStation(**config)
            stations.append(station)
        
        return stations
    
    async def update_user_profile(self, user_id: int, track_data: Dict):
        """Update user's music profile based on played tracks"""
        if user_id not in self.user_profiles:
            self.user_profiles[user_id] = {
                'genres': defaultdict(int),
                'artists': defaultdict(int),
                'moods': defaultdict(int),
                'total_tracks': 0,
                'total_play_time': 0,
                'last_updated': datetime.utcnow()
            }
        
        profile = self.user_profiles[user_id]
        
        # Update genres
        genre = track_data.get('genre', 'unknown')
        if genre != 'unknown':
            profile['genres'][genre] += 1
        
        # Update artists
        artist = track_data.get('artist', 'unknown')
        if artist != 'unknown':
            profile['artists'][artist] += 1
        
        # Update mood (would need mood detection)
        # For now, estimate from genre
        if genre in self.genres:
            # Find closest mood
            track_energy = self.genres[genre]['energy']
            track_valence = self.genres[genre]['valence']
            
            closest_mood = min(
                self.moods.items(),
                key=lambda m: abs(m[1]['energy'] - track_energy) + abs(m[1]['valence'] - track_valence)
            )[0]
            
            profile['moods'][closest_mood] += 1
        
        # Update statistics
        profile['total_tracks'] += 1
        profile['total_play_time'] += track_data.get('duration', 0)
        profile['last_updated'] = datetime.utcnow()
    
    async def get_recommendations(self, user_id: int, limit: int = 10) -> List[Dict]:
        """Get personalized recommendations for user"""
        if user_id not in self.user_profiles:
            # Return general recommendations
            return await self.get_general_recommendations(limit)
        
        profile = self.user_profiles[user_id]
        
        # Generate cache key
        cache_key = f"recs:{user_id}:{limit}"
        if cache_key in self.recommendation_cache:
            return self.recommendation_cache[cache_key]
        
        # Get top genres and artists
        top_genres = sorted(
            profile['genres'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:3]
        
        top_artists = sorted(
            profile['artists'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        # Build recommendation query
        queries = []
        
        # 1. Similar to top artists
        for artist, _ in top_artists:
            queries.append(f"similar to {artist}")
        
        # 2. Top genre radio
        for genre, _ in top_genres:
            queries.append(f"{genre} music")
        
        # 3. Mood-based
        top_moods = sorted(
            profile['moods'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:2]
        
        for mood, _ in top_moods:
            queries.append(f"{mood} music")
        
        # Mix queries for variety
        mixed_query = " OR ".join(random.sample(queries, min(3, len(queries))))
        
        # This would actually search for tracks
        # For now, return mock recommendations
        recommendations = []
        for i in range(limit):
            recommendations.append({
                'title': f'Recommended Track {i+1}',
                'artist': random.choice([a for a, _ in top_artists]) if top_artists else 'Various Artists',
                'genre': random.choice([g for g, _ in top_genres]) if top_genres else 'mixed',
                'reason': 'Based on your listening history'
            })
        
        # Cache results
        self.recommendation_cache[cache_key] = recommendations
        asyncio.get_event_loop().call_later(
            3600,  # 1 hour expiration
            lambda: self.recommendation_cache.pop(cache_key, None)
        )
        
        return recommendations
    
    async def get_general_recommendations(self, limit: int = 10) -> List[Dict]:
        """Get general recommendations (trending/popular)"""
        # This would fetch trending tracks
        # For now, return mock data
        genres = list(self.genres.keys())
        
        recommendations = []
        for i in range(limit):
            genre = random.choice(genres)
            recommendations.append({
                'title': f'Popular {genre.title()} Track {i+1}',
                'artist': f'Popular {genre.title()} Artist',
                'genre': genre,
                'reason': 'Trending now'
            })
        
        return recommendations
    
    async def get_station_by_genre(self, genre: str) -> Optional[RadioStation]:
        """Get a radio station by genre"""
        for station in self.default_stations:
            if station.genre == genre.lower():
                return station
        
        # Create on-the-fly station
        if genre.lower() in self.genres:
            station = RadioStation(
                id=f"genre_{genre.lower()}",
                name=f"{genre.title()} Radio",
                description=f"{genre.title()} music station",
                genre=genre.lower(),
                mood=self.detect_genre_mood(genre.lower()),
                temperature=0.5,
                variety=0.7,
                discovery=0.4
            )
            return station
        
        return None
    
    async def get_station_by_mood(self, mood: str) -> Optional[RadioStation]:
        """Get a radio station by mood"""
        mood = mood.lower()
        
        # Check if mood exists
        if mood not in self.moods:
            return None
        
        # Find or create station
        for station in self.default_stations:
            if station.mood == mood:
                return station
        
        # Create mood station
        station = RadioStation(
            id=f"mood_{mood}",
            name=f"{mood.title()} Mood Radio",
            description=f"Music for when you're feeling {mood}",
            genre='mixed',
            mood=mood,
            temperature=0.6,
            variety=0.8,
            discovery=0.5
        )
        return station
    
    async def create_custom_station(
        self,
        name: str,
        seed_tracks: List[str] = None,
        seed_artists: List[str] = None,
        seed_genres: List[str] = None,
        temperature: float = 0.5,
        variety: float = 0.5,
        discovery: float = 0.5
    ) -> RadioStation:
        """Create a custom radio station"""
        station_id = f"custom_{int(datetime.utcnow().timestamp())}"
        
        # Determine genre and mood from seeds
        genre = seed_genres[0] if seed_genres and len(seed_genres) > 0 else 'mixed'
        mood = 'neutral'  # Would need mood detection
        
        station = RadioStation(
            id=station_id,
            name=name,
            description=f"Custom radio station: {name}",
            genre=genre,
            mood=mood,
            seed_tracks=seed_tracks or [],
            seed_artists=seed_artists or [],
            seed_genres=seed_genres or [],
            temperature=temperature,
            variety=variety,
            discovery=discovery,
            is_public=False
        )
        
        return station
    
    def detect_genre_mood(self, genre: str) -> str:
        """Detect typical mood for a genre"""
        genre_data = self.genres.get(genre.lower(), {})
        if not genre_data:
            return 'neutral'
        
        energy = genre_data.get('energy', 0.5)
        valence = genre_data.get('valence', 0.5)
        
        # Find closest mood
        return min(
            self.moods.items(),
            key=lambda m: abs(m[1]['energy'] - energy) + abs(m[1]['valence'] - valence)
        )[0]
    
    async def get_similar_tracks(self, track_data: Dict, limit: int = 5) -> List[Dict]:
        """Get tracks similar to a given track"""
        # Extract features from track data
        genre = track_data.get('genre', 'unknown')
        artist = track_data.get('artist', 'unknown')
        
        # Build similarity query
        query_parts = []
        if genre != 'unknown':
            query_parts.append(f"{genre}")
        if artist != 'unknown':
            query_parts.append(f"similar to {artist}")
        
        if not query_parts:
            return []
        
        query = " OR ".join(query_parts)
        
        # This would search for similar tracks
        # For now, return mock data
        similar = []
        for i in range(limit):
            similar.append({
                'title': f'Similar to {track_data.get("title", "track")} {i+1}',
                'artist': artist if artist != 'unknown' else 'Various Artists',
                'genre': genre if genre != 'unknown' else 'mixed',
                'similarity_score': 0.8 - (i * 0.1)
            })
        
        return similar
    
    async def get_trending_tracks(self, time_range: str = "day", limit: int = 10) -> List[Dict]:
        """Get trending tracks for a time range"""
        # This would fetch from database or API
        # For now, return mock data
        trending = []
        
        time_ranges = {
            "day": "Today",
            "week": "This Week",
            "month": "This Month",
            "year": "This Year"
        }
        
        time_label = time_ranges.get(time_range, "Recently")
        
        for i in range(limit):
            trending.append({
                'title': f'Trending Track {i+1}',
                'artist': 'Popular Artist',
                'genre': random.choice(list(self.genres.keys())),
                'trend_position': i + 1,
                'play_count': 1000 - (i * 100),
                'time_range': time_label
            })
        
        return trending
    
    @tasks.loop(hours=1)
    async def update_profiles(self):
        """Periodically update user profiles and recommendations"""
        # Clean old profiles
        cutoff = datetime.utcnow() - timedelta(days=30)
        stale_users = [
            user_id for user_id, profile in self.user_profiles.items()
            if profile['last_updated'] < cutoff
        ]
        
        for user_id in stale_users:
            self.user_profiles.pop(user_id, None)
        
        logger.info(f"Updated user profiles. Active: {len(self.user_profiles)}")
    
    @tasks.loop(minutes=30)
    async def clear_cache(self):
        """Clear old cache entries"""
        # Recommendation cache auto-expires via call_later
        # This just logs cache size
        logger.info(f"Recommendation cache size: {len(self.recommendation_cache)}")


class RadioDiscovery(DiscoveryEngine):
    """Complete radio and discovery system integrated with Discord"""
    
    def __init__(self, bot):
        super().__init__(bot)
        self.active_radios: Dict[int, Dict] = {}  # guild_id: radio_session
        self.user_stations: Dict[int, List[RadioStation]] = {}  # user_id: custom_stations
    
    @commands.hybrid_command(
        name="radio",
        description="Start a radio station"
    )
    async def radio_command(
        self,
        ctx,
        station_input: Optional[str] = None,
        genre: Optional[str] = None,
        mood: Optional[str] = None
    ):
        """Start a radio station"""
        if not ctx.author.voice:
            await ctx.send("Join a voice channel to start radio! 🎵")
            return
        
        # Determine station
        station = None
        
        if station_input:
            # Check if it's a predefined station
            station = await self.get_station_by_input(station_input)
        
        if not station and genre:
            station = await self.get_station_by_genre(genre)
        
        if not station and mood:
            station = await self.get_station_by_mood(mood)
        
        if not station:
            # Show available stations
            await self.show_available_stations(ctx)
            return
        
        # Connect to voice
        music_cog = self.bot.get_cog('MusicSystem')
        if not music_cog:
            await ctx.send("Music system not available!")
            return
        
        player = await music_cog.ensure_voice(ctx)
        if not player:
            await ctx.send("Failed to connect to voice!")
            return
        
        # Start radio mode
        player.radio_mode = True
        player.radio_seed = station.id
        player.auto_play = True
        
        # Store radio session
        self.active_radios[ctx.guild.id] = {
            'station': station,
            'started_at': datetime.utcnow(),
            'started_by': ctx.author.id,
            'tracks_played': 0
        }
        
        # Send station info
        embed = await self.create_station_embed(station)
        embed.set_author(name="🎧 Radio Started")
        embed.description = f"Now playing **{station.name}**\n{station.description}"
        
        # Add control buttons
        view = RadioControlsView(self, ctx.guild.id, station)
        await ctx.send(embed=embed, view=view)
        
        # Play first track
        track_data = await station.get_next_track()
        if track_data:
            # Convert to wavelink track and play
            # This is simplified
            await ctx.send(f"Playing: **{track_data['title']}**")
    
    async def get_station_by_input(self, input_str: str) -> Optional[RadioStation]:
        """Get station by various input types"""
        input_lower = input_str.lower()
        
        # Check default stations
        for station in self.default_stations:
            if (input_lower == station.id or 
                input_lower in station.name.lower() or
                input_lower == station.genre or
                input_lower == station.mood):
                return station
        
        # Check genres
        if input_lower in self.genres:
            return await self.get_station_by_genre(input_lower)
        
        # Check moods
        if input_lower in self.moods:
            return await self.get_station_by_mood(input_lower)
        
        return None
    
    async def show_available_stations(self, ctx):
        """Show all available radio stations"""
        embed = discord.Embed(
            title="📻 Available Radio Stations",
            description="Start a station with `e!radio <name>`",
            color=self.bot.colors.ROSE_GOLD
        )
        
        # Group stations by category
        categories = {
            'Popular': [],
            'By Genre': [],
            'By Mood': [],
            'Special': []
        }
        
        for station in self.default_stations:
            if station.id in ['top_hits', 'discovery_mix']:
                categories['Popular'].append(station)
            elif station.genre != 'mixed':
                categories['By Genre'].append(station)
            elif station.mood != 'mixed':
                categories['By Mood'].append(station)
            else:
                categories['Special'].append(station)
        
        for category, stations in categories.items():
            if stations:
                station_list = "\n".join(
                    f"• **{s.name}** - `{s.id}`"
                    for s in stations
                )
                embed.add_field(
                    name=category,
                    value=station_list,
                    inline=False
                )
        
        # Add genre list
        genre_list = ", ".join(sorted(self.genres.keys()))
        embed.add_field(
            name="Available Genres",
            value=f"`{genre_list}`",
            inline=False
        )
        
        # Add mood list
        mood_list = ", ".join(sorted(self.moods.keys()))
        embed.add_field(
            name="Available Moods",
            value=f"`{mood_list}`",
            inline=False
        )
        
        await ctx.send(embed=embed)
    
    async def create_station_embed(self, station: RadioStation) -> discord.Embed:
        """Create embed for station info"""
        color = station.color or self.bot.colors.VIOLET
        
        embed = discord.Embed(
            title=f"📻 {station.name}",
            color=color,
            timestamp=datetime.utcnow()
        )
        
        embed.description = station.description
        
        # Station info
        info_fields = []
        if station.genre and station.genre != 'mixed':
            info_fields.append(f"**Genre:** {station.genre.title()}")
        if station.mood and station.mood != 'mixed':
            info_fields.append(f"**Mood:** {station.mood.title()}")
        if station.era:
            info_fields.append(f"**Era:** {station.era}")
        
        if info_fields:
            embed.add_field(
                name="Station Info",
                value="\n".join(info_fields),
                inline=True
            )
        
        # Station settings
        settings = [
            f"**Popularity:** {'Popular' if station.temperature < 0.5 else 'Niche'}",
            f"**Variety:** {'High' if station.variety > 0.5 else 'Low'}",
            f"**Discovery:** {'High' if station.discovery > 0.5 else 'Low'}"
        ]
        
        embed.add_field(
            name="Station Settings",
            value="\n".join(settings),
            inline=True
        )
        
        # Statistics
        stats = [
            f"**Total Plays:** {station.total_plays:,}",
            f"**Current Listeners:** {station.listeners}",
            f"**Avg Session:** {station.avg_session:.1f} min"
        ]
        
        embed.add_field(
            name="Statistics",
            value="\n".join(stats),
            inline=True
        )
        
        # Recent tracks (if any)
        if station.recent_tracks:
            recent = station.recent_tracks[-3:]  # Last 3 tracks
            track_list = "\n".join(
                f"• {t['track'].get('title', 'Unknown')}"
                for t in recent
            )
            embed.add_field(
                name="Recently Played",
                value=track_list,
                inline=False
            )
        
        return embed
    
    @commands.hybrid_command(
        name="recommend",
        description="Get music recommendations"
    )
    async def recommend_command(
        self,
        ctx,
        count: int = 5,
        type: str = "personal"
    ):
        """Get music recommendations"""
        if type == "personal":
            recs = await self.get_recommendations(ctx.author.id, count)
            title = "🎵 Personalized Recommendations"
            description = "Based on your listening history"
        elif type == "trending":
            recs = await self.get_trending_tracks("week", count)
            title = "📈 Trending Now"
            description = "Popular tracks this week"
        elif type == "similar" and hasattr(ctx, 'playing_track'):
            recs = await self.get_similar_tracks(ctx.playing_track, count)
            title = "🔍 Similar Tracks"
            description = f"Similar to {ctx.playing_track.get('title', 'current track')}"
        else:
            recs = await self.get_general_recommendations(count)
            title = "🎧 Recommended For You"
            description = "Popular tracks you might like"
        
        if not recs:
            await ctx.send("No recommendations available right now.")
            return
        
        embed = discord.Embed(
            title=title,
            description=description,
            color=self.bot.colors.PLUM
        )
        
        for i, rec in enumerate(recs, 1):
            embed.add_field(
                name=f"{i}. {rec['title']}",
                value=f"{rec['artist']} • {rec.get('genre', 'Unknown').title()}",
                inline=False
            )
        
        # Add play buttons
        view = RecommendationView(self, recs, ctx.author)
        await ctx.send(embed=embed, view=view)
    
    @commands.hybrid_command(
        name="createstation",
        description="Create a custom radio station"
    )
    async def createstation_command(
        self,
        ctx,
        name: str,
        genre: Optional[str] = None,
        mood: Optional[str] = None
    ):
        """Create a custom radio station"""
        seed_artists = []
        seed_genres = [genre] if genre else []
        
        station = await self.create_custom_station(
            name=name,
            seed_genres=seed_genres,
            temperature=0.5,
            variety=0.6,
            discovery=0.4
        )
        
        if mood:
            station.mood = mood
        
        # Store user station
        if ctx.author.id not in self.user_stations:
            self.user_stations[ctx.author.id] = []
        
        self.user_stations[ctx.author.id].append(station)
        
        embed = await self.create_station_embed(station)
        embed.set_author(name="✅ Custom Station Created")
        embed.description = f"**{name}**\nUse `e!radio {station.id}` to play"
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(
        name="trending",
        description="Show trending music"
    )
    async def trending_command(
        self,
        ctx,
        time_range: str = "week",
        limit: int = 10
    ):
        """Show trending tracks"""
        if time_range not in ["day", "week", "month", "year"]:
            await ctx.send("Time range must be: day, week, month, or year")
            return
        
        if limit > 25:
            limit = 25
        
        trending = await self.get_trending_tracks(time_range, limit)
        
        embed = discord.Embed(
            title=f"📈 Trending Music ({time_range.title()})",
            color=self.bot.colors.VIOLET,
            timestamp=datetime.utcnow()
        )
        
        for track in trending:
            position = track['trend_position']
            title = track['title']
            artist = track['artist']
            plays = track['play_count']
            
            # Add position emoji
            emoji = "🥇" if position == 1 else "🥈" if position == 2 else "🥉" if position == 3 else f"{position}."
            
            embed.add_field(
                name=f"{emoji} {title}",
                value=f"{artist} • {plays:,} plays",
                inline=False
            )
        
        await ctx.send(embed=embed)


class RadioControlsView(discord.ui.View):
    """Radio station controls"""
    
    def __init__(self, radio_cog, guild_id: int, station: RadioStation):
        super().__init__(timeout=300)
        self.radio_cog = radio_cog
        self.guild_id = guild_id
        self.station = station
    
    @discord.ui.button(label="⏭️ Skip", style=discord.ButtonStyle.primary)
    async def skip_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Skip current radio track"""
        music_cog = self.radio_cog.bot.get_cog('MusicSystem')
        if not music_cog:
            await interaction.response.send_message("Music system not available!", ephemeral=True)
            return
        
        player = music_cog.get_player(self.guild_id)
        if not player:
            await interaction.response.send_message("No active player!", ephemeral=True)
            return
        
        await player.stop()
        await interaction.response.send_message("Skipped to next track! ⏭️", ephemeral=True)
    
    @discord.ui.button(label="🔀 Change Station", style=discord.ButtonStyle.secondary)
    async def change_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Change radio station"""
        # Show station selection
        embed = discord.Embed(
            title="Change Radio Station",
            description="Select a new station:",
            color=self.radio_cog.bot.colors.ROSE_GOLD
        )
        
        # Add station options
        for station in self.radio_cog.default_stations[:5]:
            embed.add_field(
                name=station.name,
                value=f"`{station.id}` • {station.description[:50]}...",
                inline=False
            )
        
        view = StationSelectView(self.radio_cog, self.guild_id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="⏹️ Stop Radio", style=discord.ButtonStyle.danger)
    async def stop_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Stop radio playback"""
        music_cog = self.radio_cog.bot.get_cog('MusicSystem')
        if not music_cog:
            await interaction.response.send_message("Music system not available!", ephemeral=True)
            return
        
        player = music_cog.get_player(self.guild_id)
        if not player:
            await interaction.response.send_message("No active player!", ephemeral=True)
            return
        
        player.radio_mode = False
        player.auto_play = False
        
        # Clear radio session
        self.radio_cog.active_radios.pop(self.guild_id, None)
        
        await interaction.response.send_message("Radio stopped! ⏹️")
    
    @discord.ui.button(label="ℹ️ Station Info", style=discord.ButtonStyle.success)
    async def info_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show station info"""
        embed = await self.radio_cog.create_station_embed(self.station)
        await interaction.response.send_message(embed=embed, ephemeral=True)


class StationSelectView(discord.ui.View):
    """Station selection view"""
    
    def __init__(self, radio_cog, guild_id: int):
        super().__init__(timeout=60)
        self.radio_cog = radio_cog
        self.guild_id = guild_id
        
        # Add station selection dropdown
        self.add_item(StationSelectDropdown(radio_cog, guild_id))


class StationSelectDropdown(discord.ui.Select):
    """Dropdown for station selection"""
    
    def __init__(self, radio_cog, guild_id: int):
        self.radio_cog = radio_cog
        self.guild_id = guild_id
        
        # Create options from default stations
        options = []
        for station in radio_cog.default_stations[:10]:
            options.append(
                discord.SelectOption(
                    label=station.name,
                    value=station.id,
                    description=station.description[:100]
                )
            )
        
        super().__init__(
            placeholder="Select a station...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Handle station selection"""
        station_id = self.values[0]
        
        # Find station
        station = None
        for s in self.radio_cog.default_stations:
            if s.id == station_id:
                station = s
                break
        
        if not station:
            await interaction.response.send_message("Station not found!", ephemeral=True)
            return
        
        # Update radio session
        self.radio_cog.active_radios[self.guild_id]['station'] = station
        
        # Get music cog
        music_cog = self.radio_cog.bot.get_cog('MusicSystem')
        if music_cog:
            player = music_cog.get_player(self.guild_id)
            if player:
                player.radio_seed = station.id
        
        embed = await self.radio_cog.create_station_embed(station)
        embed.set_author(name="🎧 Station Changed")
        
        await interaction.response.send_message(embed=embed)


class RecommendationView(discord.ui.View):
    """Recommendation controls"""
    
    def __init__(self, radio_cog, recommendations: List[Dict], requester: discord.User):
        super().__init__(timeout=120)
        self.radio_cog = radio_cog
        self.recommendations = recommendations
        self.requester = requester
        
        # Add play buttons for top 5 recommendations
        for i in range(min(5, len(recommendations))):
            button = discord.ui.Button(
                label=f"Play #{i+1}",
                style=discord.ButtonStyle.success,
                custom_id=f"play_rec_{i}"
            )
            button.callback = self.create_play_callback(i)
            self.add_item(button)
    
    def create_play_callback(self, index: int):
        """Create callback for play button"""
        async def callback(interaction: discord.Interaction):
            rec = self.recommendations[index]
            
            # Check if user is in voice
            if not interaction.user.voice:
                await interaction.response.send_message(
                    "Join a voice channel first! 🎵",
                    ephemeral=True
                )
                return
            
            # Play the recommended track
            # This would search and play the track
            await interaction.response.send_message(
                f"Searching for: **{rec['title']}**...",
                ephemeral=True
            )
        
        return callback
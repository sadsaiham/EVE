"""
Complete history tracking and statistics system with:
- Playback history with filtering
- Detailed analytics and insights
- Listening patterns and heatmaps
- Top lists and personal records
- Export functionality
"""

import discord
from discord.ext import commands, tasks
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import asyncio
import json
from collections import defaultdict
import calendar
import statistics
import logging

logger = logging.getLogger(__name__)

class PlayHistory:
    """Track and analyze playback history"""
    
    def __init__(self, bot):
        self.bot = bot
        self.history_db: Dict[int, List[Dict]] = {}  # user_id: history_entries
        self.server_history: Dict[int, List[Dict]] = {}  # guild_id: history_entries
        self.global_history: List[Dict] = []
        
        # Statistics cache
        self.user_stats_cache: Dict[int, Dict] = {}
        self.server_stats_cache: Dict[int, Dict] = {}
        self.global_stats_cache: Dict[str, any] = {}
        
        # Export storage
        self.exports: Dict[str, Dict] = {}  # export_id: export_data
        
        # Background tasks
        self.save_history.start()
        self.cleanup_old_data.start()
        self.update_stats_cache.start()
    
    async def record_play(self, user_id: int, guild_id: int, track_data: Dict):
        """Record a track play in history"""
        timestamp = datetime.utcnow()
        
        history_entry = {
            'track_id': track_data.get('id', ''),
            'title': track_data.get('title', 'Unknown'),
            'artist': track_data.get('artist', 'Unknown'),
            'duration': track_data.get('duration', 0),
            'played_at': timestamp,
            'guild_id': guild_id,
            'source': track_data.get('source', 'unknown'),
            'listening_context': track_data.get('context', 'normal')  # normal, party, radio, etc.
        }
        
        # Add to user history
        if user_id not in self.history_db:
            self.history_db[user_id] = []
        self.history_db[user_id].append(history_entry)
        
        # Add to server history
        if guild_id not in self.server_history:
            self.server_history[guild_id] = []
        self.server_history[guild_id].append({**history_entry, 'user_id': user_id})
        
        # Add to global history
        self.global_history.append({**history_entry, 'user_id': user_id})
        
        # Limit history sizes
        self._limit_history_size(user_id, guild_id)
        
        # Invalidate cache
        self.user_stats_cache.pop(user_id, None)
        self.server_stats_cache.pop(guild_id, None)
    
    def _limit_history_size(self, user_id: int, guild_id: int):
        """Limit history size to prevent memory issues"""
        # User history: keep last 1000 entries
        if user_id in self.history_db and len(self.history_db[user_id]) > 1000:
            self.history_db[user_id] = self.history_db[user_id][-1000:]
        
        # Server history: keep last 5000 entries
        if guild_id in self.server_history and len(self.server_history[guild_id]) > 5000:
            self.server_history[guild_id] = self.server_history[guild_id][-5000:]
        
        # Global history: keep last 10000 entries
        if len(self.global_history) > 10000:
            self.global_history = self.global_history[-10000:]
    
    async def get_user_history(
        self, 
        user_id: int, 
        limit: int = 50,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        filter_artist: Optional[str] = None,
        filter_genre: Optional[str] = None
    ) -> List[Dict]:
        """Get user's playback history with filters"""
        if user_id not in self.history_db:
            return []
        
        history = self.history_db[user_id]
        
        # Apply filters
        filtered = history
        
        if start_date:
            filtered = [h for h in filtered if h['played_at'] >= start_date]
        
        if end_date:
            filtered = [h for h in filtered if h['played_at'] <= end_date]
        
        if filter_artist:
            filtered = [h for h in filtered if filter_artist.lower() in h['artist'].lower()]
        
        # Note: genre filtering would require genre data in track_data
        
        # Sort by most recent
        filtered.sort(key=lambda x: x['played_at'], reverse=True)
        
        return filtered[:limit]
    
    async def get_server_history(
        self,
        guild_id: int,
        limit: int = 100,
        period: str = "day"  # day, week, month, year, all
    ) -> List[Dict]:
        """Get server playback history"""
        if guild_id not in self.server_history:
            return []
        
        history = self.server_history[guild_id]
        
        # Filter by period
        if period != "all":
            cutoff = self._get_cutoff_date(period)
            history = [h for h in history if h['played_at'] >= cutoff]
        
        # Sort by most recent
        history.sort(key=lambda x: x['played_at'], reverse=True)
        
        return history[:limit]
    
    async def get_user_statistics(self, user_id: int, period: str = "all") -> Dict:
        """Get detailed statistics for a user"""
        cache_key = f"{user_id}:{period}"
        if cache_key in self.user_stats_cache:
            return self.user_stats_cache[cache_key]
        
        if user_id not in self.history_db:
            stats = self._get_empty_stats()
            self.user_stats_cache[cache_key] = stats
            return stats
        
        history = self.history_db[user_id]
        
        # Filter by period
        if period != "all":
            cutoff = self._get_cutoff_date(period)
            history = [h for h in history if h['played_at'] >= cutoff]
        
        if not history:
            stats = self._get_empty_stats()
            self.user_stats_cache[cache_key] = stats
            return stats
        
        # Calculate statistics
        stats = self._calculate_statistics(history, user_id)
        
        # Add period-specific data
        stats['period'] = period
        stats['period_start'] = cutoff if period != "all" else None
        
        # Calculate trends
        if period == "all" and len(history) > 7:
            stats['trends'] = await self._calculate_trends(user_id, history)
        
        self.user_stats_cache[cache_key] = stats
        return stats
    
    async def get_server_statistics(self, guild_id: int, period: str = "all") -> Dict:
        """Get detailed statistics for a server"""
        cache_key = f"server:{guild_id}:{period}"
        if cache_key in self.server_stats_cache:
            return self.server_stats_cache[cache_key]
        
        if guild_id not in self.server_history:
            stats = self._get_empty_server_stats()
            self.server_stats_cache[cache_key] = stats
            return stats
        
        history = self.server_history[guild_id]
        
        # Filter by period
        if period != "all":
            cutoff = self._get_cutoff_date(period)
            history = [h for h in history if h['played_at'] >= cutoff]
        
        if not history:
            stats = self._get_empty_server_stats()
            self.server_stats_cache[cache_key] = stats
            return stats
        
        # Calculate server statistics
        stats = self._calculate_server_statistics(history, guild_id)
        
        self.server_stats_cache[cache_key] = stats
        return stats
    
    async def get_global_statistics(self, period: str = "all") -> Dict:
        """Get global statistics"""
        cache_key = f"global:{period}"
        if cache_key in self.global_stats_cache:
            return self.global_stats_cache[cache_key]
        
        history = self.global_history
        
        # Filter by period
        if period != "all":
            cutoff = self._get_cutoff_date(period)
            history = [h for h in history if h['played_at'] >= cutoff]
        
        if not history:
            stats = self._get_empty_global_stats()
            self.global_stats_cache[cache_key] = stats
            return stats
        
        # Calculate global statistics
        stats = self._calculate_global_statistics(history)
        
        self.global_stats_cache[cache_key] = stats
        return stats
    
    def _get_cutoff_date(self, period: str) -> datetime:
        """Get cutoff date for period"""
        now = datetime.utcnow()
        
        if period == "day":
            return now - timedelta(days=1)
        elif period == "week":
            return now - timedelta(weeks=1)
        elif period == "month":
            return now - timedelta(days=30)
        elif period == "year":
            return now - timedelta(days=365)
        else:
            return now - timedelta(days=365*10)  # All time
    
    def _calculate_statistics(self, history: List[Dict], user_id: int) -> Dict:
        """Calculate user statistics from history"""
        if not history:
            return self._get_empty_stats()
        
        # Basic counts
        total_tracks = len(history)
        total_duration = sum(h.get('duration', 0) for h in history)
        unique_artists = len(set(h.get('artist', '') for h in history))
        
        # Most played artists
        artist_counts = defaultdict(int)
        for h in history:
            artist = h.get('artist', 'Unknown')
            artist_counts[artist] += 1
        
        top_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Most played tracks
        track_counts = defaultdict(int)
        for h in history:
            key = f"{h.get('title', 'Unknown')} - {h.get('artist', 'Unknown')}"
            track_counts[key] += 1
        
        top_tracks = sorted(track_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Listening patterns
        listening_by_hour = defaultdict(int)
        listening_by_day = defaultdict(int)
        listening_by_weekday = defaultdict(int)
        
        for h in history:
            played_at = h['played_at']
            listening_by_hour[played_at.hour] += 1
            listening_by_day[played_at.strftime('%Y-%m-%d')] += 1
            listening_by_weekday[played_at.weekday()] += 1
        
        # Calculate averages
        avg_tracks_per_day = total_tracks / max(len(listening_by_day), 1)
        avg_duration_per_track = total_duration / max(total_tracks, 1)
        
        # Find peak listening times
        peak_hour = max(listening_by_hour.items(), key=lambda x: x[1]) if listening_by_hour else (0, 0)
        peak_weekday = max(listening_by_weekday.items(), key=lambda x: x[1]) if listening_by_weekday else (0, 0)
        
        return {
            'user_id': user_id,
            'total_tracks': total_tracks,
            'total_duration': total_duration,
            'unique_artists': unique_artists,
            'avg_tracks_per_day': avg_tracks_per_day,
            'avg_duration_per_track': avg_duration_per_track,
            'top_artists': [{'artist': a, 'count': c} for a, c in top_artists],
            'top_tracks': [{'track': t, 'count': c} for t, c in top_tracks],
            'listening_by_hour': dict(listening_by_hour),
            'listening_by_day': dict(listening_by_day),
            'listening_by_weekday': dict(listening_by_weekday),
            'peak_hour': {'hour': peak_hour[0], 'count': peak_hour[1]},
            'peak_weekday': {'weekday': peak_weekday[0], 'count': peak_weekday[1], 'name': calendar.day_name[peak_weekday[0]]},
            'first_listen': history[-1]['played_at'] if history else None,
            'last_listen': history[0]['played_at'] if history else None,
            'history_span_days': (history[0]['played_at'] - history[-1]['played_at']).days if len(history) > 1 else 0
        }
    
    def _calculate_server_statistics(self, history: List[Dict], guild_id: int) -> Dict:
        """Calculate server statistics from history"""
        if not history:
            return self._get_empty_server_stats()
        
        # Basic counts
        total_tracks = len(history)
        total_duration = sum(h.get('duration', 0) for h in history)
        unique_users = len(set(h.get('user_id', 0) for h in history))
        unique_artists = len(set(h.get('artist', '') for h in history))
        
        # Most active users
        user_counts = defaultdict(int)
        for h in history:
            user_id = h.get('user_id', 0)
            user_counts[user_id] += 1
        
        top_users = sorted(user_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Most played artists
        artist_counts = defaultdict(int)
        for h in history:
            artist = h.get('artist', 'Unknown')
            artist_counts[artist] += 1
        
        top_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Server activity patterns
        activity_by_hour = defaultdict(int)
        activity_by_day = defaultdict(int)
        
        for h in history:
            played_at = h['played_at']
            activity_by_hour[played_at.hour] += 1
            activity_by_day[played_at.strftime('%Y-%m-%d')] += 1
        
        # Calculate averages
        avg_tracks_per_day = total_tracks / max(len(activity_by_day), 1)
        avg_tracks_per_user = total_tracks / max(unique_users, 1)
        
        return {
            'guild_id': guild_id,
            'total_tracks': total_tracks,
            'total_duration': total_duration,
            'unique_users': unique_users,
            'unique_artists': unique_artists,
            'avg_tracks_per_day': avg_tracks_per_day,
            'avg_tracks_per_user': avg_tracks_per_user,
            'top_users': [{'user_id': u, 'count': c} for u, c in top_users],
            'top_artists': [{'artist': a, 'count': c} for a, c in top_artists],
            'activity_by_hour': dict(activity_by_hour),
            'activity_by_day': dict(activity_by_day),
            'first_activity': history[-1]['played_at'] if history else None,
            'last_activity': history[0]['played_at'] if history else None
        }
    
    def _calculate_global_statistics(self, history: List[Dict]) -> Dict:
        """Calculate global statistics from history"""
        if not history:
            return self._get_empty_global_stats()
        
        # Basic counts
        total_tracks = len(history)
        total_duration = sum(h.get('duration', 0) for h in history)
        unique_users = len(set(h.get('user_id', 0) for h in history))
        unique_guilds = len(set(h.get('guild_id', 0) for h in history))
        unique_artists = len(set(h.get('artist', '') for h in history))
        
        # Most active guilds
        guild_counts = defaultdict(int)
        for h in history:
            guild_id = h.get('guild_id', 0)
            guild_counts[guild_id] += 1
        
        top_guilds = sorted(guild_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Most played artists globally
        artist_counts = defaultdict(int)
        for h in history:
            artist = h.get('artist', 'Unknown')
            artist_counts[artist] += 1
        
        top_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Global trends
        trends_by_hour = defaultdict(int)
        trends_by_day = defaultdict(int)
        
        for h in history:
            played_at = h['played_at']
            trends_by_hour[played_at.hour] += 1
            trends_by_day[played_at.strftime('%Y-%m-%d')] += 1
        
        return {
            'total_tracks': total_tracks,
            'total_duration': total_duration,
            'unique_users': unique_users,
            'unique_guilds': unique_guilds,
            'unique_artists': unique_artists,
            'top_guilds': [{'guild_id': g, 'count': c} for g, c in top_guilds],
            'top_artists': [{'artist': a, 'count': c} for a, c in top_artists],
            'trends_by_hour': dict(trends_by_hour),
            'trends_by_day': dict(trends_by_day),
            'avg_tracks_per_guild': total_tracks / max(unique_guilds, 1),
            'avg_tracks_per_user': total_tracks / max(unique_users, 1),
            'first_global_activity': history[-1]['played_at'] if history else None,
            'last_global_activity': history[0]['played_at'] if history else None
        }
    
    async def _calculate_trends(self, user_id: int, history: List[Dict]) -> Dict:
        """Calculate listening trends for a user"""
        if len(history) < 7:
            return {}
        
        # Group by week
        weekly_counts = defaultdict(int)
        for h in history:
            week_key = h['played_at'].strftime('%Y-W%W')
            weekly_counts[week_key] += 1
        
        # Calculate trend
        if len(weekly_counts) >= 2:
            weeks = sorted(weekly_counts.keys())
            recent_week = weeks[-1]
            previous_week = weeks[-2] if len(weeks) >= 2 else weeks[-1]
            
            recent_count = weekly_counts[recent_week]
            previous_count = weekly_counts[previous_week]
            
            if previous_count > 0:
                trend_percent = ((recent_count - previous_count) / previous_count) * 100
            else:
                trend_percent = 100 if recent_count > 0 else 0
            
            trend_direction = "up" if trend_percent > 0 else "down" if trend_percent < 0 else "stable"
        else:
            trend_percent = 0
            trend_direction = "stable"
        
        # Calculate genre trends (would need genre data)
        
        return {
            'weekly_trend': trend_percent,
            'trend_direction': trend_direction,
            'weekly_counts': dict(weekly_counts),
            'current_week_count': weekly_counts.get(sorted(weekly_counts.keys())[-1], 0) if weekly_counts else 0
        }
    
    def _get_empty_stats(self) -> Dict:
        """Get empty statistics structure"""
        return {
            'user_id': 0,
            'total_tracks': 0,
            'total_duration': 0,
            'unique_artists': 0,
            'avg_tracks_per_day': 0,
            'avg_duration_per_track': 0,
            'top_artists': [],
            'top_tracks': [],
            'listening_by_hour': {},
            'listening_by_day': {},
            'listening_by_weekday': {},
            'peak_hour': {'hour': 0, 'count': 0},
            'peak_weekday': {'weekday': 0, 'count': 0, 'name': 'Monday'},
            'first_listen': None,
            'last_listen': None,
            'history_span_days': 0,
            'trends': {}
        }
    
    def _get_empty_server_stats(self) -> Dict:
        """Get empty server statistics structure"""
        return {
            'guild_id': 0,
            'total_tracks': 0,
            'total_duration': 0,
            'unique_users': 0,
            'unique_artists': 0,
            'avg_tracks_per_day': 0,
            'avg_tracks_per_user': 0,
            'top_users': [],
            'top_artists': [],
            'activity_by_hour': {},
            'activity_by_day': {},
            'first_activity': None,
            'last_activity': None
        }
    
    def _get_empty_global_stats(self) -> Dict:
        """Get empty global statistics structure"""
        return {
            'total_tracks': 0,
            'total_duration': 0,
            'unique_users': 0,
            'unique_guilds': 0,
            'unique_artists': 0,
            'top_guilds': [],
            'top_artists': [],
            'trends_by_hour': {},
            'trends_by_day': {},
            'avg_tracks_per_guild': 0,
            'avg_tracks_per_user': 0,
            'first_global_activity': None,
            'last_global_activity': None
        }
    
    async def create_heatmap_data(self, user_id: int, period: str = "month") -> List[List[int]]:
        """Create heatmap data for visualization"""
        if user_id not in self.history_db:
            return []
        
        history = self.history_db[user_id]
        
        # Filter by period
        if period == "month":
            cutoff = datetime.utcnow() - timedelta(days=30)
            history = [h for h in history if h['played_at'] >= cutoff]
        
        # Group by hour and weekday
        heatmap = [[0 for _ in range(24)] for _ in range(7)]  # 7 days, 24 hours
        
        for h in history:
            played_at = h['played_at']
            weekday = played_at.weekday()  # 0 = Monday
            hour = played_at.hour
            heatmap[weekday][hour] += 1
        
        return heatmap
    
    async def export_history(self, user_id: int, format: str = "json") -> Optional[str]:
        """Export user history in specified format"""
        if user_id not in self.history_db:
            return None
        
        history = self.history_db[user_id]
        
        if format.lower() == "json":
            export_data = {
                'user_id': user_id,
                'export_date': datetime.utcnow().isoformat(),
                'total_tracks': len(history),
                'history': [
                    {
                        'title': h['title'],
                        'artist': h['artist'],
                        'duration': h['duration'],
                        'played_at': h['played_at'].isoformat(),
                        'source': h.get('source', 'unknown')
                    }
                    for h in history
                ]
            }
            return json.dumps(export_data, indent=2, default=str)
        
        elif format.lower() == "csv":
            csv_lines = ["Title,Artist,Duration (ms),Played At,Source"]
            for h in history:
                csv_lines.append(
                    f'"{h["title"]}","{h["artist"]}",{h["duration"]},'
                    f'{h["played_at"].isoformat()},{h.get("source", "unknown")}'
                )
            return "\n".join(csv_lines)
        
        return None
    
    @commands.hybrid_command(
        name="history",
        description="Show your playback history"
    )
    async def history_command(
        self,
        ctx,
        limit: int = 10,
        period: str = "day",
        user: Optional[discord.User] = None
    ):
        """Show playback history"""
        target_user = user or ctx.author
        
        # Check permissions
        if user and user != ctx.author:
            if not ctx.author.guild_permissions.manage_guild:
                await ctx.send("You can only view your own history!")
                return
        
        # Get history
        history = await self.get_user_history(
            target_user.id,
            limit=limit,
            start_date=self._get_cutoff_date(period) if period != "all" else None
        )
        
        if not history:
            await ctx.send(f"No playback history found for {period} period.")
            return
        
        # Create embed
        embed = discord.Embed(
            title=f"📜 Playback History - {target_user.name}",
            description=f"Last {len(history)} tracks ({period})",
            color=self.bot.colors.PLUM,
            timestamp=datetime.utcnow()
        )
        
        for i, entry in enumerate(history, 1):
            played_at = entry['played_at']
            time_ago = self._format_time_ago(played_at)
            
            embed.add_field(
                name=f"{i}. {entry['title'][:50]}",
                value=(
                    f"**Artist:** {entry['artist']}\n"
                    f"**Duration:** {self._format_duration(entry['duration'])}\n"
                    f"**Played:** {time_ago}\n"
                    f"**Source:** {entry.get('source', 'Unknown')}"
                ),
                inline=False
            )
        
        # Add statistics summary
        stats = await self.get_user_statistics(target_user.id, period)
        embed.set_footer(
            text=f"Total: {stats['total_tracks']} tracks • {self._format_duration(stats['total_duration'])}"
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(
        name="stats",
        description="Show your music statistics"
    )
    async def stats_command(
        self,
        ctx,
        period: str = "all",
        user: Optional[discord.User] = None
    ):
        """Show detailed statistics"""
        target_user = user or ctx.author
        
        # Check permissions
        if user and user != ctx.author:
            if not ctx.author.guild_permissions.manage_guild:
                await ctx.send("You can only view your own statistics!")
                return
        
        # Get statistics
        stats = await self.get_user_statistics(target_user.id, period)
        
        if stats['total_tracks'] == 0:
            await ctx.send(f"No statistics available for {target_user.name}.")
            return
        
        # Create embed
        period_name = "All Time" if period == "all" else period.title()
        embed = discord.Embed(
            title=f"📊 Music Statistics - {target_user.name}",
            description=f"{period_name} Listening Overview",
            color=self.bot.colors.VIOLET,
            timestamp=datetime.utcnow()
        )
        
        # Basic statistics
        embed.add_field(
            name="📈 Overview",
            value=(
                f"**Total Tracks:** {stats['total_tracks']:,}\n"
                f"**Total Duration:** {self._format_duration(stats['total_duration'])}\n"
                f"**Unique Artists:** {stats['unique_artists']:,}\n"
                f"**Avg Tracks/Day:** {stats['avg_tracks_per_day']:.1f}"
            ),
            inline=True
        )
        
        # Listening patterns
        if stats['peak_hour']['count'] > 0:
            embed.add_field(
                name="🕒 Listening Patterns",
                value=(
                    f"**Peak Hour:** {stats['peak_hour']['hour']:02d}:00\n"
                    f"**Peak Day:** {stats['peak_weekday']['name']}\n"
                    f"**First Listen:** {stats['first_listen'].strftime('%Y-%m-%d') if stats['first_listen'] else 'Never'}\n"
                    f"**Last Listen:** {self._format_time_ago(stats['last_listen']) if stats['last_listen'] else 'Never'}"
                ),
                inline=True
            )
        
        # Top artists (if any)
        if stats['top_artists']:
            top_artist = stats['top_artists'][0]
            embed.add_field(
                name="🎤 Top Artist",
                value=(
                    f"**{top_artist['artist']}**\n"
                    f"Played {top_artist['count']} times\n"
                    f"({top_artist['count']/stats['total_tracks']*100:.1f}% of tracks)"
                ),
                inline=True
            )
        
        # Top tracks (if any)
        if stats['top_tracks']:
            top_track = stats['top_tracks'][0]
            embed.add_field(
                name="🎵 Most Played Track",
                value=(
                    f"**{top_track['track']}**\n"
                    f"Played {top_track['count']} times"
                ),
                inline=False
            )
        
        # Trends (if available)
        if 'trends' in stats and stats['trends']:
            trends = stats['trends']
            trend_emoji = "📈" if trends['trend_direction'] == 'up' else "📉" if trends['trend_direction'] == 'down' else "➡️"
            
            embed.add_field(
                name="📊 Weekly Trend",
                value=(
                    f"{trend_emoji} {abs(trends['weekly_trend']):.1f}% {trends['trend_direction']}\n"
                    f"This week: {trends['current_week_count']} tracks"
                ),
                inline=True
            )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(
        name="serverstats",
        description="Show server music statistics"
    )
    @commands.has_permissions(manage_guild=True)
    async def serverstats_command(self, ctx, period: str = "all"):
        """Show server statistics"""
        # Get server statistics
        stats = await self.get_server_statistics(ctx.guild.id, period)
        
        if stats['total_tracks'] == 0:
            await ctx.send("No music statistics available for this server.")
            return
        
        # Create embed
        period_name = "All Time" if period == "all" else period.title()
        embed = discord.Embed(
            title=f"📊 Server Music Statistics",
            description=f"{ctx.guild.name} • {period_name}",
            color=self.bot.colors.ROSE_GOLD,
            timestamp=datetime.utcnow()
        )
        
        # Basic statistics
        embed.add_field(
            name="📈 Overview",
            value=(
                f"**Total Tracks:** {stats['total_tracks']:,}\n"
                f"**Total Duration:** {self._format_duration(stats['total_duration'])}\n"
                f"**Unique Users:** {stats['unique_users']:,}\n"
                f"**Unique Artists:** {stats['unique_artists']:,}"
            ),
            inline=True
        )
        
        # Activity patterns
        embed.add_field(
            name="🎮 Activity",
            value=(
                f"**Avg Tracks/Day:** {stats['avg_tracks_per_day']:.1f}\n"
                f"**Avg Tracks/User:** {stats['avg_tracks_per_user']:.1f}\n"
                f"**First Activity:** {stats['first_activity'].strftime('%Y-%m-%d') if stats['first_activity'] else 'Never'}\n"
                f"**Last Activity:** {self._format_time_ago(stats['last_activity']) if stats['last_activity'] else 'Never'}"
            ),
            inline=True
        )
        
        # Top users (if any)
        if stats['top_users']:
            top_users_text = ""
            for i, user_data in enumerate(stats['top_users'][:3], 1):
                user = ctx.guild.get_member(user_data['user_id'])
                username = user.name if user else f"User {user_data['user_id']}"
                top_users_text += f"{i}. **{username}** - {user_data['count']} tracks\n"
            
            embed.add_field(
                name="👥 Top Listeners",
                value=top_users_text,
                inline=False
            )
        
        # Top artists (if any)
        if stats['top_artists']:
            top_artists_text = ""
            for i, artist_data in enumerate(stats['top_artists'][:3], 1):
                top_artists_text += f"{i}. **{artist_data['artist']}** - {artist_data['count']} plays\n"
            
            embed.add_field(
                name="🎤 Popular Artists",
                value=top_artists_text,
                inline=False
            )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(
        name="export",
        description="Export your listening history"
    )
    async def export_command(self, ctx, format: str = "json"):
        """Export listening history"""
        if format.lower() not in ["json", "csv"]:
            await ctx.send("Available formats: json, csv")
            return
        
        # Export data
        export_data = await self.export_history(ctx.author.id, format)
        
        if not export_data:
            await ctx.send("No history to export!")
            return
        
        # Create file
        filename = f"eve_history_{ctx.author.id}_{datetime.utcnow().strftime('%Y%m%d')}.{format.lower()}"
        
        # Send as file (Discord has attachment size limits)
        if len(export_data) > 8000:  # Approximate Discord limit
            # For large files, we'd need to split or use external storage
            await ctx.send("History is too large to export via Discord. Try a shorter period.")
            return
        
        # Create and send file
        file = discord.File(
            filename=filename,
            fp=export_data.encode('utf-8')
        )
        
        await ctx.send(
            f"Here's your listening history export! 📁",
            file=file
        )
    
    @commands.hybrid_command(
        name="yearinreview",
        description="Get your year in review"
    )
    async def yearinreview_command(self, ctx, year: Optional[int] = None):
        """Get personalized year in review"""
        target_year = year or datetime.utcnow().year
        
        # Get history for the year
        start_date = datetime(target_year, 1, 1)
        end_date = datetime(target_year + 1, 1, 1)
        
        history = await self.get_user_history(
            ctx.author.id,
            limit=10000,  # Large limit to get all
            start_date=start_date,
            end_date=end_date
        )
        
        if not history:
            await ctx.send(f"No listening data found for {target_year}.")
            return
        
        # Calculate year statistics
        stats = self._calculate_statistics(history, ctx.author.id)
        
        # Create year in review embed
        embed = discord.Embed(
            title=f"🎉 {target_year} Year in Review",
            description=f"{ctx.author.name}'s Music Journey",
            color=0xFF6B6B,  # Special color for year review
            timestamp=datetime.utcnow()
        )
        
        # Add thumbnail
        if ctx.author.avatar:
            embed.set_thumbnail(url=ctx.author.avatar.url)
        
        # Year statistics
        embed.add_field(
            name="📊 Year Summary",
            value=(
                f"**Total Tracks:** {stats['total_tracks']:,}\n"
                f"**Total Listening:** {self._format_duration(stats['total_duration'])}\n"
                f"**Unique Artists:** {stats['unique_artists']:,}\n"
                f"**Most Active Month:** {self._get_most_active_month(history)}"
            ),
            inline=False
        )
        
        # Top artist
        if stats['top_artists']:
            top_artist = stats['top_artists'][0]
            embed.add_field(
                name="🎤 Artist of the Year",
                value=f"**{top_artist['artist']}**\n{top_artist['count']} plays",
                inline=True
            )
        
        # Top track
        if stats['top_tracks']:
            top_track = stats['top_tracks'][0]
            embed.add_field(
                name="🎵 Track of the Year",
                value=f"**{top_track['track']}**\n{top_track['count']} plays",
                inline=True
            )
        
        # Listening personality
        personality = self._get_listening_personality(stats)
        embed.add_field(
            name="🎭 Listening Personality",
            value=personality,
            inline=False
        )
        
        # Fun facts
        fun_facts = self._generate_fun_facts(stats, history)
        if fun_facts:
            embed.add_field(
                name="✨ Fun Facts",
                value="\n".join(f"• {fact}" for fact in fun_facts[:3]),
                inline=False
            )
        
        embed.set_footer(text="Thanks for listening with EVE! ✨")
        
        await ctx.send(embed=embed)
    
    def _format_duration(self, milliseconds: int) -> str:
        """Format duration for display"""
        seconds = milliseconds // 1000
        
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
    
    def _format_time_ago(self, dt: datetime) -> str:
        """Format time ago string"""
        now = datetime.utcnow()
        diff = now - dt
        
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
    
    def _get_most_active_month(self, history: List[Dict]) -> str:
        """Get most active month from history"""
        if not history:
            return "None"
        
        month_counts = defaultdict(int)
        for h in history:
            month_key = h['played_at'].strftime('%B')  # Full month name
            month_counts[month_key] += 1
        
        if month_counts:
            most_active = max(month_counts.items(), key=lambda x: x[1])
            return f"{most_active[0]} ({most_active[1]} tracks)"
        
        return "None"
    
    def _get_listening_personality(self, stats: Dict) -> str:
        """Generate listening personality based on statistics"""
        if stats['total_tracks'] == 0:
            return "Just getting started!"
        
        traits = []
        
        # Based on track count
        if stats['total_tracks'] > 1000:
            traits.append("Music Addict")
        elif stats['total_tracks'] > 500:
            traits.append("Music Enthusiast")
        elif stats['total_tracks'] > 100:
            traits.append("Casual Listener")
        else:
            traits.append("New Explorer")
        
        # Based on artist diversity
        diversity_ratio = stats['unique_artists'] / max(stats['total_tracks'], 1)
        if diversity_ratio > 0.7:
            traits.append("Eclectic Explorer")
        elif diversity_ratio > 0.4:
            traits.append("Balanced Taste")
        else:
            traits.append("Loyal Fan")
        
        # Based on listening patterns
        if stats['peak_hour']['hour'] < 6 or stats['peak_hour']['hour'] > 22:
            traits.append("Night Owl")
        elif 9 <= stats['peak_hour']['hour'] <= 17:
            traits.append("Daytime Listener")
        
        return " • ".join(traits)
    
    def _generate_fun_facts(self, stats: Dict, history: List[Dict]) -> List[str]:
        """Generate fun facts from statistics"""
        facts = []
        
        if not history:
            return facts
        
        # Longest listening session
        if len(history) > 1:
            # Calculate consecutive listens within 30 minutes
            session_durations = []
            current_session_start = history[0]['played_at']
            current_session_duration = 0
            
            for i in range(len(history) - 1):
                time_diff = (history[i]['played_at'] - history[i + 1]['played_at']).total_seconds()
                
                if time_diff < 1800:  # 30 minutes
                    current_session_duration += history[i]['duration']
                else:
                    session_durations.append(current_session_duration)
                    current_session_start = history[i + 1]['played_at']
                    current_session_duration = history[i + 1]['duration']
            
            if session_durations:
                longest_session = max(session_durations)
                facts.append(f"Longest listening session: {self._format_duration(longest_session)}")
        
        # Most played hour
        if stats['peak_hour']['count'] > 0:
            hour = stats['peak_hour']['hour']
            hour_str = f"{hour:02d}:00"
            facts.append(f"Most active hour: {hour_str}")
        
        # Top artist percentage
        if stats['top_artists']:
            top_artist = stats['top_artists'][0]
            percentage = (top_artist['count'] / stats['total_tracks']) * 100
            facts.append(f"{top_artist['artist']} was {percentage:.1f}% of your listening")
        
        return facts
    
    @tasks.loop(minutes=5)
    async def save_history(self):
        """Save history to database"""
        # This would save to persistent storage
        # For now, just log
        logger.debug(f"History stats: {len(self.history_db)} users, {len(self.global_history)} total plays")
    
    @tasks.loop(hours=6)
    async def cleanup_old_data(self):
        """Clean up old data to prevent memory issues"""
        cutoff = datetime.utcnow() - timedelta(days=90)  # Keep 90 days
        
        # Clean user history
        for user_id in list(self.history_db.keys()):
            self.history_db[user_id] = [
                h for h in self.history_db[user_id]
                if h['played_at'] >= cutoff
            ]
            if not self.history_db[user_id]:
                self.history_db.pop(user_id, None)
        
        # Clean server history
        for guild_id in list(self.server_history.keys()):
            self.server_history[guild_id] = [
                h for h in self.server_history[guild_id]
                if h['played_at'] >= cutoff
            ]
            if not self.server_history[guild_id]:
                self.server_history.pop(guild_id, None)
        
        # Clean global history
        self.global_history = [
            h for h in self.global_history
            if h['played_at'] >= cutoff
        ]
        
        # Clear cache
        self.user_stats_cache.clear()
        self.server_stats_cache.clear()
        self.global_stats_cache.clear()
        
        logger.info("Cleaned up old history data")
    
    @tasks.loop(minutes=30)
    async def update_stats_cache(self):
        """Update statistics cache"""
        # Update global stats cache periodically
        for period in ["day", "week", "month", "all"]:
            await self.get_global_statistics(period)
"""
Professional caching system with:
- Audio cache for faster playback
- Search result caching
- Metadata caching
- Intelligent prefetching
- Cache optimization and LRU eviction
"""

import discord
from discord.ext import commands, tasks
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import asyncio
import hashlib
import json
import pickle
from collections import OrderedDict
import logging

logger = logging.getLogger(__name__)

class CacheEntry:
    """Cache entry with metadata"""
    
    def __init__(self, data: Any, ttl: int = 3600):
        self.data = data
        self.created_at = datetime.utcnow()
        self.last_accessed = datetime.utcnow()
        self.access_count = 0
        self.ttl = ttl  # Time to live in seconds
        self.size = self._calculate_size(data)
    
    def _calculate_size(self, data: Any) -> int:
        """Calculate approximate size of data"""
        try:
            return len(pickle.dumps(data))
        except:
            return 1024  # Default 1KB
    
    def is_expired(self) -> bool:
        """Check if cache entry is expired"""
        return (datetime.utcnow() - self.created_at).total_seconds() > self.ttl
    
    def access(self) -> Any:
        """Record access and return data"""
        self.last_accessed = datetime.utcnow()
        self.access_count += 1
        return self.data
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'data': self.data,
            'created_at': self.created_at.isoformat(),
            'last_accessed': self.last_accessed.isoformat(),
            'access_count': self.access_count,
            'ttl': self.ttl,
            'size': self.size
        }


class CacheManager:
    """Professional cache management system"""
    
    def __init__(self, bot, max_size_mb: int = 100):
        self.bot = bot
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.current_size_bytes = 0
        
        # Cache stores
        self.search_cache: Dict[str, CacheEntry] = OrderedDict()  # search_query: results
        self.track_cache: Dict[str, CacheEntry] = OrderedDict()   # track_id: track_data
        self.metadata_cache: Dict[str, CacheEntry] = OrderedDict()  # url: metadata
        self.user_cache: Dict[str, CacheEntry] = OrderedDict()    # user_data: cached
        
        # Statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'total_queries': 0,
            'size_history': [],
            'hit_rate': 0.0
        }
        
        # Prefetch prediction
        self.prefetch_predictor = PrefetchPredictor()
        
        # Background tasks
        self.cleanup_expired.start()
        self.save_cache_stats.start()
        self.optimize_cache.start()
    
    def generate_cache_key(self, *args, **kwargs) -> str:
        """Generate consistent cache key from arguments"""
        key_parts = []
        
        # Add args
        for arg in args:
            if isinstance(arg, (str, int, float, bool)):
                key_parts.append(str(arg))
            elif arg is None:
                key_parts.append('None')
            else:
                key_parts.append(hashlib.md5(pickle.dumps(arg)).hexdigest()[:8])
        
        # Add kwargs
        for key, value in sorted(kwargs.items()):
            key_parts.append(f"{key}:{str(value)}")
        
        # Combine and hash
        key_string = ":".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    async def cache_search_results(self, query: str, results: List[Any], ttl: int = 1800):
        """Cache search results"""
        cache_key = self.generate_cache_key("search", query.lower())
        
        # Check if already cached
        if cache_key in self.search_cache:
            return
        
        # Create cache entry
        entry = CacheEntry(results, ttl)
        
        # Store in cache
        self.search_cache[cache_key] = entry
        self.current_size_bytes += entry.size
        
        # Evict if needed
        await self._evict_if_needed()
        
        # Update prefetch predictor
        self.prefetch_predictor.record_search(query)
    
    async def get_cached_search(self, query: str) -> Optional[List[Any]]:
        """Get cached search results"""
        cache_key = self.generate_cache_key("search", query.lower())
        
        self.stats['total_queries'] += 1
        
        if cache_key in self.search_cache:
            entry = self.search_cache[cache_key]
            
            if entry.is_expired():
                # Remove expired entry
                self.search_cache.pop(cache_key)
                self.current_size_bytes -= entry.size
                self.stats['misses'] += 1
                return None
            
            # Update LRU order
            self.search_cache.move_to_end(cache_key)
            
            self.stats['hits'] += 1
            return entry.access()
        
        self.stats['misses'] += 1
        return None
    
    async def cache_track(self, track_id: str, track_data: Dict, ttl: int = 86400):
        """Cache track data for faster playback"""
        cache_key = self.generate_cache_key("track", track_id)
        
        # Check if already cached
        if cache_key in self.track_cache:
            # Update existing entry
            old_entry = self.track_cache[cache_key]
            self.current_size_bytes -= old_entry.size
        
        # Create cache entry
        entry = CacheEntry(track_data, ttl)
        
        # Store in cache
        self.track_cache[cache_key] = entry
        self.current_size_bytes += entry.size
        
        # Evict if needed
        await self._evict_if_needed()
    
    async def get_cached_track(self, track_id: str) -> Optional[Dict]:
        """Get cached track data"""
        cache_key = self.generate_cache_key("track", track_id)
        
        self.stats['total_queries'] += 1
        
        if cache_key in self.track_cache:
            entry = self.track_cache[cache_key]
            
            if entry.is_expired():
                # Remove expired entry
                self.track_cache.pop(cache_key)
                self.current_size_bytes -= entry.size
                self.stats['misses'] += 1
                return None
            
            # Update LRU order
            self.track_cache.move_to_end(cache_key)
            
            self.stats['hits'] += 1
            return entry.access()
        
        self.stats['misses'] += 1
        return None
    
    async def cache_metadata(self, url: str, metadata: Dict, ttl: int = 3600):
        """Cache metadata (artist info, album art, etc.)"""
        cache_key = self.generate_cache_key("metadata", url)
        
        # Check if already cached
        if cache_key in self.metadata_cache:
            # Update existing entry
            old_entry = self.metadata_cache[cache_key]
            self.current_size_bytes -= old_entry.size
        
        # Create cache entry
        entry = CacheEntry(metadata, ttl)
        
        # Store in cache
        self.metadata_cache[cache_key] = entry
        self.current_size_bytes += entry.size
        
        # Evict if needed
        await self._evict_if_needed()
    
    async def get_cached_metadata(self, url: str) -> Optional[Dict]:
        """Get cached metadata"""
        cache_key = self.generate_cache_key("metadata", url)
        
        self.stats['total_queries'] += 1
        
        if cache_key in self.metadata_cache:
            entry = self.metadata_cache[cache_key]
            
            if entry.is_expired():
                # Remove expired entry
                self.metadata_cache.pop(cache_key)
                self.current_size_bytes -= entry.size
                self.stats['misses'] += 1
                return None
            
            # Update LRU order
            self.metadata_cache.move_to_end(cache_key)
            
            self.stats['hits'] += 1
            return entry.access()
        
        self.stats['misses'] += 1
        return None
    
    async def cache_user_data(self, user_id: int, data_type: str, data: Any, ttl: int = 1800):
        """Cache user-specific data"""
        cache_key = self.generate_cache_key("user", user_id, data_type)
        
        # Check if already cached
        if cache_key in self.user_cache:
            # Update existing entry
            old_entry = self.user_cache[cache_key]
            self.current_size_bytes -= old_entry.size
        
        # Create cache entry
        entry = CacheEntry(data, ttl)
        
        # Store in cache
        self.user_cache[cache_key] = entry
        self.current_size_bytes += entry.size
        
        # Evict if needed
        await self._evict_if_needed()
    
    async def get_cached_user_data(self, user_id: int, data_type: str) -> Optional[Any]:
        """Get cached user data"""
        cache_key = self.generate_cache_key("user", user_id, data_type)
        
        self.stats['total_queries'] += 1
        
        if cache_key in self.user_cache:
            entry = self.user_cache[cache_key]
            
            if entry.is_expired():
                # Remove expired entry
                self.user_cache.pop(cache_key)
                self.current_size_bytes -= entry.size
                self.stats['misses'] += 1
                return None
            
            # Update LRU order
            self.user_cache.move_to_end(cache_key)
            
            self.stats['hits'] += 1
            return entry.access()
        
        self.stats['misses'] += 1
        return None
    
    async def _evict_if_needed(self):
        """Evict cache entries if size limit exceeded"""
        while self.current_size_bytes > self.max_size_bytes:
            await self._evict_oldest()
    
    async def _evict_oldest(self, count: int = 1):
        """Evict oldest cache entries"""
        evicted = 0
        
        # Try to evict from search cache first (usually largest)
        while evicted < count and self.search_cache:
            key, entry = self.search_cache.popitem(last=False)
            self.current_size_bytes -= entry.size
            evicted += 1
            self.stats['evictions'] += 1
        
        # Then metadata cache
        while evicted < count and self.metadata_cache:
            key, entry = self.metadata_cache.popitem(last=False)
            self.current_size_bytes -= entry.size
            evicted += 1
            self.stats['evictions'] += 1
        
        # Then user cache
        while evicted < count and self.user_cache:
            key, entry = self.user_cache.popitem(last=False)
            self.current_size_bytes -= entry.size
            evicted += 1
            self.stats['evictions'] += 1
        
        # Finally track cache (most valuable)
        while evicted < count and self.track_cache:
            key, entry = self.track_cache.popitem(last=False)
            self.current_size_bytes -= entry.size
            evicted += 1
            self.stats['evictions'] += 1
        
        if evicted > 0:
            logger.debug(f"Evicted {evicted} cache entries. Current size: {self.current_size_bytes / 1024 / 1024:.2f} MB")
    
    async def prefetch_related(self, current_track: Dict):
        """Prefetch related tracks for faster playback"""
        try:
            # Get related tracks
            artist = current_track.get('artist', '')
            title = current_track.get('title', '')
            
            # Predict what might be played next
            predictions = self.prefetch_predictor.predict_next(artist, title)
            
            for prediction in predictions[:3]:  # Prefetch top 3 predictions
                # This would trigger async prefetching
                # For now, just log
                logger.debug(f"Prefetch prediction: {prediction}")
        
        except Exception as e:
            logger.error(f"Prefetch error: {e}")
    
    async def clear_cache(self, cache_type: Optional[str] = None):
        """Clear cache or specific cache type"""
        cleared_size = 0
        
        if cache_type is None or cache_type == "search":
            cleared_size += sum(entry.size for entry in self.search_cache.values())
            self.search_cache.clear()
        
        if cache_type is None or cache_type == "track":
            cleared_size += sum(entry.size for entry in self.track_cache.values())
            self.track_cache.clear()
        
        if cache_type is None or cache_type == "metadata":
            cleared_size += sum(entry.size for entry in self.metadata_cache.values())
            self.metadata_cache.clear()
        
        if cache_type is None or cache_type == "user":
            cleared_size += sum(entry.size for entry in self.user_cache.values())
            self.user_cache.clear()
        
        self.current_size_bytes -= cleared_size
        
        logger.info(f"Cleared cache: {cleared_size / 1024 / 1024:.2f} MB")
        return cleared_size
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        # Calculate hit rate
        total = self.stats['hits'] + self.stats['misses']
        hit_rate = (self.stats['hits'] / total * 100) if total > 0 else 0
        
        # Update stats
        self.stats['hit_rate'] = hit_rate
        self.stats['current_size_mb'] = self.current_size_bytes / 1024 / 1024
        self.stats['max_size_mb'] = self.max_size_bytes / 1024 / 1024
        
        # Cache counts
        self.stats['search_cache_count'] = len(self.search_cache)
        self.stats['track_cache_count'] = len(self.track_cache)
        self.stats['metadata_cache_count'] = len(self.metadata_cache)
        self.stats['user_cache_count'] = len(self.user_cache)
        
        return self.stats
    
    @tasks.loop(minutes=5)
    async def cleanup_expired(self):
        """Clean up expired cache entries"""
        now = datetime.utcnow()
        expired_count = 0
        freed_size = 0
        
        # Clean search cache
        expired_keys = []
        for key, entry in self.search_cache.items():
            if entry.is_expired():
                expired_keys.append(key)
                freed_size += entry.size
        
        for key in expired_keys:
            self.search_cache.pop(key, None)
        expired_count += len(expired_keys)
        
        # Clean track cache
        expired_keys = []
        for key, entry in self.track_cache.items():
            if entry.is_expired():
                expired_keys.append(key)
                freed_size += entry.size
        
        for key in expired_keys:
            self.track_cache.pop(key, None)
        expired_count += len(expired_keys)
        
        # Clean metadata cache
        expired_keys = []
        for key, entry in self.metadata_cache.items():
            if entry.is_expired():
                expired_keys.append(key)
                freed_size += entry.size
        
        for key in expired_keys:
            self.metadata_cache.pop(key, None)
        expired_count += len(expired_keys)
        
        # Clean user cache
        expired_keys = []
        for key, entry in self.user_cache.items():
            if entry.is_expired():
                expired_keys.append(key)
                freed_size += entry.size
        
        for key in expired_keys:
            self.user_cache.pop(key, None)
        expired_count += len(expired_keys)
        
        # Update current size
        self.current_size_bytes -= freed_size
        
        if expired_count > 0:
            logger.debug(f"Cleaned {expired_count} expired cache entries, freed {freed_size / 1024:.2f} KB")
    
    @tasks.loop(minutes=15)
    async def save_cache_stats(self):
        """Save cache statistics"""
        # Record size history
        self.stats['size_history'].append({
            'timestamp': datetime.utcnow().isoformat(),
            'size_mb': self.current_size_bytes / 1024 / 1024,
            'hit_rate': self.stats['hit_rate']
        })
        
        # Keep only last 100 entries
        if len(self.stats['size_history']) > 100:
            self.stats['size_history'] = self.stats['size_history'][-100:]
    
    @tasks.loop(hours=1)
    async def optimize_cache(self):
        """Optimize cache by rebalancing TTLs"""
        # Calculate access patterns
        search_access_rate = self._calculate_access_rate(self.search_cache)
        track_access_rate = self._calculate_access_rate(self.track_cache)
        
        # Adjust TTLs based on access patterns
        # Frequently accessed items get longer TTL
        for key, entry in self.search_cache.items():
            if entry.access_count > 10:  # Frequently accessed
                entry.ttl = min(entry.ttl * 1.5, 86400)  # Increase TTL, max 1 day
            elif entry.access_count == 0:  # Never accessed
                entry.ttl = max(entry.ttl * 0.5, 300)  # Decrease TTL, min 5 minutes
        
        for key, entry in self.track_cache.items():
            if entry.access_count > 5:  # Frequently accessed tracks
                entry.ttl = min(entry.ttl * 2, 604800)  # Increase TTL, max 1 week
    
    def _calculate_access_rate(self, cache: Dict[str, CacheEntry]) -> float:
        """Calculate average access rate for cache"""
        if not cache:
            return 0.0
        
        total_accesses = sum(entry.access_count for entry in cache.values())
        avg_age = sum(
            (datetime.utcnow() - entry.created_at).total_seconds()
            for entry in cache.values()
        ) / len(cache)
        
        if avg_age > 0:
            return total_accesses / avg_age
        return 0.0
    
    @commands.hybrid_command(
        name="cachestats",
        description="Show cache statistics"
    )
    async def cachestats_command(self, ctx):
        """Show cache statistics"""
        stats = self.get_cache_stats()
        
        embed = discord.Embed(
            title="📊 Cache Statistics",
            color=self.bot.colors.VIOLET,
            timestamp=datetime.utcnow()
        )
        
        # Performance
        embed.add_field(
            name="🎯 Performance",
            value=(
                f"**Hit Rate:** {stats['hit_rate']:.1f}%\n"
                f"**Total Queries:** {stats['total_queries']:,}\n"
                f"**Hits:** {stats['hits']:,}\n"
                f"**Misses:** {stats['misses']:,}\n"
                f"**Evictions:** {stats['evictions']:,}"
            ),
            inline=True
        )
        
        # Size
        embed.add_field(
            name="💾 Size",
            value=(
                f"**Current:** {stats['current_size_mb']:.2f} MB\n"
                f"**Max:** {stats['max_size_mb']:.2f} MB\n"
                f"**Usage:** {(stats['current_size_mb']/stats['max_size_mb']*100):.1f}%"
            ),
            inline=True
        )
        
        # Cache counts
        embed.add_field(
            name="📁 Cache Counts",
            value=(
                f"**Search:** {stats['search_cache_count']:,}\n"
                f"**Tracks:** {stats['track_cache_count']:,}\n"
                f"**Metadata:** {stats['metadata_cache_count']:,}\n"
                f"**User Data:** {stats['user_cache_count']:,}"
            ),
            inline=True
        )
        
        # Recommendations
        recommendations = []
        if stats['hit_rate'] < 50:
            recommendations.append("Consider increasing cache size")
        if stats['evictions'] > 100:
            recommendations.append("Frequent evictions - cache may be too small")
        
        if recommendations:
            embed.add_field(
                name="💡 Recommendations",
                value="\n".join(f"• {rec}" for rec in recommendations),
                inline=False
            )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(
        name="clearcache",
        description="Clear cache"
    )
    @commands.has_permissions(administrator=True)
    async def clearcache_command(self, ctx, cache_type: Optional[str] = None):
        """Clear cache"""
        valid_types = ["search", "track", "metadata", "user", None]
        
        if cache_type not in valid_types:
            await ctx.send(f"Valid cache types: {', '.join(t for t in valid_types if t)}")
            return
        
        cleared_size = await self.clear_cache(cache_type)
        
        embed = discord.Embed(
            title="🧹 Cache Cleared",
            description=f"Cleared {cache_type or 'all'} cache",
            color=self.bot.colors.ROSE_GOLD
        )
        
        embed.add_field(
            name="Freed Space",
            value=f"{cleared_size / 1024 / 1024:.2f} MB",
            inline=True
        )
        
        # Update stats
        stats = self.get_cache_stats()
        embed.add_field(
            name="New Size",
            value=f"{stats['current_size_mb']:.2f} MB / {stats['max_size_mb']:.2f} MB",
            inline=True
        )
        
        await ctx.send(embed=embed)


class PrefetchPredictor:
    """Predict what to prefetch based on listening patterns"""
    
    def __init__(self):
        self.transition_counts: Dict[str, Dict[str, int]] = {}  # from_track -> {to_track: count}
        self.artist_transitions: Dict[str, Dict[str, int]] = {}  # from_artist -> {to_artist: count}
        self.recent_plays: List[Tuple[str, str]] = []  # (artist, title)
        
        # Learning parameters
        self.learning_rate = 0.1
        self.decay_factor = 0.99
    
    def record_play(self, artist: str, title: str):
        """Record a track play for learning"""
        track_key = f"{artist} - {title}"
        
        # Record transition from previous track
        if self.recent_plays:
            prev_artist, prev_title = self.recent_plays[-1]
            prev_key = f"{prev_artist} - {prev_title}"
            
            # Update track-to-track transitions
            if prev_key not in self.transition_counts:
                self.transition_counts[prev_key] = {}
            
            self.transition_counts[prev_key][track_key] = \
                self.transition_counts[prev_key].get(track_key, 0) + 1
            
            # Update artist-to-artist transitions
            if prev_artist not in self.artist_transitions:
                self.artist_transitions[prev_artist] = {}
            
            self.artist_transitions[prev_artist][artist] = \
                self.artist_transitions[prev_artist].get(artist, 0) + 1
        
        # Add to recent plays
        self.recent_plays.append((artist, title))
        
        # Keep only recent history
        if len(self.recent_plays) > 100:
            self.recent_plays.pop(0)
        
        # Apply decay to old transitions
        self._apply_decay()
    
    def record_search(self, query: str):
        """Record search for prediction"""
        # This could be used to predict what users search for after certain tracks
        pass
    
    def predict_next(self, current_artist: str, current_title: str) -> List[str]:
        """Predict next tracks to play"""
        current_key = f"{current_artist} - {current_title}"
        predictions = []
        
        # Predict based on track transitions
        if current_key in self.transition_counts:
            transitions = self.transition_counts[current_key]
            sorted_transitions = sorted(transitions.items(), key=lambda x: x[1], reverse=True)
            
            for track_key, count in sorted_transitions[:5]:  # Top 5
                predictions.append(track_key)
        
        # Predict based on artist transitions
        if current_artist in self.artist_transitions:
            artist_transitions = self.artist_transitions[current_artist]
            sorted_artists = sorted(artist_transitions.items(), key=lambda x: x[1], reverse=True)
            
            for artist, count in sorted_artists[:3]:  # Top 3 artists
                predictions.append(f"{artist} - ")
        
        # Add some randomness for exploration
        if len(predictions) < 3:
            predictions.extend([
                f"{current_artist} - ",
                "similar to ",
                "popular "
            ])
        
        return predictions[:5]  # Return top 5 predictions
    
    def _apply_decay(self):
        """Apply decay to transition counts to favor recent patterns"""
        for from_track in self.transition_counts:
            for to_track in list(self.transition_counts[from_track].keys()):
                self.transition_counts[from_track][to_track] *= self.decay_factor
                
                # Remove very low probability transitions
                if self.transition_counts[from_track][to_track] < 0.1:
                    self.transition_counts[from_track].pop(to_track)
        
        for from_artist in self.artist_transitions:
            for to_artist in list(self.artist_transitions[from_artist].keys()):
                self.artist_transitions[from_artist][to_artist] *= self.decay_factor
                
                # Remove very low probability transitions
                if self.artist_transitions[from_artist][to_artist] < 0.1:
                    self.artist_transitions[from_artist].pop(to_artist)
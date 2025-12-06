"""
Professional Lavalink client with connection pooling, failover, 
auto-recovery, and optimization for 500+ concurrent players
"""

import wavelink
try:
    from wavelink import spotify
except ImportError:
    spotify = None
import aiohttp
import asyncio
from typing import Optional, Dict, List, Tuple, TYPE_CHECKING, Any
import logging
import json
from datetime import datetime, timedelta
import random
from wavelink import Node, Player, Track, Playlist

logger = logging.getLogger(__name__)

class OptimizedLavalinkClient:
    """Professional Lavalink client with all optimizations"""
    
    def __init__(self, bot):
        self.bot = bot
        self.nodes: List[wavelink.Node] = []
        self.active_node: Optional[wavelink.Node] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.spotify_client: Optional[Any] = None
        
        # Node statistics
        self.node_stats: Dict[str, Dict] = {}
        
        # Connection pool
        self.connection_pool = aiohttp.TCPConnector(
            limit=200,  # Increased for 500+ players
            limit_per_host=100,
            ttl_dns_cache=600,
            enable_cleanup_closed=True,
            force_close=True,
            use_dns_cache=True
        )
        
        # Node configuration
        self.node_configs = [
            {
                'identifier': 'MAIN_NODE',
                'host': 'localhost',
                'port': 2333,
                'password': 'youshallnotpass',
                'region': 'global',
                'resume_key': 'EVE_MUSIC_MAIN',
                'resume_timeout': 600,
                'reconnect_attempts': 5,
                'retries': 3,
                'pool_size': 100,
                'max_players': 500,
                'player_update_interval': 15,
                'ping_interval': 30
            }
        ]
        
        # Add fallback nodes if configured
        if hasattr(bot.config, 'LAVALINK_FALLBACK_NODES'):
            self.node_configs.extend(bot.config.LAVALINK_FALLBACK_NODES)
        
        # Performance tracking
        self.performance_metrics = {
            'search_times': [],
            'connection_times': [],
            'player_create_times': []
        }
        
        # Source priority (for auto-selection)
        self.source_priority = [
            wavelink.TrackSource.YouTubeMusic,
            wavelink.TrackSource.YouTube,
            wavelink.TrackSource.SoundCloud,
            wavelink.TrackSource.Spotify
        ]
    
    async def create_session(self):
        """Create optimized aiohttp session"""
        timeout = aiohttp.ClientTimeout(
            total=45,
            connect=15,
            sock_read=30,
            sock_connect=15
        )
        
        self.session = aiohttp.ClientSession(
            connector=self.connection_pool,
            timeout=timeout,
            headers={
                'User-Agent': 'EVE-Music-Bot/1.0 (Professional Music System)'
            }
        )
        
        # Set session for wavelink
        wavelink.Pool.set_session(self.session)
        logger.info("Created optimized HTTP session")
    
    async def connect_lavalink(self):
        """Connect to Lavalink with all optimizations"""
        if not self.session:
            await self.create_session()
        
        connected = False
        for config in self.node_configs:
            try:
                logger.info(f"Connecting to node: {config['identifier']}")
                
                # Create node with optimized settings
                node = await wavelink.NodePool.create_node(
                    bot=self.bot,
                    identifier=config['identifier'],
                    host=config['host'],
                    port=config['port'],
                    password=config['password'],
                    region=config.get('region', 'global'),
                    resume_key=config.get('resume_key', 'EVE_MUSIC'),
                    resume_timeout=config.get('resume_timeout', 300),
                    reconnect_attempts=config.get('reconnect_attempts', 3),
                    retries=config.get('retries', 3),
                    pool_size=config.get('pool_size', 50),
                    secure=config.get('secure', False)
                )
                
                self.nodes.append(node)
                self.node_stats[config['identifier']] = {
                    'connected_at': datetime.utcnow(),
                    'connection_attempts': 1,
                    'last_ping': None,
                    'players': 0,
                    'playing': 0,
                    'load': 0
                }
                
                if not self.active_node:
                    self.active_node = node
                
                logger.info(f"✅ Connected to {config['identifier']} at {config['host']}:{config['port']}")
                connected = True
                
                # Initialize Spotify if credentials available
                await self.initialize_spotify()
                
            except Exception as e:
                logger.error(f"❌ Failed to connect to {config['identifier']}: {e}")
                continue
        
        if not connected:
            logger.critical("❌ Failed to connect to any Lavalink node!")
            raise ConnectionError("Could not connect to any Lavalink node")
        
        # Start monitoring task
        self.bot.loop.create_task(self.monitor_nodes())
        
        return connected
    
    async def initialize_spotify(self):
        """Initialize Spotify client if credentials available"""
        try:
            if (hasattr(self.bot.config, 'SPOTIFY_CLIENT_ID') and 
                hasattr(self.bot.config, 'SPOTIFY_CLIENT_SECRET')):
                
                self.spotify_client = spotify.SpotifyClient(
                    client_id=self.bot.config.SPOTIFY_CLIENT_ID,
                    client_secret=self.bot.config.SPOTIFY_CLIENT_SECRET
                )
                
                logger.info("✅ Spotify client initialized")
        except Exception as e:
            logger.warning(f"⚠️ Could not initialize Spotify: {e}")
    
    async def monitor_nodes(self):
        """Monitor node health and switch if needed"""
        while True:
            try:
                for node in self.nodes:
                    try:
                        # Get node stats
                        stats = await node.fetch_stats()
                        
                        self.node_stats[node.identifier].update({
                            'last_ping': datetime.utcnow(),
                            'players': stats.players,
                            'playing': stats.playing,
                            'cpu_cores': stats.cpu_cores,
                            'system_load': stats.system_load,
                            'memory_used': stats.memory_used,
                            'memory_free': stats.memory_free,
                            'uptime': stats.uptime
                        })
                        
                        # Calculate load percentage
                        load = (stats.playing / max(stats.players, 1)) * 100
                        self.node_stats[node.identifier]['load'] = load
                        
                        # Switch to less loaded node if current is overloaded
                        if (node == self.active_node and load > 80 and 
                            len(self.nodes) > 1):
                            
                            less_loaded = min(
                                self.nodes, 
                                key=lambda n: self.node_stats[n.identifier].get('load', 0)
                            )
                            
                            if less_loaded != node:
                                logger.info(f"Switching from {node.identifier} (load: {load:.1f}%) "
                                          f"to {less_loaded.identifier} "
                                          f"(load: {self.node_stats[less_loaded.identifier].get('load', 0):.1f}%)")
                                self.active_node = less_loaded
                        
                    except Exception as e:
                        logger.warning(f"Failed to fetch stats for {node.identifier}: {e}")
                        self.node_stats[node.identifier]['last_ping'] = None
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Node monitoring error: {e}")
                await asyncio.sleep(60)
    
    async def get_best_node(self, guild_id: Optional[int] = None) -> wavelink.Node:
        """Get the best node based on load and region"""
        if not self.nodes:
            raise ConnectionError("No Lavalink nodes available")
        
        if len(self.nodes) == 1:
            return self.nodes[0]
        
        # If guild_id provided, try to match region
        if guild_id:
            guild = self.bot.get_guild(guild_id)
            if guild:
                # Simple region detection (expand as needed)
                guild_region = str(guild.region)
                
                for node in self.nodes:
                    node_region = self.node_stats[node.identifier].get('region', '')
                    if guild_region.lower() in node_region.lower():
                        return node
        
        # Return least loaded node
        return min(
            self.nodes,
            key=lambda n: self.node_stats.get(n.identifier, {}).get('load', 0)
        )
    
    async def search_tracks(self, query: str, **kwargs) -> List[wavelink.Track]:
        """Search for tracks with advanced features"""
        start_time = asyncio.get_event_loop().time()
        
        try:
            # Auto-detect source
            source = self.detect_source(query)
            
            # Set search parameters
            search_kwargs = {
                'query': query,
                'cls': wavelink.Track,
                'source': source
            }
            
            # Add Spotify specific parameters
            if source == wavelink.TrackSource.Spotify and self.spotify_client:
                search_kwargs['spotify_client'] = self.spotify_client
            
            # Perform search
            node = await self.get_best_node()
            tracks = await wavelink.NodePool.get_tracks(**search_kwargs)
            
            # Measure performance
            search_time = asyncio.get_event_loop().time() - start_time
            self.performance_metrics['search_times'].append(search_time)
            
            # Keep only last 100 measurements
            if len(self.performance_metrics['search_times']) > 100:
                self.performance_metrics['search_times'].pop(0)
            
            # Cache results
            if tracks:
                await self.cache_search(query, tracks)
            
            return tracks
            
        except asyncio.TimeoutError:
            logger.warning(f"Search timeout for: {query}")
            return []
        except Exception as e:
            logger.error(f"Search error for '{query}': {e}")
            return []
    
    def detect_source(self, query: str) -> wavelink.TrackSource:
        """Auto-detect source from query"""
        query_lower = query.lower()
        
        # URL detection
        if 'spotify.com' in query_lower or query_lower.startswith('spotify:'):
            return wavelink.TrackSource.Spotify
        elif 'soundcloud.com' in query_lower:
            return wavelink.TrackSource.SoundCloud
        elif 'youtube.com' in query_lower or 'youtu.be' in query_lower:
            return wavelink.TrackSource.YouTube
        elif 'bandcamp.com' in query_lower:
            return wavelink.TrackSource.BandCamp
        elif 'twitch.tv' in query_lower:
            return wavelink.TrackSource.Twitch
        elif 'apple.co' in query_lower or 'music.apple.com' in query_lower:
            return getattr(wavelink.TrackSource, 'AppleMusic', wavelink.TrackSource.YouTube)
        
        # Check if it's a direct search
        if any(query_lower.startswith(prefix) for prefix in ['ytsearch:', 'scsearch:', 'spsearch:']):
            prefix = query_lower.split(':')[0]
            if prefix == 'spsearch':
                return wavelink.TrackSource.Spotify
            elif prefix == 'scsearch':
                return wavelink.TrackSource.SoundCloud
            else:
                return wavelink.TrackSource.YouTube
        
        # Default to YouTube Music for best quality
        return wavelink.TrackSource.YouTubeMusic
    
    async def cache_search(self, query: str, tracks: List[wavelink.Track]):
        """Cache search results"""
        # This would interface with CacheManager
        pass
    
    async def get_track_recommendations(self, track: wavelink.Track, limit: int = 10) -> List[wavelink.Track]:
        """Get track recommendations"""
        try:
            if track.source == wavelink.TrackSource.Spotify and self.spotify_client:
                # Spotify recommendations
                pass
            else:
                # YouTube recommendations
                query = f"Related to: {track.title} {track.author}"
                return await self.search_tracks(query)[:limit]
        except:
            return []
    
    async def get_playlist_tracks(self, playlist_url: str) -> List[wavelink.Track]:
        """Get all tracks from a playlist"""
        try:
            node = await self.get_best_node()
            
            if 'spotify.com/playlist' in playlist_url.lower() and self.spotify_client:
                # Spotify playlist
                playlist = await spotify.SpotifyTrack.search(
                    query=playlist_url,
                    node=node,
                    spotify_client=self.spotify_client
                )
                return playlist.tracks if hasattr(playlist, 'tracks') else []
            else:
                # YouTube/SoundCloud playlist
                tracks = await wavelink.NodePool.get_playlist(
                    query=playlist_url,
                    cls=wavelink.Track,
                    node=node
                )
                return tracks if isinstance(tracks, list) else []
        except Exception as e:
            logger.error(f"Playlist fetch error: {e}")
            return []
    
    async def get_node_stats(self) -> Dict:
        """Get comprehensive node statistics"""
        stats = {
            'total_nodes': len(self.nodes),
            'active_node': self.active_node.identifier if self.active_node else None,
            'nodes': {}
        }
        
        for node in self.nodes:
            node_stat = self.node_stats.get(node.identifier, {})
            stats['nodes'][node.identifier] = {
                'players': node_stat.get('players', 0),
                'playing': node_stat.get('playing', 0),
                'load': node_stat.get('load', 0),
                'uptime': node_stat.get('uptime', 0),
                'last_ping': node_stat.get('last_ping'),
                'connected_at': node_stat.get('connected_at')
            }
        
        # Performance metrics
        if self.performance_metrics['search_times']:
            avg_search = sum(self.performance_metrics['search_times']) / len(self.performance_metrics['search_times'])
            stats['avg_search_time'] = f"{avg_search:.2f}s"
        
        return stats
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.session:
            await self.session.close()
        
        for node in self.nodes:
            try:
                await node.disconnect()
            except:
                pass
        
        if self.spotify_client:
            await self.spotify_client.close()
        
        logger.info("Lavalink client cleaned up")
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
from datetime import datetime, timedelta, timezone
import random

# Fix wavelink imports
try:
    from wavelink import Node, Player, Track, Playlist
except ImportError:
    # For wavelink 3.x
    from wavelink.tracks import Playable as Track
    from wavelink import Node, Player, Playlist

logger = logging.getLogger(__name__)

class OptimizedLavalinkClient:
    """Professional Lavalink client with all optimizations"""
    
    def __init__(self, bot):
        self.bot = bot
        self.nodes: List[Node] = []
        self.active_node: Optional[Node] = None
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
            force_close=False,  # Changed to False for better connection reuse
            use_dns_cache=True
        )
        
        # Node configuration - update these with your actual Lavalink server details
        self.node_configs = [
            {
                'identifier': 'MAIN_NODE',
                'host': 'localhost',  # Change to your Lavalink server host
                'port': 2333,
                'password': 'youshallnotpass',  # Change to your Lavalink password
                'region': 'global',
                'resume_key': 'EVE_MUSIC_MAIN',
                'resume_timeout': 600,
                'reconnect_attempts': 5,
                'secure': False,  # Change to True if using SSL
                'spotify_client': None  # Will be set if available
            }
        ]
        
        # Load additional nodes from environment if available
        self.load_additional_nodes()
        
        # Performance tracking
        self.performance_metrics = {
            'search_times': [],
            'connection_times': [],
            'player_create_times': []
        }
        
        # Source priority (for auto-selection)
        self.source_priority = [
            'youtube',  # Updated source names for wavelink 3.x
            'youtubemusic',
            'soundcloud',
            'spotify'
        ]
    
    def load_additional_nodes(self):
        """Load additional nodes from environment variables"""
        import os
        import json
        
        # Load from environment variable
        nodes_json = os.getenv('LAVALINK_NODES', '[]')
        try:
            additional_nodes = json.loads(nodes_json)
            self.node_configs.extend(additional_nodes)
            logger.info(f"Loaded {len(additional_nodes)} additional Lavalink nodes from environment")
        except json.JSONDecodeError as e:
            logger.warning(f"Could not parse LAVALINK_NODES environment variable: {e}")
    
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
        
        logger.info("Created optimized HTTP session")
    
    async def connect_lavalink(self):
        """Connect to Lavalink with all optimizations"""
        if not self.session:
            await self.create_session()
        
        connected = False
        for config in self.node_configs:
            try:
                logger.info(f"Connecting to node: {config['identifier']}")
                
                # Initialize Spotify client for this node if needed
                spotify_client = None
                if config.get('spotify_credentials'):
                    try:
                        from wavelink.ext import spotify
                        spotify_client = spotify.SpotifyClient(
                            client_id=config['spotify_credentials']['client_id'],
                            client_secret=config['spotify_credentials']['client_secret']
                        )
                    except ImportError:
                        logger.warning("Spotify extension not available")
                
                # Create node with wavelink 3.x syntax
                node = Node(
                    uri=f"{'https' if config.get('secure', False) else 'http'}://{config['host']}:{config['port']}",
                    password=config['password'],
                    identifier=config['identifier'],
                    resume_key=config.get('resume_key', 'EVE_MUSIC'),
                    resume_timeout=config.get('resume_timeout', 300),
                    spotify=spotify_client
                )
                
                # Connect the node
                await wavelink.Pool.connect(client=self.bot, nodes=[node])
                
                self.nodes.append(node)
                self.node_stats[config['identifier']] = {
                    'connected_at': datetime.now(timezone.utc),
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
                
            except Exception as e:
                logger.error(f"❌ Failed to connect to {config['identifier']}: {e}")
                continue
        
        if not connected:
            logger.critical("❌ Failed to connect to any Lavalink node!")
            raise ConnectionError("Could not connect to any Lavalink node")
        
        # Start monitoring task
        asyncio.create_task(self.monitor_nodes())
        
        return connected
    
    async def monitor_nodes(self):
        """Monitor node health and switch if needed"""
        while True:
            try:
                for node in self.nodes:
                    try:
                        # Check if node is available
                        stats = await node.fetch_stats()
                        
                        self.node_stats[node.identifier].update({
                            'last_ping': datetime.now(timezone.utc),
                            'players': getattr(stats, 'players', 0),
                            'playing': getattr(stats, 'playing_players', 0),
                            'cpu_cores': getattr(stats, 'cpu_cores', 0),
                            'system_load': getattr(stats, 'system_load', 0),
                            'memory_used': getattr(stats, 'memory_used', 0),
                            'memory_free': getattr(stats, 'memory_free', 0),
                            'uptime': getattr(stats, 'uptime', 0)
                        })
                        
                        # Calculate load percentage
                        players = getattr(stats, 'players', 1)
                        playing = getattr(stats, 'playing_players', 0)
                        load = (playing / max(players, 1)) * 100
                        self.node_stats[node.identifier]['load'] = load
                        
                        # Switch to less loaded node if current is overloaded
                        if (node == self.active_node and load > 80 and 
                            len(self.nodes) > 1):
                            
                            less_loaded = min(
                                self.nodes, 
                                key=lambda n: self.node_stats.get(n.identifier, {}).get('load', 100)
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
    
    async def get_best_node(self, guild_id: Optional[int] = None) -> Node:
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
            key=lambda n: self.node_stats.get(n.identifier, {}).get('load', 100)
        )
    
    async def search_tracks(self, query: str, **kwargs) -> List[Track]:
        """Search for tracks with advanced features"""
        start_time = asyncio.get_event_loop().time()
        
        try:
            # Get best node
            node = await self.get_best_node()
            
            # Perform search with wavelink 3.x syntax
            tracks = await wavelink.Pool.fetch_tracks(query, node=node)
            
            # Measure performance
            search_time = asyncio.get_event_loop().time() - start_time
            self.performance_metrics['search_times'].append(search_time)
            
            # Keep only last 100 measurements
            if len(self.performance_metrics['search_times']) > 100:
                self.performance_metrics['search_times'].pop(0)
            
            return tracks if isinstance(tracks, list) else []
            
        except asyncio.TimeoutError:
            logger.warning(f"Search timeout for: {query}")
            return []
        except Exception as e:
            logger.error(f"Search error for '{query}': {e}")
            return []
    
    def detect_source(self, query: str) -> str:
        """Auto-detect source from query"""
        query_lower = query.lower()
        
        # URL detection
        if 'spotify.com' in query_lower or query_lower.startswith('spotify:'):
            return 'spotify'
        elif 'soundcloud.com' in query_lower:
            return 'soundcloud'
        elif 'youtube.com' in query_lower or 'youtu.be' in query_lower:
            return 'youtube'
        elif 'bandcamp.com' in query_lower:
            return 'bandcamp'
        elif 'twitch.tv' in query_lower:
            return 'twitch'
        
        # Check if it's a direct search
        if any(query_lower.startswith(prefix) for prefix in ['ytsearch:', 'scsearch:', 'spsearch:']):
            prefix = query_lower.split(':')[0]
            if prefix == 'spsearch':
                return 'spotify'
            elif prefix == 'scsearch':
                return 'soundcloud'
            else:
                return 'youtube'
        
        # Default to YouTube for best compatibility
        return 'youtube'
    
    async def get_track_recommendations(self, track: Track, limit: int = 10) -> List[Track]:
        """Get track recommendations"""
        try:
            query = f"Related to: {track.title} {track.author}"
            tracks = await self.search_tracks(query)
            return tracks[:limit] if tracks else []
        except Exception as e:
            logger.error(f"Error getting recommendations: {e}")
            return []
    
    async def get_playlist_tracks(self, playlist_url: str) -> List[Track]:
        """Get all tracks from a playlist"""
        try:
            node = await self.get_best_node()
            
            # For wavelink 3.x, playlists are handled differently
            if 'spotify.com/playlist' in playlist_url.lower():
                # Spotify playlist - requires spotify extension
                try:
                    from wavelink.ext import spotify
                    decoded = spotify.decode_url(playlist_url)
                    if decoded and decoded.type == spotify.SpotifySearchType.playlist:
                        return await spotify.SpotifyTrack.search(
                            query=playlist_url,
                            node=node
                        )
                except ImportError:
                    logger.warning("Spotify extension not available")
            else:
                # Regular playlist
                tracks = await wavelink.Pool.fetch_tracks(playlist_url, node=node)
                if isinstance(tracks, wavelink.Playlist):
                    return tracks.tracks
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
    
    async def disconnect(self):
        """Disconnect from all nodes"""
        for node in self.nodes:
            try:
                await node.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting node {node.identifier}: {e}")
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.disconnect()
        
        if self.session:
            await self.session.close()
        
        logger.info("Lavalink client cleaned up")
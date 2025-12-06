"""
Complete social features system including:
- Listening parties (synchronized playback)
- Music games (trivia, name that tune, etc.)
- Taste compatibility and sharing
- Collaborative queue building
- Music challenges and achievements
"""

import discord
from discord.ext import commands, tasks
from typing import Dict, List, Optional, Set, Tuple
import asyncio
import random
from datetime import datetime, timedelta
import json
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

class ListeningParty:
    """Listening party with synchronized playback"""
    
    def __init__(self, **kwargs):
        self.id: str = kwargs.get('id', '')
        self.host_id: int = kwargs.get('host_id', 0)
        self.guild_id: int = kwargs.get('guild_id', 0)
        self.channel_id: int = kwargs.get('channel_id', 0)
        self.voice_channel_id: int = kwargs.get('voice_channel_id', 0)
        
        # Party configuration
        self.name: str = kwargs.get('name', 'Listening Party')
        self.description: str = kwargs.get('description', '')
        self.privacy: str = kwargs.get('privacy', 'public')  # public, private, invite
        self.max_participants: int = kwargs.get('max_participants', 50)
        
        # Party state
        self.participants: Set[int] = set(kwargs.get('participants', []))
        self.invited_users: Set[int] = set(kwargs.get('invited_users', []))
        self.queue: List[Dict] = kwargs.get('queue', [])
        self.current_track: Optional[Dict] = kwargs.get('current_track')
        self.current_position: int = kwargs.get('current_position', 0)
        self.is_playing: bool = kwargs.get('is_playing', False)
        self.is_paused: bool = kwargs.get('is_paused', False)
        
        # Party metadata
        self.created_at: datetime = kwargs.get('created_at', datetime.utcnow())
        self.started_at: Optional[datetime] = kwargs.get('started_at')
        self.ended_at: Optional[datetime] = kwargs.get('ended_at')
        
        # Party statistics
        self.total_tracks_played: int = kwargs.get('total_tracks_played', 0)
        self.total_listen_time: int = kwargs.get('total_listen_time', 0)
        
        # Party settings
        self.vote_to_skip: bool = kwargs.get('vote_to_skip', True)
        self.allow_queue_adds: bool = kwargs.get('allow_queue_adds', True)
        self.require_host_approval: bool = kwargs.get('require_host_approval', False)
        self.auto_play: bool = kwargs.get('auto_play', True)
        
        # Chat integration
        self.chat_channel_id: Optional[int] = kwargs.get('chat_channel_id')
        self.chat_enabled: bool = kwargs.get('chat_enabled', True)
        
        # Party theme
        self.theme_color: Optional[int] = kwargs.get('theme_color')
        self.emoji: str = kwargs.get('emoji', '🎧')
    
    def add_participant(self, user_id: int) -> bool:
        """Add participant to party"""
        if len(self.participants) >= self.max_participants:
            return False
        
        if self.privacy == 'invite' and user_id not in self.invited_users:
            return False
        
        self.participants.add(user_id)
        return True
    
    def remove_participant(self, user_id: int):
        """Remove participant from party"""
        self.participants.discard(user_id)
    
    def add_to_queue(self, track_data: Dict, user_id: int) -> bool:
        """Add track to party queue"""
        if not self.allow_queue_adds:
            return False
        
        if self.require_host_approval and user_id != self.host_id:
            # Would need approval system
            return False
        
        track_data['added_by'] = user_id
        track_data['added_at'] = datetime.utcnow()
        self.queue.append(track_data)
        return True
    
    def get_queue_info(self) -> Dict:
        """Get queue information"""
        return {
            'length': len(self.queue),
            'duration': sum(t.get('duration', 0) for t in self.queue),
            'up_next': self.queue[0] if self.queue else None
        }
    
    def to_dict(self) -> Dict:
        """Convert party to dictionary"""
        return {
            'id': self.id,
            'host_id': self.host_id,
            'guild_id': self.guild_id,
            'channel_id': self.channel_id,
            'voice_channel_id': self.voice_channel_id,
            'name': self.name,
            'description': self.description,
            'privacy': self.privacy,
            'max_participants': self.max_participants,
            'participants': list(self.participants),
            'invited_users': list(self.invited_users),
            'queue': self.queue,
            'current_track': self.current_track,
            'current_position': self.current_position,
            'is_playing': self.is_playing,
            'is_paused': self.is_paused,
            'created_at': self.created_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'ended_at': self.ended_at.isoformat() if self.ended_at else None,
            'total_tracks_played': self.total_tracks_played,
            'total_listen_time': self.total_listen_time,
            'vote_to_skip': self.vote_to_skip,
            'allow_queue_adds': self.allow_queue_adds,
            'require_host_approval': self.require_host_approval,
            'auto_play': self.auto_play,
            'chat_channel_id': self.chat_channel_id,
            'chat_enabled': self.chat_enabled,
            'theme_color': self.theme_color,
            'emoji': self.emoji
        }


class MusicGame:
    """Base class for music games"""
    
    def __init__(self, game_type: str, host_id: int, guild_id: int):
        self.game_type = game_type
        self.host_id = host_id
        self.guild_id = guild_id
        self.id = f"{game_type}_{int(datetime.utcnow().timestamp())}"
        
        # Game state
        self.players: Dict[int, Dict] = {}  # user_id: player_data
        self.is_active = False
        self.is_paused = False
        self.current_round = 0
        self.total_rounds = 10
        
        # Game settings
        self.difficulty = "medium"
        self.time_limit = 30  # seconds per question
        self.points_per_correct = 100
        
        # Game statistics
        self.created_at = datetime.utcnow()
        self.started_at: Optional[datetime] = None
        self.ended_at: Optional[datetime] = None
    
    async def start_game(self):
        """Start the game"""
        self.is_active = True
        self.started_at = datetime.utcnow()
        self.current_round = 1
    
    async def end_game(self):
        """End the game"""
        self.is_active = False
        self.ended_at = datetime.utcnow()
    
    async def add_player(self, user_id: int, user_name: str):
        """Add player to game"""
        self.players[user_id] = {
            'name': user_name,
            'score': 0,
            'correct_answers': 0,
            'incorrect_answers': 0,
            'streak': 0,
            'best_streak': 0,
            'joined_at': datetime.utcnow()
        }
    
    async def remove_player(self, user_id: int):
        """Remove player from game"""
        self.players.pop(user_id, None)
    
    async def submit_answer(self, user_id: int, answer: str) -> Tuple[bool, int]:
        """Submit answer for current question"""
        # To be implemented by specific game
        return False, 0
    
    async def get_leaderboard(self) -> List[Tuple[int, str, int]]:
        """Get game leaderboard"""
        sorted_players = sorted(
            self.players.items(),
            key=lambda x: x[1]['score'],
            reverse=True
        )
        
        return [
            (user_id, data['name'], data['score'])
            for user_id, data in sorted_players
        ]
    
    async def get_current_question(self) -> Dict:
        """Get current question"""
        # To be implemented by specific game
        return {}


class MusicTrivia(MusicGame):
    """Music trivia game"""
    
    def __init__(self, host_id: int, guild_id: int):
        super().__init__("trivia", host_id, guild_id)
        
        # Trivia-specific
        self.categories = ["general", "artists", "albums", "lyrics", "genres"]
        self.selected_category = "general"
        
        # Game questions
        self.questions: List[Dict] = []
        self.current_question: Optional[Dict] = None
        self.correct_answer: Optional[str] = None
        
        # Timing
        self.question_start_time: Optional[datetime] = None
        self.time_remaining = self.time_limit
    
    async def generate_questions(self):
        """Generate trivia questions"""
        # This would fetch from a database or API
        # For now, create sample questions
        
        sample_questions = [
            {
                'question': "Which artist sang 'Bohemian Rhapsody'?",
                'answers': ["Queen", "The Beatles", "Led Zeppelin", "Pink Floyd"],
                'correct': 0,
                'category': 'artists',
                'difficulty': 'easy'
            },
            {
                'question': "What year did Michael Jackson release 'Thriller'?",
                'answers': ["1982", "1979", "1985", "1988"],
                'correct': 0,
                'category': 'albums',
                'difficulty': 'medium'
            },
            {
                'question': "Finish the lyric: 'Never gonna give you up, never gonna...'",
                'answers': ["let you down", "run around", "hurt you", "say goodbye"],
                'correct': 0,
                'category': 'lyrics',
                'difficulty': 'easy'
            },
            {
                'question': "Which genre is characterized by heavy bass and syncopated rhythms?",
                'answers': ["Dubstep", "Jazz", "Classical", "Country"],
                'correct': 0,
                'category': 'genres',
                'difficulty': 'medium'
            }
        ]
        
        self.questions = sample_questions * 3  # Repeat to get enough questions
        random.shuffle(self.questions)
        self.total_rounds = min(len(self.questions), 10)
    
    async def start_round(self):
        """Start a new round"""
        if self.current_round > self.total_rounds:
            await self.end_game()
            return
        
        # Get question
        self.current_question = self.questions[self.current_round - 1]
        self.correct_answer = self.current_question['answers'][self.current_question['correct']]
        self.question_start_time = datetime.utcnow()
        self.time_remaining = self.time_limit
        
        # Start timer
        asyncio.create_task(self.round_timer())
    
    async def round_timer(self):
        """Round timer countdown"""
        while self.time_remaining > 0 and self.is_active and not self.is_paused:
            await asyncio.sleep(1)
            self.time_remaining -= 1
        
        if self.time_remaining == 0:
            # Time's up!
            await self.end_round()
    
    async def end_round(self):
        """End current round"""
        # Calculate scores for this round
        # Move to next round
        self.current_round += 1
        if self.current_round <= self.total_rounds:
            await self.start_round()
        else:
            await self.end_game()
    
    async def submit_answer(self, user_id: int, answer: str) -> Tuple[bool, int]:
        """Submit answer for trivia question"""
        if not self.is_active or user_id not in self.players:
            return False, 0
        
        player = self.players[user_id]
        is_correct = (answer.lower() == self.correct_answer.lower())
        
        if is_correct:
            # Calculate points based on time remaining
            time_bonus = int((self.time_remaining / self.time_limit) * 50)
            points = self.points_per_correct + time_bonus
            
            player['score'] += points
            player['correct_answers'] += 1
            player['streak'] += 1
            
            if player['streak'] > player['best_streak']:
                player['best_streak'] = player['streak']
        else:
            player['incorrect_answers'] += 1
            player['streak'] = 0
            points = 0
        
        return is_correct, points
    
    async def get_current_question(self) -> Dict:
        """Get current question"""
        if not self.current_question:
            return {}
        
        return {
            'question': self.current_question['question'],
            'answers': self.current_question['answers'],
            'category': self.current_question['category'],
            'difficulty': self.current_question['difficulty'],
            'time_remaining': self.time_remaining
        }


class NameThatTune(MusicGame):
    """Name That Tune game"""
    
    def __init__(self, host_id: int, guild_id: int):
        super().__init__("namethattune", host_id, guild_id)
        
        # Game-specific
        self.audio_preview_duration = 15  # seconds
        self.current_track: Optional[Dict] = None
        self.preview_url: Optional[str] = None
        
        # Hint system
        self.hints_given = 0
        self.max_hints = 3
        self.hint_penalty = 25  # points deducted per hint
    
    async def start_round(self):
        """Start a new round"""
        # Get a random track
        self.current_track = await self.get_random_track()
        
        if self.current_track:
            # Generate preview URL (would need actual audio)
            self.preview_url = f"preview_{self.current_track.get('id', '')}"
            
            # Reset hints
            self.hints_given = 0
            self.question_start_time = datetime.utcnow()
            self.time_remaining = self.time_limit
    
    async def get_random_track(self) -> Optional[Dict]:
        """Get random track for game"""
        # This would fetch from database or API
        # For now, return mock track
        tracks = [
            {
                'title': "Bohemian Rhapsody",
                'artist': "Queen",
                'year': 1975,
                'genre': "Rock",
                'id': "track_1"
            },
            {
                'title': "Billie Jean",
                'artist': "Michael Jackson",
                'year': 1982,
                'genre': "Pop",
                'id': "track_2"
            },
            {
                'title': "Smells Like Teen Spirit",
                'artist': "Nirvana",
                'year': 1991,
                'genre': "Grunge",
                'id': "track_3"
            }
        ]
        
        return random.choice(tracks)
    
    async def get_hint(self) -> str:
        """Get a hint for current track"""
        if not self.current_track or self.hints_given >= self.max_hints:
            return "No more hints available!"
        
        self.hints_given += 1
        
        hints = [
            f"Artist starts with: {self.current_track['artist'][:3]}...",
            f"Released in the {str(self.current_track['year'])[:3]}0s",
            f"Genre: {self.current_track['genre']}",
            f"Title contains: '{random.choice(self.current_track['title'].split())}'"
        ]
        
        return hints[min(self.hints_given - 1, len(hints) - 1)]
    
    async def submit_answer(self, user_id: int, answer: str) -> Tuple[bool, int]:
        """Submit answer for Name That Tune"""
        if not self.is_active or user_id not in self.players:
            return False, 0
        
        player = self.players[user_id]
        
        # Check if answer matches track title or artist
        is_correct = False
        if self.current_track:
            correct_title = self.current_track['title'].lower()
            correct_artist = self.current_track['artist'].lower()
            user_answer = answer.lower()
            
            is_correct = (correct_title in user_answer or 
                         correct_artist in user_answer or
                         user_answer in correct_title or
                         user_answer in correct_artist)
        
        if is_correct:
            # Calculate points
            time_bonus = int((self.time_remaining / self.time_limit) * 100)
            hint_penalty = self.hints_given * self.hint_penalty
            points = max(50, self.points_per_correct + time_bonus - hint_penalty)
            
            player['score'] += points
            player['correct_answers'] += 1
            player['streak'] += 1
            
            if player['streak'] > player['best_streak']:
                player['best_streak'] = player['streak']
        else:
            player['incorrect_answers'] += 1
            player['streak'] = 0
            points = 0
        
        return is_correct, points


class SocialFeatures:
    """Complete social features system"""
    
    def __init__(self, bot):
        self.bot = bot
        
        # Listening parties
        self.listening_parties: Dict[str, ListeningParty] = {}  # party_id: party
        self.user_parties: Dict[int, Set[str]] = {}  # user_id: set of party_ids
        
        # Music games
        self.active_games: Dict[str, MusicGame] = {}  # game_id: game
        self.user_games: Dict[int, Set[str]] = {}  # user_id: set of game_ids
        
        # Taste compatibility
        self.user_tastes: Dict[int, Dict] = {}  # user_id: taste_profile
        
        # Collaborative queues
        self.collaborative_queues: Dict[int, List[Dict]] = {}  # guild_id: queue
        
        # Music challenges
        self.active_challenges: Dict[str, Dict] = {}  # challenge_id: challenge
        self.user_challenges: Dict[int, Set[str]] = {}  # user_id: set of challenge_ids
        
        # Achievements
        self.achievements: Dict[int, Set[str]] = {}  # user_id: set of achievement_ids
        
        # Background tasks
        self.cleanup_parties.start()
        self.update_tastes.start()
    
    @commands.hybrid_group(
        name="party",
        description="Listening party commands"
    )
    async def party_group(self, ctx):
        """Listening party management"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @party_group.command(
        name="create",
        description="Create a listening party"
    )
    async def party_create(
        self,
        ctx,
        name: str,
        privacy: str = "public",
        max_users: int = 25
    ):
        """Create a new listening party"""
        # Check if user is in voice
        if not ctx.author.voice:
            await ctx.send("Join a voice channel to create a listening party! 🎧")
            return
        
        # Create party ID
        party_id = f"{ctx.guild.id}_{ctx.author.id}_{int(datetime.utcnow().timestamp())}"
        
        # Create party
        party = ListeningParty(
            id=party_id,
            host_id=ctx.author.id,
            guild_id=ctx.guild.id,
            channel_id=ctx.channel.id,
            voice_channel_id=ctx.author.voice.channel.id,
            name=name,
            privacy=privacy,
            max_participants=max_users,
            created_at=datetime.utcnow()
        )
        
        # Add host as participant
        party.add_participant(ctx.author.id)
        
        # Store party
        self.listening_parties[party_id] = party
        
        # Update user parties
        if ctx.author.id not in self.user_parties:
            self.user_parties[ctx.author.id] = set()
        self.user_parties[ctx.author.id].add(party_id)
        
        # Create party embed
        embed = await self.create_party_embed(party)
        embed.set_author(name="🎉 Listening Party Created!")
        
        # Add party controls
        view = PartyControlsView(self, party_id)
        
        await ctx.send(embed=embed, view=view)
    
    @party_group.command(
        name="join",
        description="Join a listening party"
    )
    async def party_join(self, ctx, party_id: Optional[str] = None):
        """Join a listening party"""
        if not ctx.author.voice:
            await ctx.send("Join a voice channel first! 🎵")
            return
        
        # Find party
        party = None
        
        if party_id:
            party = self.listening_parties.get(party_id)
        else:
            # Find active party in current guild
            for p in self.listening_parties.values():
                if p.guild_id == ctx.guild.id and p.is_playing:
                    party = p
                    break
        
        if not party:
            await ctx.send("No active listening party found!")
            return
        
        # Check if party is in same voice channel
        if party.voice_channel_id != ctx.author.voice.channel.id:
            await ctx.send("Join the same voice channel as the party!")
            return
        
        # Add participant
        success = party.add_participant(ctx.author.id)
        
        if success:
            # Update user parties
            if ctx.author.id not in self.user_parties:
                self.user_parties[ctx.author.id] = set()
            self.user_parties[ctx.author.id].add(party.id)
            
            await ctx.send(f"🎉 {ctx.author.mention} joined the listening party!")
            
            # Update party embed if exists
            # (Would need message ID tracking)
        else:
            await ctx.send("Could not join party. It might be full or private.")
    
    @party_group.command(
        name="list",
        description="List active listening parties"
    )
    async def party_list(self, ctx):
        """List active listening parties"""
        active_parties = [
            p for p in self.listening_parties.values()
            if p.guild_id == ctx.guild.id and p.is_playing
        ]
        
        if not active_parties:
            await ctx.send("No active listening parties in this server.")
            return
        
        embed = discord.Embed(
            title="🎧 Active Listening Parties",
            color=self.bot.colors.ROSE_GOLD,
            timestamp=datetime.utcnow()
        )
        
        for party in active_parties[:10]:  # Show first 10
            host = self.bot.get_user(party.host_id)
            host_name = host.name if host else f"User {party.host_id}"
            
            embed.add_field(
                name=f"{party.emoji} {party.name}",
                value=(
                    f"**Host:** {host_name}\n"
                    f"**Participants:** {len(party.participants)}/{party.max_participants}\n"
                    f"**Privacy:** {party.privacy.title()}\n"
                    f"**ID:** `{party.id}`"
                ),
                inline=True
            )
        
        if len(active_parties) > 10:
            embed.set_footer(text=f"And {len(active_parties) - 10} more parties...")
        
        await ctx.send(embed=embed)
    
    async def create_party_embed(self, party: ListeningParty) -> discord.Embed:
        """Create embed for party info"""
        color = party.theme_color or self.bot.colors.VIOLET
        
        embed = discord.Embed(
            title=f"{party.emoji} {party.name}",
            description=party.description or "A listening party",
            color=color,
            timestamp=datetime.utcnow()
        )
        
        # Party info
        host = self.bot.get_user(party.host_id)
        host_name = host.name if host else f"User {party.host_id}"
        
        embed.add_field(
            name="Party Info",
            value=(
                f"**Host:** {host_name}\n"
                f"**Participants:** {len(party.participants)}/{party.max_participants}\n"
                f"**Privacy:** {party.privacy.title()}\n"
                f"**Status:** {'Playing' if party.is_playing else 'Paused' if party.is_paused else 'Not Started'}"
            ),
            inline=True
        )
        
        # Current track
        if party.current_track:
            embed.add_field(
                name="Now Playing",
                value=(
                    f"**{party.current_track.get('title', 'Unknown')}**\n"
                    f"{party.current_track.get('artist', 'Unknown')}\n"
                    f"Position: {self.format_duration(party.current_position)}"
                ),
                inline=True
            )
        
        # Queue info
        queue_info = party.get_queue_info()
        embed.add_field(
            name="Queue",
            value=(
                f"**Tracks:** {queue_info['length']}\n"
                f"**Duration:** {self.format_duration(queue_info['duration'])}\n"
                f"**Up Next:** {queue_info['up_next'].get('title', 'None') if queue_info['up_next'] else 'None'}"
            ),
            inline=True
        )
        
        # Party settings
        settings = []
        if party.vote_to_skip:
            settings.append("✅ Vote to Skip")
        if party.allow_queue_adds:
            settings.append("✅ Queue Adds")
        if party.require_host_approval:
            settings.append("✅ Host Approval")
        if party.auto_play:
            settings.append("✅ Auto-play")
        
        if settings:
            embed.add_field(
                name="Settings",
                value="\n".join(settings),
                inline=False
            )
        
        # Party ID
        embed.set_footer(text=f"Party ID: {party.id}")
        
        return embed
    
    @commands.hybrid_group(
        name="game",
        description="Music game commands"
    )
    async def game_group(self, ctx):
        """Music game management"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @game_group.command(
        name="start",
        description="Start a music game"
    )
    async def game_start(
        self,
        ctx,
        game_type: str = "trivia",
        rounds: int = 10,
        difficulty: str = "medium"
    ):
        """Start a music game"""
        game_type = game_type.lower()
        
        if game_type not in ["trivia", "namethattune", "lyrics"]:
            await ctx.send("Available games: trivia, namethattune, lyrics")
            return
        
        # Check if user already in a game
        if ctx.author.id in self.user_games:
            # Check if any active games
            for game_id in self.user_games[ctx.author.id]:
                game = self.active_games.get(game_id)
                if game and game.is_active:
                    await ctx.send("You're already in an active game!")
                    return
        
        # Create game
        if game_type == "trivia":
            game = MusicTrivia(ctx.author.id, ctx.guild.id)
        elif game_type == "namethattune":
            game = NameThatTune(ctx.author.id, ctx.guild.id)
        else:
            game = MusicTrivia(ctx.author.id, ctx.guild.id)  # Default
        
        # Configure game
        game.total_rounds = min(max(rounds, 1), 20)
        game.difficulty = difficulty
        game.time_limit = 30 if difficulty == "easy" else 20 if difficulty == "medium" else 15
        
        # Add host as player
        await game.add_player(ctx.author.id, ctx.author.name)
        
        # Generate questions
        if hasattr(game, 'generate_questions'):
            await game.generate_questions()
        
        # Store game
        self.active_games[game.id] = game
        
        # Update user games
        if ctx.author.id not in self.user_games:
            self.user_games[ctx.author.id] = set()
        self.user_games[ctx.author.id].add(game.id)
        
        # Create game embed
        embed = discord.Embed(
            title=f"🎮 {game_type.title()} Game",
            description=f"Hosted by {ctx.author.mention}",
            color=self.bot.colors.PLUM
        )
        
        embed.add_field(
            name="Game Info",
            value=(
                f"**Rounds:** {game.total_rounds}\n"
                f"**Difficulty:** {game.difficulty.title()}\n"
                f"**Time Limit:** {game.time_limit}s per question\n"
                f"**Players:** 1/{game.max_participants}"
            ),
            inline=True
        )
        
        embed.add_field(
            name="How to Join",
            value=f"Click **Join Game** below!\nGame ID: `{game.id}`",
            inline=True
        )
        
        # Add game controls
        view = GameLobbyView(self, game.id)
        
        await ctx.send(embed=embed, view=view)
    
    @game_group.command(
        name="join",
        description="Join a music game"
    )
    async def game_join(self, ctx, game_id: Optional[str] = None):
        """Join a music game"""
        # Find game
        game = None
        
        if game_id:
            game = self.active_games.get(game_id)
        else:
            # Find active game in guild
            for g in self.active_games.values():
                if g.guild_id == ctx.guild.id and not g.is_active:
                    game = g
                    break
        
        if not game:
            await ctx.send("No game lobby found to join!")
            return
        
        # Check if already in game
        if ctx.author.id in game.players:
            await ctx.send("You're already in this game!")
            return
        
        # Check if game is full
        if len(game.players) >= game.max_participants:
            await ctx.send("Game is full!")
            return
        
        # Add player
        await game.add_player(ctx.author.id, ctx.author.name)
        
        # Update user games
        if ctx.author.id not in self.user_games:
            self.user_games[ctx.author.id] = set()
        self.user_games[ctx.author.id].add(game.id)
        
        await ctx.send(f"🎮 {ctx.author.mention} joined the game!")
    
    @commands.hybrid_command(
        name="taste",
        description="Compare music taste with another user"
    )
    async def taste_command(self, ctx, user: discord.User):
        """Compare music taste with another user"""
        # Get taste profiles
        user1_taste = self.user_tastes.get(ctx.author.id, {})
        user2_taste = self.user_tastes.get(user.id, {})
        
        if not user1_taste or not user2_taste:
            await ctx.send("Not enough listening data to compare tastes yet!")
            return
        
        # Calculate compatibility
        compatibility = await self.calculate_compatibility(user1_taste, user2_taste)
        
        # Create compatibility embed
        embed = discord.Embed(
            title="🎵 Music Taste Compatibility",
            description=f"{ctx.author.name} 🆚 {user.name}",
            color=self.bot.colors.ROSE_GOLD
        )
        
        # Compatibility score
        score = compatibility['score']
        emoji = "💖" if score >= 80 else "👍" if score >= 60 else "🤝" if score >= 40 else "👎"
        
        embed.add_field(
            name="Compatibility Score",
            value=f"{emoji} **{score}%**",
            inline=False
        )
        
        # Common genres
        common_genres = compatibility.get('common_genres', [])
        if common_genres:
            genres_text = ", ".join(common_genres[:5])
            embed.add_field(
                name="Common Genres",
                value=genres_text,
                inline=True
            )
        
        # Common artists
        common_artists = compatibility.get('common_artists', [])
        if common_artists:
            artists_text = ", ".join(common_artists[:3])
            embed.add_field(
                name="Common Artists",
                value=artists_text,
                inline=True
            )
        
        # Recommendations
        recommendations = compatibility.get('recommendations', [])
        if recommendations:
            rec_text = "\n".join(f"• {rec}" for rec in recommendations[:3])
            embed.add_field(
                name="You Might Like",
                value=rec_text,
                inline=False
            )
        
        await ctx.send(embed=embed)
    
    async def calculate_compatibility(self, taste1: Dict, taste2: Dict) -> Dict:
        """Calculate music taste compatibility"""
        # Extract data
        genres1 = set(taste1.get('top_genres', []))
        genres2 = set(taste2.get('top_genres', []))
        
        artists1 = set(taste1.get('top_artists', []))
        artists2 = set(taste2.get('top_artists', []))
        
        # Calculate overlap
        genre_overlap = len(genres1 & genres2)
        artist_overlap = len(artists1 & artists2)
        
        total_genres = len(genres1 | genres2)
        total_artists = len(artists1 | artists2)
        
        # Calculate scores
        genre_score = (genre_overlap / max(total_genres, 1)) * 50
        artist_score = (artist_overlap / max(total_artists, 1)) * 50
        
        total_score = int(genre_score + artist_score)
        
        # Get recommendations
        recommendations = []
        
        # Recommend artists from other user
        for artist in artists2 - artists1:
            if len(recommendations) < 3:
                recommendations.append(f"**{artist}** (liked by {self.bot.get_user(taste2.get('user_id', 0)) or 'them'})")
        
        for artist in artists1 - artists2:
            if len(recommendations) < 6:
                recommendations.append(f"**{artist}** (you like them)")
        
        return {
            'score': total_score,
            'common_genres': list(genres1 & genres2),
            'common_artists': list(artists1 & artists2),
            'recommendations': recommendations[:5]
        }
    
    @commands.hybrid_command(
        name="collabqueue",
        description="Start a collaborative queue"
    )
    async def collabqueue_command(self, ctx):
        """Start a collaborative queue"""
        # Check if already exists
        if ctx.guild.id in self.collaborative_queues:
            await ctx.send("A collaborative queue is already active in this server!")
            return
        
        # Create collaborative queue
        self.collaborative_queues[ctx.guild.id] = {
            'creator_id': ctx.author.id,
            'created_at': datetime.utcnow(),
            'tracks': [],
            'contributors': set([ctx.author.id]),
            'vote_skip_threshold': 3,
            'max_tracks_per_user': 10
        }
        
        embed = discord.Embed(
            title="🤝 Collaborative Queue Started!",
            description="Everyone can add tracks to the queue!",
            color=self.bot.colors.VIOLET
        )
        
        embed.add_field(
            name="How to Add",
            value="Use `e!play` as normal - tracks will go to the collaborative queue",
            inline=False
        )
        
        embed.add_field(
            name="Settings",
            value=(
                f"**Vote Skip:** {self.collaborative_queues[ctx.guild.id]['vote_skip_threshold']} votes\n"
                f"**Max per User:** {self.collaborative_queues[ctx.guild.id]['max_tracks_per_user']} tracks"
            ),
            inline=True
        )
        
        view = CollabQueueView(self, ctx.guild.id)
        
        await ctx.send(embed=embed, view=view)
    
    @commands.hybrid_command(
        name="achievements",
        description="Show your music achievements"
    )
    async def achievements_command(self, ctx, user: Optional[discord.User] = None):
        """Show music achievements"""
        target_user = user or ctx.author
        user_achievements = self.achievements.get(target_user.id, set())
        
        if not user_achievements:
            await ctx.send(f"{target_user.name} hasn't earned any achievements yet!")
            return
        
        embed = discord.Embed(
            title="🏆 Music Achievements",
            description=f"{target_user.name}'s achievements",
            color=self.bot.colors.PLUM
        )
        
        # Group achievements by category
        achievement_data = self.get_achievement_data()
        
        for category, achievements in achievement_data.items():
            user_category_achievements = [
                a for a in achievements
                if a['id'] in user_achievements
            ]
            
            if user_category_achievements:
                achievement_list = "\n".join(
                    f"• {a['name']} - {a['description']}"
                    for a in user_category_achievements[:3]
                )
                
                if len(user_category_achievements) > 3:
                    achievement_list += f"\n*...and {len(user_category_achievements) - 3} more*"
                
                embed.add_field(
                    name=category,
                    value=achievement_list,
                    inline=False
                )
        
        # Total count
        embed.set_footer(text=f"Total: {len(user_achievements)} achievements")
        
        await ctx.send(embed=embed)
    
    def get_achievement_data(self) -> Dict:
        """Get achievement data"""
        return {
            "Listening": [
                {
                    'id': 'listener_1',
                    'name': 'First Listen',
                    'description': 'Listen to your first track'
                },
                {
                    'id': 'listener_100',
                    'name': 'Music Enthusiast',
                    'description': 'Listen to 100 tracks'
                },
                {
                    'id': 'listener_1000',
                    'name': 'Music Addict',
                    'description': 'Listen to 1000 tracks'
                }
            ],
            "Party": [
                {
                    'id': 'party_host',
                    'name': 'Party Host',
                    'description': 'Host a listening party'
                },
                {
                    'id': 'party_joiner',
                    'name': 'Social Butterfly',
                    'description': 'Join 10 different parties'
                }
            ],
            "Games": [
                {
                    'id': 'game_winner',
                    'name': 'Game Champion',
                    'description': 'Win a music game'
                },
                {
                    'id': 'trivia_master',
                    'name': 'Trivia Master',
                    'description': 'Answer 50 trivia questions correctly'
                }
            ]
        }
    
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
    
    @tasks.loop(minutes=5)
    async def cleanup_parties(self):
        """Clean up inactive parties and games"""
        now = datetime.utcnow()
        
        # Clean up old parties
        inactive_parties = []
        for party_id, party in self.listening_parties.items():
            # Check if party ended more than 1 hour ago
            if party.ended_at and (now - party.ended_at).total_seconds() > 3600:
                inactive_parties.append(party_id)
            # Check if party created but never started (more than 24 hours)
            elif not party.started_at and (now - party.created_at).total_seconds() > 86400:
                inactive_parties.append(party_id)
        
        for party_id in inactive_parties:
            party = self.listening_parties.pop(party_id, None)
            if party:
                # Remove from user parties
                for user_id in party.participants:
                    if user_id in self.user_parties:
                        self.user_parties[user_id].discard(party_id)
        
        # Clean up old games
        inactive_games = []
        for game_id, game in self.active_games.items():
            # Check if game ended more than 30 minutes ago
            if game.ended_at and (now - game.ended_at).total_seconds() > 1800:
                inactive_games.append(game_id)
            # Check if game created but never started (more than 1 hour)
            elif not game.started_at and (now - game.created_at).total_seconds() > 3600:
                inactive_games.append(game_id)
        
        for game_id in inactive_games:
            game = self.active_games.pop(game_id, None)
            if game:
                # Remove from user games
                for user_id in game.players:
                    if user_id in self.user_games:
                        self.user_games[user_id].discard(game_id)
        
        if inactive_parties or inactive_games:
            logger.info(f"Cleaned up {len(inactive_parties)} parties and {len(inactive_games)} games")
    
    @tasks.loop(hours=1)
    async def update_tastes(self):
        """Update user taste profiles"""
        # This would update from listening history
        pass


class CollabQueueView(discord.ui.View):
    """Collaborative queue controls"""
    
    def __init__(self, social_cog, guild_id: int):
        super().__init__(timeout=None)
        self.social_cog = social_cog
        self.guild_id = guild_id
    
    @discord.ui.button(label="➕ Add Track", style=discord.ButtonStyle.success)
    async def add_track_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Add track to collaborative queue"""
        await interaction.response.send_message(
            "Use `e!play` to add tracks to the collaborative queue!",
            ephemeral=True
        )
    
    @discord.ui.button(label="📋 View Queue", style=discord.ButtonStyle.primary)
    async def view_queue_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """View collaborative queue"""
        queue_data = self.social_cog.collaborative_queues.get(self.guild_id)
        if not queue_data or not queue_data['tracks']:
            await interaction.response.send_message("Queue is empty!", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="🤝 Collaborative Queue",
            color=self.social_cog.bot.colors.VIOLET
        )
        
        for i, track in enumerate(queue_data['tracks'][:10], 1):
            embed.add_field(
                name=f"{i}. {track.get('title', 'Unknown')}",
                value=f"Added by: <@{track.get('added_by', 'Unknown')}>",
                inline=False
            )
        
        if len(queue_data['tracks']) > 10:
            embed.set_footer(text=f"...and {len(queue_data['tracks']) - 10} more tracks")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="⏹️ Stop Queue", style=discord.ButtonStyle.danger)
    async def stop_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Stop collaborative queue"""
        queue_data = self.social_cog.collaborative_queues.get(self.guild_id)
        if not queue_data:
            await interaction.response.send_message("No collaborative queue active!", ephemeral=True)
            return
        
        # Check if user is creator
        if interaction.user.id != queue_data['creator_id']:
            await interaction.response.send_message("Only the creator can stop the queue!", ephemeral=True)
            return
        
        self.social_cog.collaborative_queues.pop(self.guild_id, None)
        await interaction.response.send_message("🛑 Collaborative queue stopped!")


class PartyControlsView(discord.ui.View):
    """Listening party controls"""
    
    def __init__(self, social_cog, party_id: str):
        super().__init__(timeout=300)
        self.social_cog = social_cog
        self.party_id = party_id
    
    @discord.ui.button(label="🎵 Start Party", style=discord.ButtonStyle.success)
    async def start_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Start the listening party"""
        party = self.social_cog.listening_parties.get(self.party_id)
        if not party:
            await interaction.response.send_message("Party not found!", ephemeral=True)
            return
        
        # Check if user is host
        if interaction.user.id != party.host_id:
            await interaction.response.send_message("Only the host can start the party!", ephemeral=True)
            return
        
        # Check if in voice
        if not interaction.user.voice:
            await interaction.response.send_message("Join the party voice channel first!", ephemeral=True)
            return
        
        # Start party
        party.is_playing = True
        party.started_at = datetime.utcnow()
        
        # Connect music bot if needed
        music_cog = self.social_cog.bot.get_cog('MusicSystem')
        if music_cog:
            # This would set up synchronized playback
            pass
        
        await interaction.response.send_message(
            f"🎉 Listening party started! {len(party.participants)} participants ready!"
        )
    
    @discord.ui.button(label="👥 Invite", style=discord.ButtonStyle.primary)
    async def invite_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Invite users to party"""
        party = self.social_cog.listening_parties.get(self.party_id)
        if not party:
            await interaction.response.send_message("Party not found!", ephemeral=True)
            return
        
        # Create invite embed
        embed = discord.Embed(
            title=f"🎧 Invitation to {party.name}",
            description=party.description or "Join our listening party!",
            color=party.theme_color or self.social_cog.bot.colors.VIOLET
        )
        
        embed.add_field(
            name="Party Info",
            value=(
                f"**Host:** <@{party.host_id}>\n"
                f"**Participants:** {len(party.participants)}/{party.max_participants}\n"
                f"**Voice Channel:** <#{party.voice_channel_id}>"
            ),
            inline=False
        )
        
        embed.add_field(
            name="How to Join",
            value=f"1. Join the voice channel\n2. Click **Join Party** below\n3. Or use `e!party join {party.id}`",
            inline=False
        )
        
        view = PartyInviteView(self.social_cog, party.id)
        
        await interaction.response.send_message(
            content=f"Invite users to the party!",
            embed=embed,
            view=view,
            ephemeral=True
        )
class PartyInviteView(discord.ui.View):
    """Party invitation view"""
    
    def __init__(self, social_cog, party_id: str):
        super().__init__(timeout=300)
        self.social_cog = social_cog
        self.party_id = party_id
    
    @discord.ui.button(label="👋 Join Party", style=discord.ButtonStyle.success)
    async def join_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Join the party"""
        party = self.social_cog.listening_parties.get(self.party_id)
        if not party:
            await interaction.response.send_message("Party not found!", ephemeral=True)
            return
        
        # Check if in voice
        if not interaction.user.voice:
            await interaction.response.send_message("Join a voice channel first!", ephemeral=True)
            return
        
        # Add participant
        success = party.add_participant(interaction.user.id)
        
        if success:
            await interaction.response.send_message(
                f"🎉 Joined {party.name}!",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "Could not join party. It might be full or private.",
                ephemeral=True
            )


class PartySettingsView(discord.ui.View):
    """Party settings view"""
    
    def __init__(self, social_cog, party_id: str):
        super().__init__(timeout=300)
        self.social_cog = social_cog
        self.party_id = party_id
    
    @discord.ui.button(label="Vote to Skip", style=discord.ButtonStyle.primary)
    async def vote_skip_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Toggle vote to skip"""
        party = self.social_cog.listening_parties.get(self.party_id)
        if party:
            party.vote_to_skip = not party.vote_to_skip
            status = "enabled" if party.vote_to_skip else "disabled"
            await interaction.response.send_message(f"Vote to skip {status}!", ephemeral=True)
    
    @discord.ui.button(label="Queue Adds", style=discord.ButtonStyle.primary)
    async def queue_adds_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Toggle queue adds"""
        party = self.social_cog.listening_parties.get(self.party_id)
        if party:
            party.allow_queue_adds = not party.allow_queue_adds
            status = "enabled" if party.allow_queue_adds else "disabled"
            await interaction.response.send_message(f"Queue adds {status}!", ephemeral=True)


class GameLobbyView(discord.ui.View):
    """Game lobby controls"""

    def __init__(self, social_cog, game_id: str):
        super().__init__(timeout=300)
        self.social_cog = social_cog
        self.game_id = game_id

    # The erroneous code block is removed. The correct GameLobbyView is defined below.

    @discord.ui.button(label="🎮 Join Game", style=discord.ButtonStyle.success)
    async def join_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Join the game"""
        game = self.social_cog.active_games.get(self.game_id)
        if not game:
            await interaction.response.send_message("Game not found!", ephemeral=True)
            return

        # Check if already in game
        if interaction.user.id in game.players:
            await interaction.response.send_message("You're already in this game!", ephemeral=True)
            return

        # Check if game is full
        if len(game.players) >= game.max_participants:
            await interaction.response.send_message("Game is full!", ephemeral=True)
            return

        # Add player
        await game.add_player(interaction.user.id, interaction.user.name)

        # Update user games
        if interaction.user.id not in self.social_cog.user_games:
            self.social_cog.user_games[interaction.user.id] = set()
        self.social_cog.user_games[interaction.user.id].add(game.id)

        await interaction.response.send_message(
            f"🎮 {interaction.user.mention} joined the game! "
            f"({len(game.players)}/{game.max_participants} players)"
        )

    @discord.ui.button(label="▶️ Start Game", style=discord.ButtonStyle.primary)
    async def start_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Start the game"""
        game = self.social_cog.active_games.get(self.game_id)
        if not game:
            await interaction.response.send_message("Game not found!", ephemeral=True)
            return

        # Check if user is host
        if interaction.user.id != game.host_id:
            await interaction.response.send_message("Only the host can start the game!", ephemeral=True)
            return

        # Check if enough players
        if len(game.players) < 2:
            await interaction.response.send_message("Need at least 2 players to start!", ephemeral=True)
            return

        # Start game
        await game.start_game()

        # Start first round
        if hasattr(game, 'start_round'):
            await game.start_round()

        await interaction.response.send_message(
            f"🎮 Game started! {len(game.players)} players competing!"
        )


class GameLobbyView(discord.ui.View):
    """Game lobby controls"""
    
    def __init__(self, social_cog, game_id: str):
        super().__init__(timeout=300)
        self.social_cog = social_cog
        self.game_id = game_id
    
    @discord.ui.button(label="🎮 Join Game", style=discord.ButtonStyle.success)
    async def join_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Join the game"""
        game = self.social_cog.active_games.get(self.game_id)
        if not game:
            await interaction.response.send_message("Game not found!", ephemeral=True)
            return
        
        # Check if already in game
        if interaction.user.id in game.players:
            await interaction.response.send_message("You're already in this game!", ephemeral=True)
            return
        
        # Check if game is full
        if len(game.players) >= game.max_participants:
            await interaction.response.send_message("Game is full!", ephemeral=True)
            return
        
        # Add player
        await game.add_player(interaction.user.id, interaction.user.name)
        
        # Update user games
        if interaction.user.id not in self.social_cog.user_games:
            self.social_cog.user_games[interaction.user.id] = set()
        self.social_cog.user_games[interaction.user.id].add(game.id)
        
        await interaction.response.send_message(
            f"🎮 {interaction.user.mention} joined the game! "
            f"({len(game.players)}/{game.max_participants} players)"
        )
    
    @discord.ui.button(label="▶️ Start Game", style=discord.ButtonStyle.primary)
    async def start_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Start the game"""
        game = self.social_cog.active_games.get(self.game_id)
        if not game:
            await interaction.response.send_message("Game not found!", ephemeral=True)
            return
        
        # Check if user is host
        if interaction.user.id != game.host_id:
            await interaction.response.send_message("Only the host can start the game!", ephemeral=True)
            return
        
        # Check if enough players
        if len(game.players) < 2:
            await interaction.response.send_message("Need at least 2 players to start!", ephemeral=True)
            return
        
        # Start game
        await game.start_game()
        
        # Start first round
        if hasattr(game, 'start_round'):
            await game.start_round()
        
        await interaction.response.send_message(
            f"🎮 Game started! {len(game.players)} players competing!"
        )
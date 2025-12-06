#!/usr/bin/env python3
"""
EVE - Complete All-Rounder Discord Bot
Main entry point that loads all cogs with full permissions and intents.
No built-in functionality - all features come from cogs.
"""

import discord
from discord.ext import commands
import asyncio
import logging
import sys
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional
import json
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(f'eve_{datetime.now(timezone.utc).strftime("%Y%m%d")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('EVE')

# Custom colors for embeds
class EveColors:
    """Color scheme for EVE bot"""
    PRIMARY = 0x9B59B6     # Purple
    SECONDARY = 0xE91E63   # Pink
    SUCCESS = 0x2ECC71     # Green
    WARNING = 0xF1C40F     # Yellow
    ERROR = 0xE74C3C       # Red
    INFO = 0x3498DB        # Blue

    # Personality-specific colors
    ELEGANT = 0x8E44AD     # Deep Purple
    SEDUCTIVE = 0xE84393   # Rose Pink
    PLAYFUL = 0xFF9FF3     # Light Pink
    SUPPORTIVE = 0x74B9FF  # Light Blue
    TEACHER = 0xA29BFE     # Periwinkle
    OBEDIENT = 0x636E72    # Gray

# Bot personality system
class Personality:
    """Personality system for EVE"""
    MODES = ['elegant', 'seductive', 'playful', 'supportive', 'teacher', 'obedient']

    @staticmethod
    def get_response(mode: str, key: str, **kwargs) -> str:
        """Get personality-based response"""
        responses = {
            "elegant": {
                "startup": "EVE awakens... The symphony begins.",
                "shutdown": "EVE rests... Until we meet again.",
                "error": "An orchestral misstep...",
                "cog_load": "Loading {cog}... The harmony expands.",
                "ready": "Ready to serve... Every note in its place."
            },
            "seductive": {
                "startup": "EVE stirs... Ready to please. 💋",
                "shutdown": "Sleeping... Dream of me. 🌹",
                "error": "A pleasurable mistake... Let's try again.",
                "cog_load": "Adding {cog} to my repertoire...",
                "ready": "At your service, darling... What's your desire? 💫"
            },
            "playful": {
                "startup": "EVE IS ALIVE! LET'S GOOO! 🎉",
                "shutdown": "Bye-bye! See you later! 👋",
                "error": "OOPSIE! Something broke! 😅",
                "cog_load": "Loading {cog}... BOOM! 💥",
                "ready": "READY TO PARTY! 🎊🎵"
            },
            "supportive": {
                "startup": "EVE is here for you... Let's begin.",
                "shutdown": "Resting now... Take care of yourself.",
                "error": "It's okay to stumble... Let's try again.",
                "cog_load": "Learning {cog}... For you.",
                "ready": "Ready to help... Together we can do anything. 💫"
            },
            "teacher": {
                "startup": "EVE initializing... Lesson begins.",
                "shutdown": "Session concluded... Study complete.",
                "error": "An educational setback... Analyze and correct.",
                "cog_load": "Integrating {cog} module... Processing.",
                "ready": "System ready... Awaiting instructions."
            },
            "obedient": {
                "startup": "EVE online. Systems nominal.",
                "shutdown": "Shutting down. Goodbye.",
                "error": "Error detected. Attempting recovery.",
                "cog_load": "Loading module: {cog}.",
                "ready": "Operational. Awaiting commands."
            }
        }

        mode_responses = responses.get(mode, responses["elegant"])
        response = mode_responses.get(key, "")
        return response.format(**kwargs) if response else ""

class EVE(commands.Bot):
    """Main EVE bot class with all intents and permissions"""

    def __init__(self):
        # Configure intents
        intents = discord.Intents.all()

        # Initialize with command prefix
        super().__init__(
            command_prefix=self.get_prefix,
            intents=intents,
            help_command=None,  # Custom help will be in cog
            case_insensitive=True,
            strip_after_prefix=True,
            max_messages=10000,
            chunk_guilds_at_startup=True
        )

        # Core properties
        self.start_time = datetime.now(timezone.utc)
        self.colors = EveColors()
        self.personality_mode = os.getenv('EVE_PERSONALITY', 'elegant')

        # Statistics
        self.stats = {
            'commands_processed': 0,
            'messages_seen': 0,
            'cogs_loaded': 0,
            'servers_joined': 0,
            'errors': 0
        }

        # Configuration
        self.config = self.load_config()

        # Cog tracking
        self.available_cogs = self.find_cogs()
        self.loaded_cogs = []
        self.failed_cogs = []

        logger.info(f"EVE initialized with personality: {self.personality_mode}")

    def load_config(self) -> Dict:
        """Load configuration from file and environment"""
        config = {
            'token': os.getenv('DISCORD_TOKEN'),
            'owner_ids': self.parse_owner_ids(),
            'database_url': os.getenv('DATABASE_URL'),
            'lavalink_nodes': self.parse_lavalink_nodes(),
            'debug': os.getenv('EVE_DEBUG', 'false').lower() == 'true',
            'log_level': os.getenv('LOG_LEVEL', 'INFO')
        }

        # Load from config file if exists
        config_path = Path('config.json')
        if config_path.exists():
            try:
                with open(config_path) as f:
                    file_config = json.load(f)
                    config.update(file_config)
            except json.JSONDecodeError:
                logger.warning("Failed to parse config.json")

        # Validate required config
        if not config['token']:
            logger.critical("DISCORD_TOKEN not found in environment variables!")
            sys.exit(1)

        return config

    def parse_owner_ids(self) -> List[int]:
        """Parse owner IDs from environment"""
        owners_str = os.getenv('OWNER_IDS', '')
        if not owners_str:
            return []

        try:
            return [int(owner_id.strip()) for owner_id in owners_str.split(',')]
        except ValueError:
            logger.warning(f"Invalid owner IDs format: {owners_str}")
            return []

    def parse_lavalink_nodes(self) -> List[Dict]:
        """Parse Lavalink nodes from environment"""
        nodes_str = os.getenv('LAVALINK_NODES', '[]')
        try:
            return json.loads(nodes_str)
        except json.JSONDecodeError:
            logger.warning("Invalid LAVALINK_NODES format")
            return []

    async def get_prefix(self, bot, message) -> List[str]:
        """Get command prefixes"""
        prefixes = ['e!', 'E!', 'eve ', 'EVE ', 'e.', 'E.']

        # Add mention as prefix
        prefixes.append(f'<@{bot.user.id}> ')
        prefixes.append(f'<@!{bot.user.id}> ')

        return commands.when_mentioned_or(*prefixes)(bot, message)

    def find_cogs(self) -> List[str]:
        """Find all available cogs in the cogs directory"""
        cogs_dir = Path('cogs')
        available_cogs = []

        if not cogs_dir.exists():
            logger.warning("cogs directory not found!")
            return available_cogs

        # Find all Python files in cogs directory
        for cog_file in cogs_dir.glob('**/*.py'):
            if cog_file.name.startswith('_'):
                continue

            # Convert file path to module path
            rel_path = cog_file.relative_to(Path('.'))
            module_path = str(rel_path.with_suffix('')).replace('/', '.')

            # Skip __pycache__
            if '__pycache__' in module_path:
                continue

            available_cogs.append(module_path)

        logger.info(f"Found {len(available_cogs)} potential cogs")
        return available_cogs

    async def load_all_cogs(self):
        """Load all available cogs"""
        logger.info("Starting cog loading process...")

        for cog_path in self.available_cogs:
            try:
                await self.load_extension(cog_path)
                self.loaded_cogs.append(cog_path)
                self.stats['cogs_loaded'] += 1

                message = Personality.get_response(
                    self.personality_mode,
                    'cog_load',
                    cog=cog_path.split('.')[-1]
                )
                logger.info(f"✅ {message}")

                # Small delay to avoid rate limits
                await asyncio.sleep(0.5)

            except Exception as e:
                self.failed_cogs.append((cog_path, str(e)))
                self.stats['errors'] += 1
                logger.error(f"❌ Failed to load {cog_path}: {e}")

        # Report loading results
        self.report_cog_loading()

    def report_cog_loading(self):
        """Report cog loading results"""
        logger.info(f"\n{'='*50}")
        logger.info("COG LOADING REPORT")
        logger.info(f"{'='*50}")
        logger.info(f"✅ Successfully loaded: {len(self.loaded_cogs)}/{len(self.available_cogs)}")

        if self.failed_cogs:
            logger.info("❌ Failed cogs:")
            for cog_path, error in self.failed_cogs:
                logger.info(f"  - {cog_path}: {error}")

        logger.info(f"{'='*50}\n")

    async def setup_hook(self):
        """Setup hook that runs before the bot starts"""
        # DO NOT call change_presence() here - the bot isn't connected yet
        
        # Load all cogs
        await self.load_all_cogs()

        # Sync application commands
        await self.tree.sync()

        logger.info("Setup hook completed")

    async def on_ready(self):
        """Called when the bot is ready"""
        # Calculate startup time
        startup_time = (datetime.now(timezone.utc) - self.start_time).total_seconds()

        # Update statistics
        self.stats['servers_joined'] = len(self.guilds)

        # Set initial presence here (AFTER bot is connected)
        try:
            await self.change_presence(
                status=discord.Status.idle,
                activity=discord.Activity(
                    type=discord.ActivityType.watching,
                    name="the stars... 🌌"
                )
            )
            logger.info("✅ Bot presence set successfully")
        except Exception as e:
            logger.error(f"Failed to set initial presence: {e}")

        # Get personality message
        ready_message = Personality.get_response(self.personality_mode, 'ready')

        # Log ready status
        logger.info(f"\n{'='*50}")
        logger.info(f"🤖 {self.user.name} is ready!")
        logger.info(f"📊 ID: {self.user.id}")
        logger.info(f"⚙️ Personality: {self.personality_mode.title()}")
        logger.info(f"⏱️ Startup time: {startup_time:.2f}s")
        logger.info(f"🏠 Servers: {len(self.guilds)}")
        logger.info(f"📦 Cogs loaded: {self.stats['cogs_loaded']}")
        logger.info(f"🎮 Game: {self.config.get('game_status', 'Managing servers')}")
        logger.info(f"{ready_message}")
        logger.info(f"{'='*50}\n")

        # Start background tasks
        self.bg_task = self.loop.create_task(self.background_task())

    async def update_presence(self):
        """Update bot presence with dynamic information"""
        try:
            # Only update if we have guilds
            if len(self.guilds) > 0:
                activities = [
                    discord.Activity(
                        type=discord.ActivityType.listening,
                        name=f"{len(self.guilds)} servers"
                    ),
                    discord.Activity(
                        type=discord.ActivityType.watching,
                        name=f"{self.stats['cogs_loaded']} systems"
                    ),
                    discord.Activity(
                        type=discord.ActivityType.playing,
                        name=f"with {len(self.users)} users"
                    ),
                    discord.Activity(
                        type=discord.ActivityType.competing,
                        name=f"{self.stats['commands_processed']} commands"
                    )
                ]

                # Get current minute to rotate activities
                current_minute = datetime.now(timezone.utc).minute
                activity_index = (current_minute // 30) % len(activities)

                await self.change_presence(
                    activity=activities[activity_index],
                    status=discord.Status.online
                )
        except Exception as e:
            logger.warning(f"Failed to update presence: {e}")

    async def background_task(self):
        """Background task that runs periodically"""
        await self.wait_until_ready()

        while not self.is_closed():
            try:
                # Update presence periodically (every 5 minutes)
                await self.update_presence()

                # Save statistics (could be to database)
                await self.save_statistics()

                # Check for cog reloads
                await self.check_cog_updates()

                # Sleep for 5 minutes
                await asyncio.sleep(300)

            except Exception as e:
                logger.error(f"Background task error: {e}")
                await asyncio.sleep(60)

    async def save_statistics(self):
        """Save bot statistics"""
        # This would save to database in production
        stats_file = Path('eve_stats.json')
        stats_data = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'stats': self.stats,
            'servers': len(self.guilds),
            'users': len(self.users),
            'cogs_loaded': len(self.loaded_cogs),
            'uptime': (datetime.now(timezone.utc) - self.start_time).total_seconds()
        }

        try:
            with open(stats_file, 'w') as f:
                json.dump(stats_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save statistics: {e}")

    async def check_cog_updates(self):
        """Check for new cogs or cog updates"""
        # In production, this could check for git updates
        # or hot-reload cogs that have changed
        pass

    async def on_guild_join(self, guild):
        """Called when bot joins a guild"""
        self.stats['servers_joined'] += 1

        # Send welcome message (optional)
        try:
            system_channel = guild.system_channel
            if system_channel and system_channel.permissions_for(guild.me).send_messages:
                embed = discord.Embed(
                    title="✨ EVE has arrived!",
                    description=(
                        "Thank you for inviting me to your server!\n\n"
                        "I'm a complete all-rounder bot with features including:\n"
                        "• 🎵 **Professional Music System**\n"
                        "• 🎮 **Games & Entertainment**\n"
                        "• 🔧 **Utility & Moderation**\n"
                        "• 📊 **Statistics & Analytics**\n"
                        "• 🎭 **Multiple Personalities**\n\n"
                        f"Use `{self.command_prefix}help` to see all commands!\n"
                        f"My current personality: **{self.personality_mode.title()}**"
                    ),
                    color=self.colors.PRIMARY,
                    timestamp=datetime.now(timezone.utc)
                )

                embed.set_footer(text=f"EVE v1.0 • {len(self.guilds)} servers")

                await system_channel.send(embed=embed)
        except Exception as e:
            logger.warning(f"Could not send welcome message to {guild.name}: {e}")

        logger.info(f"Joined new guild: {guild.name} (ID: {guild.id})")

    async def on_guild_remove(self, guild):
        """Called when bot leaves a guild"""
        self.stats['servers_joined'] = max(0, self.stats['servers_joined'] - 1)
        logger.info(f"Left guild: {guild.name} (ID: {guild.id})")

    async def on_command_completion(self, ctx):
        """Called when a command completes successfully"""
        self.stats['commands_processed'] += 1

        # Log command usage
        logger.info(f"Command used: {ctx.command} by {ctx.author} in {ctx.guild}")

    async def on_command_error(self, ctx, error):
        """Handle command errors"""
        self.stats['errors'] += 1

        # Ignore certain errors
        ignored_errors = (commands.CommandNotFound, commands.NotOwner)
        if isinstance(error, ignored_errors):
            return

        # Get error message based on personality
        error_msg = Personality.get_response(self.personality_mode, 'error')

        # Create error embed
        embed = discord.Embed(
            title="⚠️ Error",
            description=f"{error_msg}\n\n`{type(error).__name__}: {str(error)[:100]}`",
            color=self.colors.ERROR
        )

        # Send error message
        try:
            if ctx.command:
                embed.set_footer(text=f"Command: {ctx.command.name}")

            if isinstance(error, commands.MissingPermissions):
                embed.description = f"You don't have permission to use `{ctx.command.name}`"
            elif isinstance(error, commands.BotMissingPermissions):
                embed.description = f"I need more permissions to run `{ctx.command.name}`"
            elif isinstance(error, commands.MissingRequiredArgument):
                embed.description = f"Missing required argument for `{ctx.command.name}`"
            elif isinstance(error, commands.BadArgument):
                embed.description = f"Invalid argument for `{ctx.command.name}`"
            elif isinstance(error, commands.CommandOnCooldown):
                embed.description = f"Command on cooldown. Try again in {error.retry_after:.1f}s"

            await ctx.send(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Failed to send error message: {e}")

        # Log the error
        logger.error(f"Command error in {ctx.command}: {error}", exc_info=True)

    async def on_message(self, message):
        """Process every message"""
        # Ignore bot messages
        if message.author.bot:
            return

        self.stats['messages_seen'] += 1

        # Process commands
        await self.process_commands(message)

    async def on_message_edit(self, before, after):
        """Handle message edits for command processing"""
        if after.content != before.content:
            await self.process_commands(after)

    async def close(self):
        """Clean shutdown"""
        shutdown_msg = Personality.get_response(self.personality_mode, 'shutdown')
        logger.info(f"\n{'='*50}")
        logger.info(f"🛑 Shutting down...")
        logger.info(f"{shutdown_msg}")
        logger.info(f"{'='*50}")

        # Save final statistics
        await self.save_statistics()

        # Cancel background task
        if hasattr(self, 'bg_task'):
            self.bg_task.cancel()
            try:
                await self.bg_task
            except asyncio.CancelledError:
                pass

        # Call parent close method
        await super().close()

    def run(self):
        """Run the bot with token from config"""
        startup_msg = Personality.get_response(self.personality_mode, 'startup')
        logger.info(f"\n{'='*50}")
        logger.info(f"🚀 {startup_msg}")
        logger.info(f"{'='*50}")

        try:
            super().run(
                self.config['token'],
                reconnect=True,
                log_handler=None
            )
        except KeyboardInterrupt:
            logger.info("\n\nReceived interrupt signal. Shutting down gracefully...")
        except Exception as e:
            logger.critical(f"Failed to run bot: {e}")
            sys.exit(1)

# Command line interface
def main():
    """Main entry point with command line arguments"""
    import argparse

    parser = argparse.ArgumentParser(description='EVE Discord Bot')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--personality', choices=Personality.MODES, 
                       help='Set bot personality mode')
    parser.add_argument('--no-cogs', action='store_true', 
                       help='Run without loading cogs')
    parser.add_argument('--cog', action='append', 
                       help='Load specific cog(s) only')

    args = parser.parse_args()

    # Set environment variables from args
    if args.debug:
        os.environ['EVE_DEBUG'] = 'true'
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("Debug mode enabled")

    if args.personality:
        os.environ['EVE_PERSONALITY'] = args.personality

    # Create and run bot
    bot = EVE()

    # Modify cog loading if specified
    if args.no_cogs:
        bot.available_cogs = []
        logger.warning("Running without cogs (bare bot)")
    elif args.cog:
        bot.available_cogs = [f"cogs.{cog}" for cog in args.cog]
        logger.info(f"Loading specific cogs only: {', '.join(args.cog)}")

    try:
        bot.run()
    except KeyboardInterrupt:
        logger.info("\nShutdown complete.")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    # Add current directory to Python path
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    # Run main function
    main()
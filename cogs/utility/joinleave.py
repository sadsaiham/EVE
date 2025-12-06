import discord
from discord.ext import commands, tasks
from discord import app_commands
import sqlite3
import json
import logging
import asyncio
from pathlib import Path
import random
from typing import Literal, Optional, List
from datetime import datetime, timedelta
from collections import defaultdict
import io
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

logger = logging.getLogger("JoinLeaveTracker")

# Database file
DB_PATH = Path("data/joinleave.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Default messages
DEFAULT_BOT_WELCOME = [
    "Hope you won't raid us!",
    "Have you come to steal my job?",
    "Another bot? This place is getting crowded!",
    "Great, another bot. What are YOU gonna do?",
    "Oh no, the robot uprising has begun!",
]

DEFAULT_BOT_GOODBYE = [
    "RIP bot. We hardly knew ye.",
    "One less bot to compete with. Phew!",
]

DEFAULT_HUMAN_WELCOME = [
    "New member alert! Let's be nice to them.",
    "Fresh blood has entered the server!",
    "Another legend just joined!",
    "We've got a new member in the house!",
]

DEFAULT_HUMAN_GOODBYE = [
    "Goodbye! Thanks for the memories!",
    "A member has left us. F.",
    "They've abandoned us!",
    "Goodbye, friend! Come back soon!",
]

# Developer IDs
DEVELOPER_IDS = [
    1313333441525448704,
]

def init_db():
    """Initialize database tables with proper error handling - FIXED TO PRESERVE DATA"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()

        # Config table - ONLY CREATE IF IT DOESN'T EXIST (FIXED)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS joinleave_config (
                guild_id INTEGER PRIMARY KEY,
                tracker_enabled BOOLEAN DEFAULT 1,
                human_welcome_enabled BOOLEAN DEFAULT 1,
                bot_welcome_enabled BOOLEAN DEFAULT 1,
                ping_enabled BOOLEAN DEFAULT 1,
                stats_channel_id INTEGER,
                welcome_channel_id INTEGER,
                anniversary_channel_id INTEGER,
                roles_channel_id INTEGER,
                rules_channel_id INTEGER,
                error_log_channel_id INTEGER,
                bot_role_id INTEGER,
                member_role_id INTEGER,
                fallback_stats_channel_id INTEGER,
                fallback_welcome_channel_id INTEGER,
                welcome_roles TEXT,
                welcome_users TEXT,
                bot_welcome_messages TEXT,
                bot_goodbye_messages TEXT,
                human_welcome_messages TEXT,
                human_goodbye_messages TEXT
            )
        """)

        # Join/Leave history table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS joinleave_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                event_type TEXT NOT NULL,
                is_bot BOOLEAN NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                reason TEXT
            )
        """)

        # Member join dates table (for anniversaries) - UPDATED WITH NEW COLUMNS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS member_join_dates (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                join_date DATETIME NOT NULL,
                username TEXT,
                last_anniversary_year INTEGER DEFAULT 0,
                anniversary_notified BOOLEAN DEFAULT 0,
                PRIMARY KEY (guild_id, user_id)
            )
        """)

        # Analytics cache table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS analytics_cache (
                guild_id INTEGER PRIMARY KEY,
                daily_joins TEXT,
                weekly_joins TEXT,
                monthly_joins TEXT,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        conn.close()
        logger.info("✅ JoinLeaveTracker database initialized - existing data preserved")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        # Try a simpler approach if the above fails
        try:
            conn = sqlite3.connect(str(DB_PATH))
            cur = conn.cursor()
            # Just create the main config table with minimal columns IF NOT EXISTS
            cur.execute("""
                CREATE TABLE IF NOT EXISTS joinleave_config (
                    guild_id INTEGER PRIMARY KEY,
                    tracker_enabled BOOLEAN DEFAULT 1
                )
            """)
            conn.commit()
            conn.close()
            logger.info("✅ Basic database initialized - existing data preserved")
        except Exception as e2:
            logger.error(f"Failed to initialize basic database: {e2}")

def get_config(guild_id: int) -> dict:
    """Load config for a specific guild from database with error handling"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # First check if the table exists and has the expected columns
        cur.execute("PRAGMA table_info(joinleave_config)")
        columns = [row[1] for row in cur.fetchall()]
        
        # Build query based on available columns
        if columns:
            # Create a safe SELECT statement with only existing columns
            select_columns = []
            for col in [
                "guild_id", "tracker_enabled", "human_welcome_enabled", "bot_welcome_enabled",
                "ping_enabled", "stats_channel_id", "welcome_channel_id", "anniversary_channel_id",
                "roles_channel_id", "rules_channel_id", "error_log_channel_id", "bot_role_id", "member_role_id",
                "fallback_stats_channel_id", "fallback_welcome_channel_id", "welcome_roles",
                "welcome_users", "bot_welcome_messages", "bot_goodbye_messages",
                "human_welcome_messages", "human_goodbye_messages"
            ]:
                if col in columns:
                    select_columns.append(col)
            
            if select_columns:
                query = f"SELECT {', '.join(select_columns)} FROM joinleave_config WHERE guild_id = ?"
                cur.execute(query, (guild_id,))
                row = cur.fetchone()
                
                if row:
                    config = {}
                    for col in select_columns:
                        config[col] = row[col]
                    
                    # Ensure all expected keys exist
                    full_config = get_default_config()
                    full_config.update(config)
                    
                    # Parse JSON fields
                    for json_field in ["welcome_roles", "welcome_users", "bot_welcome_messages", 
                                     "bot_goodbye_messages", "human_welcome_messages", "human_goodbye_messages"]:
                        if json_field in config and config[json_field]:
                            try:
                                full_config[json_field] = json.loads(config[json_field])
                            except (json.JSONDecodeError, TypeError):
                                # Keep default if parsing fails
                                pass
                    
                    conn.close()
                    return full_config
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Failed to load config from database for guild {guild_id}: {e}")

    return get_default_config()

def get_default_config() -> dict:
    """Get default config structure"""
    return {
        "guild_id": None,
        "tracker_enabled": True,
        "human_welcome_enabled": True,
        "bot_welcome_enabled": True,
        "ping_enabled": True,
        "stats_channel_id": None,
        "welcome_channel_id": None,
        "anniversary_channel_id": None,
        "roles_channel_id": None,
        "rules_channel_id": None,
        "error_log_channel_id": None,
        "bot_role_id": None,
        "member_role_id": None,
        "fallback_stats_channel_id": None,
        "fallback_welcome_channel_id": None,
        "welcome_roles": [],
        "welcome_users": [],
        "bot_welcome_messages": DEFAULT_BOT_WELCOME,
        "bot_goodbye_messages": DEFAULT_BOT_GOODBYE,
        "human_welcome_messages": DEFAULT_HUMAN_WELCOME,
        "human_goodbye_messages": DEFAULT_HUMAN_GOODBYE,
    }

def save_config(guild_id: int, config: dict):
    """Save config for a specific guild to database"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()

        # Check which columns exist
        cur.execute("PRAGMA table_info(joinleave_config)")
        existing_columns = [row[1] for row in cur.fetchall()]
        
        # Prepare data for insertion
        columns = ["guild_id"]
        placeholders = ["?"]
        values = [guild_id]
        
        # Only include columns that exist in the table
        for key, value in config.items():
            if key != "guild_id" and key in existing_columns:
                columns.append(key)
                placeholders.append("?")
                
                # Convert lists to JSON
                if key in ["welcome_roles", "welcome_users", "bot_welcome_messages", 
                          "bot_goodbye_messages", "human_welcome_messages", "human_goodbye_messages"]:
                    values.append(json.dumps(value))
                else:
                    values.append(value)
        
        # Use INSERT OR REPLACE to handle both new and existing records
        query = f"""
            INSERT OR REPLACE INTO joinleave_config ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """
        
        cur.execute(query, values)
        conn.commit()
        conn.close()
        logger.info(f"✅ Config saved for guild {guild_id}")
        
    except Exception as e:
        logger.error(f"Failed to save config to database for guild {guild_id}: {e}")
        # Try to initialize database and retry
        try:
            init_db()
            save_config(guild_id, config)
        except Exception as e2:
            logger.error(f"Failed to save config after reinitialization: {e2}")

def log_event(guild_id: int, user_id: int, username: str, event_type: str, is_bot: bool, reason: str = None):
    """Log join/leave events to database"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO joinleave_history (guild_id, user_id, username, event_type, is_bot, reason)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (guild_id, user_id, username, event_type, is_bot, reason))
        
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("Failed to log event")

def save_join_date(guild_id: int, user_id: int, join_date: datetime, username: str = None):
    """Save member join date for anniversary tracking with enhanced columns"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()
        
        cur.execute("""
            INSERT OR REPLACE INTO member_join_dates 
            (guild_id, user_id, join_date, username, last_anniversary_year, anniversary_notified)
            VALUES (?, ?, ?, ?, 0, 0)
        """, (guild_id, user_id, join_date.isoformat(), username))
        
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("Failed to save join date")

def get_analytics(guild_id: int, period: str = "daily") -> dict:
    """Get join/leave analytics for specified period"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()
        
        now = datetime.now()
        if period == "daily":
            start_date = now - timedelta(days=30)
        elif period == "weekly":
            start_date = now - timedelta(weeks=12)
        else:  # monthly
            start_date = now - timedelta(days=365)
        
        cur.execute("""
            SELECT DATE(timestamp) as date, event_type, is_bot, COUNT(*) as count
            FROM joinleave_history
            WHERE guild_id = ? AND timestamp >= ?
            GROUP BY DATE(timestamp), event_type, is_bot
            ORDER BY date
        """, (guild_id, start_date.isoformat()))
        
        results = cur.fetchall()
        conn.close()
        
        analytics = defaultdict(lambda: {"joins": 0, "leaves": 0, "bot_joins": 0, "bot_leaves": 0})
        
        for row in results:
            date, event_type, is_bot, count = row
            if event_type == "join":
                analytics[date]["joins"] += count
                if is_bot:
                    analytics[date]["bot_joins"] += count
            else:
                analytics[date]["leaves"] += count
                if is_bot:
                    analytics[date]["bot_leaves"] += count
        
        return dict(analytics)
        
    except Exception:
        logger.exception("Failed to get analytics")
        return {}

def get_member_stats(guild: discord.Guild) -> dict:
    """Calculate member stats"""
    total = guild.member_count
    bots = sum(1 for m in guild.members if m.bot)
    humans = total - bots
    return {"total": total, "bots": bots, "humans": humans}

def is_developer(user: discord.User) -> bool:
    """Check if user is a developer"""
    return user.id in DEVELOPER_IDS

def detect_suspicious_activity(guild_id: int, time_window_minutes: int = 5, threshold: int = 10) -> dict:
    """Detect suspicious join/leave patterns"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()
        
        cutoff_time = datetime.now() - timedelta(minutes=time_window_minutes)
        
        cur.execute("""
            SELECT event_type, COUNT(*) as count
            FROM joinleave_history
            WHERE guild_id = ? AND timestamp >= ?
            GROUP BY event_type
        """, (guild_id, cutoff_time.isoformat()))
        
        results = cur.fetchall()
        conn.close()
        
        suspicious = {}
        for event_type, count in results:
            if count >= threshold:
                suspicious[event_type] = count
        
        return suspicious
        
    except Exception:
        logger.exception("Failed to detect suspicious activity")
        return {}

# ============ FIXED VIEWS WITH PROPER EMBED METHODS ============

class RefreshButton(discord.ui.Button):
    """Button to refresh and return to main menu"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(label="↻ Main Menu", style=discord.ButtonStyle.primary, row=4)
        self.guild_id = guild_id
        self.user_id = user_id
    
    async def callback(self, interaction: discord.Interaction):
        new_view = MainMenuView(self.guild_id, self.user_id)
        embed = new_view.get_embed()
        await interaction.response.edit_message(embed=embed, view=new_view)

class CloseButton(discord.ui.Button):
    """Button to close the control panel"""
    def __init__(self):
        super().__init__(label="❌ Close", style=discord.ButtonStyle.danger, row=4)
    
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="✅ Control panel closed.", embed=None, view=None)

class BackButton(discord.ui.Button):
    """Button to go back to previous menu"""
    def __init__(self, guild_id: int, user_id: int, back_view_class, label="⬅️ Back"):
        super().__init__(label=label, style=discord.ButtonStyle.secondary, row=4)
        self.guild_id = guild_id
        self.user_id = user_id
        self.back_view_class = back_view_class
    
    async def callback(self, interaction: discord.Interaction):
        view = self.back_view_class(self.guild_id, self.user_id)
        embed = view.get_embed()
        await interaction.response.edit_message(embed=embed, view=view)

def get_main_embed(guild_id: int) -> discord.Embed:
    """Get main menu embed"""
    config = get_config(guild_id)
    
    embed = discord.Embed(
        title="👋 Join/Leave Tracker Control Panel",
        description="Select an option from the dropdown menu below to manage your join/leave tracking system.",
        color=discord.Color.blue()
    )
    
    # Quick status
    status_lines = []
    status_lines.append(f"**Tracker:** {'✅ Enabled' if config['tracker_enabled'] else '❌ Disabled'}")
    status_lines.append(f"**Human Welcome:** {'✅ Enabled' if config['human_welcome_enabled'] else '❌ Disabled'}")
    status_lines.append(f"**Bot Welcome:** {'✅ Enabled' if config['bot_welcome_enabled'] else '❌ Disabled'}")
    status_lines.append(f"**Pings:** {'✅ Enabled' if config['ping_enabled'] else '❌ Disabled'}")
    
    embed.add_field(name="📊 Quick Status", value="\n".join(status_lines), inline=True)
    
    # Channel status
    channel_lines = []
    channel_lines.append(f"**Stats:** {'✅' if config['stats_channel_id'] else '❌'}")
    channel_lines.append(f"**Welcome:** {'✅' if config['welcome_channel_id'] else '❌'}")
    channel_lines.append(f"**Anniversary:** {'✅' if config['anniversary_channel_id'] else '❌'}")
    channel_lines.append(f"**Roles:** {'✅' if config['roles_channel_id'] else '❌'}")
    
    embed.add_field(name="📋 Channels", value="\n".join(channel_lines), inline=True)
    
    embed.set_footer(text="All interactions are private • Use dropdown to navigate")
    
    return embed

class MainMenuView(discord.ui.View):
    """Main control panel with dropdown menu"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        self.add_item(MainMenuDropdown(guild_id, user_id))
        self.add_item(CloseButton())
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This control panel is not for you!", ephemeral=True)
            return False
        return True

    def get_embed(self) -> discord.Embed:
        """Get the main menu embed"""
        return get_main_embed(self.guild_id)

class MainMenuDropdown(discord.ui.Select):
    """Main dropdown menu for all options"""
    def __init__(self, guild_id: int, user_id: int):
        self.guild_id = guild_id
        self.user_id = user_id
        
        options = [
            discord.SelectOption(
                label="📋 Setup Channels",
                description="Configure stats, welcome, and other channels",
                value="setup_channels",
                emoji="📋"
            ),
            discord.SelectOption(
                label="🎭 Setup Roles",
                description="Configure auto-role assignment",
                value="setup_roles",
                emoji="🎭"
            ),
            discord.SelectOption(
                label="🔔 Manage Pings",
                description="Set who gets pinged on welcomes",
                value="manage_pings",
                emoji="🔔"
            ),
            discord.SelectOption(
                label="💬 Manage Messages",
                description="Add/remove/view welcome messages",
                value="manage_messages",
                emoji="💬"
            ),
            discord.SelectOption(
                label="⚙️ Toggle Features",
                description="Enable/disable features",
                value="toggle_features",
                emoji="⚙️"
            ),
            discord.SelectOption(
                label="📊 View Analytics",
                description="See join/leave statistics",
                value="view_analytics",
                emoji="📊"
            ),
            discord.SelectOption(
                label="🧪 Test",
                description="Test welcome messages and anniversary",
                value="test",
                emoji="🧪"
            ),
        ]
        
        super().__init__(
            placeholder="Choose an option...",
            options=options,
            min_values=1,
            max_values=1
        )
    
    async def callback(self, interaction: discord.Interaction):
        value = self.values[0]
        
        if value == "setup_channels":
            view = ChannelSetupView(self.guild_id, self.user_id)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        
        elif value == "setup_roles":
            view = RoleSetupView(self.guild_id, self.user_id)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        
        elif value == "manage_pings":
            view = PingManagementView(self.guild_id, self.user_id)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        
        elif value == "manage_messages":
            view = MessageTypeSelectView(self.guild_id, self.user_id)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        
        elif value == "toggle_features":
            view = ToggleView(self.guild_id, self.user_id)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        
        elif value == "view_analytics":
            view = AnalyticsView(self.guild_id, self.user_id)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        
        elif value == "test":
            # Show test type selection
            view = TestTypeSelectView(self.guild_id, self.user_id)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)

# ============ NEW TEST TYPE SELECTION VIEW ============

class TestTypeSelectView(discord.ui.View):
    """View for selecting test type"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        self.add_item(TestTypeDropdown(guild_id, user_id))
        self.add_item(BackButton(guild_id, user_id, MainMenuView, "⬅️ Main Menu"))
        self.add_item(CloseButton())
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not for you!", ephemeral=True)
            return False
        return True
    
    def get_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="🧪 Test Messages",
            description="Select what type of message you want to test.",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="Available Tests",
            value=(
                "🤖 **Bot Welcome** - Test bot welcome message\n"
                "👤 **Human Welcome** - Test human welcome message\n"
                "🎉 **Anniversary** - Test anniversary celebration message"
            ),
            inline=False
        )
        
        return embed

class TestTypeDropdown(discord.ui.Select):
    """Dropdown for selecting test type"""
    def __init__(self, guild_id: int, user_id: int):
        self.guild_id = guild_id
        self.user_id = user_id
        
        options = [
            discord.SelectOption(label="Test Bot Welcome", value="test_bot_welcome", emoji="🤖"),
            discord.SelectOption(label="Test Human Welcome", value="test_human_welcome", emoji="👤"),
            discord.SelectOption(label="Test Anniversary", value="test_anniversary", emoji="🎉"),
        ]
        
        super().__init__(placeholder="Select test type...", options=options)
    
    async def callback(self, interaction: discord.Interaction):
        test_type = self.values[0]
        
        if test_type == "test_bot_welcome":
            await self.test_bot_welcome(interaction)
        elif test_type == "test_human_welcome":
            await self.test_human_welcome(interaction)
        elif test_type == "test_anniversary":
            await self.test_anniversary(interaction)
    
    async def test_bot_welcome(self, interaction: discord.Interaction):
        """Send test bot welcome message"""
        config = get_config(self.guild_id)
        
        if not config["welcome_channel_id"]:
            await interaction.response.send_message("❌ Welcome channel not configured!", ephemeral=True)
            return

        channel = interaction.guild.get_channel(config["welcome_channel_id"])
        if not channel:
            await interaction.response.send_message("❌ Welcome channel not found!", ephemeral=True)
            return

        from_cog = interaction.client.get_cog("JoinLeaveTracker")
        bot_messages = config["bot_welcome_messages"]
        
        # Bot welcome format: roast message + line breaks + welcome
        base_message = random.choice(bot_messages)
        welcome_msg = f"{base_message}\n\nAnyways, welcome {interaction.user.mention}!"

        try:
            await channel.send(welcome_msg)
            await interaction.response.send_message(f"✅ Test bot welcome message sent to {channel.mention}!", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed to send test message: {str(e)}", ephemeral=True)
    
    async def test_human_welcome(self, interaction: discord.Interaction):
        """Send test human welcome message"""
        config = get_config(self.guild_id)
        
        if not config["welcome_channel_id"]:
            await interaction.response.send_message("❌ Welcome channel not configured!", ephemeral=True)
            return

        channel = interaction.guild.get_channel(config["welcome_channel_id"])
        if not channel:
            await interaction.response.send_message("❌ Welcome channel not found!", ephemeral=True)
            return

        from_cog = interaction.client.get_cog("JoinLeaveTracker")
        ping_string = from_cog.build_welcome_ping_string(interaction.guild, config) if from_cog else ""
        
        human_messages = config["human_welcome_messages"]
        
        # FIXED FORMAT: Random welcome message + ping + "come, welcome them!" + line breaks + welcome + roles/rules
        base_message = random.choice(human_messages)
        
        # Build the ping part with "come, welcome them!"
        if ping_string:
            welcome_msg = f"{base_message} {ping_string} come, welcome them!\n\nWelcome to the server {interaction.user.mention}!"
        else:
            welcome_msg = f"{base_message}\n\nWelcome to the server {interaction.user.mention}!"
        
        additional_info = []
        if config["roles_channel_id"]:
            roles_channel = interaction.guild.get_channel(config["roles_channel_id"])
            if roles_channel:
                additional_info.append(f"Don't forget to get your roles from {roles_channel.mention}.")
        
        if config["rules_channel_id"]:
            rules_channel = interaction.guild.get_channel(config["rules_channel_id"])
            if rules_channel:
                additional_info.append(f"Also, you must check out {rules_channel.mention}.")
        
        if additional_info:
            welcome_msg += " " + " ".join(additional_info)

        try:
            await channel.send(welcome_msg, allowed_mentions=discord.AllowedMentions(roles=True, users=True))
            await interaction.response.send_message(f"✅ Test human welcome message sent to {channel.mention}!", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed to send test message: {str(e)}", ephemeral=True)
    
    async def test_anniversary(self, interaction: discord.Interaction):
        """Send test anniversary message"""
        config = get_config(self.guild_id)
        
        # Use anniversary channel if set, otherwise fall back to welcome channel
        channel_id = config["anniversary_channel_id"] or config["welcome_channel_id"]
        
        if not channel_id:
            await interaction.response.send_message("❌ No anniversary channel or welcome channel configured!", ephemeral=True)
            return

        channel = interaction.guild.get_channel(channel_id)
        if not channel:
            await interaction.response.send_message("❌ Channel not found!", ephemeral=True)
            return

        # Create a test anniversary embed
        years = 1  # Test with 1 year anniversary
        
        embed = discord.Embed(
            title=f"🎉 {years} Year Anniversary!",
            description=f"{interaction.user.mention} joined the server {years} year{'s' if years > 1 else ''} ago today!",
            color=discord.Color.gold()
        )
        embed.set_thumbnail(url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.default_avatar.url)
        embed.set_footer(text="Congratulations on this milestone! 🎊")

        try:
            await channel.send(embed=embed)
            channel_type = "anniversary" if config["anniversary_channel_id"] else "welcome (fallback)"
            await interaction.response.send_message(f"✅ Test anniversary message sent to {channel.mention} ({channel_type} channel)!", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed to send test message: {str(e)}", ephemeral=True)

# ============ CHANNEL SETUP VIEWS ============

class ChannelSetupView(discord.ui.View):
    """Channel setup interface with dropdowns"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        
        # Add dropdown for channel type selection
        self.add_item(ChannelTypeDropdown(guild_id, user_id))
        self.add_item(BackButton(guild_id, user_id, MainMenuView, "⬅️ Main Menu"))
        self.add_item(CloseButton())
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not for you!", ephemeral=True)
            return False
        return True
    
    def get_embed(self) -> discord.Embed:
        config = get_config(self.guild_id)
        embed = discord.Embed(
            title="📋 Channel Setup",
            description="Select a channel type from the dropdown to configure it.",
            color=discord.Color.blue()
        )
        
        channel_info = []
        if config["stats_channel_id"]:
            channel_info.append(f"**📊 Stats:** <#{config['stats_channel_id']}>")
        else:
            channel_info.append("**📊 Stats:** Not set")
            
        if config["welcome_channel_id"]:
            channel_info.append(f"**👋 Welcome:** <#{config['welcome_channel_id']}>")
        else:
            channel_info.append("**👋 Welcome:** Not set")
            
        if config["anniversary_channel_id"]:
            channel_info.append(f"**🎉 Anniversary:** <#{config['anniversary_channel_id']}>")
        else:
            channel_info.append("**🎉 Anniversary:** Not set")
            
        if config["roles_channel_id"]:
            channel_info.append(f"**🎭 Roles:** <#{config['roles_channel_id']}>")
        else:
            channel_info.append("**🎭 Roles:** Not set")
            
        if config["rules_channel_id"]:
            channel_info.append(f"**📜 Rules:** <#{config['rules_channel_id']}>")
        else:
            channel_info.append("**📜 Rules:** Not set")
            
        if config["error_log_channel_id"]:
            channel_info.append(f"**⚠️ Error Log:** <#{config['error_log_channel_id']}>")
        else:
            channel_info.append("**⚠️ Error Log:** Not set")
        
        embed.add_field(name="Current Configuration", value="\n".join(channel_info), inline=False)
        
        return embed

class ChannelTypeDropdown(discord.ui.Select):
    """Dropdown for selecting which channel type to configure"""
    def __init__(self, guild_id: int, user_id: int):
        self.guild_id = guild_id
        self.user_id = user_id
        
        options = [
            discord.SelectOption(label="Stats Channel", value="stats_channel", emoji="📊"),
            discord.SelectOption(label="Welcome Channel", value="welcome_channel", emoji="👋"),
            discord.SelectOption(label="Anniversary Channel", value="anniversary_channel", emoji="🎉"),
            discord.SelectOption(label="Roles Channel", value="roles_channel", emoji="🎭"),
            discord.SelectOption(label="Rules Channel", value="rules_channel", emoji="📜"),
            discord.SelectOption(label="Error Log Channel", value="error_log_channel", emoji="⚠️"),
        ]
        
        super().__init__(placeholder="Select channel type to configure...", options=options)
    
    async def callback(self, interaction: discord.Interaction):
        channel_type = self.values[0]
        
        try:
            # Get ALL channels from the current guild
            all_channels = [channel for channel in interaction.guild.channels 
                           if isinstance(channel, (discord.TextChannel, discord.VoiceChannel, discord.StageChannel, discord.ForumChannel))]
            
            # Sort channels by position
            all_channels.sort(key=lambda x: x.position)
            
            # Create paginated channel selection
            view = PaginatedChannelSelectionView(self.guild_id, self.user_id, channel_type, all_channels)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception as e:
            logger.error(f"Error in channel selection: {e}")
            await interaction.response.send_message("❌ Error loading channels. Please try again.", ephemeral=True)

class PaginatedChannelSelectionView(discord.ui.View):
    """View for paginated channel selection"""
    def __init__(self, guild_id: int, user_id: int, channel_type: str, all_channels: list, page: int = 0):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        self.channel_type = channel_type
        self.all_channels = all_channels
        self.page = page
        self.channels_per_page = 24  # 24 + "None" = 25 total options
        
        # Calculate total pages
        self.total_pages = max(1, (len(all_channels) + self.channels_per_page - 1) // self.channels_per_page)
        
        # Get current page channels
        current_page_channels = self.get_current_page_channels()
        
        # Create options for dropdown
        options = self.create_channel_options(current_page_channels)
        
        # Add channel selection dropdown
        self.add_item(ChannelSelectDropdown(guild_id, user_id, channel_type, options))
        
        # Add pagination buttons if needed
        if self.total_pages > 1:
            self.add_item(PreviousPageButton())
            self.add_item(NextPageButton())
            self.add_item(PageIndicatorButton(self.page, self.total_pages))
        
        self.add_item(BackButton(guild_id, user_id, ChannelSetupView, "⬅️ Back"))
        self.add_item(CloseButton())
    
    def get_current_page_channels(self):
        """Get channels for current page"""
        start_idx = self.page * self.channels_per_page
        end_idx = start_idx + self.channels_per_page
        return self.all_channels[start_idx:end_idx]
    
    def create_channel_options(self, page_channels):
        """Create dropdown options from channels"""
        options = [discord.SelectOption(label="None", value="0", description="Clear this channel setting")]
        
        for channel in page_channels:
            channel_type_emoji = {
                discord.TextChannel: "💬",
                discord.VoiceChannel: "🔊", 
                discord.StageChannel: "🎤",
                discord.ForumChannel: "📝",
                discord.CategoryChannel: "📁"
            }.get(type(channel), "📁")
            
            # Truncate long channel names
            channel_name = channel.name
            if len(channel_name) > 25:
                channel_name = channel_name[:22] + "..."
            
            channel_description = f"#{channel.name}" if isinstance(channel, discord.TextChannel) else channel.name
            if len(channel_description) > 50:
                channel_description = channel_description[:47] + "..."
            
            options.append(discord.SelectOption(
                label=f"{channel_type_emoji} {channel_name}",
                value=str(channel.id),
                description=channel_description
            ))
        
        return options
    
    def get_embed(self) -> discord.Embed:
        type_names = {
            "stats_channel": "Stats Channel",
            "welcome_channel": "Welcome Channel", 
            "anniversary_channel": "Anniversary Channel",
            "roles_channel": "Roles Channel",
            "rules_channel": "Rules Channel",
            "error_log_channel": "Error Log Channel"
        }
        
        embed = discord.Embed(
            title=f"📋 Configure {type_names.get(self.channel_type, self.channel_type)}",
            description=f"Select a channel from the dropdown below. Page {self.page + 1}/{self.total_pages}",
            color=discord.Color.blue()
        )
        
        # Show current selection info
        current_config = get_config(self.guild_id)
        current_channel_id = current_config.get(f"{self.channel_type}_id")
        if current_channel_id:
            current_channel = discord.utils.get(self.all_channels, id=current_channel_id)
            if current_channel:
                embed.add_field(
                    name="Current Selection",
                    value=f"Currently set to: {current_channel.mention}",
                    inline=False
                )
        
        embed.set_footer(text=f"Total channels: {len(self.all_channels)} • Use pagination buttons to browse all channels")
        
        return embed

class ChannelSelectDropdown(discord.ui.Select):
    """Dropdown for selecting a specific channel"""
    def __init__(self, guild_id: int, user_id: int, channel_type: str, options: list):
        self.guild_id = guild_id
        self.user_id = user_id
        self.channel_type = channel_type
        
        # Ensure we have at least one option
        if not options:
            options = [discord.SelectOption(label="No channels available", value="0", description="No channels found")]
        
        super().__init__(
            placeholder="Select a channel...",
            options=options,
            min_values=1,
            max_values=1
        )
    
    async def callback(self, interaction: discord.Interaction):
        config = get_config(self.guild_id)
        selected_value = self.values[0]
        
        if selected_value == "0":
            # Clear the channel
            config[f"{self.channel_type}_id"] = None
            message = f"✅ {self.channel_type.replace('_', ' ').title()} cleared!"
        else:
            # Set the channel
            channel_id = int(selected_value)
            config[f"{self.channel_type}_id"] = channel_id
            channel = interaction.guild.get_channel(channel_id)
            if channel:
                message = f"✅ {self.channel_type.replace('_', ' ').title()} set to {channel.mention}!"
            else:
                message = f"✅ {self.channel_type.replace('_', ' ').title()} set to channel ID: {channel_id}!"
        
        save_config(self.guild_id, config)
        
        # Return to channel setup view
        view = ChannelSetupView(self.guild_id, self.user_id)
        embed = view.get_embed()
        await interaction.response.edit_message(embed=embed, view=view)
        await interaction.followup.send(message, ephemeral=True)

class PreviousPageButton(discord.ui.Button):
    """Button to go to previous page"""
    def __init__(self):
        super().__init__(label="⬅️ Previous", style=discord.ButtonStyle.secondary, row=2)
    
    async def callback(self, interaction: discord.Interaction):
        view: PaginatedChannelSelectionView = self.view
        if view.page > 0:
            view.page -= 1
            new_view = PaginatedChannelSelectionView(view.guild_id, view.user_id, view.channel_type, view.all_channels, view.page)
            embed = new_view.get_embed()
            await interaction.response.edit_message(embed=embed, view=new_view)
        else:
            await interaction.response.defer()

class NextPageButton(discord.ui.Button):
    """Button to go to next page"""
    def __init__(self):
        super().__init__(label="Next ➡️", style=discord.ButtonStyle.secondary, row=2)
    
    async def callback(self, interaction: discord.Interaction):
        view: PaginatedChannelSelectionView = self.view
        if view.page < view.total_pages - 1:
            view.page += 1
            new_view = PaginatedChannelSelectionView(view.guild_id, view.user_id, view.channel_type, view.all_channels, view.page)
            embed = new_view.get_embed()
            await interaction.response.edit_message(embed=embed, view=new_view)
        else:
            await interaction.response.defer()

class PageIndicatorButton(discord.ui.Button):
    """Button showing current page"""
    def __init__(self, current_page: int, total_pages: int):
        super().__init__(
            label=f"Page {current_page + 1}/{total_pages}", 
            style=discord.ButtonStyle.grey, 
            row=2,
            disabled=True
        )

# ============ ROLE SETUP VIEWS ============

class RoleSetupView(discord.ui.View):
    """Role setup interface with dropdowns"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        
        # Add dropdown for role type selection
        self.add_item(RoleTypeDropdown(guild_id, user_id))
        self.add_item(BackButton(guild_id, user_id, MainMenuView, "⬅️ Main Menu"))
        self.add_item(CloseButton())
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not for you!", ephemeral=True)
            return False
        return True
    
    def get_embed(self) -> discord.Embed:
        config = get_config(self.guild_id)
        embed = discord.Embed(
            title="🎭 Role Setup",
            description="Configure automatic role assignment for new members.",
            color=discord.Color.blue()
        )
        
        role_info = []
        if config["bot_role_id"]:
            role_info.append(f"**🤖 Bot Role:** <@&{config['bot_role_id']}>")
        else:
            role_info.append("**🤖 Bot Role:** Not set")
            
        if config["member_role_id"]:
            role_info.append(f"**👤 Member Role:** <@&{config['member_role_id']}>")
        else:
            role_info.append("**👤 Member Role:** Not set")
        
        embed.add_field(name="Current Configuration", value="\n".join(role_info), inline=False)
        embed.add_field(
            name="How to Use", 
            value="Select a role type from the dropdown to configure it.", 
            inline=False
        )
        embed.set_footer(text="Roles are automatically assigned when members join")
        
        return embed

class RoleTypeDropdown(discord.ui.Select):
    """Dropdown for selecting which role type to configure"""
    def __init__(self, guild_id: int, user_id: int):
        self.guild_id = guild_id
        self.user_id = user_id
        
        options = [
            discord.SelectOption(label="Bot Role", value="bot_role", emoji="🤖"),
            discord.SelectOption(label="Member Role", value="member_role", emoji="👤"),
        ]
        
        super().__init__(placeholder="Select role type to configure...", options=options)
    
    async def callback(self, interaction: discord.Interaction):
        role_type = self.values[0]
        
        try:
            # Get ALL roles from the current guild (excluding @everyone)
            all_roles = [role for role in interaction.guild.roles if role.name != "@everyone"]
            
            # Sort roles by position (highest first)
            all_roles.sort(key=lambda x: x.position, reverse=True)
            
            # Create paginated role selection
            view = PaginatedRoleSelectionView(self.guild_id, self.user_id, role_type, all_roles)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception as e:
            logger.error(f"Error in role selection: {e}")
            await interaction.response.send_message("❌ Error loading roles. Please try again.", ephemeral=True)

class PaginatedRoleSelectionView(discord.ui.View):
    """View for paginated role selection"""
    def __init__(self, guild_id: int, user_id: int, role_type: str, all_roles: list, page: int = 0):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        self.role_type = role_type
        self.all_roles = all_roles
        self.page = page
        self.roles_per_page = 24  # 24 + "None" = 25 total options
        
        # Calculate total pages
        self.total_pages = max(1, (len(all_roles) + self.roles_per_page - 1) // self.roles_per_page)
        
        # Get current page roles
        current_page_roles = self.get_current_page_roles()
        
        # Create options for dropdown
        options = self.create_role_options(current_page_roles)
        
        # Add role selection dropdown
        self.add_item(RoleSelectDropdown(guild_id, user_id, role_type, options))
        
        # Add pagination buttons if needed
        if self.total_pages > 1:
            self.add_item(PreviousRolePageButton())
            self.add_item(NextRolePageButton())
            self.add_item(RolePageIndicatorButton(self.page, self.total_pages))
        
        self.add_item(BackButton(guild_id, user_id, RoleSetupView, "⬅️ Back"))
        self.add_item(CloseButton())
    
    def get_current_page_roles(self):
        """Get roles for current page"""
        start_idx = self.page * self.roles_per_page
        end_idx = start_idx + self.roles_per_page
        return self.all_roles[start_idx:end_idx]
    
    def create_role_options(self, page_roles):
        """Create dropdown options from roles"""
        options = [discord.SelectOption(label="None", value="0", description="Clear this role setting")]
        
        for role in page_roles:
            # Truncate long role names
            role_name = role.name
            if len(role_name) > 25:
                role_name = role_name[:22] + "..."
            
            description = f"Position: {role.position} • Members: {len(role.members)}"
            if len(description) > 50:
                description = description[:47] + "..."
            
            options.append(discord.SelectOption(
                label=f"@{role_name}",
                value=str(role.id),
                description=description
            ))
        
        return options
    
    def get_embed(self) -> discord.Embed:
        type_names = {
            "bot_role": "Bot Role",
            "member_role": "Member Role",
        }
        
        embed = discord.Embed(
            title=f"🎭 Configure {type_names.get(self.role_type, self.role_type)}",
            description=f"Select a role from the dropdown below. Page {self.page + 1}/{self.total_pages}",
            color=discord.Color.blue()
        )
        
        # Show current selection info
        current_config = get_config(self.guild_id)
        current_role_id = current_config.get(f"{self.role_type}_id")
        if current_role_id:
            current_role = discord.utils.get(self.all_roles, id=current_role_id)
            if current_role:
                embed.add_field(
                    name="Current Selection",
                    value=f"Currently set to: {current_role.mention}",
                    inline=False
                )
        
        embed.set_footer(text=f"Total roles: {len(self.all_roles)} • Use pagination buttons to browse all roles")
        
        return embed

class RoleSelectDropdown(discord.ui.Select):
    """Dropdown for selecting a specific role"""
    def __init__(self, guild_id: int, user_id: int, role_type: str, options: list):
        self.guild_id = guild_id
        self.user_id = user_id
        self.role_type = role_type
        
        # Ensure we have at least one option
        if not options:
            options = [discord.SelectOption(label="No roles available", value="0", description="No roles found")]
        
        super().__init__(
            placeholder="Select a role...",
            options=options,
            min_values=1,
            max_values=1
        )
    
    async def callback(self, interaction: discord.Interaction):
        config = get_config(self.guild_id)
        selected_value = self.values[0]
        
        if selected_value == "0":
            # Clear the role
            config[f"{self.role_type}_id"] = None
            message = f"✅ {self.role_type.replace('_', ' ').title()} cleared!"
        else:
            # Set the role
            role_id = int(selected_value)
            config[f"{self.role_type}_id"] = role_id
            role = interaction.guild.get_role(role_id)
            if role:
                message = f"✅ {self.role_type.replace('_', ' ').title()} set to {role.mention}!"
            else:
                message = f"✅ {self.role_type.replace('_', ' ').title()} set to role ID: {role_id}!"
        
        save_config(self.guild_id, config)
        
        # Return to role setup view
        view = RoleSetupView(self.guild_id, self.user_id)
        embed = view.get_embed()
        await interaction.response.edit_message(embed=embed, view=view)
        await interaction.followup.send(message, ephemeral=True)

class PreviousRolePageButton(discord.ui.Button):
    """Button to go to previous role page"""
    def __init__(self):
        super().__init__(label="⬅️ Previous", style=discord.ButtonStyle.secondary, row=2)
    
    async def callback(self, interaction: discord.Interaction):
        view: PaginatedRoleSelectionView = self.view
        if view.page > 0:
            view.page -= 1
            new_view = PaginatedRoleSelectionView(view.guild_id, view.user_id, view.role_type, view.all_roles, view.page)
            embed = new_view.get_embed()
            await interaction.response.edit_message(embed=embed, view=new_view)
        else:
            await interaction.response.defer()

class NextRolePageButton(discord.ui.Button):
    """Button to go to next role page"""
    def __init__(self):
        super().__init__(label="Next ➡️", style=discord.ButtonStyle.secondary, row=2)
    
    async def callback(self, interaction: discord.Interaction):
        view: PaginatedRoleSelectionView = self.view
        if view.page < view.total_pages - 1:
            view.page += 1
            new_view = PaginatedRoleSelectionView(view.guild_id, view.user_id, view.role_type, view.all_roles, view.page)
            embed = new_view.get_embed()
            await interaction.response.edit_message(embed=embed, view=new_view)
        else:
            await interaction.response.defer()

class RolePageIndicatorButton(discord.ui.Button):
    """Button showing current role page"""
    def __init__(self, current_page: int, total_pages: int):
        super().__init__(
            label=f"Page {current_page + 1}/{total_pages}", 
            style=discord.ButtonStyle.grey, 
            row=2,
            disabled=True
        )

# ============ FIXED PING MANAGEMENT VIEWS ============

class PingManagementView(discord.ui.View):
    """Ping management interface with dropdowns"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        
        # Add action buttons
        self.add_item(AddRolePingButton(guild_id, user_id))
        self.add_item(AddUserPingButton(guild_id, user_id))
        self.add_item(ClearAllPingsButton(guild_id, user_id))
        self.add_item(BackButton(guild_id, user_id, MainMenuView, "⬅️ Main Menu"))
        self.add_item(CloseButton())
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not for you!", ephemeral=True)
            return False
        return True
    
    def get_embed(self) -> discord.Embed:
        config = get_config(self.guild_id)
        embed = discord.Embed(
            title="🔔 Welcome Ping Management",
            description="Manage who gets pinged when new members join.",
            color=discord.Color.blue()
        )
        
        # Show current pings - FIXED: Use mentions instead of trying to fetch objects
        roles_list = []
        for role_id in config["welcome_roles"]:
            roles_list.append(f"<@&{role_id}>")
        
        users_list = []
        for user_id in config["welcome_users"]:
            users_list.append(f"<@{user_id}>")
        
        if roles_list:
            embed.add_field(name="📋 Ping Roles", value="\n".join(roles_list), inline=False)
        else:
            embed.add_field(name="📋 Ping Roles", value="None", inline=False)
            
        if users_list:
            embed.add_field(name="👤 Ping Users", value="\n".join(users_list), inline=False)
        else:
            embed.add_field(name="👤 Ping Users", value="None", inline=False)
        
        embed.add_field(
            name="⚙️ Status",
            value=f"Pings are currently **{'enabled' if config['ping_enabled'] else 'disabled'}**",
            inline=False
        )
        
        embed.add_field(
            name="🔧 Actions",
            value="Use the buttons below to manage ping roles and users:",
            inline=False
        )
        
        embed.set_footer(text="Only you can see this • Dismiss message")
        
        return embed

class AddRolePingButton(discord.ui.Button):
    """Button to add role pings"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(
            label="Add Role Ping",
            style=discord.ButtonStyle.primary,
            emoji="➕",
            row=0
        )
        self.guild_id = guild_id
        self.user_id = user_id
    
    async def callback(self, interaction: discord.Interaction):
        try:
            # Get ALL roles from the current guild (excluding @everyone)
            all_roles = [role for role in interaction.guild.roles if role.name != "@everyone"]
            
            # Sort roles by position (highest first)
            all_roles.sort(key=lambda x: x.position, reverse=True)
            
            # Create paginated role selection
            view = PaginatedRolePingSelectionView(self.guild_id, self.user_id, all_roles)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception as e:
            logger.error(f"Error in role ping selection: {e}")
            await interaction.response.send_message("❌ Error loading roles. Please try again.", ephemeral=True)

class AddUserPingButton(discord.ui.Button):
    """Button to add user pings"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(
            label="Add User Ping",
            style=discord.ButtonStyle.primary,
            emoji="➕",
            row=0
        )
        self.guild_id = guild_id
        self.user_id = user_id
    
    async def callback(self, interaction: discord.Interaction):
        try:
            # Get ALL members from the current guild
            all_members = [member for member in interaction.guild.members if not member.bot]
            
            # Sort members by display name
            all_members.sort(key=lambda x: x.display_name.lower())
            
            # Create paginated member selection
            view = PaginatedUserPingSelectionView(self.guild_id, self.user_id, all_members)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception as e:
            logger.error(f"Error in user ping selection: {e}")
            await interaction.response.send_message("❌ Error loading members. Please try again.", ephemeral=True)

class ClearAllPingsButton(discord.ui.Button):
    """Button to clear all pings"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(
            label="Clear All Pings",
            style=discord.ButtonStyle.danger,
            emoji="🧹",
            row=0
        )
        self.guild_id = guild_id
        self.user_id = user_id
    
    async def callback(self, interaction: discord.Interaction):
        config = get_config(self.guild_id)
        config["welcome_roles"] = []
        config["welcome_users"] = []
        save_config(self.guild_id, config)
        
        # Return to updated ping management view
        view = PingManagementView(self.guild_id, self.user_id)
        embed = view.get_embed()
        await interaction.response.edit_message(embed=embed, view=view)
        await interaction.followup.send("✅ Cleared all welcome pings!", ephemeral=True)

class PaginatedRolePingSelectionView(discord.ui.View):
    """View for paginated role ping selection"""
    def __init__(self, guild_id: int, user_id: int, all_roles: list, page: int = 0):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        self.all_roles = all_roles
        self.page = page
        self.roles_per_page = 24  # 24 + "None" = 25 total options
        
        # Calculate total pages
        self.total_pages = max(1, (len(all_roles) + self.roles_per_page - 1) // self.roles_per_page)
        
        # Get current page roles
        current_page_roles = self.get_current_page_roles()
        
        # Create options for dropdown
        options = self.create_role_options(current_page_roles)
        
        # Add role selection dropdown
        self.add_item(RolePingSelectDropdown(guild_id, user_id, options))
        
        # Add pagination buttons if needed
        if self.total_pages > 1:
            self.add_item(PreviousRolePingPageButton())
            self.add_item(NextRolePingPageButton())
            self.add_item(RolePingPageIndicatorButton(self.page, self.total_pages))
        
        self.add_item(BackButton(guild_id, user_id, PingManagementView, "⬅️ Back"))
        self.add_item(CloseButton())
    
    def get_current_page_roles(self):
        """Get roles for current page"""
        start_idx = self.page * self.roles_per_page
        end_idx = start_idx + self.roles_per_page
        return self.all_roles[start_idx:end_idx]
    
    def create_role_options(self, page_roles):
        """Create dropdown options from roles"""
        options = [discord.SelectOption(label="Cancel", value="0", description="Go back without adding")]
        
        for role in page_roles:
            # Truncate long role names
            role_name = role.name
            if len(role_name) > 25:
                role_name = role_name[:22] + "..."
            
            description = f"Position: {role.position} • Members: {len(role.members)}"
            if len(description) > 50:
                description = description[:47] + "..."
            
            options.append(discord.SelectOption(
                label=f"@{role_name}",
                value=str(role.id),
                description=description
            ))
        
        return options
    
    def get_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="🔔 Add Role Ping",
            description=f"Select a role to ping when new members join. Page {self.page + 1}/{self.total_pages}",
            color=discord.Color.blue()
        )
        
        config = get_config(self.guild_id)
        if config["welcome_roles"]:
            current_roles = []
            for role_id in config["welcome_roles"]:
                current_roles.append(f"<@&{role_id}>")
            
            if current_roles:
                embed.add_field(
                    name="Current Ping Roles",
                    value="\n".join(current_roles),
                    inline=False
                )
        
        embed.set_footer(text=f"Total roles: {len(self.all_roles)} • Use pagination buttons to browse all roles")
        
        return embed

class RolePingSelectDropdown(discord.ui.Select):
    """Dropdown for selecting a role to ping"""
    def __init__(self, guild_id: int, user_id: int, options: list):
        self.guild_id = guild_id
        self.user_id = user_id
        
        # Ensure we have at least one option
        if not options:
            options = [discord.SelectOption(label="No roles available", value="0", description="No roles found")]
        
        super().__init__(
            placeholder="Select a role to ping...",
            options=options,
            min_values=1,
            max_values=1
        )
    
    async def callback(self, interaction: discord.Interaction):
        selected_value = self.values[0]
        
        if selected_value == "0":
            # Cancel - return to ping management
            view = PingManagementView(self.guild_id, self.user_id)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
            return
        
        config = get_config(self.guild_id)
        role_id = int(selected_value)
        
        # Check if role is already in the list
        if role_id in config["welcome_roles"]:
            await interaction.response.send_message("❌ This role is already in the ping list!", ephemeral=True)
            return
        
        # Add the role
        config["welcome_roles"].append(role_id)
        save_config(self.guild_id, config)
        
        role = interaction.guild.get_role(role_id)
        if role:
            message = f"✅ Added {role.mention} to welcome pings!"
        else:
            message = f"✅ Added role to welcome pings!"
        
        # FIXED: Return to UPDATED ping management view
        view = PingManagementView(self.guild_id, self.user_id)
        embed = view.get_embed()
        await interaction.response.edit_message(embed=embed, view=view)
        await interaction.followup.send(message, ephemeral=True)

class PreviousRolePingPageButton(discord.ui.Button):
    """Button to go to previous role ping page"""
    def __init__(self):
        super().__init__(label="⬅️ Previous", style=discord.ButtonStyle.secondary, row=2)
    
    async def callback(self, interaction: discord.Interaction):
        view: PaginatedRolePingSelectionView = self.view
        if view.page > 0:
            view.page -= 1
            new_view = PaginatedRolePingSelectionView(view.guild_id, view.user_id, view.all_roles, view.page)
            embed = new_view.get_embed()
            await interaction.response.edit_message(embed=embed, view=new_view)
        else:
            await interaction.response.defer()

class NextRolePingPageButton(discord.ui.Button):
    """Button to go to next role ping page"""
    def __init__(self):
        super().__init__(label="Next ➡️", style=discord.ButtonStyle.secondary, row=2)
    
    async def callback(self, interaction: discord.Interaction):
        view: PaginatedRolePingSelectionView = self.view
        if view.page < view.total_pages - 1:
            view.page += 1
            new_view = PaginatedRolePingSelectionView(view.guild_id, view.user_id, view.all_roles, view.page)
            embed = new_view.get_embed()
            await interaction.response.edit_message(embed=embed, view=new_view)
        else:
            await interaction.response.defer()

class RolePingPageIndicatorButton(discord.ui.Button):
    """Button showing current role ping page"""
    def __init__(self, current_page: int, total_pages: int):
        super().__init__(
            label=f"Page {current_page + 1}/{total_pages}", 
            style=discord.ButtonStyle.grey, 
            row=2,
            disabled=True
        )

class PaginatedUserPingSelectionView(discord.ui.View):
    """View for paginated user ping selection"""
    def __init__(self, guild_id: int, user_id: int, all_members: list, page: int = 0):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        self.all_members = all_members
        self.page = page
        self.members_per_page = 24  # 24 + "None" = 25 total options
        
        # Calculate total pages
        self.total_pages = max(1, (len(all_members) + self.members_per_page - 1) // self.members_per_page)
        
        # Get current page members
        current_page_members = self.get_current_page_members()
        
        # Create options for dropdown
        options = self.create_member_options(current_page_members)
        
        # Add member selection dropdown
        self.add_item(UserPingSelectDropdown(guild_id, user_id, options))
        
        # Add pagination buttons if needed
        if self.total_pages > 1:
            self.add_item(PreviousUserPingPageButton())
            self.add_item(NextUserPingPageButton())
            self.add_item(UserPingPageIndicatorButton(self.page, self.total_pages))
        
        self.add_item(BackButton(guild_id, user_id, PingManagementView, "⬅️ Back"))
        self.add_item(CloseButton())
    
    def get_current_page_members(self):
        """Get members for current page"""
        start_idx = self.page * self.members_per_page
        end_idx = start_idx + self.members_per_page
        return self.all_members[start_idx:end_idx]
    
    def create_member_options(self, page_members):
        """Create dropdown options from members"""
        options = [discord.SelectOption(label="Cancel", value="0", description="Go back without adding")]
        
        for member in page_members:
            # Truncate long member names
            member_name = member.display_name
            if len(member_name) > 25:
                member_name = member_name[:22] + "..."
            
            description = f"#{member.discriminator} • Joined: {member.joined_at.strftime('%Y-%m-%d') if member.joined_at else 'Unknown'}"
            if len(description) > 50:
                description = description[:47] + "..."
            
            options.append(discord.SelectOption(
                label=f"👤 {member_name}",
                value=str(member.id),
                description=description
            ))
        
        return options
    
    def get_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="🔔 Add User Ping",
            description=f"Select a user to ping when new members join. Page {self.page + 1}/{self.total_pages}",
            color=discord.Color.blue()
        )
        
        config = get_config(self.guild_id)
        if config["welcome_users"]:
            current_users = []
            for user_id in config["welcome_users"]:
                current_users.append(f"<@{user_id}>")
            
            if current_users:
                embed.add_field(
                    name="Current Ping Users",
                    value="\n".join(current_users),
                    inline=False
                )
        
        embed.set_footer(text=f"Total members: {len(self.all_members)} • Use pagination buttons to browse all members")
        
        return embed

class UserPingSelectDropdown(discord.ui.Select):
    """Dropdown for selecting a user to ping"""
    def __init__(self, guild_id: int, user_id: int, options: list):
        self.guild_id = guild_id
        self.user_id = user_id
        
        # Ensure we have at least one option
        if not options:
            options = [discord.SelectOption(label="No members available", value="0", description="No members found")]
        
        super().__init__(
            placeholder="Select a user to ping...",
            options=options,
            min_values=1,
            max_values=1
        )
    
    async def callback(self, interaction: discord.Interaction):
        selected_value = self.values[0]
        
        if selected_value == "0":
            # Cancel - return to ping management
            view = PingManagementView(self.guild_id, self.user_id)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
            return
        
        config = get_config(self.guild_id)
        user_id = int(selected_value)
        
        # Check if user is already in the list
        if user_id in config["welcome_users"]:
            await interaction.response.send_message("❌ This user is already in the ping list!", ephemeral=True)
            return
        
        # Add the user
        config["welcome_users"].append(user_id)
        save_config(self.guild_id, config)
        
        user = interaction.guild.get_member(user_id)
        if user:
            message = f"✅ Added {user.mention} to welcome pings!"
        else:
            message = f"✅ Added user to welcome pings!"
        
        # FIXED: Return to UPDATED ping management view
        view = PingManagementView(self.guild_id, self.user_id)
        embed = view.get_embed()
        await interaction.response.edit_message(embed=embed, view=view)
        await interaction.followup.send(message, ephemeral=True)

class PreviousUserPingPageButton(discord.ui.Button):
    """Button to go to previous user ping page"""
    def __init__(self):
        super().__init__(label="⬅️ Previous", style=discord.ButtonStyle.secondary, row=2)
    
    async def callback(self, interaction: discord.Interaction):
        view: PaginatedUserPingSelectionView = self.view
        if view.page > 0:
            view.page -= 1
            new_view = PaginatedUserPingSelectionView(view.guild_id, view.user_id, view.all_members, view.page)
            embed = new_view.get_embed()
            await interaction.response.edit_message(embed=embed, view=new_view)
        else:
            await interaction.response.defer()

class NextUserPingPageButton(discord.ui.Button):
    """Button to go to next user ping page"""
    def __init__(self):
        super().__init__(label="Next ➡️", style=discord.ButtonStyle.secondary, row=2)
    
    async def callback(self, interaction: discord.Interaction):
        view: PaginatedUserPingSelectionView = self.view
        if view.page < view.total_pages - 1:
            view.page += 1
            new_view = PaginatedUserPingSelectionView(view.guild_id, view.user_id, view.all_members, view.page)
            embed = new_view.get_embed()
            await interaction.response.edit_message(embed=embed, view=new_view)
        else:
            await interaction.response.defer()

class UserPingPageIndicatorButton(discord.ui.Button):
    """Button showing current user ping page"""
    def __init__(self, current_page: int, total_pages: int):
        super().__init__(
            label=f"Page {current_page + 1}/{total_pages}", 
            style=discord.ButtonStyle.grey, 
            row=2,
            disabled=True
        )

# ============ MESSAGE MANAGEMENT VIEWS ============

class MessageTypeSelectView(discord.ui.View):
    """View for selecting message type to manage"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        self.add_item(MessageTypeDropdown(guild_id, user_id))
        self.add_item(BackButton(guild_id, user_id, MainMenuView, "⬅️ Main Menu"))
        self.add_item(CloseButton())
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not for you!", ephemeral=True)
            return False
        return True
    
    def get_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="💬 Message Management",
            description="Select a message type to manage from the dropdown below.",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="Available Message Types",
            value=(
                "🟢 **Human Welcome** - Messages for new human members\n"
                "🔴 **Human Goodbye** - Messages when humans leave\n"
                "🔵 **Bot Welcome** - Messages for new bots\n"
                "🟠 **Bot Goodbye** - Messages when bots leave\n"
                "🎉 **Anniversary Settings** - Configure anniversary channel and settings"
            ),
            inline=False
        )
        
        return embed

class MessageTypeDropdown(discord.ui.Select):
    """Dropdown for selecting message type"""
    def __init__(self, guild_id: int, user_id: int):
        self.guild_id = guild_id
        self.user_id = user_id
        
        options = [
            discord.SelectOption(label="Human Welcome Messages", value="human_welcome", emoji="🟢"),
            discord.SelectOption(label="Human Goodbye Messages", value="human_goodbye", emoji="🔴"),
            discord.SelectOption(label="Bot Welcome Messages", value="bot_welcome", emoji="🔵"),
            discord.SelectOption(label="Bot Goodbye Messages", value="bot_goodbye", emoji="🟠"),
            discord.SelectOption(label="Anniversary Settings", value="anniversary_settings", emoji="🎉"),
        ]
        
        super().__init__(placeholder="Select message type...", options=options)
    
    async def callback(self, interaction: discord.Interaction):
        message_type = self.values[0]
        if message_type == "anniversary_settings":
            view = AnniversarySettingsView(self.guild_id, self.user_id)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        else:
            view = MessageManagementView(self.guild_id, self.user_id, message_type)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)

# NEW: Anniversary Settings View
class AnniversarySettingsView(discord.ui.View):
    """View for managing anniversary settings"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        
        # Add action buttons
        self.add_item(SetAnniversaryChannelButton(guild_id, user_id))
        self.add_item(TestAnniversaryButton(guild_id, user_id))
        self.add_item(BackButton(guild_id, user_id, MessageTypeSelectView, "⬅️ Back"))
        self.add_item(CloseButton())
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not for you!", ephemeral=True)
            return False
        return True
    
    def get_embed(self) -> discord.Embed:
        config = get_config(self.guild_id)
        embed = discord.Embed(
            title="🎉 Anniversary Settings",
            description="Configure anniversary celebration settings.",
            color=discord.Color.gold()
        )
        
        # Show current anniversary channel
        if config["anniversary_channel_id"]:
            channel_mention = f"<#{config['anniversary_channel_id']}>"
            embed.add_field(
                name="Current Anniversary Channel",
                value=channel_mention,
                inline=False
            )
        else:
            embed.add_field(
                name="Current Anniversary Channel",
                value="❌ Not set (will use welcome channel as fallback)",
                inline=False
            )
        
        # Show anniversary system status
        embed.add_field(
            name="System Status",
            value="✅ Anniversary tracking is active\n✅ Daily checks enabled\n✅ Automatic celebrations",
            inline=False
        )
        
        embed.add_field(
            name="Actions",
            value=(
                "**Set Anniversary Channel** - Choose where anniversary messages are sent\n"
                "**Test Anniversary** - Send a test anniversary message"
            ),
            inline=False
        )
        
        embed.set_footer(text="Anniversaries are automatically detected and celebrated daily")
        
        return embed

class SetAnniversaryChannelButton(discord.ui.Button):
    """Button to set anniversary channel"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(
            label="Set Anniversary Channel",
            style=discord.ButtonStyle.primary,
            emoji="🎉",
            row=0
        )
        self.guild_id = guild_id
        self.user_id = user_id
    
    async def callback(self, interaction: discord.Interaction):
        try:
            # Get ALL channels from the current guild
            all_channels = [channel for channel in interaction.guild.channels 
                           if isinstance(channel, (discord.TextChannel, discord.VoiceChannel, discord.StageChannel, discord.ForumChannel))]
            
            # Sort channels by position
            all_channels.sort(key=lambda x: x.position)
            
            # Create paginated channel selection
            view = PaginatedChannelSelectionView(self.guild_id, self.user_id, "anniversary_channel", all_channels)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception as e:
            logger.error(f"Error in anniversary channel selection: {e}")
            await interaction.response.send_message("❌ Error loading channels. Please try again.", ephemeral=True)

class TestAnniversaryButton(discord.ui.Button):
    """Button to test anniversary message"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(
            label="Test Anniversary",
            style=discord.ButtonStyle.secondary,
            emoji="🧪",
            row=0
        )
        self.guild_id = guild_id
        self.user_id = user_id
    
    async def callback(self, interaction: discord.Interaction):
        config = get_config(self.guild_id)
        
        # Determine which channel to use (anniversary channel first, then welcome channel as fallback)
        channel_id = config["anniversary_channel_id"] or config["welcome_channel_id"]
        
        if not channel_id:
            await interaction.response.send_message("❌ No anniversary channel or welcome channel configured!", ephemeral=True)
            return

        channel = interaction.guild.get_channel(channel_id)
        if not channel:
            await interaction.response.send_message("❌ Channel not found!", ephemeral=True)
            return

        # Create a test anniversary embed
        years = 1  # Test with 1 year anniversary
        
        embed = discord.Embed(
            title=f"🎉 {years} Year Anniversary!",
            description=f"{interaction.user.mention} joined the server {years} year{'s' if years > 1 else ''} ago today!",
            color=discord.Color.gold()
        )
        embed.set_thumbnail(url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.default_avatar.url)
        embed.set_footer(text="Congratulations on this milestone! 🎊")

        try:
            await channel.send(embed=embed)
            channel_type = "anniversary" if config["anniversary_channel_id"] else "welcome (fallback)"
            await interaction.response.send_message(f"✅ Test anniversary message sent to {channel.mention} ({channel_type} channel)!", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed to send test message: {str(e)}", ephemeral=True)

class MessageManagementView(discord.ui.View):
    """View for managing specific message type"""
    def __init__(self, guild_id: int, user_id: int, message_type: str):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        self.message_type = message_type
        self.add_item(MessageActionDropdown(guild_id, user_id, message_type))
        self.add_item(BackButton(guild_id, user_id, MessageTypeSelectView, "⬅️ Back"))
        self.add_item(CloseButton())
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not for you!", ephemeral=True)
            return False
        return True
    
    def get_embed(self) -> discord.Embed:
        config = get_config(self.guild_id)
        messages = config[f"{self.message_type}_messages"]
        
        embed = discord.Embed(
            title=f"💬 {self.message_type.replace('_', ' ').title()} Messages",
            description="Manage messages for this type.",
            color=discord.Color.blue()
        )
        
        if messages:
            message_list = "\n\n".join([f"**{i+1}.** {msg}" for i, msg in enumerate(messages)])
            # Truncate if too long
            if len(message_list) > 1024:
                message_list = message_list[:1020] + "..."
            embed.add_field(name=f"Current Messages ({len(messages)})", value=message_list, inline=False)
        else:
            embed.add_field(name="Current Messages", value="No messages set", inline=False)
        
        return embed

class MessageActionDropdown(discord.ui.Select):
    """Dropdown for message actions"""
    def __init__(self, guild_id: int, user_id: int, message_type: str):
        self.guild_id = guild_id
        self.user_id = user_id
        self.message_type = message_type
        
        options = [
            discord.SelectOption(label="Add Message", value="add", emoji="➕"),
            discord.SelectOption(label="Remove Message", value="remove", emoji="➖"),
            discord.SelectOption(label="Preview Random", value="preview", emoji="🎲"),
            discord.SelectOption(label="List All", value="list", emoji="📋"),
        ]
        
        super().__init__(placeholder="Select an action...", options=options)
    
    async def callback(self, interaction: discord.Interaction):
        action = self.values[0]
        config = get_config(self.guild_id)
        messages = config[f"{self.message_type}_messages"]
        
        if action == "add":
            modal = AddMessageModal(self.guild_id, self.message_type)
            await interaction.response.send_modal(modal)
        
        elif action == "remove":
            if not messages:
                await interaction.response.send_message("❌ No messages to remove!", ephemeral=True)
                return
            modal = RemoveMessageModal(self.guild_id, self.message_type, len(messages))
            await interaction.response.send_modal(modal)
        
        elif action == "preview":
            if not messages:
                await interaction.response.send_message("❌ No messages to preview!", ephemeral=True)
                return
            
            random_msg = random.choice(messages)
            embed = discord.Embed(
                title="🎲 Random Preview",
                description=random_msg,
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
        
        elif action == "list":
            if not messages:
                await interaction.response.send_message("❌ No messages to list!", ephemeral=True)
                return
            
            embed = discord.Embed(
                title=f"📋 All {self.message_type.replace('_', ' ').title()} Messages",
                description="\n\n".join([f"**{i+1}.** {msg}" for i, msg in enumerate(messages)]),
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"Total: {len(messages)} messages")
            await interaction.response.send_message(embed=embed, ephemeral=True)

class AddMessageModal(discord.ui.Modal):
    """Modal for adding a message"""
    def __init__(self, guild_id: int, message_type: str):
        self.guild_id = guild_id
        self.message_type = message_type
        super().__init__(title=f"Add {message_type.replace('_', ' ').title()}")
        
        self.message_input = discord.ui.TextInput(
            label="Message",
            style=discord.TextStyle.paragraph,
            placeholder="Enter your message here...",
            required=True,
            max_length=500
        )
        self.add_item(self.message_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        config = get_config(self.guild_id)
        new_message = self.message_input.value.strip()
        
        message_key = f"{self.message_type}_messages"
        
        if new_message in config[message_key]:
            await interaction.response.send_message("❌ This message already exists!", ephemeral=True)
            return
        
        config[message_key].append(new_message)
        save_config(self.guild_id, config)
        
        await interaction.response.send_message(f"✅ Added message to {self.message_type.replace('_', ' ')}!", ephemeral=True)

class RemoveMessageModal(discord.ui.Modal):
    """Modal for removing a message"""
    def __init__(self, guild_id: int, message_type: str, max_number: int):
        self.guild_id = guild_id
        self.message_type = message_type
        self.max_number = max_number
        super().__init__(title=f"Remove {message_type.replace('_', ' ').title()}")
        
        self.message_input = discord.ui.TextInput(
            label="Message Number",
            style=discord.TextStyle.short,
            placeholder=f"Enter the message number to remove (1-{max_number})",
            required=True,
            max_length=10
        )
        self.add_item(self.message_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        config = get_config(self.guild_id)
        message_key = f"{self.message_type}_messages"
        
        try:
            index = int(self.message_input.value.strip()) - 1
            
            if index < 0 or index >= len(config[message_key]):
                await interaction.response.send_message(f"❌ Invalid message number! Must be between 1 and {self.max_number}.", ephemeral=True)
                return
            
            removed_message = config[message_key].pop(index)
            save_config(self.guild_id, config)
            
            embed = discord.Embed(
                title="✅ Message Removed",
                description=f"**Removed:** {removed_message}",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number!", ephemeral=True)

# ============ FIXED TOGGLE VIEWS ============

class ToggleView(discord.ui.View):
    """View for toggling features"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        self.add_item(ToggleDropdown(guild_id, user_id))
        self.add_item(BackButton(guild_id, user_id, MainMenuView, "⬅️ Main Menu"))
        self.add_item(CloseButton())
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not for you!", ephemeral=True)
            return False
        return True
    
    def get_embed(self) -> discord.Embed:
        config = get_config(self.guild_id)
        embed = discord.Embed(
            title="⚙️ Feature Toggles",
            description="Enable or disable specific features.",
            color=discord.Color.blue()
        )
        
        features = [
            ("Tracker", config["tracker_enabled"]),
            ("Human Welcome", config["human_welcome_enabled"]),
            ("Bot Welcome", config["bot_welcome_enabled"]),
            ("Welcome Pings", config["ping_enabled"]),
        ]
        
        feature_text = "\n".join([f"**{name}:** {'✅ Enabled' if enabled else '❌ Disabled'}" for name, enabled in features])
        embed.add_field(name="Current Status", value=feature_text, inline=False)
        
        return embed

class ToggleDropdown(discord.ui.Select):
    """Dropdown for toggling features - FIXED TO UPDATE THE VIEW"""
    def __init__(self, guild_id: int, user_id: int):
        self.guild_id = guild_id
        self.user_id = user_id
        config = get_config(guild_id)
        
        options = [
            discord.SelectOption(
                label=f"Tracker: {'ON' if config['tracker_enabled'] else 'OFF'}",
                value="tracker",
                emoji="✅" if config['tracker_enabled'] else "❌",
                description="Toggle join/leave tracking"
            ),
            discord.SelectOption(
                label=f"Human Welcome: {'ON' if config['human_welcome_enabled'] else 'OFF'}",
                value="human_welcome",
                emoji="✅" if config['human_welcome_enabled'] else "❌",
                description="Toggle human welcome messages"
            ),
            discord.SelectOption(
                label=f"Bot Welcome: {'ON' if config['bot_welcome_enabled'] else 'OFF'}",
                value="bot_welcome",
                emoji="✅" if config['bot_welcome_enabled'] else "❌",
                description="Toggle bot welcome messages"
            ),
            discord.SelectOption(
                label=f"Welcome Pings: {'ON' if config['ping_enabled'] else 'OFF'}",
                value="welcome_pings",
                emoji="✅" if config['ping_enabled'] else "❌",
                description="Toggle welcome pings"
            ),
        ]
        
        super().__init__(placeholder="Select feature to toggle...", options=options)
    
    async def callback(self, interaction: discord.Interaction):
        feature = self.values[0]
        config = get_config(self.guild_id)
        
        feature_map = {
            "tracker": "tracker_enabled",
            "human_welcome": "human_welcome_enabled",
            "bot_welcome": "bot_welcome_enabled",
            "welcome_pings": "ping_enabled",
        }
        
        config_key = feature_map[feature]
        config[config_key] = not config[config_key]
        save_config(self.guild_id, config)
        
        new_state = "enabled" if config[config_key] else "disabled"
        
        # FIXED: Update the view and embed to show the new state
        view = ToggleView(self.guild_id, self.user_id)
        embed = view.get_embed()
        
        await interaction.response.edit_message(embed=embed, view=view)
        await interaction.followup.send(
            f"✅ {feature.replace('_', ' ').title()} is now **{new_state}**!",
            ephemeral=True
        )

# ============ ANALYTICS VIEWS ============

class AnalyticsView(discord.ui.View):
    """View for analytics"""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        self.add_item(AnalyticsDropdown(guild_id, user_id))
        self.add_item(BackButton(guild_id, user_id, MainMenuView, "⬅️ Main Menu"))
        self.add_item(CloseButton())
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not for you!", ephemeral=True)
            return False
        return True
    
    def get_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="📊 Analytics",
            description="Select a time period to view join/leave statistics.",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="Available Periods",
            value=(
                "📅 **Daily** - Last 30 days\n"
                "📆 **Weekly** - Last 12 weeks\n"
                "📋 **Monthly** - Last 12 months"
            ),
            inline=False
        )
        
        return embed

class AnalyticsDropdown(discord.ui.Select):
    """Dropdown for analytics period"""
    def __init__(self, guild_id: int, user_id: int):
        self.guild_id = guild_id
        self.user_id = user_id
        
        options = [
            discord.SelectOption(label="Daily Analytics", value="daily", emoji="📅"),
            discord.SelectOption(label="Weekly Analytics", value="weekly", emoji="📆"),
            discord.SelectOption(label="Monthly Analytics", value="monthly", emoji="📋"),
        ]
        
        super().__init__(placeholder="Select time period...", options=options)
    
    async def callback(self, interaction: discord.Interaction):
        period = self.values[0]
        await interaction.response.defer(ephemeral=True)
        
        analytics_data = get_analytics(self.guild_id, period)
        
        if not analytics_data:
            await interaction.followup.send("❌ No analytics data available yet!", ephemeral=True)
            return
        
        # Create chart
        try:
            dates = sorted(analytics_data.keys())
            joins = [analytics_data[d]["joins"] - analytics_data[d]["bot_joins"] for d in dates]
            leaves = [analytics_data[d]["leaves"] - analytics_data[d]["bot_leaves"] for d in dates]
            bot_joins = [analytics_data[d]["bot_joins"] for d in dates]
            bot_leaves = [analytics_data[d]["bot_leaves"] for d in dates]

            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
            
            # Human activity
            ax1.plot(dates, joins, label="Human Joins", color="green", marker="o")
            ax1.plot(dates, leaves, label="Human Leaves", color="red", marker="o")
            ax1.set_title(f"Human Activity - {period.capitalize()}")
            ax1.set_xlabel("Date")
            ax1.set_ylabel("Count")
            ax1.legend()
            ax1.grid(True, alpha=0.3)
            
            # Bot activity
            ax2.plot(dates, bot_joins, label="Bot Joins", color="blue", marker="o")
            ax2.plot(dates, bot_leaves, label="Bot Leaves", color="orange", marker="o")
            ax2.set_title(f"Bot Activity - {period.capitalize()}")
            ax2.set_xlabel("Date")
            ax2.set_ylabel("Count")
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            # Save to buffer
            buffer = io.BytesIO()
            plt.savefig(buffer, format="png", dpi=100, bbox_inches="tight")
            buffer.seek(0)
            plt.close()

            file = discord.File(buffer, filename="analytics.png")
            
            # Calculate totals
            total_joins = sum(joins)
            total_leaves = sum(leaves)
            total_bot_joins = sum(bot_joins)
            total_bot_leaves = sum(bot_leaves)
            net_growth = total_joins - total_leaves

            embed = discord.Embed(
                title=f"📊 {period.capitalize()} Analytics",
                color=discord.Color.blue()
            )
            embed.add_field(name="👥 Human Joins", value=str(total_joins), inline=True)
            embed.add_field(name="👋 Human Leaves", value=str(total_leaves), inline=True)
            embed.add_field(name="📈 Net Growth", value=str(net_growth), inline=True)
            embed.add_field(name="🤖 Bot Joins", value=str(total_bot_joins), inline=True)
            embed.add_field(name="👾 Bot Leaves", value=str(total_bot_leaves), inline=True)
            embed.set_image(url="attachment://analytics.png")

            await interaction.followup.send(embed=embed, file=file, ephemeral=True)

        except Exception as e:
            logger.exception("Failed to generate analytics")
            await interaction.followup.send(f"❌ Failed to generate analytics: {str(e)}", ephemeral=True)

# ============ FIXED COG IMPLEMENTATION WITH SEPARATE ANNIVERSARY CHANNEL ============

class JoinLeaveTracker(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.retry_queue = []
        self.check_anniversaries.start()
    
    def cog_unload(self):
        self.check_anniversaries.cancel()
    
    @tasks.loop(hours=24)
    async def check_anniversaries(self):
        """Check for member anniversaries with enhanced tracking"""
        try:
            conn = sqlite3.connect(str(DB_PATH))
            cur = conn.cursor()
            
            today = datetime.now()
            
            cur.execute("""
                SELECT guild_id, user_id, join_date, username, last_anniversary_year 
                FROM member_join_dates 
                WHERE join_date IS NOT NULL
            """)
            
            for guild_id, user_id, join_date_str, username, last_anniversary_year in cur.fetchall():
                try:
                    join_date = datetime.fromisoformat(join_date_str)
                    
                    # Calculate years since joining
                    years = today.year - join_date.year
                    
                    # Check if it's their join anniversary today AND they haven't been notified for this year
                    if (join_date.month == today.month and 
                        join_date.day == today.day and 
                        years > 0 and 
                        years != last_anniversary_year):
                        
                        guild = self.bot.get_guild(guild_id)
                        if not guild:
                            continue
                        
                        config = get_config(guild_id)
                        
                        # Use anniversary channel if set, otherwise fall back to welcome channel
                        channel_id = config["anniversary_channel_id"] or config["welcome_channel_id"]
                        if not channel_id:
                            continue
                        
                        channel = guild.get_channel(channel_id)
                        if not channel:
                            continue
                        
                        member = guild.get_member(user_id)
                        if not member:
                            continue
                        
                        # Send anniversary message
                        embed = discord.Embed(
                            title=f"🎉 {years} Year Anniversary!",
                            description=f"{member.mention} joined the server {years} year{'s' if years > 1 else ''} ago today!",
                            color=discord.Color.gold()
                        )
                        embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)
                        embed.set_footer(text="Congratulations on this milestone! 🎊")
                        
                        await channel.send(embed=embed)
                        
                        # Update the database to mark this anniversary as notified
                        cur.execute("""
                            UPDATE member_join_dates 
                            SET last_anniversary_year = ?, anniversary_notified = 1 
                            WHERE guild_id = ? AND user_id = ?
                        """, (years, guild_id, user_id))
                        
                        logger.info(f"🎉 Celebrated {years} year anniversary for {member} in guild {guild_id}")
                
                except Exception as e:
                    logger.error(f"Error processing anniversary for user {user_id} in guild {guild_id}: {e}")
                    continue
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.exception("Failed to check anniversaries")
    
    @check_anniversaries.before_loop
    async def before_check_anniversaries(self):
        await self.bot.wait_until_ready()

    async def log_error(self, guild_id: int, error_message: str):
        """Log errors to configured error channel"""
        try:
            config = get_config(guild_id)
            if config["error_log_channel_id"]:
                guild = self.bot.get_guild(guild_id)
                if guild:
                    channel = guild.get_channel(config["error_log_channel_id"])
                    if channel:
                        embed = discord.Embed(
                            title="⚠️ Error Log",
                            description=error_message,
                            color=discord.Color.red(),
                            timestamp=datetime.now()
                        )
                        await channel.send(embed=embed)
        except Exception:
            logger.exception("Failed to log error to channel")

    def build_welcome_ping_string(self, guild: discord.Guild, config: dict) -> str:
        """Build the ping string for welcome messages"""
        if not config["ping_enabled"]:
            return ""
            
        pings = []
        
        for role_id in config["welcome_roles"]:
            role = guild.get_role(role_id)
            if role:
                pings.append(role.mention)
        
        for user_id in config["welcome_users"]:
            user = guild.get_member(user_id)
            if user:
                pings.append(user.mention)
        
        if pings:
            return " ".join(pings)
        return ""

    async def assign_role_with_retry(self, member: discord.Member, role: discord.Role, max_retries: int = 3):
        """Assign role with retry mechanism"""
        for attempt in range(max_retries):
            try:
                await member.add_roles(role)
                logger.info(f"✅ Assigned {role.name} to {member} (attempt {attempt + 1})")
                return True
            except discord.Forbidden:
                logger.error(f"❌ Missing permissions to assign {role.name} to {member}")
                await self.log_error(member.guild.id, f"Missing permissions to assign role {role.name} to {member}")
                return False
            except discord.HTTPException as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.error(f"❌ Failed to assign {role.name} to {member} after {max_retries} attempts")
                    await self.log_error(member.guild.id, f"Failed to assign role {role.name} to {member} after {max_retries} attempts: {str(e)}")
                    return False
        return False

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Handle member join"""
        logger.info("🔔 on_member_join fired for %s in guild %s", member, member.guild.id)
        
        guild = member.guild
        config = get_config(guild.id)
        
        # Log event
        log_event(guild.id, member.id, str(member), "join", member.bot)
        
        # Save join date for non-bots with enhanced tracking
        if not member.bot:
            save_join_date(guild.id, member.id, datetime.now(), str(member))
        
        # Check for suspicious activity
        suspicious = detect_suspicious_activity(guild.id)
        if suspicious:
            alert_msg = f"⚠️ Suspicious activity detected: {suspicious.get('join', 0)} joins in the last 5 minutes"
            await self.log_error(guild.id, alert_msg)

        # Tracker message
        if config["tracker_enabled"] and config["stats_channel_id"]:
            stats_channel = guild.get_channel(config["stats_channel_id"])
            
            # Try fallback if primary fails
            if not stats_channel and config["fallback_stats_channel_id"]:
                stats_channel = guild.get_channel(config["fallback_stats_channel_id"])
            
            if stats_channel:
                try:
                    await asyncio.sleep(0.5)
                    stats = get_member_stats(guild)

                    if member.bot:
                        messages = config["bot_welcome_messages"]
                        title = "✅ Bot Joined!"
                    else:
                        messages = config["human_welcome_messages"]
                        title = "✅ Human Joined!"

                    funny_msg = random.choice(messages)

                    embed = discord.Embed(
                        title=title,
                        description=f"**{member}** joined the server!\n\n**{funny_msg}**",
                        color=discord.Color.green()
                    )
                    embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)
                    embed.add_field(name="👥 Server Stats", value=f"**Total:** {stats['total']}\n**Humans:** {stats['humans']}\n**Bots:** {stats['bots']}", inline=False)
                    embed.set_footer(text=f"ID: {member.id}")

                    await stats_channel.send(embed=embed)
                    logger.info("Tracker message sent for %s", member)
                except Exception as e:
                    logger.exception("Failed to process tracker for member %s", member)
                    await self.log_error(guild.id, f"Failed to send tracker message for {member}: {str(e)}")

        # Welcome message
        if config["welcome_channel_id"]:
            welcome_channel = guild.get_channel(config["welcome_channel_id"])
            
            # Try fallback if primary fails
            if not welcome_channel and config["fallback_welcome_channel_id"]:
                welcome_channel = guild.get_channel(config["fallback_welcome_channel_id"])
            
            if welcome_channel:
                try:
                    ping_string = self.build_welcome_ping_string(guild, config)
                    
                    if member.bot:
                        if not config["bot_welcome_enabled"]:
                            logger.info("Bot welcome disabled for %s", member)
                        else:
                            bot_messages = config["bot_welcome_messages"]
                            # Bot welcome format: roast message + line breaks + welcome
                            welcome_msg = f"{random.choice(bot_messages)}\n\nAnyways, welcome {member.mention}!"
                            await welcome_channel.send(welcome_msg)
                            logger.info("Welcome message sent for %s", member)
                        
                    else:
                        if not config["human_welcome_enabled"]:
                            logger.info("Human welcome disabled for %s", member)
                        else:
                            human_messages = config["human_welcome_messages"]
                            
                            # FIXED FORMAT: Random welcome message + ping + "come, welcome them!" + line breaks + welcome + roles/rules
                            base_message = random.choice(human_messages)
                            
                            # Build the ping part with "come, welcome them!"
                            if ping_string:
                                welcome_msg = f"{base_message} {ping_string} come, welcome them!\n\nWelcome to the server {member.mention}!"
                            else:
                                welcome_msg = f"{base_message}\n\nWelcome to the server {member.mention}!"
                            
                            # Add roles and rules information
                            additional_info = []
                            if config["roles_channel_id"]:
                                roles_channel = guild.get_channel(config["roles_channel_id"])
                                if roles_channel:
                                    additional_info.append(f"Don't forget to get your roles from {roles_channel.mention}.")
                            
                            if config["rules_channel_id"]:
                                rules_channel = guild.get_channel(config["rules_channel_id"])
                                if rules_channel:
                                    additional_info.append(f"Also, you must check out {rules_channel.mention}.")
                            
                            if additional_info:
                                welcome_msg += " " + " ".join(additional_info)
                            
                            # FIXED: Add allowed_mentions to actually ping the users/roles
                            await welcome_channel.send(welcome_msg, allowed_mentions=discord.AllowedMentions(roles=True, users=True))
                            logger.info("Welcome message sent for %s", member)
                except Exception as e:
                    logger.exception("Failed to process welcome for member %s", member)
                    await self.log_error(guild.id, f"Failed to send welcome message for {member}: {str(e)}")

        # Role assignment
        try:
            if member.bot and config["bot_role_id"]:
                bot_role = guild.get_role(config["bot_role_id"])
                if bot_role:
                    success = await self.assign_role_with_retry(member, bot_role)
                    if not success:
                        await self.log_error(guild.id, f"Failed to assign bot role to {member}")
            elif not member.bot and config["member_role_id"]:
                member_role = guild.get_role(config["member_role_id"])
                if member_role:
                    success = await self.assign_role_with_retry(member, member_role)
                    if not success:
                        await self.log_error(guild.id, f"Failed to assign member role to {member}")
        except Exception as e:
            logger.exception("Failed to assign role to %s", member)
            await self.log_error(guild.id, f"Error assigning role to {member}: {str(e)}")

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Handle member leave"""
        guild = member.guild
        config = get_config(guild.id)
        
        # Log event
        log_event(guild.id, member.id, str(member), "leave", member.bot)
        
        # Check for suspicious activity
        suspicious = detect_suspicious_activity(guild.id)
        if suspicious:
            alert_msg = f"⚠️ Suspicious activity detected: {suspicious.get('leave', 0)} leaves in the last 5 minutes"
            await self.log_error(guild.id, alert_msg)

        if not config["tracker_enabled"] or not config["stats_channel_id"]:
            return

        stats_channel = guild.get_channel(config["stats_channel_id"])
        
        # Try fallback if primary fails
        if not stats_channel and config["fallback_stats_channel_id"]:
            stats_channel = guild.get_channel(config["fallback_stats_channel_id"])
        
        if not stats_channel:
            logger.warning("Stats channel not found for guild %s", guild.id)
            return

        try:
            stats = get_member_stats(guild)

            if member.bot:
                messages = config["bot_goodbye_messages"]
                title = "❌ Bot Left!"
            else:
                messages = config["human_goodbye_messages"]
                title = "❌ Human Left!"

            goodbye_msg = random.choice(messages)

            embed = discord.Embed(
                title=title,
                description=f"**{member}** left the server.\n\n**{goodbye_msg}**",
                color=discord.Color.red()
            )
            embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)
            embed.add_field(name="👥 Server Stats", value=f"**Total:** {stats['total']}\n**Humans:** {stats['humans']}\n**Bots:** {stats['bots']}", inline=False)
            embed.set_footer(text=f"ID: {member.id}")

            await stats_channel.send(embed=embed)

            logger.info("Leave event for %s (%s) in guild %s", member, member.id, guild.id)
        except Exception as e:
            logger.exception("Failed to process leave event for member %s", member)
            await self.log_error(guild.id, f"Failed to send leave message for {member}: {str(e)}")

    async def check_permissions(self, interaction: discord.Interaction) -> bool:
        """Check if user has permission"""
        if is_developer(interaction.user):
            return True
        if interaction.user.guild_permissions.administrator:
            return True
        return False

    @app_commands.command(name="joinleave", description="Open the JoinLeave Tracker control panel")
    async def joinleave(self, interaction: discord.Interaction):
        """Main command that opens the control panel"""
        if not await self.check_permissions(interaction):
            await interaction.response.send_message("❌ You need administrator permissions to use this command.", ephemeral=True)
            return

        view = MainMenuView(interaction.guild.id, interaction.user.id)
        embed = view.get_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

async def setup(bot):
    # Initialize database WITHOUT dropping existing data
    init_db()
    
    # Add error handling for cog loading
    try:
        await bot.add_cog(JoinLeaveTracker(bot))
        logger.info("✅ JoinLeaveTracker cog loaded successfully - settings preserved")
    except Exception as e:
        logger.error(f"❌ Failed to load JoinLeaveTracker cog: {e}")
        raise
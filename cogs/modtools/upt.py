import discord
from discord.ext import commands, tasks
from discord import ui, app_commands
import asyncio
from typing import Dict, List, Optional, Set, Any, Tuple
import datetime
from dataclasses import dataclass, asdict
import json
import os
import re
from enum import Enum
import time
import sqlite3
import logging
from logging.handlers import RotatingFileHandler
import aiosqlite
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DEVELOPER_ID = int(os.getenv('UPT_DEVELOPER_ID', '1313333441525448704'))
DATABASE_PATH = "data/upt_database.db"
LOG_FILE = "data/upt.log"

# Safety limits
MAX_MESSAGES_PER_TASK = 100000
MAX_CHANNELS_PER_TASK = 50
MAX_MEMORY_BATCH_SIZE = 500

# Configure logging
def setup_logging():
    os.makedirs("data", exist_ok=True)
    
    logger = logging.getLogger('UPT')
    logger.setLevel(logging.INFO)
    
    # File handler with rotation
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

class UPTState(Enum):
    MAIN_MENU = 1
    TARGET_SELECTION = 2
    CHANNEL_SELECTION = 3
    AMOUNT_SELECTION = 4
    ACTIVE_TASKS = 5

@dataclass
class UPTConfig:
    target_users: Set[int]
    target_words: Set[str]
    channels: Set[int]
    threads: Set[int]
    amount: int = 100
    per_channel: bool = False
    notify: bool = True
    remove_reactions: bool = False
    delete_all_users: bool = False
    # Implemented features
    whitelist_users: Set[int] = None
    whitelist_roles: Set[int] = None
    whitelist_channels: Set[int] = None
    has_embed: Optional[bool] = None
    has_attachment: Optional[bool] = None
    has_link: Optional[bool] = None
    use_bulk_delete: bool = True
    dry_run: bool = False
    include_bots: bool = False
    use_regex: bool = False
    last_accessed: float = None
    
    def __post_init__(self):
        if self.whitelist_users is None:
            self.whitelist_users = set()
        if self.whitelist_roles is None:
            self.whitelist_roles = set()
        if self.whitelist_channels is None:
            self.whitelist_channels = set()
        if self.last_accessed is None:
            self.last_accessed = time.time()

@dataclass
class UPTTask:
    task_id: str
    config: UPTConfig
    start_time: datetime.datetime
    progress_msg: discord.Message
    cancelled: bool = False
    messages_deleted: int = 0
    reactions_removed: int = 0
    channels_processed: int = 0
    total_channels: int = 0
    current_channel: str = "Starting..."
    current_action: str = "Scanning..."
    estimated_total: int = 0
    pre_scanned: bool = False
    deletion_speed: float = 0.0
    last_50_times: List[float] = None
    last_update: datetime.datetime = None
    last_progress_update: float = 0
    
    def __post_init__(self):
        if self.last_50_times is None:
            self.last_50_times = []
        if self.last_update is None:
            self.last_update = datetime.datetime.now(datetime.timezone.utc)
        if self.last_progress_update == 0:
            self.last_progress_update = time.time()

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize database tables"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Deletion history table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS deletion_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        guild_id INTEGER NOT NULL,
                        target_users TEXT,
                        target_words TEXT,
                        whitelist_users TEXT,
                        whitelist_roles TEXT,
                        whitelist_channels TEXT,
                        delete_all_users BOOLEAN DEFAULT FALSE,
                        include_bots BOOLEAN DEFAULT FALSE,
                        use_regex BOOLEAN DEFAULT FALSE,
                        has_embed BOOLEAN,
                        has_attachment BOOLEAN,
                        has_link BOOLEAN,
                        messages_deleted INTEGER DEFAULT 0,
                        reactions_removed INTEGER DEFAULT 0,
                        channels_processed INTEGER DEFAULT 0,
                        threads_processed INTEGER DEFAULT 0,
                        time_taken TEXT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        remove_reactions BOOLEAN DEFAULT FALSE,
                        used_bulk_delete BOOLEAN DEFAULT TRUE
                    )
                ''')
                
                # System stats table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS system_stats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        total_messages_deleted INTEGER DEFAULT 0,
                        total_reactions_removed INTEGER DEFAULT 0,
                        total_tasks_completed INTEGER DEFAULT 0,
                        total_time_elapsed REAL DEFAULT 0,
                        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Initialize stats if not exists
                cursor.execute('SELECT COUNT(*) FROM system_stats')
                if cursor.fetchone()[0] == 0:
                    cursor.execute('''
                        INSERT INTO system_stats 
                        (total_messages_deleted, total_reactions_removed, total_tasks_completed, total_time_elapsed)
                        VALUES (0, 0, 0, 0)
                    ''')
                
                conn.commit()
                logger.info("Database initialized successfully")
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
    
    async def save_deletion_history(self, record: dict):
        """Save deletion history to database"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute('''
                    INSERT INTO deletion_history (
                        user_id, guild_id, target_users, target_words, whitelist_users,
                        whitelist_roles, whitelist_channels, delete_all_users, include_bots,
                        use_regex, has_embed, has_attachment, has_link, messages_deleted,
                        reactions_removed, channels_processed, threads_processed, time_taken,
                        timestamp, remove_reactions, used_bulk_delete
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    record['user_id'],
                    record.get('guild_id', 0),
                    json.dumps(list(record.get('target_users', []))),
                    json.dumps(list(record.get('target_words', []))),
                    json.dumps(list(record.get('whitelist_users', []))),
                    json.dumps(list(record.get('whitelist_roles', []))),
                    json.dumps(list(record.get('whitelist_channels', []))),
                    record.get('delete_all_users', False),
                    record.get('include_bots', False),
                    record.get('use_regex', False),
                    record.get('has_embed'),
                    record.get('has_attachment'),
                    record.get('has_link'),
                    record.get('messages_deleted', 0),
                    record.get('reactions_removed', 0),
                    record.get('channels_processed', 0),
                    record.get('threads_processed', 0),
                    record.get('time_taken', ''),
                    record.get('timestamp'),
                    record.get('remove_reactions', False),
                    record.get('used_bulk_delete', True)
                ))
                await conn.commit()
                logger.info(f"Saved deletion history for user {record['user_id']}")
        except Exception as e:
            logger.error(f"Failed to save deletion history: {e}")
    
    async def update_system_stats(self, stats: dict):
        """Update system statistics"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute('''
                    UPDATE system_stats SET
                    total_messages_deleted = ?,
                    total_reactions_removed = ?,
                    total_tasks_completed = ?,
                    total_time_elapsed = ?,
                    last_updated = CURRENT_TIMESTAMP
                ''', (
                    stats['total_messages_deleted'],
                    stats['total_reactions_removed'],
                    stats['total_tasks_completed'],
                    stats['total_time_elapsed']
                ))
                await conn.commit()
        except Exception as e:
            logger.error(f"Failed to update system stats: {e}")
    
    async def get_system_stats(self) -> dict:
        """Get system statistics"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute('SELECT * FROM system_stats LIMIT 1')
                row = await cursor.fetchone()
                if row:
                    return {
                        'total_messages_deleted': row[1],
                        'total_reactions_removed': row[2],
                        'total_tasks_completed': row[3],
                        'total_time_elapsed': row[4]
                    }
                return {
                    'total_messages_deleted': 0,
                    'total_reactions_removed': 0,
                    'total_tasks_completed': 0,
                    'total_time_elapsed': 0
                }
        except Exception as e:
            logger.error(f"Failed to get system stats: {e}")
            return {
                'total_messages_deleted': 0,
                'total_reactions_removed': 0,
                'total_tasks_completed': 0,
                'total_time_elapsed': 0
            }
    
    async def get_deletion_history(self, limit: int = 100) -> List[dict]:
        """Get deletion history with limit"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute(
                    'SELECT * FROM deletion_history ORDER BY timestamp DESC LIMIT ?',
                    (limit,)
                )
                rows = await cursor.fetchall()
                
                history = []
                for row in rows:
                    history.append({
                        'id': row[0],
                        'user_id': row[1],
                        'guild_id': row[2],
                        'target_users': json.loads(row[3] or '[]'),
                        'target_words': json.loads(row[4] or '[]'),
                        'whitelist_users': json.loads(row[5] or '[]'),
                        'whitelist_roles': json.loads(row[6] or '[]'),
                        'whitelist_channels': json.loads(row[7] or '[]'),
                        'delete_all_users': bool(row[8]),
                        'include_bots': bool(row[9]),
                        'use_regex': bool(row[10]),
                        'has_embed': row[11],
                        'has_attachment': row[12],
                        'has_link': row[13],
                        'messages_deleted': row[14],
                        'reactions_removed': row[15],
                        'channels_processed': row[16],
                        'threads_processed': row[17],
                        'time_taken': row[18],
                        'timestamp': row[19],
                        'remove_reactions': bool(row[20]),
                        'used_bulk_delete': bool(row[21])
                    })
                return history
        except Exception as e:
            logger.error(f"Failed to get deletion history: {e}")
            return []

# ============ PAGINATION VIEWS ============

class RefreshButton(discord.ui.Button):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(label="↻ Main Menu", style=discord.ButtonStyle.primary, row=4)
        self.guild_id = guild_id
        self.user_id = user_id
    
    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("UserPresenceTerminator")
        if cog:
            new_view = UserPresenceTerminator.UPTMainView(cog, self.user_id, self.guild_id)
            config = cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            embed = cog.get_main_embed(config, interaction.guild)
            await interaction.response.edit_message(embed=embed, view=new_view)

class CloseButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="❌ Close", style=discord.ButtonStyle.danger, row=4)
    
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="✅ Control panel closed.", embed=None, view=None)

class BackButton(discord.ui.Button):
    def __init__(self, cog, user_id: int, guild_id: int, back_view_class, label="⬅️ Back"):
        super().__init__(label=label, style=discord.ButtonStyle.secondary, row=4)
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        self.back_view_class = back_view_class
    
    async def callback(self, interaction: discord.Interaction):
        view = self.back_view_class(self.cog, self.user_id, self.guild_id)
        config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
        
        if self.back_view_class == UserPresenceTerminator.TargetSelectionView:
            embed = self.cog.get_target_selection_embed(config, interaction.guild)
        elif self.back_view_class == UserPresenceTerminator.ChannelSelectionView:
            embed = self.cog.get_channel_selection_embed(interaction.guild, config)
        elif self.back_view_class == UserPresenceTerminator.AmountSelectionView:
            embed = self.cog.get_amount_selection_embed(config)
        elif self.back_view_class == UserPresenceTerminator.AdvancedSettingsView:
            embed = self.cog.get_advanced_settings_embed(config, interaction.guild)
        else:
            embed = self.cog.get_main_embed(config, interaction.guild)
            
        await interaction.response.edit_message(embed=embed, view=view)

class PreviousPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="⬅️ Previous", style=discord.ButtonStyle.secondary, row=2)
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if hasattr(view, 'page') and view.page > 0:
            view.page -= 1
            await view.update_view(interaction)

class NextPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Next ➡️", style=discord.ButtonStyle.secondary, row=2)
    
    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if hasattr(view, 'page') and view.page < getattr(view, 'total_pages', 1) - 1:
            view.page += 1
            await view.update_view(interaction)

class PageIndicatorButton(discord.ui.Button):
    def __init__(self, current_page: int, total_pages: int):
        super().__init__(
            label=f"Page {current_page + 1}/{total_pages}", 
            style=discord.ButtonStyle.grey, 
            row=2,
            disabled=True
        )

# ============ ADD USER BY ID MODAL ============

class AddUserByIDModal(ui.Modal, title="Add User by ID"):
    def __init__(self, cog, user_id: int, guild_id: int, is_whitelist: bool = False):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        self.is_whitelist = is_whitelist

    user_id_input = ui.TextInput(
        label="User ID",
        placeholder="Enter the user ID to add...",
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            user_id = int(self.user_id_input.value)
        except ValueError:
            await interaction.response.send_message("❌ Invalid user ID. Please enter a numeric ID.", ephemeral=True)
            return

        config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
        if self.is_whitelist:
            config.whitelist_users.add(user_id)
            message = f"✅ Added user ID {user_id} to whitelist."
        else:
            config.target_users.add(user_id)
            config.delete_all_users = False
            message = f"✅ Added user ID {user_id} to target users."

        self.cog.active_configs[self.user_id] = config

        guild = interaction.guild
        all_members = self.cog._get_all_members_with_fakes(guild, 
            config.whitelist_users if self.is_whitelist else config.target_users)
        
        view = PaginatedUserSelectionView(self.cog, self.user_id, self.guild_id, all_members, is_whitelist=self.is_whitelist)
        embed = view.get_embed()
        await interaction.response.edit_message(embed=embed, view=view)
        await interaction.followup.send(message, ephemeral=True)

# ============ ADD USER BY ID BUTTON ============

class AddByIDButton(discord.ui.Button):
    def __init__(self, cog, user_id: int, guild_id: int, is_whitelist: bool = False):
        super().__init__(label="Add by ID", style=discord.ButtonStyle.primary, row=3)
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        self.is_whitelist = is_whitelist

    async def callback(self, interaction: discord.Interaction):
        modal = AddUserByIDModal(self.cog, self.user_id, self.guild_id, self.is_whitelist)
        await interaction.response.send_modal(modal)

# ============ CHANNEL SELECTION WITH WHITELIST ============

class PaginatedChannelSelectionView(discord.ui.View):
    def __init__(self, cog, user_id: int, guild_id: int, all_channels: list, page: int = 0, is_whitelist: bool = False):
        super().__init__(timeout=600)
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        self.all_channels = all_channels
        self.page = page
        self.is_whitelist = is_whitelist
        self.channels_per_page = 24
        self.total_pages = max(1, (len(all_channels) + self.channels_per_page - 1) // self.channels_per_page)
        self.update_components()
    
    def update_components(self):
        self.clear_items()
        current_page_channels = self.get_current_page_channels()
        options = self.create_channel_options(current_page_channels)
        
        if self.is_whitelist:
            self.add_item(WhitelistChannelSelectDropdown(self.cog, self.user_id, self.guild_id, options))
        else:
            self.add_item(ChannelSelectDropdown(self.cog, self.user_id, self.guild_id, options))
        
        if self.total_pages > 1:
            self.add_item(PreviousPageButton())
            self.add_item(NextPageButton())
            self.add_item(PageIndicatorButton(self.page, self.total_pages))
        
        if self.is_whitelist:
            back_view = UserPresenceTerminator.AdvancedSettingsView
        else:
            back_view = UserPresenceTerminator.ChannelSelectionView
            
        self.add_item(BackButton(self.cog, self.user_id, self.guild_id, back_view, "⬅️ Back"))
        self.add_item(CloseButton())
    
    def get_current_page_channels(self):
        start_idx = self.page * self.channels_per_page
        end_idx = start_idx + self.channels_per_page
        return self.all_channels[start_idx:end_idx]
    
    def create_channel_options(self, page_channels):
        if self.is_whitelist:
            options = [discord.SelectOption(label="❌ Clear All Whitelist", value="clear_all", description="Clear all whitelisted channels")]
            config_field = "whitelist_channels"
        else:
            options = [discord.SelectOption(label="❌ Clear All", value="clear_all", description="Clear all channel selections")]
            config_field = "channels"
        
        config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
        target_set = getattr(config, config_field, set())
        
        for channel in page_channels:
            channel_type_emoji = {
                discord.TextChannel: "💬",
                discord.VoiceChannel: "🔊", 
                discord.StageChannel: "🎤",
                discord.ForumChannel: "📝",
                discord.CategoryChannel: "📁"
            }.get(type(channel), "📁")
            
            channel_name = channel.name
            if len(channel_name) > 25:
                channel_name = channel_name[:22] + "..."
            
            channel_description = f"#{channel.name}" if isinstance(channel, discord.TextChannel) else channel.name
            if len(channel_description) > 50:
                channel_description = channel_description[:47] + "..."
            
            is_selected = channel.id in target_set
            prefix = "✅" if is_selected else "❌"
            
            options.append(discord.SelectOption(
                label=f"{prefix} {channel_type_emoji} {channel_name}",
                value=str(channel.id),
                description=channel_description
            ))
        
        return options
    
    async def update_view(self, interaction: discord.Interaction):
        self.update_components()
        embed = self.get_embed()
        await interaction.response.edit_message(embed=embed, view=self)
    
    def get_embed(self) -> discord.Embed:
        title = "🛡️ Whitelist Channels" if self.is_whitelist else "📁 Channel Selection"
        description = f"Select channels to whitelist from deletion. Page {self.page + 1}/{self.total_pages}" if self.is_whitelist else f"Select channels to scan for deletion. Page {self.page + 1}/{self.total_pages}"
        
        embed = discord.Embed(title=title, description=description, color=discord.Color.blue())
        config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
        
        if self.is_whitelist:
            target_set = config.whitelist_channels
            field_name = "Whitelisted Channels"
        else:
            target_set = config.channels
            field_name = "Current Selection"
        
        if target_set:
            current_channels = []
            for channel_id in list(target_set)[:5]:
                channel = discord.utils.get(self.all_channels, id=channel_id)
                if channel:
                    channel_type = "🔊" if isinstance(channel, discord.VoiceChannel) else "💬"
                    current_channels.append(f"• {channel_type} {channel.mention}")
            
            if current_channels:
                embed.add_field(
                    name=field_name,
                    value="\n".join(current_channels) + (f"\n• ... and {len(target_set) - 5} more" if len(target_set) > 5 else ""),
                    inline=False
                )
        else:
            embed.add_field(name=field_name, value="*No channels selected*", inline=False)
        
        embed.set_footer(text=f"Total channels: {len(self.all_channels)} • Use pagination buttons to browse all channels")
        return embed

class ChannelSelectDropdown(discord.ui.Select):
    def __init__(self, cog, user_id: int, guild_id: int, options: list):
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        super().__init__(
            placeholder="Select channels...",
            options=options,
            min_values=1,
            max_values=min(len(options), 25)
        )
    
    async def callback(self, interaction: discord.Interaction):
        config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
        
        if "clear_all" in self.values:
            config.channels.clear()
            message = "✅ Cleared all channel selections!"
        else:
            for value in self.values:
                channel_id = int(value)
                if channel_id in config.channels:
                    config.channels.remove(channel_id)
                else:
                    config.channels.add(channel_id)
            
            selected_count = len([cid for cid in self.values if cid != "clear_all" and int(cid) in config.channels])
            message = f"✅ Updated {selected_count} channel selection(s)!"
        
        self.cog.active_configs[self.user_id] = config
        guild = interaction.guild
        all_channels = [ch for ch in guild.text_channels + guild.voice_channels]
        all_channels.sort(key=lambda x: x.position)
        
        view = PaginatedChannelSelectionView(self.cog, self.user_id, self.guild_id, all_channels)
        embed = view.get_embed()
        await interaction.response.edit_message(embed=embed, view=view)
        await interaction.followup.send(message, ephemeral=True)

class WhitelistChannelSelectDropdown(discord.ui.Select):
    def __init__(self, cog, user_id: int, guild_id: int, options: list):
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        super().__init__(
            placeholder="Select channels to whitelist...",
            options=options,
            min_values=1,
            max_values=min(len(options), 25)
        )
    
    async def callback(self, interaction: discord.Interaction):
        config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
        
        if "clear_all" in self.values:
            config.whitelist_channels.clear()
            message = "✅ Cleared all whitelisted channels!"
        else:
            for value in self.values:
                channel_id = int(value)
                if channel_id in config.whitelist_channels:
                    config.whitelist_channels.remove(channel_id)
                else:
                    config.whitelist_channels.add(channel_id)
            
            selected_count = len([cid for cid in self.values if cid != "clear_all" and int(cid) in config.whitelist_channels])
            message = f"✅ Updated {selected_count} whitelist selection(s)!"
        
        self.cog.active_configs[self.user_id] = config
        guild = interaction.guild
        all_channels = [ch for ch in guild.text_channels + guild.voice_channels]
        all_channels.sort(key=lambda x: x.position)
        
        view = PaginatedChannelSelectionView(self.cog, self.user_id, self.guild_id, all_channels, is_whitelist=True)
        embed = view.get_embed()
        await interaction.response.edit_message(embed=embed, view=view)
        await interaction.followup.send(message, ephemeral=True)

# ============ USER SELECTION WITH WHITELIST ============

class PaginatedUserSelectionView(discord.ui.View):
    def __init__(self, cog, user_id: int, guild_id: int, all_members: list, page: int = 0, is_whitelist: bool = False):
        super().__init__(timeout=600)
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        self.all_members = all_members
        self.page = page
        self.is_whitelist = is_whitelist
        self.members_per_page = 24
        self.total_pages = max(1, (len(all_members) + self.members_per_page - 1) // self.members_per_page)
        self.update_components()
    
    def update_components(self):
        self.clear_items()
        current_page_members = self.get_current_page_members()
        options = self.create_member_options(current_page_members)
        
        if self.is_whitelist:
            self.add_item(WhitelistUserSelectDropdown(self.cog, self.user_id, self.guild_id, options))
        else:
            self.add_item(UserSelectDropdown(self.cog, self.user_id, self.guild_id, options))
        
        if self.total_pages > 1:
            self.add_item(PreviousPageButton())
            self.add_item(NextPageButton())
            self.add_item(PageIndicatorButton(self.page, self.total_pages))
        
        self.add_item(AddByIDButton(self.cog, self.user_id, self.guild_id, self.is_whitelist))
        
        if self.is_whitelist:
            back_view = UserPresenceTerminator.AdvancedSettingsView
        else:
            back_view = UserPresenceTerminator.TargetSelectionView
            
        self.add_item(BackButton(self.cog, self.user_id, self.guild_id, back_view, "⬅️ Back"))
        self.add_item(CloseButton())
    
    def get_current_page_members(self):
        start_idx = self.page * self.members_per_page
        end_idx = start_idx + self.members_per_page
        return self.all_members[start_idx:end_idx]
    
    def create_member_options(self, page_members):
        if self.is_whitelist:
            options = [discord.SelectOption(label="❌ Clear All Whitelist", value="clear_all", description="Clear all whitelisted users")]
            config_field = "whitelist_users"
        else:
            options = [discord.SelectOption(label="❌ Clear All Users", value="clear_all", description="Clear all user selections")]
            config_field = "target_users"
        
        config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
        target_set = getattr(config, config_field, set())
        
        for member in page_members:
            is_fake_member = hasattr(member, 'is_fake') and member.is_fake
            
            if is_fake_member:
                member_name = member.display_name
                description = "Not in server"
                is_bot = False
            else:
                member_name = member.display_name
                description = f"@{member.name}" if hasattr(member, 'name') else f"ID: {member.id}"
                is_bot = getattr(member, 'bot', False)
            
            if len(member_name) > 25:
                member_name = member_name[:22] + "..."
            
            if len(description) > 50:
                description = description[:47] + "..."
            
            is_selected = member.id in target_set
            prefix = "✅" if is_selected else "❌"
            
            if is_fake_member:
                label_prefix = "👤"
            elif is_bot:
                label_prefix = "🤖"
            else:
                label_prefix = "👤"
            
            if self.is_whitelist:
                label_prefix = "🛡️"
            
            options.append(discord.SelectOption(
                label=f"{prefix} {label_prefix} {member_name}",
                value=str(member.id),
                description=description
            ))
        
        return options
    
    async def update_view(self, interaction: discord.Interaction):
        self.update_components()
        embed = self.get_embed()
        await interaction.response.edit_message(embed=embed, view=self)
    
    def get_embed(self) -> discord.Embed:
        title = "🛡️ Whitelist Users" if self.is_whitelist else "👥 User Selection"
        description = f"Select users to whitelist from deletion. Page {self.page + 1}/{self.total_pages}" if self.is_whitelist else f"Select target users for deletion. Page {self.page + 1}/{self.total_pages}"
        
        embed = discord.Embed(title=title, description=description, color=discord.Color.blue())
        config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
        
        if self.is_whitelist:
            target_set = config.whitelist_users
            field_name = "Whitelisted Users"
        else:
            target_set = config.target_users
            field_name = "Current Selection"
        
        if target_set:
            current_users = []
            for user_id in list(target_set)[:5]:
                user_display = None
                for member in self.all_members:
                    if member.id == user_id:
                        if hasattr(member, 'is_fake') and member.is_fake:
                            user_display = f"• 👤 {member.display_name}"
                        elif getattr(member, 'bot', False):
                            user_display = f"• 🤖 {member.display_name} (Bot)"
                        else:
                            user_display = f"• 👤 {member.display_name}"
                        break
                
                if not user_display:
                    user_display = f"• 👤 User ID: {user_id} (not in server)"
                
                current_users.append(user_display)
            
            if current_users:
                embed.add_field(
                    name=field_name,
                    value="\n".join(current_users) + (f"\n• ... and {len(target_set) - 5} more" if len(target_set) > 5 else ""),
                    inline=False
                )
        else:
            embed.add_field(name=field_name, value="*No users selected*", inline=False)
        
        server_members = len([m for m in self.all_members if not hasattr(m, 'is_fake') and not getattr(m, 'bot', False)])
        bot_members = len([m for m in self.all_members if not hasattr(m, 'is_fake') and getattr(m, 'bot', False)])
        fake_members = len([m for m in self.all_members if hasattr(m, 'is_fake')])
        
        footer_text = f"Total: {server_members} users, {bot_members} bots"
        if fake_members > 0:
            footer_text += f", {fake_members} external users"
        footer_text += " • Use 'Add by ID' for users not in server"
        
        embed.set_footer(text=footer_text)
        return embed

class UserSelectDropdown(discord.ui.Select):
    def __init__(self, cog, user_id: int, guild_id: int, options: list):
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        super().__init__(
            placeholder="Select users...",
            options=options,
            min_values=1,
            max_values=min(len(options), 25)
        )
    
    async def callback(self, interaction: discord.Interaction):
        config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
        
        if "clear_all" in self.values:
            config.target_users.clear()
            config.delete_all_users = False
            message = "✅ Cleared all user selections!"
        else:
            for value in self.values:
                if value == "clear_all":
                    continue
                user_id = int(value)
                if user_id in config.target_users:
                    config.target_users.remove(user_id)
                else:
                    config.target_users.add(user_id)
            
            selected_count = len([uid for uid in self.values if uid != "clear_all" and int(uid) in config.target_users])
            message = f"✅ Updated {selected_count} user selection(s)!"
            config.delete_all_users = False
        
        self.cog.active_configs[self.user_id] = config
        
        guild = interaction.guild
        all_members = self.cog._get_all_members_with_fakes(guild, config.target_users)
        
        view = PaginatedUserSelectionView(self.cog, self.user_id, self.guild_id, all_members)
        embed = view.get_embed()
        await interaction.response.edit_message(embed=embed, view=view)
        await interaction.followup.send(message, ephemeral=True)

class WhitelistUserSelectDropdown(discord.ui.Select):
    def __init__(self, cog, user_id: int, guild_id: int, options: list):
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        super().__init__(
            placeholder="Select users to whitelist...",
            options=options,
            min_values=1,
            max_values=min(len(options), 25)
        )
    
    async def callback(self, interaction: discord.Interaction):
        config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
        
        if "clear_all" in self.values:
            config.whitelist_users.clear()
            message = "✅ Cleared all whitelisted users!"
        else:
            for value in self.values:
                if value == "clear_all":
                    continue
                user_id = int(value)
                if user_id in config.whitelist_users:
                    config.whitelist_users.remove(user_id)
                else:
                    config.whitelist_users.add(user_id)
            
            selected_count = len([uid for uid in self.values if uid != "clear_all" and int(uid) in config.whitelist_users])
            message = f"✅ Updated {selected_count} whitelist selection(s)!"
        
        self.cog.active_configs[self.user_id] = config
        
        guild = interaction.guild
        all_members = self.cog._get_all_members_with_fakes(guild, config.whitelist_users)
        
        view = PaginatedUserSelectionView(self.cog, self.user_id, self.guild_id, all_members, is_whitelist=True)
        embed = view.get_embed()
        await interaction.response.edit_message(embed=embed, view=view)
        await interaction.followup.send(message, ephemeral=True)

# ============ MAIN UPT CLASS ============

class UserPresenceTerminator(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.active_configs: Dict[int, UPTConfig] = {}
        self.active_tasks: Dict[str, UPTTask] = {}
        self.allow_admins: bool = False
        self.deletion_history: List[dict] = []
        self.stats = {
            'total_messages_deleted': 0,
            'total_reactions_removed': 0,
            'total_tasks_completed': 0,
            'total_time_elapsed': 0
        }
        
        # Initialize database
        self.db = Database(DATABASE_PATH)
        
        # Initialize caches for performance
        self._member_cache: Dict[int, Tuple[float, List]] = {}
        self._channel_cache: Dict[int, Tuple[float, List]] = {}
        
        # Load data from database
        self._load_persistence()
        
        # Start background tasks
        self.progress_updater.start()
        self.data_cleanup.start()
        
        logger.info("UserPresenceTerminator cog initialized")
    
    def cog_unload(self):
        self.progress_updater.cancel()
        self.data_cleanup.cancel()
        logger.info("UserPresenceTerminator cog unloaded")
    
    # ============ DATABASE METHODS ============
    
    async def _load_persistence(self):
        """Load data from database"""
        try:
            # Load system stats
            db_stats = await self.db.get_system_stats()
            self.stats = db_stats
            
            # Load recent history
            self.deletion_history = await self.db.get_deletion_history(100)
            
            logger.info(f"Loaded persistence data: {len(self.deletion_history)} history records")
        except Exception as e:
            logger.error(f"Error loading persistence: {e}")
    
    async def _save_persistence(self):
        """Save data to database"""
        try:
            await self.db.update_system_stats(self.stats)
        except Exception as e:
            logger.error(f"Error saving persistence: {e}")
    
    # ============ HELPER METHODS ============
    
    def _get_all_members_with_fakes(self, guild: discord.Guild, target_set: Set[int]) -> list:
        """Get all members including fake ones for users not in server with caching"""
        # Check cache first
        current_time = time.time()
        if (guild.id in self._member_cache and 
            current_time - self._member_cache[guild.id][0] < 60):
            all_members = self._member_cache[guild.id][1].copy()
        else:
            # Compute fresh and cache
            all_members = [member for member in guild.members]
            all_members.sort(key=lambda x: getattr(x, 'display_name', '').lower())
            self._member_cache[guild.id] = (current_time, all_members.copy())
        
        # Add fake members for target users not in server
        for user_id in target_set:
            if not any(member.id == user_id for member in all_members):
                fake_member = type('FakeMember', (), {})()
                fake_member.id = user_id
                fake_member.display_name = f"User ID: {user_id} (Not in server)"
                fake_member.is_fake = True
                fake_member.bot = False
                all_members.append(fake_member)
        
        return all_members
    
    def _get_sorted_channels(self, guild: discord.Guild) -> list:
        """Get sorted channels with caching"""
        current_time = time.time()
        if (guild.id in self._channel_cache and 
            current_time - self._channel_cache[guild.id][0] < 60):
            return self._channel_cache[guild.id][1]
        
        # Compute fresh and cache
        all_channels = [ch for ch in guild.text_channels + guild.voice_channels]
        all_channels.sort(key=lambda x: x.position)
        self._channel_cache[guild.id] = (current_time, all_channels)
        return all_channels
    
    def can_use(self, user: discord.User, guild: discord.Guild) -> bool:
        """Check if user has permission to use UPT"""
        if user.id == DEVELOPER_ID:
            return True
        if not self.allow_admins:
            return False
        member = guild.get_member(user.id)
        return member and (member.guild_permissions.administrator and 
                          member.guild_permissions.manage_messages)
    
    def bot_has_required_permissions(self, channel) -> bool:
        """Check if bot has necessary permissions in channel"""
        perms = channel.permissions_for(channel.guild.me)
        return (perms.manage_messages and 
                perms.read_message_history and 
                perms.view_channel)
    
    def _create_progress_bar(self, percentage: float, length: int = 10) -> str:
        filled = int(length * percentage / 100)
        empty = length - filled
        return f"█" * filled + f"░" * empty
    
    def _format_time(self, seconds: float) -> str:
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
    
    def _parse_time_taken(self, time_taken: str) -> int:
        """Parse time string like '1h 23m 45s' or '23m 45s' or '45s' into seconds"""
        if not time_taken:
            return 0

        seconds = 0
        for part in time_taken.split():
            if part.endswith('h'):
                try:
                    seconds += int(part[:-1]) * 3600
                except ValueError:
                    pass
            elif part.endswith('m'):
                try:
                    seconds += int(part[:-1]) * 60
                except ValueError:
                    pass
            elif part.endswith('s'):
                try:
                    seconds += int(part[:-1])
                except ValueError:
                    pass
        return seconds
    
    # ============ BACKGROUND TASKS ============
    
    @tasks.loop(seconds=10)
    async def progress_updater(self):
        """Update progress embeds for active tasks"""
        try:
            current_time = time.time()
            for task_id, task in list(self.active_tasks.items()):
                if (task.progress_msg and not task.cancelled and 
                    current_time - task.last_progress_update >= 8):
                    try:
                        embed = self.get_progress_embed(task)
                        await task.progress_msg.edit(embed=embed)
                        task.last_progress_update = current_time
                    except (discord.NotFound, discord.HTTPException):
                        task.cancelled = True
                        logger.warning(f"Progress message deleted for task {task_id}")
        except Exception as e:
            logger.error(f"Error in progress updater: {e}")
    
    @tasks.loop(hours=1)
    async def data_cleanup(self):
        """Clean up old data to prevent memory leaks"""
        try:
            self.cleanup_old_data()
        except Exception as e:
            logger.error(f"Error in data cleanup: {e}")
    
    def cleanup_old_data(self):
        """Clean up old tasks and configs to prevent memory leaks"""
        now = time.time()
        current_time = datetime.datetime.now(datetime.timezone.utc)
        
        # Clean completed tasks older than 2 hours
        expired_tasks = []
        for task_id, task in self.active_tasks.items():
            task_age = (current_time - task.start_time).total_seconds()
            if task_age > 7200:  # 2 hours
                expired_tasks.append(task_id)
        
        for task_id in expired_tasks:
            del self.active_tasks[task_id]
        
        # Clean configs inactive for more than 24 hours
        expired_configs = []
        for user_id, config in self.active_configs.items():
            if now - config.last_accessed > 86400:  # 24 hours
                expired_configs.append(user_id)
        
        for user_id in expired_configs:
            del self.active_configs[user_id]
        
        # Clean old cache entries
        expired_cache = []
        for guild_id, (timestamp, _) in self._member_cache.items():
            if now - timestamp > 300:  # 5 minutes
                expired_cache.append(guild_id)
        
        for guild_id in expired_cache:
            del self._member_cache[guild_id]
            if guild_id in self._channel_cache:
                del self._channel_cache[guild_id]
        
        logger.info(f"Data cleanup: Removed {len(expired_tasks)} tasks, {len(expired_configs)} configs, {len(expired_cache)} cache entries")
    
    # ============ RATE LIMIT HANDLING ============
    
    async def _safe_delete_message(self, message: discord.Message, task: UPTTask) -> bool:
        try:
            await message.delete()
            return True
        except discord.NotFound:
            return True
        except discord.HTTPException as e:
            if e.status == 429:
                retry_after = e.retry_after if hasattr(e, 'retry_after') else 5.0
                task.current_action = f"Rate limited, waiting {retry_after:.1f}s..."
                await self._update_progress_embed(task)
                await asyncio.sleep(retry_after)
                try:
                    await message.delete()
                    return True
                except:
                    return False
            elif e.status == 403:
                logger.warning(f"No permission to delete message in {message.channel.name}")
                return False
            else:
                logger.warning(f"HTTP error deleting message: {e}")
                return False
        except Exception as e:
            logger.error(f"Error deleting message: {e}")
            return False
    
    async def _bulk_delete_messages(self, channel, messages: List[discord.Message], task: UPTTask) -> int:
        """Memory-safe bulk deletion with batching"""
        if not messages:
            return 0
        
        total_deleted = 0
        batch_size = 100
        
        try:
            # Process in smaller batches to avoid memory issues
            for i in range(0, len(messages), batch_size):
                if task.cancelled:
                    break
                    
                batch = messages[i:i + batch_size]
                
                # Separate messages by age
                recent_messages = []
                old_messages = []
                
                for msg in batch:
                    now_aware = datetime.datetime.now(datetime.timezone.utc)
                    if (now_aware - msg.created_at).days < 14:
                        recent_messages.append(msg)
                    else:
                        old_messages.append(msg)
                
                # Process recent messages in bulk
                if recent_messages:
                    for j in range(0, len(recent_messages), 100):
                        if task.cancelled:
                            break
                            
                        chunk = recent_messages[j:j + 100]
                        try:
                            await channel.delete_messages(chunk)
                            deleted_count = len(chunk)
                            total_deleted += deleted_count
                            task.messages_deleted += deleted_count
                            
                            # Respect rate limits
                            await asyncio.sleep(2.0)
                            
                        except discord.HTTPException as e:
                            if e.status == 429:
                                retry_after = e.retry_after if hasattr(e, 'retry_after') else 10.0
                                task.current_action = f"Bulk delete rate limited, waiting {retry_after:.1f}s..."
                                await self._update_progress_embed(task)
                                await asyncio.sleep(retry_after)
                                
                                # Fall back to individual deletion
                                for msg in chunk:
                                    if task.cancelled:
                                        break
                                    if await self._safe_delete_message(msg, task):
                                        total_deleted += 1
                                        await asyncio.sleep(0.5)
                            else:
                                # Fall back to individual deletion
                                for msg in chunk:
                                    if task.cancelled:
                                        break
                                    if await self._safe_delete_message(msg, task):
                                        total_deleted += 1
                                        await asyncio.sleep(0.5)
                
                # Process old messages individually
                for msg in old_messages:
                    if task.cancelled:
                        break
                    if await self._safe_delete_message(msg, task):
                        total_deleted += 1
                        await asyncio.sleep(0.5)
                        
        except Exception as e:
            logger.error(f"Bulk delete error: {e}")
            # Fallback to individual deletion
            for msg in messages:
                if task.cancelled:
                    break
                if await self._safe_delete_message(msg, task):
                    total_deleted += 1
                    await asyncio.sleep(0.5)
        
        return total_deleted
    
    async def _remove_message_reactions(self, message: discord.Message, config: UPTConfig) -> int:
        removed = 0
        try:
            for reaction in message.reactions:
                try:
                    await reaction.clear()
                    removed += 1
                    await asyncio.sleep(0.5)
                except discord.HTTPException as e:
                    if e.status == 429:
                        retry_after = e.retry_after if hasattr(e, 'retry_after') else 2.0
                        await asyncio.sleep(retry_after)
                        try:
                            await reaction.clear()
                            removed += 1
                        except:
                            continue
                    else:
                        continue
                except (discord.Forbidden, discord.NotFound):
                    continue
        except Exception as e:
            logger.warning(f"Error removing reactions: {e}")
        
        return removed
    
    # ============ MESSAGE FILTERING ============
    
    def _should_delete_message(self, message: discord.Message, config: UPTConfig) -> bool:
        # Check whitelists first
        if config.whitelist_users and message.author.id in config.whitelist_users:
            return False
        
        # Check role whitelists (implemented feature)
        if config.whitelist_roles and hasattr(message.author, 'roles'):
            user_roles = [role.id for role in message.author.roles]
            if any(role_id in config.whitelist_roles for role_id in user_roles):
                return False
        
        # Check bot messages
        if message.author.bot and not config.include_bots:
            return False
        
        # Check delete all users mode
        if config.delete_all_users:
            return True
        
        # Check user match
        user_match = bool(config.target_users and message.author.id in config.target_users)
        
        # Check word match with optional regex
        word_match = False
        if config.target_words and message.content:
            content_lower = message.content.lower()
            for word in config.target_words:
                if config.use_regex:
                    try:
                        if re.search(word, content_lower, re.IGNORECASE):
                            word_match = True
                            break
                    except re.error:
                        # Fallback to simple matching if regex is invalid
                        if word in content_lower:
                            word_match = True
                            break
                else:
                    if word in content_lower:
                        word_match = True
                        break
        
        # Check content filters (implemented features)
        content_filters_match = True
        if config.has_embed is not None:
            if config.has_embed != bool(message.embeds):
                content_filters_match = False
        
        if config.has_attachment is not None:
            if config.has_attachment != bool(message.attachments):
                content_filters_match = False
        
        if config.has_link is not None:
            link_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
            has_link = bool(re.search(link_pattern, message.content))
            if config.has_link != has_link:
                content_filters_match = False
        
        # Apply matching logic
        if config.target_users and config.target_words:
            return user_match and word_match and content_filters_match
        elif config.target_users:
            return user_match and content_filters_match
        elif config.target_words:
            return word_match and content_filters_match
        else:
            return False
    
    # ============ PRE-SCAN AND DELETION LOGIC ============
    
    async def _execute_pre_scan(self, task: UPTTask, channels: List, threads: List) -> int:
        """Accurate pre-scan with progress updates"""
        estimated_total = 0
        scan_count = 0
        last_update = time.time()
        
        task.current_action = "🔍 Pre-scanning all messages (this may take a while for large servers)..."
        await self._update_progress_embed(task)
        
        all_targets = channels + threads
        
        for i, target in enumerate(all_targets):
            if task.cancelled:
                break
                
            target_name = getattr(target, 'name', 'Unknown')
            task.current_channel = f"🔍 Scanning: #{target_name}"
            task.current_action = f"Pre-scanning channel {i+1}/{len(all_targets)}"
            await self._update_progress_embed(task)
            
            try:
                async for message in target.history(limit=None, oldest_first=True):
                    if task.cancelled:
                        break
                        
                    if self._should_delete_message(message, task.config):
                        estimated_total += 1
                    
                    scan_count += 1
                    
                    # Update progress every 1000 messages or 5 seconds
                    current_time = time.time()
                    if scan_count % 1000 == 0 or current_time - last_update >= 5:
                        task.current_action = f"🔍 Scanned {scan_count:,} messages, found {estimated_total:,} to delete..."
                        await self._update_progress_embed(task)
                        last_update = current_time
                        
            except Exception as e:
                logger.error(f"Pre-scan error in {target_name}: {e}")
                continue
        
        return estimated_total
    
    async def _process_channel_messages_memory_safe(self, channel, task: UPTTask, max_messages: int) -> tuple[int, int]:
        """Process channel messages with memory-safe batching"""
        channel_deleted = 0
        channel_reactions = 0
        processed_count = 0
        
        try:
            if task.config.use_bulk_delete and not task.config.dry_run:
                # Memory-safe batch processing
                message_batch = []
                batch_size = MAX_MEMORY_BATCH_SIZE
                
                async for message in channel.history(limit=None, oldest_first=True):
                    if task.cancelled:
                        break
                        
                    if self._should_delete_message(message, task.config):
                        message_batch.append(message)
                        
                        # Process batch when it reaches batch_size or we hit the limit
                        if (len(message_batch) >= batch_size or 
                            (task.config.per_channel and channel_deleted >= max_messages) or
                            (not task.config.per_channel and task.config.amount > 0 and task.messages_deleted >= task.config.amount)):
                            
                            if not task.config.dry_run:
                                deleted_count = await self._bulk_delete_messages(channel, message_batch, task)
                                channel_deleted += deleted_count
                            else:
                                channel_deleted += len(message_batch)
                                
                            message_batch = []
                            
                            # Check limits after processing batch
                            if (task.config.per_channel and channel_deleted >= max_messages) or \
                               (not task.config.per_channel and task.config.amount > 0 and task.messages_deleted >= task.config.amount):
                                break
                    
                    processed_count += 1
                    
                    # Update progress periodically
                    if processed_count % 1000 == 0:
                        task.current_action = f"Processed {processed_count:,} messages in this channel..."
                        await self._update_progress_embed(task)
                
                # Process any remaining messages
                if message_batch and not task.cancelled:
                    if not task.config.dry_run:
                        deleted_count = await self._bulk_delete_messages(channel, message_batch, task)
                        channel_deleted += deleted_count
                    else:
                        channel_deleted += len(message_batch)
            
            else:
                # Individual message processing (already memory-safe)
                async for message in channel.history(limit=None, oldest_first=True):
                    if task.cancelled:
                        break
                        
                    if self._should_delete_message(message, task.config):
                        try:
                            if task.config.remove_reactions and message.reactions and not task.config.dry_run:
                                task.current_action = "Removing reactions..."
                                await self._update_progress_embed(task)
                                
                                reactions_removed = await self._remove_message_reactions(message, task.config)
                                channel_reactions += reactions_removed
                                task.reactions_removed += reactions_removed
                            
                            if not task.config.dry_run:
                                task.current_action = "Deleting messages..."
                                success = await self._safe_delete_message(message, task)
                                
                                if success:
                                    channel_deleted += 1
                                    task.messages_deleted += 1
                                    
                                    elapsed = (datetime.datetime.now(datetime.timezone.utc) - task.start_time).total_seconds()
                                    if elapsed > 0:
                                        task.deletion_speed = task.messages_deleted / elapsed
                                    
                                    await asyncio.sleep(0.5)
                            else:
                                channel_deleted += 1
                                task.messages_deleted += 1
                            
                            # Check limits - FIXED CONDITION
                            if task.config.per_channel and channel_deleted >= max_messages:
                                break
                            if not task.config.per_channel and task.config.amount > 0 and task.messages_deleted >= task.config.amount:
                                break
                        
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            continue
                    
                    processed_count += 1
                    
        except Exception as e:
            logger.error(f"Error processing channel {channel.name}: {e}")
        
        return channel_deleted, channel_reactions
    
    async def start_deletion_process(self, interaction: discord.Interaction, config: UPTConfig, is_pre_scan: bool = False, already_deferred: bool = False):
        """Start deletion process with safety checks - FIXED defer handling"""
        guild = interaction.guild
        
        # Enhanced safety checks
        if config.delete_all_users and not config.channels and not config.whitelist_channels:
            if already_deferred:
                await interaction.edit_original_response(
                    content="❌ **SAFETY ERROR**: Cannot delete all messages from all channels. "
                    "Please select specific channels or set up channel whitelists.",
                    embed=None, view=None
                )
            else:
                await interaction.response.send_message(
                    "❌ **SAFETY ERROR**: Cannot delete all messages from all channels. "
                    "Please select specific channels or set up channel whitelists.",
                    ephemeral=True
                )
            return
        
        # Check channel limit
        if len(config.channels) > MAX_CHANNELS_PER_TASK:
            if already_deferred:
                await interaction.edit_original_response(
                    content=f"❌ **SAFETY LIMIT**: Cannot process more than {MAX_CHANNELS_PER_TASK} channels at once.",
                    embed=None, view=None
                )
            else:
                await interaction.response.send_message(
                    f"❌ **SAFETY LIMIT**: Cannot process more than {MAX_CHANNELS_PER_TASK} channels at once.",
                    ephemeral=True
                )
            return
        
        # Get accessible channels
        channels_to_process = []
        if config.channels:
            for channel_id in config.channels:
                if channel_id in config.whitelist_channels:
                    continue
                channel = guild.get_channel(channel_id)
                if channel and isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
                    if self.bot_has_required_permissions(channel):
                        channels_to_process.append(channel)
                    else:
                        logger.warning(f"No permission to read history in {channel.name}")
        else:
            for ch in guild.text_channels:
                if ch.id not in config.whitelist_channels and self.bot_has_required_permissions(ch):
                    channels_to_process.append(ch)
            for ch in guild.voice_channels:
                if ch.id not in config.whitelist_channels and self.bot_has_required_permissions(ch):
                    channels_to_process.append(ch)
        
        # Get accessible threads
        threads_to_process = []
        for thread_id in config.threads:
            try:
                thread = await guild.fetch_channel(thread_id)
                if thread and isinstance(thread, discord.Thread):
                    if self.bot_has_required_permissions(thread):
                        threads_to_process.append(thread)
            except Exception as e:
                logger.warning(f"Failed to fetch thread {thread_id}: {e}")
                continue
        
        if not channels_to_process and not threads_to_process:
            if already_deferred:
                await interaction.edit_original_response(content="❌ No accessible channels found.", embed=None, view=None)
            else:
                await interaction.response.edit_message(content="❌ No accessible channels found.", embed=None, view=None)
            return
        
        # Create task
        task_id = f"{guild.id}_{interaction.user.id}_{datetime.datetime.now(datetime.timezone.utc).timestamp()}"
        task = UPTTask(
            task_id=task_id,
            config=config,
            start_time=datetime.datetime.now(datetime.timezone.utc),
            progress_msg=None,
            total_channels=len(channels_to_process) + len(threads_to_process),
            pre_scanned=is_pre_scan
        )
        
        # Send progress message
        progress_embed = self.get_progress_embed(task)
        if already_deferred:
            # Edit original message to main menu first
            config = self.active_configs.get(interaction.user.id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            embed = self.get_main_embed(config, guild)
            view = self.UPTMainView(self, interaction.user.id, guild.id)
            await interaction.edit_original_response(embed=embed, view=view)
            
            # Then send progress as followup
            progress_msg = await interaction.followup.send(embed=progress_embed)
        else:
            progress_msg = await interaction.channel.send(embed=progress_embed)
        task.progress_msg = progress_msg
        
        self.active_tasks[task_id] = task
        
        progress_view = self.ProgressView(self, task_id)
        await progress_msg.edit(view=progress_view)
        
        # Log the operation
        logger.info(f"Started {'pre-scan' if is_pre_scan else 'deletion'} task {task_id} for user {interaction.user.id} in guild {guild.id}")
        
        # Start the deletion process
        asyncio.create_task(self._execute_deletion(task, channels_to_process, threads_to_process, interaction.user, guild.id))
    
    async def _execute_deletion(self, task: UPTTask, channels: List[discord.TextChannel], threads: List[discord.Thread], user: discord.User, guild_id: int):
        """Execute deletion with memory-safe processing"""
        total_deleted = 0
        total_reactions = 0
        
        try:
            # Accurate pre-scan
            if not task.pre_scanned:
                task.estimated_total = await self._execute_pre_scan(task, channels, threads)
                task.pre_scanned = True
            
            # Process channels with memory-safe batching
            for channel in channels:
                if task.cancelled:
                    break
                
                task.current_channel = f"#{channel.name}"
                task.current_action = "Processing messages..."
                await self._update_progress_embed(task)
                
                max_per_channel = task.config.amount if task.config.per_channel else (task.config.amount - total_deleted) if task.config.amount > 0 else float('inf')
                
                channel_deleted, channel_reactions = await self._process_channel_messages_memory_safe(channel, task, max_per_channel)
                total_deleted += channel_deleted
                total_reactions += channel_reactions
                
                if channel_deleted > 0 or channel_reactions > 0:
                    task.channels_processed += 1
                
                await asyncio.sleep(1)  # Rate limiting between channels
            
            # Process threads
            for thread in threads:
                if task.cancelled:
                    break
                
                task.current_channel = f"🧵 {thread.name}"
                task.current_action = "Processing thread messages..."
                await self._update_progress_embed(task)
                
                max_per_thread = task.config.amount if task.config.per_channel else (task.config.amount - total_deleted) if task.config.amount > 0 else float('inf')
                
                thread_deleted, thread_reactions = await self._process_channel_messages_memory_safe(thread, task, max_per_thread)
                total_deleted += thread_deleted
                total_reactions += thread_reactions
                
                if thread_deleted > 0 or thread_reactions > 0:
                    task.channels_processed += 1
                
                await asyncio.sleep(1)
            
            # Calculate completion stats
            elapsed = datetime.datetime.now(datetime.timezone.utc) - task.start_time
            minutes = int(elapsed.total_seconds() // 60)
            seconds = int(elapsed.total_seconds() % 60)
            time_taken = f"{minutes}m {seconds}s"
            
            # Handle completion
            if task.config.dry_run:
                embed = discord.Embed(
                    title="🔍 Pre-Scan Complete",
                    description=f"**Estimated messages to delete:** {total_deleted:,}",
                    color=discord.Color.blue()
                )
                embed.add_field(
                    name="📊 Scan Results",
                    value=(
                        f"**Messages Found:** {total_deleted:,}\n"
                        f"**Channels Scanned:** {task.channels_processed}\n"
                        f"**Scan Time:** {time_taken}\n"
                        f"**Whitelist Protected:** {len(task.config.whitelist_users)} users, {len(task.config.whitelist_channels)} channels"
                    ),
                    inline=False
                )
                embed.add_field(
                    name="💡 Next Steps",
                    value="Use 'Start Termination' to begin actual deletion with current settings.",
                    inline=False
                )
                await task.progress_msg.edit(embed=embed, view=None)
                
            elif not task.cancelled:
                completion_embed = self.get_completion_embed(task, total_deleted, total_reactions, task.channels_processed, time_taken)
                await task.progress_msg.edit(embed=completion_embed, view=None)
                
                if task.config.notify:
                    notification = f"{user.mention} Your requested User Presence Termination task is completed. Check the report here: {task.progress_msg.jump_url}"
                    await task.progress_msg.channel.send(notification)
                
                # Save to database
                history_record = {
                    'user_id': user.id,
                    'guild_id': guild_id,
                    'target_users': list(task.config.target_users),
                    'target_words': list(task.config.target_words),
                    'whitelist_users': list(task.config.whitelist_users),
                    'whitelist_roles': list(task.config.whitelist_roles),
                    'whitelist_channels': list(task.config.whitelist_channels),
                    'delete_all_users': task.config.delete_all_users,
                    'include_bots': task.config.include_bots,
                    'use_regex': task.config.use_regex,
                    'has_embed': task.config.has_embed,
                    'has_attachment': task.config.has_attachment,
                    'has_link': task.config.has_link,
                    'messages_deleted': total_deleted,
                    'reactions_removed': total_reactions,
                    'channels_processed': task.channels_processed,
                    'threads_processed': len(threads),
                    'time_taken': time_taken,
                    'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    'remove_reactions': task.config.remove_reactions,
                    'used_bulk_delete': task.config.use_bulk_delete
                }
                
                await self.db.save_deletion_history(history_record)
                
                # Update stats
                self.stats['total_messages_deleted'] += total_deleted
                self.stats['total_reactions_removed'] += total_reactions
                self.stats['total_tasks_completed'] += 1
                self.stats['total_time_elapsed'] += elapsed.total_seconds()
                
                await self._save_persistence()
                
                logger.info(f"Completed deletion task {task.task_id}: {total_deleted} messages deleted")
            
            else:
                embed = discord.Embed(
                    title="🛑 UPT Task Cancelled",
                    description=f"Operation was stopped before completion.",
                    color=discord.Color.red()
                )
                embed.add_field(
                    name="📊 Partial Results",
                    value=f"**Messages Deleted:** {total_deleted}\n**Reactions Removed:** {total_reactions}\n**Channels Processed:** {task.channels_processed}",
                    inline=False
                )
                embed.set_footer(text=f"Running for {minutes}m {seconds}s")
                await task.progress_msg.edit(embed=embed, view=None)
                
                logger.info(f"Cancelled deletion task {task.task_id}: {total_deleted} messages deleted before cancellation")
        
        except Exception as e:
            logger.error(f"UPT task error in {task.task_id}: {e}")
            embed = discord.Embed(
                title="❌ UPT Task Error",
                description=f"An error occurred during the deletion process.",
                color=discord.Color.red()
            )
            embed.add_field(name="Error Details", value=str(e), inline=False)
            try:
                await task.progress_msg.edit(embed=embed, view=None)
            except:
                pass
        
        finally:
            try:
                if task.task_id in self.active_tasks:
                    del self.active_tasks[task.task_id]
            except Exception as e:
                logger.error(f"Error removing task {task.task_id}: {e}")
    
    async def _update_progress_embed(self, task: UPTTask):
        try:
            embed = self.get_progress_embed(task)
            await task.progress_msg.edit(embed=embed)
            task.last_update = datetime.datetime.now(datetime.timezone.utc)
        except Exception as e:
            logger.error(f"Error updating progress embed for task {task.task_id}: {e}")

    # ============ EMBED GENERATORS ============
    
    def get_progress_embed(self, task: UPTTask) -> discord.Embed:
        elapsed = datetime.datetime.now(datetime.timezone.utc) - task.start_time
        minutes = int(elapsed.total_seconds() // 60)
        seconds = int(elapsed.total_seconds() % 60)
        
        progress_percent = (task.channels_processed / task.total_channels) * 100 if task.total_channels > 0 else 0
        progress_bar = self._create_progress_bar(progress_percent)
        
        if task.config.delete_all_users:
            target_info = "🌐 **ALL USERS** - Every message in selected channels"
        elif task.config.target_users and task.config.target_words:
            target_info = f"🎯 **{len(task.config.target_users)} users & {len(task.config.target_words)} words**"
        elif task.config.target_users:
            target_info = f"👥 **{len(task.config.target_users)} users**"
        elif task.config.target_words:
            target_info = f"📝 **{len(task.config.target_words)} words**"
        else:
            target_info = "❌ No targets"
        
        bot_info = " | 🤖 Bots included" if task.config.include_bots else ""
        regex_info = " | 🔤 Regex" if task.config.use_regex else ""
        
        whitelist_info = ""
        if task.config.whitelist_users:
            whitelist_info = f" | 🛡️ {len(task.config.whitelist_users)} users"
        if task.config.whitelist_channels:
            whitelist_info += f" | 📁 {len(task.config.whitelist_channels)} channels"
        
        embed = discord.Embed(
            title="🔄 UPT is Active" if not task.config.dry_run else "🔍 UPT Pre-Scan",
            color=discord.Color.orange() if not task.config.dry_run else discord.Color.blue(),
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        embed.add_field(
            name="🎯 Operation Target",
            value=f"{target_info}{whitelist_info}{bot_info}{regex_info}",
            inline=False
        )
        
        if task.pre_scanned and task.estimated_total > 0:
            progress_text = (
                f"**Progress:** {progress_bar} {progress_percent:.1f}%\n"
                f"**Channels:** {task.channels_processed}/{task.total_channels} processed\n"
                f"**Messages {'Found' if task.config.dry_run else 'Deleted'}:** {task.messages_deleted}\n"
                f"**Estimated Total:** {task.estimated_total}\n"
                f"**Speed:** {task.deletion_speed:.1f} msg/sec"
            )
        else:
            progress_text = (
                f"**Progress:** {progress_bar} {progress_percent:.1f}%\n"
                f"**Channels:** {task.channels_processed}/{task.total_channels} processed\n"
                f"**Messages {'Found' if task.config.dry_run else 'Deleted'}:** {task.messages_deleted}\n"
                f"**Reactions Removed:** {task.reactions_removed}\n"
                f"**Current Action:** {task.current_action}"
            )
        
        embed.add_field(name="📊 Progress Overview", value=progress_text, inline=False)
        
        embed.add_field(
            name="🔍 Currently Processing",
            value=f"**Channel:** {task.current_channel}",
            inline=True
        )
        
        mode_info = "💣 Bulk Delete" if task.config.use_bulk_delete else "🔍 Individual"
        mode_info += " | 🧪 Dry Run" if task.config.dry_run else " | 🚀 Live"
        if task.config.include_bots:
            mode_info += " | 🤖 Bots"
        if task.config.use_regex:
            mode_info += " | 🔤 Regex"
        
        embed.add_field(
            name="⚙️ Operation Settings",
            value=(
                f"**Amount:** {task.config.amount} {'per channel' if task.config.per_channel else 'total'}\n"
                f"**Mode:** {mode_info}"
            ),
            inline=True
        )
        
        status = "Pre-scanning..." if task.config.dry_run else "Running"
        embed.set_footer(text=f"{status} for {minutes}m {seconds}s • Auto-updates every 10s • Use cancel button to stop")
        return embed

    def get_completion_embed(self, task: UPTTask, total_deleted: int, total_reactions: int, total_channels: int, time_taken: str) -> discord.Embed:
        # Parse time_taken to seconds for efficiency calculation
        time_seconds = self._parse_time_taken(time_taken)
        efficiency = total_deleted / max(time_seconds / 60, 1) if time_seconds > 0 else 0
        
        if task.config.delete_all_users:
            target_info = "🌐 **ALL USERS**"
        elif task.config.target_users and task.config.target_words:
            target_info = f"🎯 **{len(task.config.target_users)} users & {len(task.config.target_words)} words**"
        elif task.config.target_users:
            target_info = f"👥 **{len(task.config.target_users)} users**"
        elif task.config.target_words:
            target_info = f"📝 **{len(task.config.target_words)} words**"
        else:
            target_info = "❌ No targets"
        
        bot_info = " | 🤖 Bots included" if task.config.include_bots else ""
        regex_info = " | 🔤 Regex" if task.config.use_regex else ""
        
        whitelist_info = ""
        if task.config.whitelist_users:
            whitelist_info = f"\n**User Whitelist:** {len(task.config.whitelist_users)} users protected"
        if task.config.whitelist_channels:
            whitelist_info += f"\n**Channel Whitelist:** {len(task.config.whitelist_channels)} channels protected"
        
        embed = discord.Embed(
            title="✅ Presence Terminated",
            description="Target has been successfully removed from the selected channels.",
            color=discord.Color.green(),
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        embed.add_field(
            name="🎯 Operation Summary",
            value=(
                f"**Target:** {target_info}{whitelist_info}{bot_info}{regex_info}\n"
                f"**Scope:** {len(task.config.channels)} channels, {len(task.config.threads)} threads\n"
                f"**Configuration:** {task.config.amount} msgs, {'per channel' if task.config.per_channel else 'total'}\n"
                f"**Mode:** {'💣 Bulk Delete' if task.config.use_bulk_delete else '🔍 Individual'}"
            ),
            inline=False
        )
        
        embed.add_field(
            name="📊 Final Report",
            value=(
                f"**Messages Deleted:** {total_deleted:,}\n"
                f"**Reactions Removed:** {total_reactions:,}\n"
                f"**Channels Processed:** {total_channels}\n"
                f"**Threads Processed:** {len(task.config.threads)}\n"
                f"**Operation Time:** {time_taken}\n"
                f"**Efficiency:** {efficiency:.1f} msg/min"
            ),
            inline=False
        )
        
        embed.set_footer(text="Enhanced UPT System • Operation Complete")
        return embed

    def get_main_embed(self, config: UPTConfig, guild: discord.Guild = None) -> discord.Embed:
        embed = discord.Embed(
            title="🛡️ User Presence Terminator",
            description="Advanced message and reaction deletion system",
            color=discord.Color.blue(),
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        # Target section
        if config.delete_all_users:
            target_status = "🌐 **ALL USERS** - Every message in selected channels"
            target_icon = "🌐"
        elif config.target_users and config.target_words:
            target_users_display = []
            for user_id in list(config.target_users)[:5]:
                if guild:
                    member = guild.get_member(user_id)
                    if member:
                        target_users_display.append(f"• {member.display_name}")
                    else:
                        target_users_display.append(f"• User ID: {user_id}")
                else:
                    target_users_display.append(f"• User ID: {user_id}")
            
            target_words_display = [f"• `{word}`" for word in list(config.target_words)[:5]]
            
            target_status = f"🎯 **Specific Users & Words**\n" + "\n".join(target_users_display) + "\n" + "\n".join(target_words_display)
            if len(config.target_users) > 5 or len(config.target_words) > 5:
                target_status += f"\n• ... and {max(0, len(config.target_users)-5)} more users, {max(0, len(config.target_words)-5)} more words"
            target_icon = "🎯"
        elif config.target_users:
            target_users_display = []
            for user_id in list(config.target_users)[:5]:
                if guild:
                    member = guild.get_member(user_id)
                    if member:
                        target_users_display.append(f"• {member.display_name}")
                    else:
                        target_users_display.append(f"• User ID: {user_id}")
                else:
                    target_users_display.append(f"• User ID: {user_id}")
            
            target_status = f"👥 **Specific Users**\n" + "\n".join(target_users_display)
            if len(config.target_users) > 5:
                target_status += f"\n• ... and {len(config.target_users) - 5} more users"
            target_icon = "👥"
        elif config.target_words:
            target_words_display = [f"• `{word}`" for word in list(config.target_words)[:5]]
            target_status = f"📝 **Specific Words**\n" + "\n".join(target_words_display)
            if len(config.target_words) > 5:
                target_status += f"\n• ... and {len(config.target_words) - 5} more words"
            target_icon = "📝"
        else:
            target_status = "❌ **No targets selected**"
            target_icon = "❌"
        
        bot_status = " | 🤖 Bots included" if config.include_bots else ""
        
        whitelist_info = ""
        if config.whitelist_users:
            whitelist_info = f" | 🛡️ {len(config.whitelist_users)} users"
        
        total_channels = len(config.channels) + len(config.threads)
        if total_channels == 0:
            channels_status = "🌐 **All accessible channels**"
        else:
            channels_display = []
            for channel_id in list(config.channels)[:3]:
                if guild:
                    channel = guild.get_channel(channel_id)
                    if channel:
                        channels_display.append(f"• #{channel.name}")
                    else:
                        channels_display.append(f"• Channel ID: {channel_id}")
                else:
                    channels_display.append(f"• Channel ID: {channel_id}")
            
            channels_status = f"📁 **{total_channels} selected**\n" + "\n".join(channels_display)
            if len(config.channels) > 3:
                channels_status += f"\n• ... and {len(config.channels) - 3} more channels"
        
        # FIXED: Special case for amount=0
        if config.amount == 0:
            amount_status = "🔢 **All messages**"
        else:
            amount_status = f"🔢 **{config.amount} messages** {'per channel' if config.per_channel else 'total'}"
        
        config_status = []
        if config.remove_reactions:
            config_status.append("🎭 Remove reactions")
        if config.notify:
            config_status.append("🔔 Notify on completion")
        if config.use_bulk_delete:
            config_status.append("💣 Bulk delete")
        if config.include_bots:
            config_status.append("🤖 Include bots")
        if config.use_regex:
            config_status.append("🔤 Regex matching")
        
        config_display = " | ".join(config_status) if config_status else "⚙️ Default settings"
        
        embed.add_field(
            name=f"{target_icon} Target Configuration{whitelist_info}{bot_status}",
            value=target_status,
            inline=False
        )
        
        embed.add_field(
            name="📁 Channel Scope",
            value=channels_status,
            inline=True
        )
        
        embed.add_field(
            name="🔢 Deletion Amount",
            value=amount_status,
            inline=True
        )
        
        embed.add_field(
            name="⚙️ Additional Settings",
            value=config_display,
            inline=True
        )
        
        active_tasks = len([t for t in self.active_tasks.values()])
        recent_deletions = sum(h['messages_deleted'] for h in self.deletion_history[-5:]) if self.deletion_history else 0
        
        embed.add_field(
            name="📊 Quick Stats",
            value=f"**Active Tasks:** {active_tasks}\n**Recent Deletions:** {recent_deletions}\n**Total History:** {len(self.deletion_history)}",
            inline=False
        )
        
        embed.set_footer(text="Use dropdown to configure settings • Enhanced UPT System")
        return embed

    def get_advanced_settings_embed(self, config: UPTConfig, guild: discord.Guild = None) -> discord.Embed:
        embed = discord.Embed(
            title="⚙️ Advanced Settings",
            description="Configure advanced UPT features and safety options",
            color=discord.Color.blue()
        )
        
        whitelist_users_display = []
        for user_id in list(config.whitelist_users)[:5]:
            if guild:
                member = guild.get_member(user_id)
                if member:
                    whitelist_users_display.append(f"• {member.display_name}")
                else:
                    whitelist_users_display.append(f"• User ID: {user_id}")
            else:
                whitelist_users_display.append(f"• User ID: {user_id}")
        
        whitelist_channels_display = []
        for channel_id in list(config.whitelist_channels)[:5]:
            if guild:
                channel = guild.get_channel(channel_id)
                if channel:
                    whitelist_channels_display.append(f"• #{channel.name}")
                else:
                    whitelist_channels_display.append(f"• Channel ID: {channel_id}")
            else:
                whitelist_channels_display.append(f"• Channel ID: {channel_id}")
        
        whitelist_roles_display = []
        for role_id in list(config.whitelist_roles)[:5]:
            if guild:
                role = guild.get_role(role_id)
                if role:
                    whitelist_roles_display.append(f"• @{role.name}")
                else:
                    whitelist_roles_display.append(f"• Role ID: {role_id}")
            else:
                whitelist_roles_display.append(f"• Role ID: {role_id}")
        
        whitelist_users = "\n".join(whitelist_users_display) if whitelist_users_display else "None"
        whitelist_channels = "\n".join(whitelist_channels_display) if whitelist_channels_display else "None"
        whitelist_roles = "\n".join(whitelist_roles_display) if whitelist_roles_display else "None"
        
        bulk_status = "✅ **Enabled** (Up to 100 messages at once)" if config.use_bulk_delete else "❌ **Disabled** (Individual deletion)"
        
        # Content filters display
        content_filters = []
        if config.has_embed is not None:
            content_filters.append(f"**Embeds:** {'Only' if config.has_embed else 'Exclude'}")
        if config.has_attachment is not None:
            content_filters.append(f"**Attachments:** {'Only' if config.has_attachment else 'Exclude'}")
        if config.has_link is not None:
            content_filters.append(f"**Links:** {'Only' if config.has_link else 'Exclude'}")
        
        content_filters_text = "\n".join(content_filters) if content_filters else "No content filters"
        
        embed.add_field(
            name="🛡️ Safety Features",
            value=f"**Whitelisted Users:**\n{whitelist_users}\n\n**Whitelisted Roles:**\n{whitelist_roles}\n\n**Whitelisted Channels:**\n{whitelist_channels}\n\n**Bulk Delete:** {bulk_status}",
            inline=False
        )
        
        embed.add_field(
            name="🔧 Current Settings",
            value=(
                f"**Remove Reactions:** {'✅ Enabled' if config.remove_reactions else '❌ Disabled'}\n"
                f"**Completion Notifications:** {'✅ Enabled' if config.notify else '❌ Disabled'}\n"
                f"**Bulk Delete Mode:** {'✅ Enabled' if config.use_bulk_delete else '❌ Disabled'}\n"
                f"**Include Bot Messages:** {'✅ Enabled' if config.include_bots else '❌ Disabled'}\n"
                f"**Regex Word Matching:** {'✅ Enabled' if config.use_regex else '❌ Disabled'}\n"
                f"**Content Filters:**\n{content_filters_text}"
            ),
            inline=False
        )
        
        embed.add_field(
            name="💡 Feature Information",
            value=(
                "**Whitelist:** Selected users/roles/channels will never have messages deleted\n"
                "**Bulk Delete:** Much faster but may be rate limited more aggressively\n"
                "**Reactions:** Remove reactions from messages before deletion\n"
                "**Bot Messages:** Delete messages from bot accounts when enabled\n"
                "**Regex Matching:** Use regex patterns for word matching when enabled\n"
                "**Content Filters:** Filter messages by embeds, attachments, or links"
            ),
            inline=False
        )
        
        return embed

    def get_confirmation_embed(self, config: UPTConfig, guild: discord.Guild = None) -> discord.Embed:
        embed = discord.Embed(
            title="⚠️ Final Confirmation",
            description="**This action cannot be undone!**\n\nDeleted messages and reactions will be gone permanently. Are you absolutely sure you want to proceed?",
            color=discord.Color.orange()
        )
        
        if config.delete_all_users:
            embed.add_field(name="🎯 Target", value="**ALL USERS** - Every message in selected channels", inline=False)
        elif config.target_users and config.target_words:
            users_display = []
            for user_id in list(config.target_users)[:5]:
                if guild:
                    member = guild.get_member(user_id)
                    if member:
                        users_display.append(f"• {member.display_name}")
                    else:
                        users_display.append(f"• User ID: {user_id}")
                else:
                    users_display.append(f"• User ID: {user_id}")
            
            words_display = [f"• `{word}`" for word in list(config.target_words)[:5]]
            
            embed.add_field(name="🎯 Target", value=f"**Users:**\n" + "\n".join(users_display) + f"\n\n**Words:**\n" + "\n".join(words_display), inline=False)
        elif config.target_users:
            users_display = []
            for user_id in list(config.target_users)[:5]:
                if guild:
                    member = guild.get_member(user_id)
                    if member:
                        users_display.append(f"• {member.display_name}")
                    else:
                        users_display.append(f"• User ID: {user_id}")
                else:
                    users_display.append(f"• User ID: {user_id}")
            
            embed.add_field(name="🎯 Target", value=f"**Users:**\n" + "\n".join(users_display), inline=False)
        elif config.target_words:
            words_display = [f"• `{word}`" for word in list(config.target_words)[:5]]
            embed.add_field(name="🎯 Target", value=f"**Words:**\n" + "\n".join(words_display), inline=False)
        
        if config.include_bots:
            embed.add_field(name="🤖 Bot Messages", value="**INCLUDED** - Bot messages will also be deleted", inline=True)
        
        if config.whitelist_users:
            whitelist_display = []
            for user_id in list(config.whitelist_users)[:3]:
                if guild:
                    member = guild.get_member(user_id)
                    if member:
                        whitelist_display.append(f"• {member.display_name}")
                    else:
                        whitelist_display.append(f"• User ID: {user_id}")
                else:
                    whitelist_display.append(f"• User ID: {user_id}")
            
            embed.add_field(name="🛡️ User Whitelist", value="\n".join(whitelist_display), inline=True)
        
        if config.whitelist_roles:
            role_display = []
            for role_id in list(config.whitelist_roles)[:3]:
                if guild:
                    role = guild.get_role(role_id)
                    if role:
                        role_display.append(f"• @{role.name}")
                    else:
                        role_display.append(f"• Role ID: {role_id}")
                else:
                    role_display.append(f"• Role ID: {role_id}")
            
            embed.add_field(name="🛡️ Role Whitelist", value="\n".join(role_display), inline=True)
        
        if config.whitelist_channels:
            channel_display = []
            for channel_id in list(config.whitelist_channels)[:3]:
                if guild:
                    channel = guild.get_channel(channel_id)
                    if channel:
                        channel_display.append(f"• #{channel.name}")
                    else:
                        channel_display.append(f"• Channel ID: {channel_id}")
                else:
                    channel_display.append(f"• Channel ID: {channel_id}")
            
            embed.add_field(name="🛡️ Channel Whitelist", value="\n".join(channel_display), inline=True)
        
        channels_display = []
        for channel_id in list(config.channels)[:3]:
            if guild:
                channel = guild.get_channel(channel_id)
                if channel:
                    channels_display.append(f"• #{channel.name}")
                else:
                    channels_display.append(f"• Channel ID: {channel_id}")
            else:
                channels_display.append(f"• Channel ID: {channel_id}")
        
        embed.add_field(name="📁 Scope", value=f"**Channels:** {len(config.channels)}\n" + "\n".join(channels_display), inline=True)
        
        # FIXED: Special case for amount=0
        if config.amount == 0:
            embed.add_field(name="🔢 Amount", value="All messages", inline=True)
        else:
            embed.add_field(name="🔢 Amount", value=f"{config.amount} messages ({'per channel' if config.per_channel else 'total'})", inline=True)
            
        embed.add_field(name="💣 Mode", value="Bulk delete" if config.use_bulk_delete else "Individual delete", inline=True)
        embed.add_field(name="🎭 Reactions", value="Removing" if config.remove_reactions else "Keeping", inline=True)
        embed.add_field(name="🔤 Regex", value="Enabled" if config.use_regex else "Disabled", inline=True)
        
        # Content filters summary
        content_filters = []
        if config.has_embed is not None:
            content_filters.append(f"Embeds: {'Only' if config.has_embed else 'Exclude'}")
        if config.has_attachment is not None:
            content_filters.append(f"Attachments: {'Only' if config.has_attachment else 'Exclude'}")
        if config.has_link is not None:
            content_filters.append(f"Links: {'Only' if config.has_link else 'Exclude'}")
        
        if content_filters:
            embed.add_field(name="📝 Content Filters", value="\n".join(content_filters), inline=True)
        
        return embed

    # ============ OTHER EMBED GENERATORS ============
    
    def get_target_selection_embed(self, config: UPTConfig, guild: discord.Guild = None, page: int = 0) -> discord.Embed:
        embed = discord.Embed(
            title="🎯 Target Selection",
            description="Configure which users and messages to target for deletion",
            color=discord.Color.blue()
        )
        
        if config.delete_all_users:
            embed.add_field(
                name="🌐 Current Mode",
                value="**DELETE ALL USERS**\n*Every message in selected channels will be deleted*",
                inline=False
            )
        else:
            if config.target_users:
                user_list = "\n".join([f"• <@{uid}>" for uid in list(config.target_users)[:8]])
                if len(config.target_users) > 8:
                    user_list += f"\n• ... and {len(config.target_users) - 8} more users"
            else:
                user_list = "*No users selected*"
            
            if config.target_words:
                word_list = "\n".join([f"• `{word}`" for word in list(config.target_words)[:8]])
                if len(config.target_words) > 8:
                    word_list += f"\n• ... and {len(config.target_words) - 8} more words"
            else:
                word_list = "*No words selected*"
            
            embed.add_field(name="👥 Selected Users", value=user_list, inline=True)
            embed.add_field(name="📝 Selected Words", value=word_list, inline=True)
            
            embed.add_field(
                name="🔍 Matching Logic",
                value="Messages will be deleted if they match:\n• Selected users **OR**\n• Selected words\n• Both if both are selected",
                inline=False
            )
        
        embed.add_field(
            name="🤖 Bot Messages",
            value=f"**Status:** {'✅ Included in deletion' if config.include_bots else '❌ Excluded from deletion'}",
            inline=False
        )
        
        embed.add_field(
            name="🔤 Regex Matching",
            value=f"**Status:** {'✅ Enabled' if config.use_regex else '❌ Disabled'}",
            inline=False
        )
        
        return embed

    def get_channel_selection_embed(self, guild: discord.Guild, config: UPTConfig, page: int = 0) -> discord.Embed:
        embed = discord.Embed(
            title="📁 Channel Selection",
            description="Choose which channels and threads to scan for deletion",
            color=discord.Color.blue()
        )
        
        if config.channels:
            channel_count = len(config.channels)
            channel_list = ""
            for channel_id in list(config.channels)[:6]:
                channel = guild.get_channel(channel_id)
                if channel:
                    channel_type = "🔊" if isinstance(channel, discord.VoiceChannel) else "💬"
                    channel_list += f"• {channel_type} {channel.mention}\n"
            if channel_count > 6:
                channel_list += f"• ... and {channel_count - 6} more channels"
        else:
            channel_list = "🌐 *All accessible channels will be scanned*"
        
        if config.threads:
            thread_count = len(config.threads)
            thread_list = ""
            for thread_id in list(config.threads)[:4]:
                thread = guild.get_thread(thread_id)
                if thread:
                    thread_list += f"• 🧵 {thread.mention}\n"
            if thread_count > 4:
                thread_list += f"• ... and {thread_count - 4} more threads"
        else:
            thread_list = "*No threads selected*"
        
        embed.add_field(name="📺 Selected Channels", value=channel_list, inline=True)
        embed.add_field(name="🧵 Selected Threads", value=thread_list, inline=True)
        
        embed.add_field(
            name="ℹ️ Information",
            value="Select specific channels or use 'Select All' to scan all accessible channels",
            inline=False
        )
        
        return embed

    def get_amount_selection_embed(self, config: UPTConfig) -> discord.Embed:
        embed = discord.Embed(
            title="🔢 Amount Settings",
            description="Configure how many messages to delete and the deletion mode",
            color=discord.Color.blue()
        )
        
        mode = "**Per Channel** - Delete specified amount from each channel" if config.per_channel else "**Global Total** - Delete specified amount across all channels"
        
        # FIXED: Special case for amount=0
        if config.amount == 0:
            amount_display = "**All messages** (no limit)"
        else:
            amount_display = f"**{config.amount} messages**"
        
        embed.add_field(
            name="📊 Current Settings",
            value=(
                f"**Amount:** {amount_display}\n"
                f"**Mode:** {mode}\n"
                f"**Scope:** {'Each channel individually' if config.per_channel else 'Total across all channels'}"
            ),
            inline=False
        )
        
        embed.add_field(
            name="🎯 Usage Examples",
            value=(
                "• **Per Channel (100):** Delete up to 100 messages from each channel\n"
                "• **Global Total (500):** Delete 500 messages total across all channels\n"
                "• **All Messages (0):** Delete every matching message found"
            ),
            inline=False
        )
        
        return embed

    def get_active_tasks_embed(self, tasks: List[UPTTask]) -> discord.Embed:
        embed = discord.Embed(
            title="📊 Active UPT Tasks",
            description=f"Currently running {len(tasks)} deletion task(s)",
            color=discord.Color.orange(),
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        for i, task in enumerate(tasks):
            if task.config.delete_all_users:
                target_info = "🌐 **ALL USERS**"
            elif task.config.target_users:
                target_info = f"👥 **{len(task.config.target_users)} users**"
            elif task.config.target_words:
                target_info = f"📝 **{len(task.config.target_words)} words**"
            else:
                target_info = "❌ No targets"
            
            progress_percent = (task.channels_processed / task.total_channels) * 100 if task.total_channels > 0 else 0
            progress_bar = self._create_progress_bar(progress_percent)
            
            elapsed = datetime.datetime.now(datetime.timezone.utc) - task.start_time
            minutes = int(elapsed.total_seconds() // 60)
            seconds = int(elapsed.total_seconds() % 60)
            
            embed.add_field(
                name=f"Task #{i+1} | {target_info}",
                value=(
                    f"**Progress:** {progress_bar} {progress_percent:.1f}%\n"
                    f"**Deleted:** {task.messages_deleted} messages, {task.reactions_removed} reactions\n"
                    f"**Channels:** {task.channels_processed}/{task.total_channels} processed\n"
                    f"**Current:** {task.current_channel}\n"
                    f"**Status:** {task.current_action}\n"
                    f"**Running:** {minutes}m {seconds}s"
                ),
                inline=False
            )
        
        embed.set_footer(text=f"Total active tasks: {len(tasks)} • Auto-updates every 10s")
        return embed

    def get_history_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="📜 Deletion History",
            description="Recent UPT operations and their results",
            color=discord.Color.blue(),
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        if not self.deletion_history:
            embed.add_field(
                name="No History",
                value="No deletion tasks have been completed yet.",
                inline=False
            )
        else:
            for i, record in enumerate(self.deletion_history[-5:]):
                if record.get('delete_all_users'):
                    target_info = "🌐 ALL USERS"
                elif record.get('target_users'):
                    target_info = f"👥 {len(record['target_users'])} users"
                elif record.get('target_words'):
                    target_info = f"📝 {len(record['target_words'])} words"
                else:
                    target_info = "Unknown"
                
                timestamp = record['timestamp']
                if isinstance(timestamp, str):
                    try:
                        dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        time_str = dt.strftime("%m/%d %H:%M")
                    except:
                        time_str = timestamp[:16]
                else:
                    time_str = "Unknown"
                
                embed.add_field(
                    name=f"#{len(self.deletion_history)-i} | {time_str}",
                    value=(
                        f"**Target:** {target_info}\n"
                        f"**Results:** {record['messages_deleted']} messages, {record.get('reactions_removed', 0)} reactions\n"
                        f"**Scope:** {record['channels_processed']} channels, {record.get('threads_processed', 0)} threads\n"
                        f"**Time:** {record['time_taken']}"
                    ),
                    inline=True
                )
        
        return embed

    def get_system_stats_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="📈 UPT System Statistics",
            description="Comprehensive usage and performance metrics",
            color=discord.Color.green(),
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        avg_messages_per_task = self.stats['total_messages_deleted'] / max(self.stats['total_tasks_completed'], 1)
        avg_time_per_task = self.stats['total_time_elapsed'] / max(self.stats['total_tasks_completed'], 1)
        
        total_time_str = self._format_time(self.stats['total_time_elapsed'])
        avg_time_str = self._format_time(avg_time_per_task)
        
        embed.add_field(
            name="📊 Lifetime Statistics",
            value=(
                f"**Total Messages Deleted:** {self.stats['total_messages_deleted']:,}\n"
                f"**Total Reactions Removed:** {self.stats['total_reactions_removed']:,}\n"
                f"**Total Tasks Completed:** {self.stats['total_tasks_completed']}\n"
                f"**Total Operation Time:** {total_time_str}"
            ),
            inline=False
        )
        
        embed.add_field(
            name="📈 Average Performance",
            value=(
                f"**Messages per Task:** {avg_messages_per_task:.1f}\n"
                f"**Time per Task:** {avg_time_str}\n"
                f"**Efficiency:** {(avg_messages_per_task / max(avg_time_per_task/60, 1)):.1f} msg/min"
            ),
            inline=True
        )
        
        embed.add_field(
            name="🔧 Current Status",
            value=(
                f"**Active Tasks:** {len(self.active_tasks)}\n"
                f"**Stored History:** {len(self.deletion_history)} records\n"
                f"**Admin Access:** {'✅ Enabled' if self.allow_admins else '❌ Developer Only'}"
            ),
            inline=True
        )
        
        if self.deletion_history:
            recent_deletions = sum(h['messages_deleted'] for h in self.deletion_history[-10:])
            recent_tasks = min(10, len(self.deletion_history))
            embed.add_field(
                name="📅 Recent Activity (Last 10 tasks)",
                value=f"**Messages Deleted:** {recent_deletions:,}\n**Average per Task:** {recent_deletions/recent_tasks:.1f}",
                inline=False
            )
        
        embed.set_footer(text="Enhanced UPT System • Advanced Analytics • Live Stats")
        return embed

    # ============ VIEW CLASSES ============
    
    class UPTMainView(ui.View):
        def __init__(self, cog, user_id: int, guild_id: int):
            super().__init__(timeout=300)
            self.cog = cog
            self.user_id = user_id
            self.guild_id = guild_id
            self.add_item(cog.MainDropdown(cog, user_id, guild_id))
        
        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            return self.cog.can_use(interaction.user, interaction.guild)
        
        async def on_timeout(self):
            """Handle view timeout - clean up UI"""
            if hasattr(self, 'message'):
                try:
                    await self.message.edit(content="⚠️ UPT panel timed out. Use /upt to restart.", embed=None, view=None)
                except discord.NotFound:
                    pass
        
        @ui.button(label="🔍 Pre-Scan", style=discord.ButtonStyle.primary, row=1)
        async def pre_scan(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id)
            if not config or (not config.delete_all_users and not config.target_users and not config.target_words):
                await interaction.response.send_message("❌ Please select at least one target user, word, or enable 'Delete All Users' before pre-scanning.", ephemeral=True)
                return
            
            config.dry_run = True
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            await self.cog.start_deletion_process(interaction, config, is_pre_scan=True, already_deferred=False)
        
        @ui.button(label="▶️ Start Termination", style=discord.ButtonStyle.success, row=1)
        async def start_process(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id)
            if not config or (not config.delete_all_users and not config.target_users and not config.target_words):
                await interaction.response.send_message("❌ Please select at least one target user, word, or enable 'Delete All Users' before starting.", ephemeral=True)
                return
            
            # Enhanced safety check for mass deletion
            if config.delete_all_users and len(config.channels) > 10:
                embed = discord.Embed(
                    title="⚠️ MASS DELETION WARNING",
                    description=f"You are about to delete ALL messages from {len(config.channels)} channels. "
                               f"This action is irreversible and may take a very long time.",
                    color=discord.Color.red()
                )
                embed.add_field(name="Channels", value=f"{len(config.channels)} channels", inline=False)
                embed.add_field(name="Mode", value="Delete ALL messages from ALL users", inline=False)
                embed.set_footer(text="This is a highly destructive operation. Please confirm you understand the consequences.")
                
                view = self.cog.MassDeletionConfirmationView(self.cog, self.user_id, self.guild_id)
                await interaction.response.edit_message(embed=embed, view=view)
                return
            
            config.dry_run = False
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            embed = self.cog.get_confirmation_embed(config, interaction.guild)
            view = self.cog.ConfirmationView(self.cog, self.user_id, self.guild_id)
            await interaction.response.edit_message(embed=embed, view=view)
        
        @ui.button(label="❌ Close Panel", style=discord.ButtonStyle.danger, row=1)
        async def close_panel(self, interaction: discord.Interaction, button: ui.Button):
            await interaction.response.edit_message(content="🔒 UPT control panel closed.", embed=None, view=None)
    
    class MassDeletionConfirmationView(ui.View):
        def __init__(self, cog, user_id: int, guild_id: int):
            super().__init__(timeout=300)
            self.cog = cog
            self.user_id = user_id
            self.guild_id = guild_id
        
        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            return self.cog.can_use(interaction.user, interaction.guild)
        
        async def on_timeout(self):
            """Handle view timeout - clean up UI"""
            if hasattr(self, 'message'):
                try:
                    await self.message.edit(content="⚠️ Mass deletion confirmation timed out.", embed=None, view=None)
                except discord.NotFound:
                    pass
        
        @ui.button(label="🚨 CONFIRM MASS DELETION", style=discord.ButtonStyle.danger, row=0)
        async def confirm_mass(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id)
            if not config:
                await interaction.response.edit_message(content="❌ Configuration not found.", embed=None, view=None)
                return
            
            config.dry_run = False
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            # Defer the interaction first to avoid response conflicts
            await interaction.response.defer()
            
            # Log mass deletion attempt
            logger.warning(f"User {interaction.user.id} confirmed mass deletion in guild {self.guild_id} for {len(config.channels)} channels")
            
            await self.cog.start_deletion_process(interaction, config, already_deferred=True)
        
        @ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary, row=0)
        async def cancel(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            embed = self.cog.get_main_embed(config, interaction.guild)
            view = self.cog.UPTMainView(self.cog, self.user_id, self.guild_id)
            await interaction.response.edit_message(embed=embed, view=view)
    
    class MainDropdown(ui.Select):
        def __init__(self, cog, user_id: int, guild_id: int):
            self.cog = cog
            self.user_id = user_id
            self.guild_id = guild_id
            options = [
                discord.SelectOption(label="🎯 Target Selection", value="target", description="Select target users and words"),
                discord.SelectOption(label="📁 Channel Selection", value="channels", description="Choose channels and threads"),
                discord.SelectOption(label="🔢 Amount Settings", value="amount", description="Set deletion amount and mode"),
                discord.SelectOption(label="⚙️ Advanced Settings", value="advanced", description="Whitelist, bulk delete, and more"),
                discord.SelectOption(label="📊 Active Tasks", value="tasks", description="View and manage running tasks"),
                discord.SelectOption(label="📜 History & Stats", value="history", description="View deletion history and statistics")
            ]
            super().__init__(placeholder="Choose an option...", options=options, min_values=1, max_values=1)
        
        async def callback(self, interaction: discord.Interaction):
            if not self.cog.can_use(interaction.user, interaction.guild):
                await interaction.response.send_message("❌ Access denied.", ephemeral=True)
                return
            
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            if self.values[0] == "target":
                embed = self.cog.get_target_selection_embed(config, interaction.guild)
                view = self.cog.TargetSelectionView(self.cog, self.user_id, self.guild_id)
                await interaction.response.edit_message(embed=embed, view=view)
            
            elif self.values[0] == "channels":
                embed = self.cog.get_channel_selection_embed(interaction.guild, config)
                view = self.cog.ChannelSelectionView(self.cog, self.user_id, self.guild_id)
                await interaction.response.edit_message(embed=embed, view=view)
            
            elif self.values[0] == "amount":
                embed = self.cog.get_amount_selection_embed(config)
                view = self.cog.AmountSelectionView(self.cog, self.user_id, self.guild_id)
                await interaction.response.edit_message(embed=embed, view=view)
            
            elif self.values[0] == "advanced":
                embed = self.cog.get_advanced_settings_embed(config, interaction.guild)
                view = self.cog.AdvancedSettingsView(self.cog, self.user_id, self.guild_id)
                await interaction.response.edit_message(embed=embed, view=view)
            
            elif self.values[0] == "tasks":
                user_tasks = [task for task in self.cog.active_tasks.values() if task.task_id.startswith(f"{self.guild_id}_")]
                if not user_tasks:
                    await interaction.response.send_message("✅ No active UPT tasks.", ephemeral=True)
                    return
                embed = self.cog.get_active_tasks_embed(user_tasks)
                view = self.cog.ActiveTasksView(self.cog, self.user_id, self.guild_id)
                await interaction.response.edit_message(embed=embed, view=view)
            
            elif self.values[0] == "history":
                embed = self.cog.get_history_embed()
                view = self.cog.HistoryView(self.cog, self.user_id, self.guild_id)
                await interaction.response.edit_message(embed=embed, view=view)
    
    class TargetSelectionView(ui.View):
        def __init__(self, cog, user_id: int, guild_id: int, page: int = 0):
            super().__init__(timeout=300)
            self.cog = cog
            self.user_id = user_id
            self.guild_id = guild_id
            self.page = page
        
        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            return self.cog.can_use(interaction.user, interaction.guild)
        
        async def on_timeout(self):
            """Handle view timeout - clean up UI"""
            if hasattr(self, 'message'):
                try:
                    await self.message.edit(content="⚠️ Target selection timed out. Use /upt to restart.", embed=None, view=None)
                except discord.NotFound:
                    pass
        
        @ui.button(label="👥 Select Users", style=discord.ButtonStyle.primary, row=0)
        async def select_users(self, interaction: discord.Interaction, button: ui.Button):
            guild = interaction.guild
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            
            all_members = self.cog._get_all_members_with_fakes(guild, config.target_users)
            
            view = PaginatedUserSelectionView(self.cog, self.user_id, self.guild_id, all_members)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        
        @ui.button(label="📝 Add Word", style=discord.ButtonStyle.primary, row=0)
        async def add_word(self, interaction: discord.Interaction, button: ui.Button):
            modal = self.cog.AddWordModal(self.cog, self.user_id, self.guild_id)
            await interaction.response.send_modal(modal)
        
        @ui.button(label="🌐 All Users", style=discord.ButtonStyle.success, row=0)
        async def all_users(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.delete_all_users = not config.delete_all_users
            config.last_accessed = time.time()
            if config.delete_all_users:
                config.target_users.clear()
            self.cog.active_configs[self.user_id] = config
            
            embed = self.cog.get_target_selection_embed(config, interaction.guild)
            await interaction.response.edit_message(embed=embed)
        
        @ui.button(label="🤖 Include Bots", style=discord.ButtonStyle.secondary, row=0)
        async def include_bots(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.include_bots = not config.include_bots
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            status = "✅ Enabled" if config.include_bots else "❌ Disabled"
            await interaction.response.send_message(f"🤖 Bot message deletion: {status}", ephemeral=True)
            
            embed = self.cog.get_target_selection_embed(config, interaction.guild)
            try:
                await interaction.message.edit(embed=embed)
            except discord.NotFound:
                await interaction.followup.send("Message was deleted. Use the command again to restart.", ephemeral=True)
        
        @ui.button(label="🔄 Reset All", style=discord.ButtonStyle.danger, row=1)
        async def reset_all(self, interaction: discord.Interaction, button: ui.Button):
            if self.user_id in self.cog.active_configs:
                self.cog.active_configs[self.user_id] = UPTConfig(set(), set(), set(), set())
            embed = self.cog.get_target_selection_embed(UPTConfig(set(), set(), set(), set()), interaction.guild)
            await interaction.response.edit_message(embed=embed)
        
        @ui.button(label="🔙 Back to Main", style=discord.ButtonStyle.secondary, row=1)
        async def back(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            embed = self.cog.get_main_embed(config, interaction.guild)
            view = self.cog.UPTMainView(self.cog, self.user_id, self.guild_id)
            await interaction.response.edit_message(embed=embed, view=view)
    
    class AdvancedSettingsView(ui.View):
        def __init__(self, cog, user_id: int, guild_id: int):
            super().__init__(timeout=300)
            self.cog = cog
            self.user_id = user_id
            self.guild_id = guild_id
        
        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            return self.cog.can_use(interaction.user, interaction.guild)
        
        async def on_timeout(self):
            """Handle view timeout - clean up UI"""
            if hasattr(self, 'message'):
                try:
                    await self.message.edit(content="⚠️ Advanced settings timed out. Use /upt to restart.", embed=None, view=None)
                except discord.NotFound:
                    pass
        
        @ui.button(label="🛡️ Whitelist Users", style=discord.ButtonStyle.primary, row=0)
        async def whitelist_users(self, interaction: discord.Interaction, button: ui.Button):
            guild = interaction.guild
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            
            all_members = self.cog._get_all_members_with_fakes(guild, config.whitelist_users)
            
            view = PaginatedUserSelectionView(self.cog, self.user_id, self.guild_id, all_members, is_whitelist=True)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        
        @ui.button(label="📁 Whitelist Channels", style=discord.ButtonStyle.primary, row=0)
        async def whitelist_channels(self, interaction: discord.Interaction, button: ui.Button):
            guild = interaction.guild
            all_channels = self.cog._get_sorted_channels(guild)
            
            view = PaginatedChannelSelectionView(self.cog, self.user_id, self.guild_id, all_channels, is_whitelist=True)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        
        @ui.button(label="💣 Toggle Bulk Delete", style=discord.ButtonStyle.primary, row=1)
        async def toggle_bulk(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.use_bulk_delete = not config.use_bulk_delete
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            embed = self.cog.get_advanced_settings_embed(config, interaction.guild)
            await interaction.response.edit_message(embed=embed)
        
        @ui.button(label="🎭 Toggle Reactions", style=discord.ButtonStyle.secondary, row=1)
        async def toggle_reactions(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.remove_reactions = not config.remove_reactions
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            embed = self.cog.get_advanced_settings_embed(config, interaction.guild)
            await interaction.response.edit_message(embed=embed)
        
        @ui.button(label="🔔 Toggle Notify", style=discord.ButtonStyle.secondary, row=2)
        async def toggle_notify(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.notify = not config.notify
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            embed = self.cog.get_advanced_settings_embed(config, interaction.guild)
            await interaction.response.edit_message(embed=embed)
        
        @ui.button(label="🔤 Toggle Regex", style=discord.ButtonStyle.secondary, row=2)
        async def toggle_regex(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.use_regex = not config.use_regex
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            status = "✅ Enabled" if config.use_regex else "❌ Disabled"
            await interaction.response.send_message(f"🔤 Regex word matching: {status}", ephemeral=True)
            
            embed = self.cog.get_advanced_settings_embed(config, interaction.guild)
            try:
                await interaction.message.edit(embed=embed)
            except discord.NotFound:
                await interaction.followup.send("Message was deleted. Use the command again to restart.", ephemeral=True)
        
        # NEW: Content filter toggles
        @ui.button(label="📄 Toggle Embeds", style=discord.ButtonStyle.secondary, row=3)
        async def toggle_embeds(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            if config.has_embed is None:
                config.has_embed = True  # Only with embeds
                button.label = "📄 Embeds: Only"
            elif config.has_embed:
                config.has_embed = False  # Exclude embeds
                button.label = "📄 Embeds: Exclude"
            else:
                config.has_embed = None   # No filter
                button.label = "📄 Toggle Embeds"
            
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            embed = self.cog.get_advanced_settings_embed(config, interaction.guild)
            await interaction.response.edit_message(embed=embed, view=self)
        
        @ui.button(label="📎 Toggle Attachments", style=discord.ButtonStyle.secondary, row=3)
        async def toggle_attachments(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            if config.has_attachment is None:
                config.has_attachment = True  # Only with attachments
                button.label = "📎 Attachments: Only"
            elif config.has_attachment:
                config.has_attachment = False  # Exclude attachments
                button.label = "📎 Attachments: Exclude"
            else:
                config.has_attachment = None   # No filter
                button.label = "📎 Toggle Attachments"
            
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            embed = self.cog.get_advanced_settings_embed(config, interaction.guild)
            await interaction.response.edit_message(embed=embed, view=self)
        
        @ui.button(label="🔗 Toggle Links", style=discord.ButtonStyle.secondary, row=3)
        async def toggle_links(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            if config.has_link is None:
                config.has_link = True  # Only with links
                button.label = "🔗 Links: Only"
            elif config.has_link:
                config.has_link = False  # Exclude links
                button.label = "🔗 Links: Exclude"
            else:
                config.has_link = None   # No filter
                button.label = "🔗 Toggle Links"
            
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            embed = self.cog.get_advanced_settings_embed(config, interaction.guild)
            await interaction.response.edit_message(embed=embed, view=self)
        
        @ui.button(label="🔙 Back to Main", style=discord.ButtonStyle.secondary, row=4)
        async def back(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            embed = self.cog.get_main_embed(config, interaction.guild)
            view = self.cog.UPTMainView(self.cog, self.user_id, self.guild_id)
            await interaction.response.edit_message(embed=embed, view=view)
    
    class ChannelSelectionView(ui.View):
        def __init__(self, cog, user_id: int, guild_id: int, page: int = 0):
            super().__init__(timeout=300)
            self.cog = cog
            self.user_id = user_id
            self.guild_id = guild_id
            self.page = page
        
        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            return self.cog.can_use(interaction.user, interaction.guild)
        
        async def on_timeout(self):
            """Handle view timeout - clean up UI"""
            if hasattr(self, 'message'):
                try:
                    await self.message.edit(content="⚠️ Channel selection timed out. Use /upt to restart.", embed=None, view=None)
                except discord.NotFound:
                    pass
        
        @ui.button(label="📁 Select Channels", style=discord.ButtonStyle.primary, row=0)
        async def select_channels(self, interaction: discord.Interaction, button: ui.Button):
            guild = interaction.guild
            all_channels = self.cog._get_sorted_channels(guild)
            
            view = PaginatedChannelSelectionView(self.cog, self.user_id, self.guild_id, all_channels)
            embed = view.get_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        
        @ui.button(label="🧵 Add Threads", style=discord.ButtonStyle.primary, row=0)
        async def add_threads(self, interaction: discord.Interaction, button: ui.Button):
            modal = self.cog.AddThreadsModal(self.cog, self.user_id, self.guild_id)
            await interaction.response.send_modal(modal)
        
        @ui.button(label="✅ Select All", style=discord.ButtonStyle.success, row=0)
        async def select_all(self, interaction: discord.Interaction, button: ui.Button):
            guild = self.cog.bot.get_guild(self.guild_id)
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            
            config.channels.clear()
            for channel in guild.text_channels:
                config.channels.add(channel.id)
            for channel in guild.voice_channels:
                config.channels.add(channel.id)
            
            self.cog.active_configs[self.user_id] = config
            embed = self.cog.get_channel_selection_embed(guild, config)
            await interaction.response.edit_message(embed=embed)
        
        @ui.button(label="🔄 Clear All", style=discord.ButtonStyle.danger, row=0)
        async def clear_all(self, interaction: discord.Interaction, button: ui.Button):
            if self.user_id in self.cog.active_configs:
                self.cog.active_configs[self.user_id].channels.clear()
                self.cog.active_configs[self.user_id].threads.clear()
            guild = self.cog.bot.get_guild(self.guild_id)
            embed = self.cog.get_channel_selection_embed(guild, self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set())))
            await interaction.response.edit_message(embed=embed)
        
        @ui.button(label="🔙 Back to Main", style=discord.ButtonStyle.secondary, row=1)
        async def back(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            embed = self.cog.get_main_embed(config, interaction.guild)
            view = self.cog.UPTMainView(self.cog, self.user_id, self.guild_id)
            await interaction.response.edit_message(embed=embed, view=view)
    
    class AmountSelectionView(ui.View):
        def __init__(self, cog, user_id: int, guild_id: int):
            super().__init__(timeout=300)
            self.cog = cog
            self.user_id = user_id
            self.guild_id = guild_id
        
        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            return self.cog.can_use(interaction.user, interaction.guild)
        
        async def on_timeout(self):
            """Handle view timeout - clean up UI"""
            if hasattr(self, 'message'):
                try:
                    await self.message.edit(content="⚠️ Amount settings timed out. Use /upt to restart.", embed=None, view=None)
                except discord.NotFound:
                    pass
        
        @ui.button(label="✏️ Edit Amount", style=discord.ButtonStyle.primary, row=0)
        async def edit_amount(self, interaction: discord.Interaction, button: ui.Button):
            modal = self.cog.EditAmountModal(self.cog, self.user_id, self.guild_id)
            await interaction.response.send_modal(modal)
        
        @ui.button(label="🔄 Toggle Mode", style=discord.ButtonStyle.secondary, row=0)
        async def toggle_mode(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.per_channel = not config.per_channel
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            embed = self.cog.get_amount_selection_embed(config)
            await interaction.response.edit_message(embed=embed)
        
        @ui.button(label="🔙 Back to Main", style=discord.ButtonStyle.secondary, row=1)
        async def back(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            embed = self.cog.get_main_embed(config, interaction.guild)
            view = self.cog.UPTMainView(self.cog, self.user_id, self.guild_id)
            await interaction.response.edit_message(embed=embed, view=view)
    
    class ActiveTasksView(ui.View):
        def __init__(self, cog, user_id: int, guild_id: int):
            super().__init__(timeout=300)
            self.cog = cog
            self.user_id = user_id
            self.guild_id = guild_id
        
        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            return self.cog.can_use(interaction.user, interaction.guild)
        
        async def on_timeout(self):
            """Handle view timeout - clean up UI"""
            if hasattr(self, 'message'):
                try:
                    await self.message.edit(content="⚠️ Active tasks view timed out. Use /upt to restart.", embed=None, view=None)
                except discord.NotFound:
                    pass
        
        @ui.button(label="🛑 Stop All Tasks", style=discord.ButtonStyle.danger, row=0)
        async def stop_all(self, interaction: discord.Interaction, button: ui.Button):
            guild_tasks = [task for task in self.cog.active_tasks.values() if task.task_id.startswith(f"{self.guild_id}_")]
            for task in guild_tasks:
                task.cancelled = True
            
            await interaction.response.send_message(f"🛑 Stopped {len(guild_tasks)} active task(s).", ephemeral=True)
            
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            embed = self.cog.get_main_embed(config, interaction.guild)
            view = self.cog.UPTMainView(self.cog, self.user_id, self.guild_id)
            await interaction.message.edit(embed=embed, view=view)
        
        @ui.button(label="🔙 Back to Main", style=discord.ButtonStyle.secondary, row=0)
        async def back(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            embed = self.cog.get_main_embed(config, interaction.guild)
            view = self.cog.UPTMainView(self.cog, self.user_id, self.guild_id)
            await interaction.response.edit_message(embed=embed, view=view)
    
    class HistoryView(ui.View):
        def __init__(self, cog, user_id: int, guild_id: int):
            super().__init__(timeout=300)
            self.cog = cog
            self.user_id = user_id
            self.guild_id = guild_id
        
        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            return self.cog.can_use(interaction.user, interaction.guild)
        
        async def on_timeout(self):
            """Handle view timeout - clean up UI"""
            if hasattr(self, 'message'):
                try:
                    await self.message.edit(content="⚠️ History view timed out. Use /upt to restart.", embed=None, view=None)
                except discord.NotFound:
                    pass
        
        @ui.button(label="📈 System Stats", style=discord.ButtonStyle.primary, row=0)
        async def system_stats(self, interaction: discord.Interaction, button: ui.Button):
            embed = self.cog.get_system_stats_embed()
            await interaction.response.edit_message(embed=embed)
        
        @ui.button(label="📜 Recent History", style=discord.ButtonStyle.primary, row=0)
        async def recent_history(self, interaction: discord.Interaction, button: ui.Button):
            if not self.cog.deletion_history:
                await interaction.response.send_message("📜 No deletion history found.", ephemeral=True)
                return
            embed = self.cog.get_history_embed()
            await interaction.response.edit_message(embed=embed)
        
        @ui.button(label="🔙 Back to Main", style=discord.ButtonStyle.secondary, row=1)
        async def back(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            embed = self.cog.get_main_embed(config, interaction.guild)
            view = self.cog.UPTMainView(self.cog, self.user_id, self.guild_id)
            await interaction.response.edit_message(embed=embed, view=view)
    
    class ProgressView(ui.View):
        def __init__(self, cog, task_id: str):
            super().__init__(timeout=None)
            self.cog = cog
            self.task_id = task_id
        
        @ui.button(label="🛑 Cancel Task", style=discord.ButtonStyle.danger, row=0)
        async def cancel_task(self, interaction: discord.Interaction, button: ui.Button):
            task = self.cog.active_tasks.get(self.task_id)
            if task:
                task.cancelled = True
                button.disabled = True
                button.label = "🛑 Cancelling..."
                await interaction.response.edit_message(view=self)
                await interaction.followup.send("🛑 UPT task cancellation requested. Stopping...", ephemeral=True)
                logger.info(f"User {interaction.user.id} cancelled task {self.task_id}")
            else:
                await interaction.response.send_message("❌ Task not found or already completed.", ephemeral=True)
    
    class ConfirmationView(ui.View):
        def __init__(self, cog, user_id: int, guild_id: int):
            super().__init__(timeout=300)
            self.cog = cog
            self.user_id = user_id
            self.guild_id = guild_id
        
        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            return self.cog.can_use(interaction.user, interaction.guild)
        
        async def on_timeout(self):
            """Handle view timeout - clean up UI"""
            if hasattr(self, 'message'):
                try:
                    await self.message.edit(content="⚠️ Confirmation timed out. Use /upt to restart.", embed=None, view=None)
                except discord.NotFound:
                    pass
        
        @ui.button(label="✅ Confirm Termination", style=discord.ButtonStyle.success, row=0)
        async def confirm(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id)
            if not config:
                await interaction.response.edit_message(content="❌ Configuration not found.", embed=None, view=None)
                return
            
            await self.cog.start_deletion_process(interaction, config, already_deferred=False)
        
        @ui.button(label="❌ Cancel", style=discord.ButtonStyle.danger, row=0)
        async def cancel(self, interaction: discord.Interaction, button: ui.Button):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            embed = self.cog.get_main_embed(config, interaction.guild)
            view = self.cog.UPTMainView(self.cog, self.user_id, self.guild_id)
            await interaction.response.edit_message(embed=embed, view=view)
    
    # ============ MODALS ============
    
    class AddWordModal(ui.Modal, title="📝 Add Target Word"):
        def __init__(self, cog, user_id: int, guild_id: int):
            super().__init__()
            self.cog = cog
            self.user_id = user_id
            self.guild_id = guild_id
        
        word_input = ui.TextInput(
            label="Word or Phrase",
            placeholder="Enter word or phrase to target (case insensitive)",
            style=discord.TextStyle.paragraph,
            required=True
        )
        
        async def on_submit(self, interaction: discord.Interaction):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            
            words = {word.strip().lower() for word in self.word_input.value.split(',')}
            config.target_words.update(words)
            self.cog.active_configs[self.user_id] = config
            
            embed = self.cog.get_target_selection_embed(config, interaction.guild)
            view = self.cog.TargetSelectionView(self.cog, self.user_id, self.guild_id)
            await interaction.response.edit_message(embed=embed, view=view)
    
    class AddThreadsModal(ui.Modal, title="🧵 Add Threads"):
        def __init__(self, cog, user_id: int, guild_id: int):
            super().__init__()
            self.cog = cog
            self.user_id = user_id
            self.guild_id = guild_id
        
        thread_input = ui.TextInput(
            label="Thread IDs",
            placeholder="Enter thread IDs separated by spaces",
            style=discord.TextStyle.paragraph,
            required=True
        )
        
        async def on_submit(self, interaction: discord.Interaction):
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.last_accessed = time.time()
            
            thread_ids = set()
            for part in self.thread_input.value.split():
                if part.isdigit():
                    thread_ids.add(int(part))
            
            config.threads.update(thread_ids)
            self.cog.active_configs[self.user_id] = config
            
            guild = self.cog.bot.get_guild(self.guild_id)
            embed = self.cog.get_channel_selection_embed(guild, config)
            view = self.cog.ChannelSelectionView(self.cog, self.user_id, self.guild_id)
            await interaction.response.edit_message(embed=embed, view=view)
    
    class EditAmountModal(ui.Modal, title="🔢 Edit Amount"):
        def __init__(self, cog, user_id: int, guild_id: int):
            super().__init__()
            self.cog = cog
            self.user_id = user_id
            self.guild_id = guild_id
        
        amount_input = ui.TextInput(
            label="Number of Messages",
            placeholder="Enter number of messages to delete (0 = all)",
            default="100",
            required=True
        )
        
        async def on_submit(self, interaction: discord.Interaction):
            try:
                amount = int(self.amount_input.value)
                if amount < 0:
                    raise ValueError("Amount cannot be negative")
            except ValueError:
                await interaction.response.send_message("❌ Please enter a valid positive number.", ephemeral=True)
                return
            
            config = self.cog.active_configs.get(self.user_id, UPTConfig(set(), set(), set(), set()))
            config.amount = amount
            config.last_accessed = time.time()
            self.cog.active_configs[self.user_id] = config
            
            embed = self.cog.get_amount_selection_embed(config)
            await interaction.response.edit_message(embed=embed)
    
    # ============ COMMANDS ============
    
    @app_commands.command(name="upt", description="Open User Presence Terminator control panel")
    async def upt_slash(self, interaction: discord.Interaction):
        if not self.can_use(interaction.user, interaction.guild):
            await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
            return
        
        config = self.active_configs.get(interaction.user.id, UPTConfig(set(), set(), set(), set()))
        config.last_accessed = time.time()
        self.active_configs[interaction.user.id] = config
        
        embed = self.get_main_embed(config, interaction.guild)
        view = self.UPTMainView(self, interaction.user.id, interaction.guild.id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    @app_commands.command(name="uptadm", description="Toggle UPT admin access (Developer only)")
    async def upt_admin_toggle(self, interaction: discord.Interaction, allow: bool):
        if interaction.user.id != DEVELOPER_ID:
            await interaction.response.send_message("❌ This command is restricted to the bot developer only.", ephemeral=True)
            return
        
        self.allow_admins = allow
        status = "enabled" if allow else "disabled"
        await interaction.response.send_message(f"✅ UPT access {status} for server administrators.", ephemeral=True)
        
        await self._save_persistence()
        logger.info(f"Developer {interaction.user.id} {'enabled' if allow else 'disabled'} admin access")

async def setup(bot):
    await bot.add_cog(UserPresenceTerminator(bot))
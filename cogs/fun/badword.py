import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
import os
import re
from typing import List, Optional
import asyncio
import json

# Default bad words list - easy to edit
DEFAULT_BAD_WORDS = [
    "damn", "crap", "hell", "ass", "bitch", "bastard", "shit", "fuck", "piss",
    "dick", "cock", "pussy", "whore", "slut", "retard", "dumb", "stupid",
    "bad", "sucks", "suck", "awful", "terrible", "horrible", "disgusting"
]

DEVELOPER_IDS = [1313333441525448704]

def is_developer(user_id):
    return user_id in DEVELOPER_IDS

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        """Get a database connection with proper error handling"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")  # Better concurrency
        conn.execute("PRAGMA synchronous=NORMAL")  # Good balance of safety vs performance
        return conn
    
    def init_database(self):
        """Initialize the SQLite database with per-guild support"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Guild-specific settings
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS badword_settings (
                guild_id TEXT PRIMARY KEY,
                enabled BOOLEAN NOT NULL DEFAULT 0,
                bad_words TEXT NOT NULL,
                included_channels TEXT,
                excluded_channels TEXT,
                included_roles TEXT,
                excluded_roles TEXT,
                included_users TEXT,
                excluded_users TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Guild-specific user stats
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS badword_stats (
                guild_id TEXT,
                user_id TEXT,
                username TEXT,
                bad_word_count INTEGER NOT NULL DEFAULT 0,
                last_said TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        
        conn.commit()
        conn.close()

class PaginatedSelectView(discord.ui.View):
    def __init__(self, cog, guild_id, current_settings, select_type, is_exclude=False, timeout=180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = guild_id
        self.current_settings = current_settings
        self.select_type = select_type  # 'channel', 'role', 'user'
        self.is_exclude = is_exclude
        self.page = 0
        self.items_per_page = 25
        self.all_items = []
        self.message = None
        self.load_items()
        
    def load_items(self):
        """Load all items based on type"""
        guild = self.cog.bot.get_guild(self.guild_id)
        if not guild:
            return
            
        if self.select_type == 'channel':
            self.all_items = [channel for channel in guild.channels if isinstance(channel, discord.TextChannel)]
        elif self.select_type == 'role':
            self.all_items = [role for role in guild.roles if role.name != "@everyone"]
        elif self.select_type == 'user':
            self.all_items = [member for member in guild.members if not member.bot]
            
        self.all_items.sort(key=lambda x: x.name)
        
    def get_current_page_items(self):
        start = self.page * self.items_per_page
        end = start + self.items_per_page
        return self.all_items[start:end]
        
    def create_embed(self):
        current_items = self.get_current_page_items()
        total_pages = (len(self.all_items) + self.items_per_page - 1) // self.items_per_page
        
        embed = discord.Embed(
            title=f"Select {self.select_type.title()}s to {'Exclude' if self.is_exclude else 'Include'}",
            description=f"Page {self.page + 1}/{total_pages}\nSelect up to {self.items_per_page} {self.select_type}s per page.",
            color=discord.Color.blue()
        )
        
        # Show current selection
        current_setting = self.current_settings.get(f"{'excluded' if self.is_exclude else 'included'}_{self.select_type}s", [])
        if current_setting:
            embed.add_field(
                name=f"Currently {'Excluded' if self.is_exclude else 'Included'}",
                value=f"{len(current_setting)} {self.select_type}s",
                inline=False
            )
            
        return embed
        
    async def update_message(self, interaction: discord.Interaction = None):
        embed = self.create_embed()
        current_items = self.get_current_page_items()
        
        # Clear existing select menus
        self.clear_items()
        
        # Add select menu for current page
        if current_items:
            options = []
            for item in current_items:
                label = item.name
                if len(label) > 25:
                    label = label[:22] + "..."
                    
                if self.select_type == 'user':
                    description = f"User: {item.display_name}"
                else:
                    description = f"{self.select_type.title()}: {item.name}"
                    
                options.append(discord.SelectOption(
                    label=label,
                    value=str(item.id),
                    description=description[:50] if description else None
                ))
            
            select_menu = discord.ui.Select(
                placeholder=f"Select {self.select_type}s...",
                options=options,
                min_values=0,
                max_values=len(options)
            )
            select_menu.callback = self.select_callback
            self.add_item(select_menu)
        
        # Add navigation buttons
        if self.page > 0:
            prev_button = discord.ui.Button(style=discord.ButtonStyle.primary, label="◀ Previous")
            prev_button.callback = self.prev_callback
            self.add_item(prev_button)
            
        if (self.page + 1) * self.items_per_page < len(self.all_items):
            next_button = discord.ui.Button(style=discord.ButtonStyle.primary, label="Next ▶")
            next_button.callback = self.next_callback
            self.add_item(next_button)
            
        back_button = discord.ui.Button(style=discord.ButtonStyle.secondary, label="⬅ Back")
        back_button.callback = self.back_callback
        self.add_item(back_button)
        
        if interaction:
            await interaction.response.edit_message(embed=embed, view=self)
        elif self.message:
            await self.message.edit(embed=embed, view=self)
    
    async def select_callback(self, interaction: discord.Interaction):
        selected_ids = interaction.data['values']
        setting_key = f"{'excluded' if self.is_exclude else 'included'}_{self.select_type}s"
        
        self.current_settings[setting_key] = selected_ids
        self.cog.save_guild_settings(self.guild_id, **self.current_settings)
        
        await interaction.response.send_message(
            f"✅ {'Excluded' if self.is_exclude else 'Included'} {len(selected_ids)} {self.select_type}s!", 
            ephemeral=True
        )
        await self.update_message()
    
    async def prev_callback(self, interaction: discord.Interaction):
        self.page -= 1
        await self.update_message(interaction)
    
    async def next_callback(self, interaction: discord.Interaction):
        self.page += 1
        await self.update_message(interaction)
    
    async def back_callback(self, interaction: discord.Interaction):
        main_view = BadWordTrackerView(self.cog, self.guild_id)
        embed = main_view.create_status_embed()
        await interaction.response.edit_message(embed=embed, view=main_view)
        main_view.message = await interaction.original_response()

class AddWordModal(discord.ui.Modal, title="Add Bad Word"):
    word_input = discord.ui.TextInput(
        label="Word to add",
        placeholder="Enter the word you want to add...",
        max_length=50
    )
    
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id
    
    async def on_submit(self, interaction: discord.Interaction):
        word = self.word_input.value.strip()
        if not word:
            await interaction.response.send_message("❌ Please enter a valid word.", ephemeral=True)
            return
            
        settings = self.cog.get_guild_settings(self.guild_id)
        normalized_word = self.cog.normalize_word(word)
        
        if normalized_word in [self.cog.normalize_word(w) for w in settings["bad_words"]]:
            await interaction.response.send_message(f"❌ **{word}** is already in the list!", ephemeral=True)
            return
        
        settings["bad_words"].append(word)
        self.cog.save_guild_settings(self.guild_id, bad_words=settings["bad_words"])
        
        await interaction.response.send_message(f"✅ Added **{word}** to bad words list!", ephemeral=True)

class RemoveWordModal(discord.ui.Modal, title="Remove Bad Word"):
    word_input = discord.ui.TextInput(
        label="Word to remove",
        placeholder="Enter the word you want to remove...",
        max_length=50
    )
    
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id
    
    async def on_submit(self, interaction: discord.Interaction):
        word = self.word_input.value.strip()
        if not word:
            await interaction.response.send_message("❌ Please enter a valid word.", ephemeral=True)
            return
            
        settings = self.cog.get_guild_settings(self.guild_id)
        normalized_word = self.cog.normalize_word(word)
        normalized_bad_words = [self.cog.normalize_word(w) for w in settings["bad_words"]]
        
        if normalized_word not in normalized_bad_words:
            await interaction.response.send_message(f"❌ **{word}** is not in the list!", ephemeral=True)
            return
        
        index_to_remove = normalized_bad_words.index(normalized_word)
        removed_word = settings["bad_words"][index_to_remove]
        settings["bad_words"].pop(index_to_remove)
        self.cog.save_guild_settings(self.guild_id, bad_words=settings["bad_words"])
        
        await interaction.response.send_message(f"✅ Removed **{removed_word}** from bad words list!", ephemeral=True)

class ResetUserModal(discord.ui.Modal, title="Reset User Bad Words"):
    user_input = discord.ui.TextInput(
        label="User to reset",
        placeholder="Enter user ID or mention...",
        max_length=100
    )
    
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id
    
    async def on_submit(self, interaction: discord.Interaction):
        user_input = self.user_input.value.strip()
        if not user_input:
            await interaction.response.send_message("❌ Please enter a user.", ephemeral=True)
            return
            
        # Try to parse user ID from mention or use as-is
        user_id = user_input
        if user_id.startswith('<@') and user_id.endswith('>'):
            user_id = user_id[2:-1]
            if user_id.startswith('!'):
                user_id = user_id[1:]
        
        # Reset user count to 0
        self.cog.update_stats(self.guild_id, user_id, "Reset User", 0)
        
        await interaction.response.send_message(f"✅ Reset bad word count for user <@{user_id}>!", ephemeral=True)

class BadWordTrackerView(discord.ui.View):
    def __init__(self, cog, guild_id, timeout=180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = guild_id
        self.current_settings = self.cog.get_guild_settings(guild_id)
        self.message = None
        
    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            await self.message.edit(view=self)
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not await self.cog.check_permissions(interaction):
            await interaction.response.send_message("❌ You need administrator permissions or developer access.", ephemeral=True)
            return False
        return True
    
    def create_status_embed(self):
        settings = self.current_settings
        status = "🟢 **ENABLED**" if settings["enabled"] else "🔴 **DISABLED**"
        
        embed = discord.Embed(
            title="🚨 Bad Word Tracker Control Panel",
            color=0x00ff00 if settings["enabled"] else 0xff0000,
            description="Select an option from the dropdown menu below to manage your bad word tracking system."
        )
        
        # Add status fields
        embed.add_field(
            name="Quick Status", 
            value=f"**Tracker:** {status}\n**Tracked Words:** {len(settings['bad_words'])}",
            inline=False
        )
        
        # Add channel/role/user settings info
        channel_info = "All channels" if not settings.get('included_channels') else f"{len(settings.get('included_channels', []))} included"
        role_info = "All roles" if not settings.get('included_roles') else f"{len(settings.get('included_roles', []))} included"
        user_info = "All users" if not settings.get('included_users') else f"{len(settings.get('included_users', []))} included"
        
        embed.add_field(
            name="Filter Settings",
            value=f"**Channels:** {channel_info}\n**Roles:** {role_info}\n**Users:** {user_info}",
            inline=False
        )
        
        embed.set_footer(text="All interactions are private • Use dropdown to navigate")
        
        return embed

    @discord.ui.select(
        placeholder="Choose an option...",
        options=[
            discord.SelectOption(label="Toggle On/Off", description="Enable or disable bad word tracking", emoji="⚡"),
            discord.SelectOption(label="Setup", description="Configure channels, roles, and users", emoji="⚙️"),
            discord.SelectOption(label="Manage Words", description="Add, remove, or reset bad words", emoji="📝"),
            discord.SelectOption(label="Leaderboard", description="View bad word leaderboard", emoji="🏆"),
            discord.SelectOption(label="Word List", description="View current bad words list", emoji="📋")
        ]
    )
    async def select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
        choice = select.values[0]
        
        if choice == "Toggle On/Off":
            new_state = not self.current_settings["enabled"]
            self.cog.save_guild_settings(self.guild_id, enabled=new_state)
            self.current_settings["enabled"] = new_state
            
            embed = self.create_status_embed()
            await interaction.response.edit_message(embed=embed, view=self)
            await interaction.followup.send(f"✅ Bad word counter turned **{'on' if new_state else 'off'}**!", ephemeral=True)
            
        elif choice == "Setup":
            view = SetupView(self.cog, self.guild_id, self.current_settings)
            embed = discord.Embed(
                title="⚙️ Setup Configuration",
                description="Configure which channels, roles, and users to track.",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()
            
        elif choice == "Manage Words":
            view = ManageWordsView(self.cog, self.guild_id, self.current_settings)
            embed = discord.Embed(
                title="📝 Manage Bad Words",
                description="Add, remove, or reset custom bad words.",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()
            
        elif choice == "Leaderboard":
            # Create leaderboard embed in the current message with a back button
            leaderboard_embed = await self.cog.create_leaderboard_embed(self.guild_id, interaction.user.id)
            
            # Create a temporary view with back button
            view = discord.ui.View(timeout=60)
            
            async def back_callback(interaction: discord.Interaction):
                embed = self.create_status_embed()
                await interaction.response.edit_message(embed=embed, view=self)
                
            back_button = discord.ui.Button(style=discord.ButtonStyle.secondary, label="⬅ Back")
            back_button.callback = back_callback
            view.add_item(back_button)
            
            await interaction.response.edit_message(embed=leaderboard_embed, view=view)
            view.message = await interaction.original_response()
            
        elif choice == "Word List":
            settings = self.current_settings
            words_text = ", ".join(sorted(settings["bad_words"]))
            
            if len(words_text) > 1000:
                words_text = words_text[:1000] + "..."
                
            embed = discord.Embed(
                title="📋 Bad Words List",
                description=words_text,
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"Total: {len(settings['bad_words'])} words")
            
            # Create a temporary view with back button
            view = discord.ui.View(timeout=60)
            
            async def back_callback(interaction: discord.Interaction):
                embed = self.create_status_embed()
                await interaction.response.edit_message(embed=embed, view=self)
                
            back_button = discord.ui.Button(style=discord.ButtonStyle.secondary, label="⬅ Back")
            back_button.callback = back_callback
            view.add_item(back_button)
            
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()

    @discord.ui.button(label="Close", style=discord.ButtonStyle.danger, emoji="❌", row=1)
    async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await interaction.delete_original_response()

class SetupView(discord.ui.View):
    def __init__(self, cog, guild_id, current_settings, timeout=180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = guild_id
        self.current_settings = current_settings
        self.message = None
        
    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            await self.message.edit(view=self)
    
    @discord.ui.select(
        placeholder="Configure tracking settings...",
        options=[
            discord.SelectOption(label="Channel Settings", description="Include/exclude channels", emoji="📁"),
            discord.SelectOption(label="Role Settings", description="Include/exclude roles", emoji="👥"),
            discord.SelectOption(label="User Settings", description="Include/exclude users", emoji="👤")
        ]
    )
    async def setup_select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
        choice = select.values[0]
        
        if choice == "Channel Settings":
            view = ChannelSettingsView(self.cog, self.guild_id, self.current_settings)
            embed = discord.Embed(
                title="📁 Channel Settings",
                description="Configure which channels to monitor for bad words.",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()
            
        elif choice == "Role Settings":
            view = RoleSettingsView(self.cog, self.guild_id, self.current_settings)
            embed = discord.Embed(
                title="👥 Role Settings",
                description="Configure which roles to monitor for bad words.",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()
            
        elif choice == "User Settings":
            view = UserSettingsView(self.cog, self.guild_id, self.current_settings)
            embed = discord.Embed(
                title="👤 User Settings",
                description="Configure which users to monitor for bad words.",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        main_view = BadWordTrackerView(self.cog, self.guild_id)
        embed = main_view.create_status_embed()
        await interaction.response.edit_message(embed=embed, view=main_view)
        main_view.message = await interaction.original_response()

class ManageWordsView(discord.ui.View):
    def __init__(self, cog, guild_id, current_settings, timeout=180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = guild_id
        self.current_settings = current_settings
        self.message = None
        
    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            await self.message.edit(view=self)
    
    @discord.ui.select(
        placeholder="Manage bad words...",
        options=[
            discord.SelectOption(label="Add Word", description="Add a new bad word", emoji="➕"),
            discord.SelectOption(label="Remove Word", description="Remove a bad word", emoji="➖"),
            discord.SelectOption(label="Reset Words", description="Reset to default words", emoji="🔄")
        ]
    )
    async def words_select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
        choice = select.values[0]
        
        if choice == "Add Word":
            modal = AddWordModal(self.cog, self.guild_id)
            await interaction.response.send_modal(modal)
            
        elif choice == "Remove Word":
            modal = RemoveWordModal(self.cog, self.guild_id)
            await interaction.response.send_modal(modal)
            
        elif choice == "Reset Words":
            confirm_view = ConfirmResetView(self.cog, self.guild_id, self)
            embed = discord.Embed(
                title="⚠️ Reset Confirmation",
                description="**Are you sure you want to reset all custom bad words?**\nThis will remove all custom words and keep only the default ones.",
                color=discord.Color.orange()
            )
            await interaction.response.edit_message(embed=embed, view=confirm_view)
            confirm_view.message = await interaction.original_response()

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        main_view = BadWordTrackerView(self.cog, self.guild_id)
        embed = main_view.create_status_embed()
        await interaction.response.edit_message(embed=embed, view=main_view)
        main_view.message = await interaction.original_response()

class ChannelSettingsView(discord.ui.View):
    def __init__(self, cog, guild_id, current_settings, timeout=180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = guild_id
        self.current_settings = current_settings
        self.message = None
        
    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            await self.message.edit(view=self)
    
    @discord.ui.select(
        placeholder="Channel configuration...",
        options=[
            discord.SelectOption(label="Include Channels", description="Select channels to include", emoji="✅"),
            discord.SelectOption(label="Exclude Channels", description="Select channels to exclude", emoji="❌"),
            discord.SelectOption(label="Clear All", description="Reset all channel settings", emoji="🗑️")
        ]
    )
    async def channel_select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
        choice = select.values[0]
        
        if choice == "Include Channels":
            view = PaginatedSelectView(self.cog, self.guild_id, self.current_settings, 'channel', is_exclude=False)
            await view.update_message(interaction)
            view.message = await interaction.original_response()
            
        elif choice == "Exclude Channels":
            view = PaginatedSelectView(self.cog, self.guild_id, self.current_settings, 'channel', is_exclude=True)
            await view.update_message(interaction)
            view.message = await interaction.original_response()
            
        elif choice == "Clear All":
            self.current_settings['included_channels'] = []
            self.current_settings['excluded_channels'] = []
            self.cog.save_guild_settings(self.guild_id, **self.current_settings)
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="✅ Channel Settings Cleared",
                    description="All channel settings have been reset.",
                    color=discord.Color.green()
                ),
                view=self
            )

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = SetupView(self.cog, self.guild_id, self.current_settings)
        embed = discord.Embed(
            title="⚙️ Setup Configuration",
            description="Configure which channels, roles, and users to track.",
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)
        view.message = await interaction.original_response()

class RoleSettingsView(discord.ui.View):
    def __init__(self, cog, guild_id, current_settings, timeout=180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = guild_id
        self.current_settings = current_settings
        self.message = None
        
    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            await self.message.edit(view=self)
    
    @discord.ui.select(
        placeholder="Role configuration...",
        options=[
            discord.SelectOption(label="Include Roles", description="Select roles to include", emoji="✅"),
            discord.SelectOption(label="Exclude Roles", description="Select roles to exclude", emoji="❌"),
            discord.SelectOption(label="Clear All", description="Reset all role settings", emoji="🗑️")
        ]
    )
    async def role_select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
        choice = select.values[0]
        
        if choice == "Include Roles":
            view = PaginatedSelectView(self.cog, self.guild_id, self.current_settings, 'role', is_exclude=False)
            await view.update_message(interaction)
            view.message = await interaction.original_response()
            
        elif choice == "Exclude Roles":
            view = PaginatedSelectView(self.cog, self.guild_id, self.current_settings, 'role', is_exclude=True)
            await view.update_message(interaction)
            view.message = await interaction.original_response()
            
        elif choice == "Clear All":
            self.current_settings['included_roles'] = []
            self.current_settings['excluded_roles'] = []
            self.cog.save_guild_settings(self.guild_id, **self.current_settings)
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="✅ Role Settings Cleared",
                    description="All role settings have been reset.",
                    color=discord.Color.green()
                ),
                view=self
            )

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = SetupView(self.cog, self.guild_id, self.current_settings)
        embed = discord.Embed(
            title="⚙️ Setup Configuration",
            description="Configure which channels, roles, and users to track.",
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)
        view.message = await interaction.original_response()

class UserSettingsView(discord.ui.View):
    def __init__(self, cog, guild_id, current_settings, timeout=180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = guild_id
        self.current_settings = current_settings
        self.message = None
        
    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            await self.message.edit(view=self)
    
    @discord.ui.select(
        placeholder="User configuration...",
        options=[
            discord.SelectOption(label="Include Users", description="Select users to include", emoji="✅"),
            discord.SelectOption(label="Exclude Users", description="Select users to exclude", emoji="❌"),
            discord.SelectOption(label="Clear All", description="Reset all user settings", emoji="🗑️")
        ]
    )
    async def user_select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
        choice = select.values[0]
        
        if choice == "Include Users":
            view = PaginatedSelectView(self.cog, self.guild_id, self.current_settings, 'user', is_exclude=False)
            await view.update_message(interaction)
            view.message = await interaction.original_response()
            
        elif choice == "Exclude Users":
            view = PaginatedSelectView(self.cog, self.guild_id, self.current_settings, 'user', is_exclude=True)
            await view.update_message(interaction)
            view.message = await interaction.original_response()
            
        elif choice == "Clear All":
            self.current_settings['included_users'] = []
            self.current_settings['excluded_users'] = []
            self.cog.save_guild_settings(self.guild_id, **self.current_settings)
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="✅ User Settings Cleared",
                    description="All user settings have been reset.",
                    color=discord.Color.green()
                ),
                view=self
            )

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = SetupView(self.cog, self.guild_id, self.current_settings)
        embed = discord.Embed(
            title="⚙️ Setup Configuration",
            description="Configure which channels, roles, and users to track.",
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)
        view.message = await interaction.original_response()

class ConfirmResetView(discord.ui.View):
    def __init__(self, cog, guild_id, parent_view, timeout=30):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = guild_id
        self.parent_view = parent_view
        self.message = None
        
    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            await self.message.edit(content="❌ Reset confirmation timed out.", view=None)
    
    @discord.ui.button(label="Yes, Reset", style=discord.ButtonStyle.danger)
    async def confirm_reset(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Reset to default words only
        self.cog.save_guild_settings(self.guild_id, bad_words=DEFAULT_BAD_WORDS.copy())
        
        main_view = BadWordTrackerView(self.cog, self.guild_id)
        embed = main_view.create_status_embed()
        await interaction.response.edit_message(
            embed=discord.Embed(
                title="✅ Words Reset",
                description="All custom bad words have been reset to default!",
                color=discord.Color.green()
            ),
            view=main_view
        )
        main_view.message = await interaction.original_response()
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_reset(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ManageWordsView(self.cog, self.guild_id, self.parent_view.current_settings)
        embed = discord.Embed(
            title="📝 Manage Bad Words",
            description="Add, remove, or reset custom bad words.",
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)
        view.message = await interaction.original_response()

class BadWordCounter(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_path = "data/badwordcounter.db"
        self.db_manager = DatabaseManager(self.db_path)

    def get_guild_settings(self, guild_id):
        """Get settings for a specific guild"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT enabled, bad_words, included_channels, excluded_channels, included_roles, excluded_roles, included_users, excluded_users FROM badword_settings WHERE guild_id = ?', (str(guild_id),))
        result = cursor.fetchone()
        
        if result:
            enabled = bool(result[0])
            bad_words = json.loads(result[1])
            included_channels = json.loads(result[2]) if result[2] else []
            excluded_channels = json.loads(result[3]) if result[3] else []
            included_roles = json.loads(result[4]) if result[4] else []
            excluded_roles = json.loads(result[5]) if result[5] else []
            included_users = json.loads(result[6]) if result[6] else []
            excluded_users = json.loads(result[7]) if result[7] else []
        else:
            # Create default settings for this guild
            enabled = False
            bad_words = DEFAULT_BAD_WORDS.copy()
            included_channels = []
            excluded_channels = []
            included_roles = []
            excluded_roles = []
            included_users = []
            excluded_users = []
            
            cursor.execute('''
                INSERT INTO badword_settings (guild_id, enabled, bad_words, included_channels, excluded_channels, included_roles, excluded_roles, included_users, excluded_users) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (str(guild_id), enabled, json.dumps(bad_words), json.dumps(included_channels), json.dumps(excluded_channels), json.dumps(included_roles), json.dumps(excluded_roles), json.dumps(included_users), json.dumps(excluded_users)))
            conn.commit()
        
        conn.close()
        
        return {
            "enabled": enabled, 
            "bad_words": bad_words,
            "included_channels": included_channels,
            "excluded_channels": excluded_channels,
            "included_roles": included_roles,
            "excluded_roles": excluded_roles,
            "included_users": included_users,
            "excluded_users": excluded_users
        }

    def save_guild_settings(self, guild_id, enabled=None, bad_words=None, **kwargs):
        """Save settings for a specific guild"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        
        current = self.get_guild_settings(guild_id)
        
        if enabled is not None:
            current["enabled"] = enabled
        if bad_words is not None:
            current["bad_words"] = bad_words
            
        # Update any additional settings
        for key, value in kwargs.items():
            if key in current:
                current[key] = value
        
        cursor.execute('''
            INSERT OR REPLACE INTO badword_settings 
            (guild_id, enabled, bad_words, included_channels, excluded_channels, included_roles, excluded_roles, included_users, excluded_users, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            str(guild_id), 
            current["enabled"], 
            json.dumps(current["bad_words"]),
            json.dumps(current["included_channels"]),
            json.dumps(current["excluded_channels"]),
            json.dumps(current["included_roles"]),
            json.dumps(current["excluded_roles"]),
            json.dumps(current["included_users"]),
            json.dumps(current["excluded_users"])
        ))
        
        conn.commit()
        conn.close()

    def normalize_word(self, word):
        """Normalize a word - FIXED VERSION"""
        # First remove punctuation and convert to lowercase
        clean_word = re.sub(r'[^\w\s]', '', word.lower())
        
        # Only reduce excessive repetitions (3+ characters), not normal duplicates
        # This preserves words like "ass", "pussy", "hell" while catching "fuuuuck"
        normalized = re.sub(r'(.)\1{2,}', r'\1', clean_word)
        return normalized

    def get_bad_words_in_message(self, text, bad_words_list):
        """Check if message contains bad words and return them - FIXED VERSION"""
        # Create normalized versions of bad words for comparison
        normalized_bad_words = [self.normalize_word(bad_word) for bad_word in bad_words_list]
        
        # Split by word boundaries to avoid detecting words within other words
        words = re.findall(r'\b\w+\b', text.lower())
        
        found_bad_words = []
        for word in words:
            normalized = self.normalize_word(word)
            if normalized in normalized_bad_words:
                # Find which original bad word matched
                original_bad_word = bad_words_list[normalized_bad_words.index(normalized)]
                found_bad_words.append(original_bad_word)
        
        return found_bad_words

    def update_stats(self, guild_id, user_id, username, count):
        """Update bad word statistics for specific guild"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO badword_stats (guild_id, user_id, username, bad_word_count, last_said)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (str(guild_id), str(user_id), username, count))
        
        conn.commit()
        conn.close()

    def get_user_count(self, guild_id, user_id):
        """Get bad word count for a user in specific guild"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT bad_word_count FROM badword_stats WHERE guild_id = ? AND user_id = ?', 
                      (str(guild_id), str(user_id)))
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result else 0

    def should_track_message(self, message, settings):
        """Check if a message should be tracked based on channel/role/user settings"""
        # Check channel settings
        channel_id = str(message.channel.id)
        if settings['included_channels'] and channel_id not in settings['included_channels']:
            return False
        if settings['excluded_channels'] and channel_id in settings['excluded_channels']:
            return False
            
        # Check role settings
        if settings['included_roles']:
            user_roles = [str(role.id) for role in message.author.roles]
            if not any(role_id in user_roles for role_id in settings['included_roles']):
                return False
        if settings['excluded_roles']:
            user_roles = [str(role.id) for role in message.author.roles]
            if any(role_id in user_roles for role_id in settings['excluded_roles']):
                return False
                
        # Check user settings
        user_id = str(message.author.id)
        if settings['included_users'] and user_id not in settings['included_users']:
            return False
        if settings['excluded_users'] and user_id in settings['excluded_users']:
            return False
            
        return True

    @commands.Cog.listener()
    async def on_message(self, message):
        """Listen for bad words in messages"""
        if message.author == self.bot.user or not message.content:
            return
        
        # Don't ignore commands - count everyone as requested
        if not message.guild:  # Skip DMs
            return
            
        guild_settings = self.get_guild_settings(message.guild.id)
        
        if not guild_settings["enabled"]:
            return
            
        # Check if message should be tracked based on settings
        if not self.should_track_message(message, guild_settings):
            return
        
        found_bad_words = self.get_bad_words_in_message(message.content, guild_settings["bad_words"])
        
        if found_bad_words:
            # Update stats
            current_count = self.get_user_count(message.guild.id, message.author.id)
            new_count = current_count + len(found_bad_words)
            self.update_stats(message.guild.id, message.author.id, message.author.display_name, new_count)
            
            # Send public message
            bad_words_str = ", ".join(set(found_bad_words))
            embed = discord.Embed(
                title="🚨 Bad Word Detected!",
                description=f"{message.author.mention} said a bad word! **{bad_words_str}**",
                color=discord.Color.red()
            )
            embed.add_field(name="📊 Total Count", value=f"**{new_count}** bad word(s) said", inline=False)
            
            await message.channel.send(embed=embed)

    @commands.Cog.listener()
    async def on_member_remove(self, member):
        """Reset bad word count when a user leaves the server"""
        guild_id = member.guild.id
        user_id = member.id
        
        # Reset user count to 0
        self.update_stats(guild_id, user_id, member.display_name, 0)

    async def check_permissions(self, interaction: discord.Interaction):
        """Check if user has permission for admin commands"""
        if is_developer(interaction.user.id):
            return True
        if interaction.user.guild_permissions.administrator:
            return True
        return False

    async def check_permissions_ctx(self, ctx):
        """Check if user has permission for admin commands (for text commands)"""
        if is_developer(ctx.author.id):
            return True
        if ctx.author.guild_permissions.administrator:
            return True
        return False

    async def create_leaderboard_embed(self, guild_id, user_id):
        """Create leaderboard embed without sending it"""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        
        # Most bad words in this guild
        cursor.execute('''
            SELECT username, bad_word_count 
            FROM badword_stats 
            WHERE guild_id = ? AND bad_word_count > 0 
            ORDER BY bad_word_count DESC 
            LIMIT 10
        ''', (str(guild_id),))
        most_results = cursor.fetchall()
        
        conn.close()
        
        embed = discord.Embed(title="🏆 Bad Word Leaderboard", color=discord.Color.gold())
        
        if most_results:
            most_text = "\n".join([f"{i}. **{name}**: {count} bad word(s)" for i, (name, count) in enumerate(most_results, 1)])
            embed.add_field(name="🔴 Most Notorious", value=most_text, inline=False)
        else:
            embed.add_field(name="🔴 Most Notorious", value="No bad words recorded yet!", inline=False)
        
        # Add current user's position if they have any counts
        user_count = self.get_user_count(guild_id, user_id)
        if user_count > 0:
            embed.add_field(
                name="📊 Your Stats", 
                value=f"You've said **{user_count}** bad word(s) in this server!", 
                inline=False
            )
        
        return embed

    async def show_leaderboard(self, interaction: discord.Interaction = None, ctx: commands.Context = None):
        """Show leaderboard (works for both interaction and context)"""
        guild_id = interaction.guild.id if interaction else ctx.guild.id
        user_id = interaction.user.id if interaction else ctx.author.id
        
        embed = await self.create_leaderboard_embed(guild_id, user_id)
        
        if interaction:
            await interaction.response.send_message(embed=embed)
        else:
            await ctx.send(embed=embed)

    # Text Commands
    @commands.group(name="bwt", invoke_without_command=True)
    async def bwt(self, ctx):
        """Bad Word Tracker main command"""
        if ctx.invoked_subcommand is None:
            await ctx.send("**Bad Word Tracker Commands:**\n"
                          "`!bwt on` - Enable tracking\n"
                          "`!bwt off` - Disable tracking\n"
                          "`!bwt lb` - Show leaderboard\n"
                          "`!bwt add <word>` - Add a word\n"
                          "`!bwt remove <word>` - Remove a word\n"
                          "`!bwt list` - Show word list\n"
                          "`!bwt reset @user` - Reset user's count\n"
                          "`!bwt reset list` - Reset to default words")

    @bwt.command(name="on")
    async def bwt_on(self, ctx):
        """Enable bad word tracking"""
        if not await self.check_permissions_ctx(ctx):
            await ctx.send("❌ You need administrator permissions or developer access.")
            return
            
        self.save_guild_settings(ctx.guild.id, enabled=True)
        await ctx.send("✅ Bad word counter turned **on**!")

    @bwt.command(name="off")
    async def bwt_off(self, ctx):
        """Disable bad word tracking"""
        if not await self.check_permissions_ctx(ctx):
            await ctx.send("❌ You need administrator permissions or developer access.")
            return
            
        self.save_guild_settings(ctx.guild.id, enabled=False)
        await ctx.send("✅ Bad word counter turned **off**!")

    @bwt.command(name="lb")
    async def bwt_lb(self, ctx):
        """Show leaderboard"""
        await self.show_leaderboard(ctx=ctx)

    @bwt.command(name="add")
    async def bwt_add(self, ctx, *, word: str):
        """Add a word to the bad words list"""
        if not await self.check_permissions_ctx(ctx):
            await ctx.send("❌ You need administrator permissions or developer access.")
            return
            
        word = word.strip()
        if not word:
            await ctx.send("❌ Please specify a word to add.")
            return
            
        settings = self.get_guild_settings(ctx.guild.id)
        normalized_word = self.normalize_word(word)
        
        if normalized_word in [self.normalize_word(w) for w in settings["bad_words"]]:
            await ctx.send(f"❌ **{word}** is already in the list!")
            return
        
        settings["bad_words"].append(word)
        self.save_guild_settings(ctx.guild.id, bad_words=settings["bad_words"])
        
        await ctx.send(f"✅ Added **{word}** to bad words list!")

    @bwt.command(name="remove")
    async def bwt_remove(self, ctx, *, word: str):
        """Remove a word from the bad words list"""
        if not await self.check_permissions_ctx(ctx):
            await ctx.send("❌ You need administrator permissions or developer access.")
            return
            
        word = word.strip()
        if not word:
            await ctx.send("❌ Please specify a word to remove.")
            return
            
        settings = self.get_guild_settings(ctx.guild.id)
        normalized_word = self.normalize_word(word)
        normalized_bad_words = [self.normalize_word(w) for w in settings["bad_words"]]
        
        if normalized_word not in normalized_bad_words:
            await ctx.send(f"❌ **{word}** is not in the list!")
            return
        
        index_to_remove = normalized_bad_words.index(normalized_word)
        removed_word = settings["bad_words"][index_to_remove]
        settings["bad_words"].pop(index_to_remove)
        self.save_guild_settings(ctx.guild.id, bad_words=settings["bad_words"])
        
        await ctx.send(f"✅ Removed **{removed_word}** from bad words list!")

    @bwt.command(name="list")
    async def bwt_list(self, ctx):
        """Show the bad words list"""
        if not await self.check_permissions_ctx(ctx):
            await ctx.send("❌ You need administrator permissions or developer access.")
            return
            
        settings = self.get_guild_settings(ctx.guild.id)
        words_text = ", ".join(sorted(settings["bad_words"]))
        
        if len(words_text) > 1000:
            words_text = words_text[:1000] + "..."
            
        embed = discord.Embed(
            title="📋 Bad Words List",
            description=words_text,
            color=discord.Color.blue()
        )
        embed.set_footer(text=f"Total: {len(settings['bad_words'])} words")
        await ctx.send(embed=embed)

    @bwt.command(name="reset")
    async def bwt_reset(self, ctx, *, target: str = None):
        """Reset user's count or reset word list"""
        if not await self.check_permissions_ctx(ctx):
            await ctx.send("❌ You need administrator permissions or developer access.")
            return
            
        if not target:
            await ctx.send("❌ Please specify a user to reset or 'list' to reset word list.")
            return
            
        if target.lower() == "list":
            # Reset to default words only
            self.save_guild_settings(ctx.guild.id, bad_words=DEFAULT_BAD_WORDS.copy())
            await ctx.send("✅ All custom bad words have been reset to default!")
        else:
            # Reset user
            user_id = target
            if user_id.startswith('<@') and user_id.endswith('>'):
                user_id = user_id[2:-1]
                if user_id.startswith('!'):
                    user_id = user_id[1:]
            
            # Reset user count to 0
            self.update_stats(ctx.guild.id, user_id, "Reset User", 0)
            await ctx.send(f"✅ Reset bad word count for user <@{user_id}>!")

    # Single Slash Command - Interactive Setup Wizard
    @app_commands.command(name="badwordtracker", description="Manage the bad word counter with an interactive menu")
    async def badwordtracker(self, interaction: discord.Interaction):
        """Main command to open the interactive setup wizard"""
        if not await self.check_permissions(interaction):
            await interaction.response.send_message("❌ You need administrator permissions or developer access.", ephemeral=True)
            return
            
        view = BadWordTrackerView(self, interaction.guild.id)
        embed = view.create_status_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()


async def setup(bot):
    cog = BadWordCounter(bot)
    await bot.add_cog(cog)
    print("BadWordCounter cog loaded!")
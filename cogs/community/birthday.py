from logging import config
import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite
import asyncio
import datetime
import json
import pytz
from dateutil import parser
from typing import Optional, List, Dict, Tuple, Any
import re
import os
import io
import csv
from collections import defaultdict
import calendar
from math import ceil
from cachetools import TTLCache
import logging
import random

# Configuration
DEVELOPER_ID = int(os.getenv('DEVELOPER_ID', '0'))
if DEVELOPER_ID == 0:
    print("Warning: DEVELOPER_ID not set. Developer features disabled.")

CACHE_TTL = 300
DEFAULT_TIMEZONE = 'UTC'
DEFAULT_ANNOUNCEMENT_HOUR = 12
RATE_LIMITS = {
    'user_set_birthday': 86400,
    'admin_set_birthday': 3600,
    'user_remove': 3600,
    'admin_remove': 300,
}

# Constants
MAX_BIRTHDAY_LIST_PAGE_SIZE = 25
MAX_UPCOMING_BIRTHDAYS_DISPLAY = 10
CONFIRMATION_MESSAGE_LIFETIME = 60
MAX_CACHE_SIZE_CONFIG = 1000
MAX_CACHE_SIZE_BIRTHDAYS = 5000

# Age milestones
AGE_MILESTONES = {
    1: "🎀 First birthday!",
    5: "🎠 Five years old!",
    10: "🔟 Double digits!",
    13: "📱 Teenager now!",
    16: "🎯 Sweet sixteen!",
    18: "🎊 Legal adult!",
    21: "🍾 Twenty-one!",
    30: "🎉 Dirty thirty!",
    40: "🦋 Forty and fabulous!",
    50: "🌟 Golden birthday!",
    60: "💎 Diamond jubilee!",
    70: "👑 Platinum years!",
    80: "🎪 Eighty and amazing!",
    90: "💫 Ninety and noble!",
    100: "💯 Centenarian! What a milestone!"
}

# Allowed configuration columns for SQL injection protection
ALLOWED_CONFIG_COLUMNS = frozenset([
    'birthday_channel_id', 'announcement_channel_id', 'default_message',
    'clean_toggle', 'reminder_roles', 'reminder_users', 'instructions_message_id',
    'timezone', 'birthday_role_id', 'enabled', 'announcement_hour',
    'announcement_image', 'announcement_gif', 'enable_age_milestones',
    'enable_birthday_streaks'
])

# Set up logging
logger = logging.getLogger('BirthdayBot')

class InstructionsView(discord.ui.View):
    """Persistent view for the instructions panel that never stops working"""
    def __init__(self, cog, guild_id):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id

    @discord.ui.button(label="🎂 Birthdays", style=discord.ButtonStyle.primary, custom_id="birthdays_button")
    async def view_birthdays(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.cog.show_birthday_list(interaction, self.guild_id)
        except Exception as e:
            await self.cog.log_error(str(e), "view_birthdays_button")
            try:
                await interaction.response.send_message("❌ Error showing birthdays", ephemeral=True)
            except discord.NotFound:
                pass

    @discord.ui.button(label="⏰ Next Birthday", style=discord.ButtonStyle.primary, custom_id="next_birthday_button")
    async def next_birthday(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.cog.show_next_birthday(interaction, self.guild_id)
        except Exception as e:
            await self.cog.log_error(str(e), "next_birthday_button")
            try:
                await interaction.response.send_message("❌ Error showing next birthday", ephemeral=True)
            except discord.NotFound:
                pass

    @discord.ui.button(label="🌍 Timezones", style=discord.ButtonStyle.secondary, custom_id="timezones_button")
    async def view_timezones(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.cog.show_timezone_list(interaction)
        except Exception as e:
            await self.cog.log_error(str(e), "view_timezones_button")
            try:
                await interaction.response.send_message("❌ Error showing timezones", ephemeral=True)
            except discord.NotFound:
                pass

    @discord.ui.button(label="📖 Full Instructions", style=discord.ButtonStyle.secondary, custom_id="instructions_button")
    async def full_instructions(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.cog.show_full_instructions(interaction)
        except Exception as e:
            await self.cog.log_error(str(e), "full_instructions_button")
            try:
                await interaction.response.send_message("❌ Error showing instructions", ephemeral=True)
            except discord.NotFound:
                pass

class BirthdaySetupView(discord.ui.View):
    def __init__(self, cog, guild_id):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild_id = guild_id

    @discord.ui.select(
        placeholder="Select a configuration category...",
        options=[
            discord.SelectOption(label="Channel Setup", description="Configure birthday channels", emoji="📁"),
            discord.SelectOption(label="Message Customization", description="Customize birthday messages", emoji="💬"),
            discord.SelectOption(label="Reminder Settings", description="Configure birthday reminders", emoji="⏰"),
            discord.SelectOption(label="Timezone & Roles", description="Set timezone and birthday roles", emoji="🌍"),
            discord.SelectOption(label="Media & Appearance", description="Set images, GIFs, and appearance", emoji="🖼️"),
            discord.SelectOption(label="Statistics & Calendar", description="View birthday statistics", emoji="📊"),
            discord.SelectOption(label="View Current Settings", description="See current configuration", emoji="🔍"),
            discord.SelectOption(label="Test Functionality", description="Test birthday features", emoji="🧪"),
            discord.SelectOption(label="Import/Export", description="Backup and restore birthdays", emoji="💾"),
            discord.SelectOption(label="Management", description="Transfer channels or stop the bot", emoji="⚙️"),
        ]
    )
    async def select_category(self, interaction: discord.Interaction, select: discord.ui.Select):
        category = select.values[0]

        if category == "Channel Setup":
            embed = await self.cog.create_channel_setup_embed(self.guild_id)
            view = ChannelSetupView(self.cog, self.guild_id, self)
        elif category == "Message Customization":
            embed = await self.cog.create_message_customization_embed(self.guild_id)
            view = MessageCustomizationView(self.cog, self.guild_id, self)
        elif category == "Reminder Settings":
            embed = await self.cog.create_reminder_settings_embed(self.guild_id)
            view = ReminderSettingsView(self.cog, self.guild_id, self)
        elif category == "Timezone & Roles":
            embed = await self.cog.create_timezone_roles_embed(self.guild_id)
            view = TimezoneRolesView(self.cog, self.guild_id, self)
        elif category == "Media & Appearance":
            embed = await self.cog.create_media_appearance_embed(self.guild_id)
            view = MediaAppearanceView(self.cog, self.guild_id, self)
        elif category == "Statistics & Calendar":
            embed = await self.cog.create_statistics_embed(self.guild_id)
            view = StatisticsView(self.cog, self.guild_id, self)
        elif category == "View Current Settings":
            embed = await self.cog.create_current_settings_embed(self.guild_id)
            view = self
        elif category == "Test Functionality":
            embed = await self.cog.create_test_embed(self.guild_id)
            view = TestFunctionalityView(self.cog, self.guild_id, self)
        elif category == "Import/Export":
            embed = await self.cog.create_import_export_embed(self.guild_id)
            view = ImportExportView(self.cog, self.guild_id, self)
        elif category == "Management":
            embed = await self.cog.create_management_embed(self.guild_id)
            view = ManagementView(self.cog, self.guild_id, self)

        await interaction.response.edit_message(embed=embed, view=view)

    async def on_timeout(self):
        try:
            for item in self.children:
                item.disabled = True
            if hasattr(self, 'message'):
                await self.message.edit(view=self)
        except:
            pass

class ChannelSetupView(discord.ui.View):
    def __init__(self, cog, guild_id, parent_view):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild_id = guild_id
        self.parent_view = parent_view
        self.birthday_page = 0
        self.announcement_page = 0
        self.update_dropdowns()

    @discord.ui.select(
        placeholder="Select birthday channel...",
        options=[],
        custom_id="birthday_channel_select"
    )
    async def select_birthday_channel(self, interaction: discord.Interaction, select: discord.ui.Select):
        if select.values[0] == "next_page":
            self.birthday_page += 1
            self.update_dropdowns()
            embed = await self.cog.create_channel_setup_embed(self.guild_id)
            await interaction.response.edit_message(embed=embed, view=self)
            return
        elif select.values[0] == "prev_page":
            self.birthday_page = max(0, self.birthday_page - 1)
            self.update_dropdowns()
            embed = await self.cog.create_channel_setup_embed(self.guild_id)
            await interaction.response.edit_message(embed=embed, view=self)
            return
        elif select.values[0] == "manual_id":
            await interaction.response.send_modal(ManualIDModal(self.cog, self.guild_id, 'birthday'))
            return
            
        channel_id = int(select.values[0])
        # FIX: Don't call response.edit_message here, let set_birthday_channel handle it
        await self.cog.set_birthday_channel(interaction, self.guild_id, channel_id)

    @discord.ui.select(
        placeholder="Select announcement channel...",
        options=[],
        custom_id="announcement_channel_select"
    )
    async def select_announcement_channel(self, interaction: discord.Interaction, select: discord.ui.Select):
        if select.values[0] == "next_page":
            self.announcement_page += 1
            self.update_dropdowns()
            embed = await self.cog.create_channel_setup_embed(self.guild_id)
            await interaction.response.edit_message(embed=embed, view=self)
            return
        elif select.values[0] == "prev_page":
            self.announcement_page = max(0, self.announcement_page - 1)
            self.update_dropdowns()
            embed = await self.cog.create_channel_setup_embed(self.guild_id)
            await interaction.response.edit_message(embed=embed, view=self)
            return
        elif select.values[0] == "manual_id":
            await interaction.response.send_modal(ManualIDModal(self.cog, self.guild_id, 'announcement'))
            return
            
        channel_id = int(select.values[0])
        # FIX: Don't call response.edit_message here, let set_announcement_channel handle it
        await self.cog.set_announcement_channel(interaction, self.guild_id, channel_id)

    @discord.ui.button(label="🔄 Refresh Channel List", style=discord.ButtonStyle.secondary)
    async def refresh_channels(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.birthday_page = 0
        self.announcement_page = 0
        self.update_dropdowns()
        embed = await self.cog.create_channel_setup_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="🧹 Clean Birthday Channel", style=discord.ButtonStyle.danger)
    async def clean_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.clean_birthday_channel(interaction, self.guild_id)

    @discord.ui.button(label="⬅️ Back to Main", style=discord.ButtonStyle.secondary)
    async def back_to_main(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog.create_main_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self.parent_view)

    def update_dropdowns(self):
        """Update dropdown options with pagination"""
        guild = self.cog.bot.get_guild(self.guild_id)
        if not guild:
            return

        text_channels = [channel for channel in guild.text_channels if isinstance(channel, discord.TextChannel)]
        
        # Update birthday channel dropdown with pagination
        birthday_select = discord.utils.get(self.children, custom_id="birthday_channel_select")
        if birthday_select:
            options = []
            
            # Add navigation options if needed
            if self.birthday_page > 0:
                options.append(discord.SelectOption(label="⬅️ Previous Page", value="prev_page", description="View previous channels"))
            
            # Add channel options for current page
            start_idx = self.birthday_page * 20
            end_idx = start_idx + 20
            page_channels = text_channels[start_idx:end_idx]
            
            for channel in page_channels:
                options.append(discord.SelectOption(
                    label=f"#{channel.name}"[:25],
                    value=str(channel.id),
                    description=f"ID: {channel.id}"[:50]
                ))
            
            # Add next page option if there are more channels
            if end_idx < len(text_channels):
                options.append(discord.SelectOption(label="➡️ Next Page", value="next_page", description="View more channels"))
            
            # Add manual ID option
            options.append(discord.SelectOption(label="🔢 Enter Channel ID", value="manual_id", description="Manually enter channel ID"))
            
            if not options:
                options = [discord.SelectOption(label="No channels available", value="0")]
                
            birthday_select.options = options

        # Update announcement channel dropdown with pagination
        announcement_select = discord.utils.get(self.children, custom_id="announcement_channel_select")
        if announcement_select:
            options = []
            
            # Add navigation options if needed
            if self.announcement_page > 0:
                options.append(discord.SelectOption(label="⬅️ Previous Page", value="prev_page", description="View previous channels"))
            
            # Add channel options for current page
            start_idx = self.announcement_page * 20
            end_idx = start_idx + 20
            page_channels = text_channels[start_idx:end_idx]
            
            for channel in page_channels:
                options.append(discord.SelectOption(
                    label=f"#{channel.name}"[:25],
                    value=str(channel.id),
                    description=f"ID: {channel.id}"[:50]
                ))
            
            # Add next page option if there are more channels
            if end_idx < len(text_channels):
                options.append(discord.SelectOption(label="➡️ Next Page", value="next_page", description="View more channels"))
            
            # Add manual ID option
            options.append(discord.SelectOption(label="🔢 Enter Channel ID", value="manual_id", description="Manually enter channel ID"))
            
            if not options:
                options = [discord.SelectOption(label="No channels available", value="0")]
                
            announcement_select.options = options

    async def on_timeout(self):
        try:
            for item in self.children:
                item.disabled = True
            if hasattr(self, 'message'):
                await self.message.edit(view=self)
        except:
            pass

class TimezoneRolesView(discord.ui.View):
    def __init__(self, cog, guild_id, parent_view):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild_id = guild_id
        self.parent_view = parent_view
        self.role_page = 0
        self.update_role_dropdown()

    @discord.ui.select(
        placeholder="Select birthday role...",
        options=[],
        custom_id="birthday_role_select"
    )
    async def select_birthday_role(self, interaction: discord.Interaction, select: discord.ui.Select):
        if select.values[0] == "next_page":
            self.role_page += 1
            self.update_role_dropdown()
            embed = await self.cog.create_timezone_roles_embed(self.guild_id)
            await interaction.response.edit_message(embed=embed, view=self)
            return
        elif select.values[0] == "prev_page":
            self.role_page = max(0, self.role_page - 1)
            self.update_role_dropdown()
            embed = await self.cog.create_timezone_roles_embed(self.guild_id)
            await interaction.response.edit_message(embed=embed, view=self)
            return
        elif select.values[0] == "manual_id":
            await interaction.response.send_modal(ManualRoleIDModal(self.cog, self.guild_id))
            return
            
        role_id = int(select.values[0])
        await self.cog.set_birthday_role(interaction, self.guild_id, role_id)

    @discord.ui.button(label="Set Server Timezone", style=discord.ButtonStyle.primary, emoji="🌍")
    async def set_timezone(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SetTimezoneModal(self.cog, self.guild_id))

    @discord.ui.button(label="🔄 Refresh Role List", style=discord.ButtonStyle.secondary)
    async def refresh_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.role_page = 0
        self.update_role_dropdown()
        embed = await self.cog.create_timezone_roles_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="⬅️ Back to Main", style=discord.ButtonStyle.secondary)
    async def back_to_main(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog.create_main_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self.parent_view)

    def update_role_dropdown(self):
        """Update role dropdown options with pagination"""
        guild = self.cog.bot.get_guild(self.guild_id)
        if not guild:
            return

        roles = [role for role in guild.roles if role != guild.default_role]
        
        role_select = discord.utils.get(self.children, custom_id="birthday_role_select")
        if role_select:
            options = []
            
            # Add navigation options if needed
            if self.role_page > 0:
                options.append(discord.SelectOption(label="⬅️ Previous Page", value="prev_page", description="View previous roles"))
            
            # Add role options for current page
            start_idx = self.role_page * 20
            end_idx = start_idx + 20
            page_roles = roles[start_idx:end_idx]
            
            for role in page_roles:
                options.append(discord.SelectOption(
                    label=f"@{role.name}"[:25],
                    value=str(role.id),
                    description="Role"
                ))
            
            # Add next page option if there are more roles
            if end_idx < len(roles):
                options.append(discord.SelectOption(label="➡️ Next Page", value="next_page", description="View more roles"))
            
            # Add manual ID option
            options.append(discord.SelectOption(label="🔢 Enter Role ID", value="manual_id", description="Manually enter role ID"))
            
            if not options:
                options = [discord.SelectOption(label="No roles available", value="0")]
                
            role_select.options = options

    async def on_timeout(self):
        try:
            for item in self.children:
                item.disabled = True
            if hasattr(self, 'message'):
                await self.message.edit(view=self)
        except:
            pass

class ReminderSettingsView(discord.ui.View):
    def __init__(self, cog, guild_id, parent_view):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild_id = guild_id
        self.parent_view = parent_view
        self.role_page = 0
        self.user_page = 0
        self.update_dropdowns()

    @discord.ui.select(
        placeholder="Select role for reminders...",
        options=[],
        custom_id="reminder_role_select"
    )
    async def select_reminder_role(self, interaction: discord.Interaction, select: discord.ui.Select):
        if select.values[0] == "next_page":
            self.role_page += 1
            self.update_dropdowns()
            embed = await self.cog.create_reminder_settings_embed(self.guild_id)
            await interaction.response.edit_message(embed=embed, view=self)
            return
        elif select.values[0] == "prev_page":
            self.role_page = max(0, self.role_page - 1)
            self.update_dropdowns()
            embed = await self.cog.create_reminder_settings_embed(self.guild_id)
            await interaction.response.edit_message(embed=embed, view=self)
            return
        elif select.values[0] == "manual_id":
            await interaction.response.send_modal(ManualReminderRoleIDModal(self.cog, self.guild_id))
            return
            
        role_id = int(select.values[0])
        await self.cog.add_reminder_role(interaction, self.guild_id, role_id)

    @discord.ui.select(
        placeholder="Select user for reminders...",
        options=[],
        custom_id="reminder_user_select"
    )
    async def select_reminder_user(self, interaction: discord.Interaction, select: discord.ui.Select):
        if select.values[0] == "next_page":
            self.user_page += 1
            self.update_dropdowns()
            embed = await self.cog.create_reminder_settings_embed(self.guild_id)
            await interaction.response.edit_message(embed=embed, view=self)
            return
        elif select.values[0] == "prev_page":
            self.user_page = max(0, self.user_page - 1)
            self.update_dropdowns()
            embed = await self.cog.create_reminder_settings_embed(self.guild_id)
            await interaction.response.edit_message(embed=embed, view=self)
            return
        elif select.values[0] == "manual_id":
            await interaction.response.send_modal(ManualReminderUserIDModal(self.cog, self.guild_id))
            return
            
        user_id = int(select.values[0])
        await self.cog.add_reminder_user(interaction, self.guild_id, user_id)

    @discord.ui.button(label="🔄 Refresh Lists", style=discord.ButtonStyle.secondary)
    async def refresh_lists(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.role_page = 0
        self.user_page = 0
        self.update_dropdowns()
        embed = await self.cog.create_reminder_settings_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="❌ Remove Reminder", style=discord.ButtonStyle.danger)
    async def remove_reminder(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_removal_selection(interaction, self.guild_id)

    @discord.ui.button(label="⬅️ Back to Main", style=discord.ButtonStyle.secondary)
    async def back_to_main(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog.create_main_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self.parent_view)

    def update_dropdowns(self):
        """Update dropdown options with pagination"""
        guild = self.cog.bot.get_guild(self.guild_id)
        if not guild:
            role_select = discord.utils.get(self.children, custom_id="reminder_role_select")
            user_select = discord.utils.get(self.children, custom_id="reminder_user_select")
            
            if role_select:
                role_select.options = [discord.SelectOption(label="Guild not found", value="0")]
            if user_select:
                user_select.options = [discord.SelectOption(label="Guild not found", value="0")]
            return

        # Update role dropdown with pagination
        roles = [role for role in guild.roles if role != guild.default_role]
        role_select = discord.utils.get(self.children, custom_id="reminder_role_select")
        if role_select:
            options = []
            
            # Add navigation options if needed
            if self.role_page > 0:
                options.append(discord.SelectOption(label="⬅️ Previous Page", value="prev_page", description="View previous roles"))
            
            # Add role options for current page
            start_idx = self.role_page * 20
            end_idx = start_idx + 20
            page_roles = roles[start_idx:end_idx]
            
            for role in page_roles:
                options.append(discord.SelectOption(
                    label=f"@{role.name}"[:25],
                    value=str(role.id),
                    description=f"Role: {len(role.members)} members"[:50]
                ))
            
            # Add next page option if there are more roles
            if end_idx < len(roles):
                options.append(discord.SelectOption(label="➡️ Next Page", value="next_page", description="View more roles"))
            
            # Add manual ID option
            options.append(discord.SelectOption(label="🔢 Enter Role ID", value="manual_id", description="Manually enter role ID"))
            
            if not options:
                options = [discord.SelectOption(label="No roles available", value="0")]
                
            role_select.options = options

        # Update user dropdown with pagination
        members = [member for member in guild.members if not member.bot]
        user_select = discord.utils.get(self.children, custom_id="reminder_user_select")
        if user_select:
            options = []
            
            # Add navigation options if needed
            if self.user_page > 0:
                options.append(discord.SelectOption(label="⬅️ Previous Page", value="prev_page", description="View previous users"))
            
            # Add user options for current page
            start_idx = self.user_page * 20
            end_idx = start_idx + 20
            page_users = members[start_idx:end_idx]
            
            for user in page_users:
                options.append(discord.SelectOption(
                    label=f"👤 {user.display_name}"[:25],
                    value=str(user.id),
                    description=f"User: {user.name}"[:50]
                ))
            
            # Add next page option if there are more users
            if end_idx < len(members):
                options.append(discord.SelectOption(label="➡️ Next Page", value="next_page", description="View more users"))
            
            # Add manual ID option
            options.append(discord.SelectOption(label="🔢 Enter User ID", value="manual_id", description="Manually enter user ID"))
            
            if not options:
                options = [discord.SelectOption(label="No users available", value="0")]
                
            user_select.options = options

    async def on_timeout(self):
        try:
            for item in self.children:
                item.disabled = True
            if hasattr(self, 'message'):
                await self.message.edit(view=self)
        except:
            pass

# Add these new modal classes for reminder manual ID input
class ManualReminderRoleIDModal(discord.ui.Modal, title='Enter Role ID for Reminders'):
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id

    role_id = discord.ui.TextInput(
        label='Role ID',
        placeholder='Enter the role ID to add to reminders...',
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            role_id = int(self.role_id.value)
            await self.cog.add_reminder_role(interaction, self.guild_id, role_id)
        except ValueError:
            await interaction.response.send_message("❌ Invalid role ID!", ephemeral=True)

class ManualReminderUserIDModal(discord.ui.Modal, title='Enter User ID for Reminders'):
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id

    user_id = discord.ui.TextInput(
        label='User ID',
        placeholder='Enter the user ID to add to reminders...',
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            user_id = int(self.user_id.value)
            await self.cog.add_reminder_user(interaction, self.guild_id, user_id)
        except ValueError:
            await interaction.response.send_message("❌ Invalid user ID!", ephemeral=True)
        
class MediaAppearanceView(discord.ui.View):
    def __init__(self, cog, guild_id, parent_view):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild_id = guild_id
        self.parent_view = parent_view

    @discord.ui.button(label="Set Announcement Image", style=discord.ButtonStyle.primary, emoji="🖼️")
    async def set_image(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SetImageModal(self.cog, self.guild_id))

    @discord.ui.button(label="Set Announcement GIF", style=discord.ButtonStyle.primary, emoji="🎬")
    async def set_gif(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SetGIFModal(self.cog, self.guild_id))

    @discord.ui.button(label="Set Announcement Time", style=discord.ButtonStyle.primary, emoji="⏰")
    async def set_announcement_time(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SetAnnouncementTimeModal(self.cog, self.guild_id))

    @discord.ui.button(label="Toggle Age Milestones", style=discord.ButtonStyle.secondary, emoji="🎯")
    async def toggle_milestones(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.toggle_age_milestones(interaction, self.guild_id)

    @discord.ui.button(label="Toggle Birthday Streaks", style=discord.ButtonStyle.secondary, emoji="🔥")
    async def toggle_streaks(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.toggle_birthday_streaks(interaction, self.guild_id)

    @discord.ui.button(label="View Age Milestones", style=discord.ButtonStyle.secondary, emoji="📋")
    async def view_milestones(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_age_milestones_list(interaction)

    @discord.ui.button(label="⬅️ Back to Main", style=discord.ButtonStyle.secondary)
    async def back_to_main(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog.create_main_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self.parent_view)

    async def on_timeout(self):
        try:
            for item in self.children:
                item.disabled = True
            if hasattr(self, 'message'):
                await self.message.edit(view=self)
        except:
            pass

class MessageCustomizationView(discord.ui.View):
    def __init__(self, cog, guild_id, parent_view):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild_id = guild_id
        self.parent_view = parent_view

    @discord.ui.button(label="Set Default Message", style=discord.ButtonStyle.primary, emoji="💬")
    async def set_default_message(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SetDefaultMessageModal(self.cog, self.guild_id))

    @discord.ui.button(label="Set User Message", style=discord.ButtonStyle.primary, emoji="👤")
    async def set_user_message(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SetUserMessageModal(self.cog, self.guild_id))

    @discord.ui.button(label="⬅️ Back to Main", style=discord.ButtonStyle.secondary)
    async def back_to_main(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog.create_main_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self.parent_view)

    async def on_timeout(self):
        try:
            for item in self.children:
                item.disabled = True
            if hasattr(self, 'message'):
                await self.message.edit(view=self)
        except:
            pass

class StatisticsView(discord.ui.View):
    def __init__(self, cog, guild_id, parent_view):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild_id = guild_id
        self.parent_view = parent_view

    @discord.ui.button(label="Monthly Calendar", style=discord.ButtonStyle.primary, emoji="📅")
    async def monthly_calendar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_monthly_calendar(interaction, self.guild_id)

    @discord.ui.button(label="Birthday Stats", style=discord.ButtonStyle.primary, emoji="📈")
    async def birthday_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_birthday_stats(interaction, self.guild_id)

    @discord.ui.button(label="Upcoming Birthdays", style=discord.ButtonStyle.primary, emoji="🎉")
    async def upcoming_birthdays(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_upcoming_birthdays(interaction, self.guild_id)

    @discord.ui.button(label="Birthday Analytics", style=discord.ButtonStyle.primary, emoji="📊")
    async def birthday_analytics(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_birthday_analytics(interaction, self.guild_id)

    @discord.ui.button(label="Birthday Streaks", style=discord.ButtonStyle.primary, emoji="🔥")
    async def birthday_streaks(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_birthday_streaks(interaction, self.guild_id)

    @discord.ui.button(label="⬅️ Back to Main", style=discord.ButtonStyle.secondary)
    async def back_to_main(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog.create_main_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self.parent_view)

    async def on_timeout(self):
        try:
            for item in self.children:
                item.disabled = True
            if hasattr(self, 'message'):
                await self.message.edit(view=self)
        except:
            pass

class ImportExportView(discord.ui.View):
    def __init__(self, cog, guild_id, parent_view):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild_id = guild_id
        self.parent_view = parent_view

    @discord.ui.button(label="Export JSON", style=discord.ButtonStyle.primary, emoji="📤")
    async def export_json(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.export_birthdays_json(interaction, self.guild_id)

    @discord.ui.button(label="Export CSV", style=discord.ButtonStyle.primary, emoji="📊")
    async def export_csv(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.export_birthdays_csv(interaction, self.guild_id)

    @discord.ui.button(label="Import Data", style=discord.ButtonStyle.primary, emoji="📥")
    async def import_data(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Please upload a JSON or CSV file with birthday data. Use the export function to see the format.",
            ephemeral=True
        )

    @discord.ui.button(label="⬅️ Back to Main", style=discord.ButtonStyle.secondary)
    async def back_to_main(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog.create_main_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self.parent_view)

    async def on_timeout(self):
        try:
            for item in self.children:
                item.disabled = True
            if hasattr(self, 'message'):
                await self.message.edit(view=self)
        except:
            pass

class TestFunctionalityView(discord.ui.View):
    def __init__(self, cog, guild_id, parent_view):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild_id = guild_id
        self.parent_view = parent_view

    @discord.ui.button(label="Test Birthday Parsing", style=discord.ButtonStyle.primary, emoji="🧪")
    async def test_parsing(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(TestParsingModal(self.cog, self.guild_id))

    @discord.ui.button(label="Send Test Announcement", style=discord.ButtonStyle.primary, emoji="📢")
    async def test_announcement(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.send_test_announcement(interaction, self.guild_id)

    @discord.ui.button(label="View All Birthdays", style=discord.ButtonStyle.secondary, emoji="📋")
    async def view_all_birthdays(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_birthday_list(interaction, self.guild_id)

    @discord.ui.button(label="Test Role Assignment", style=discord.ButtonStyle.primary, emoji="👑")
    async def test_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.test_role_assignment(interaction, self.guild_id)

    @discord.ui.button(label="Test Media Features", style=discord.ButtonStyle.primary, emoji="🖼️")
    async def test_media(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.test_media_features(interaction, self.guild_id)

    @discord.ui.button(label="⬅️ Back to Main", style=discord.ButtonStyle.secondary)
    async def back_to_main(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog.create_main_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self.parent_view)

    async def on_timeout(self):
        try:
            for item in self.children:
                item.disabled = True
            if hasattr(self, 'message'):
                await self.message.edit(view=self)
        except:
            pass

class ManagementView(discord.ui.View):
    def __init__(self, cog, guild_id, parent_view):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild_id = guild_id
        self.parent_view = parent_view

    @discord.ui.button(label="Reload Instructions Panel", style=discord.ButtonStyle.success, emoji="🔄")
    async def reload_instructions(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.reload_instructions_panel(interaction, self.guild_id)

    @discord.ui.button(label="Stop Birthday Bot", style=discord.ButtonStyle.danger, emoji="🛑")
    async def stop_bot(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.stop_birthday_bot(interaction, self.guild_id)

    @discord.ui.button(label="Enable Birthday Bot", style=discord.ButtonStyle.success, emoji="✅")
    async def enable_bot(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.enable_birthday_bot(interaction, self.guild_id)

    @discord.ui.button(label="Audit Log", style=discord.ButtonStyle.secondary, emoji="📝")
    async def audit_log(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_audit_log(interaction, self.guild_id)

    # ADD THESE CACHE MANAGEMENT BUTTONS:
    @discord.ui.button(label="Cache Stats", style=discord.ButtonStyle.secondary, emoji="📊")
    async def cache_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_cache_stats(interaction, self.guild_id)

    @discord.ui.button(label="Clear Cache", style=discord.ButtonStyle.secondary, emoji="🧹")
    async def clear_cache(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.clear_guild_cache(interaction, self.guild_id)

    @discord.ui.button(label="⬅️ Back to Main", style=discord.ButtonStyle.secondary)
    async def back_to_main(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog.create_main_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self.parent_view)

    async def on_timeout(self):
        try:
            for item in self.children:
                item.disabled = True
            if hasattr(self, 'message'):
                await self.message.edit(view=self)
        except:
            pass

class SetTimezoneModal(discord.ui.Modal, title='Set Server Timezone'):
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id

    timezone = discord.ui.TextInput(
        label='Timezone',
        placeholder='Enter timezone (e.g., US/Eastern, Europe/London, Asia/Tokyo)',
        default='UTC',
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog.set_server_timezone(interaction, self.guild_id, self.timezone.value)

class SetDefaultMessageModal(discord.ui.Modal, title='Set Default Birthday Message'):
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id

    message = discord.ui.TextInput(
        label='Default Message',
        placeholder='Enter the default birthday announcement message...',
        style=discord.TextStyle.paragraph,
        default="🎉 Happy birthday to {user_mention}! {age_text}",
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog.set_default_message(interaction, self.guild_id, self.message.value)

class SetUserMessageModal(discord.ui.Modal, title='Set User Birthday Message'):
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id

    user_id = discord.ui.TextInput(
        label='User ID',
        placeholder='Enter the user ID...',
        required=True
    )

    message = discord.ui.TextInput(
        label='Custom Message',
        placeholder='Enter custom birthday message for this user...',
        style=discord.TextStyle.paragraph,
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            user_id = int(self.user_id.value)
            await self.cog.set_user_message(interaction, self.guild_id, user_id, self.message.value)
        except ValueError:
            await interaction.response.send_message("❌ Invalid user ID!", ephemeral=True)

class TestParsingModal(discord.ui.Modal, title='Test Birthday Parsing'):
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id

    test_date = discord.ui.TextInput(
        label='Date to Test',
        placeholder='Enter a date in any format to test parsing...',
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog.test_date_parsing(interaction, self.guild_id, self.test_date.value)

class TransferChannelModal(discord.ui.Modal, title='Transfer Channel'):
    def __init__(self, cog, guild_id, channel_type):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id
        self.channel_type = channel_type

    channel_id = discord.ui.TextInput(
        label='New Channel ID',
        placeholder='Enter the new channel ID...',
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            new_channel_id = int(self.channel_id.value)
            if self.channel_type == 'birthday':
                await self.cog.transfer_birthday_channel(interaction, self.guild_id, new_channel_id)
            elif self.channel_type == 'announcement':
                await self.cog.transfer_announcement_channel(interaction, self.guild_id, new_channel_id)
        except ValueError:
            await interaction.response.send_message("❌ Invalid channel ID!", ephemeral=True)

class SetImageModal(discord.ui.Modal, title='Set Announcement Image URL'):
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id

    image_url = discord.ui.TextInput(
        label='Image URL',
        placeholder='Enter URL for birthday announcement image...',
        default='',
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog.set_announcement_image(interaction, self.guild_id, self.image_url.value)

class SetGIFModal(discord.ui.Modal, title='Set Announcement GIF URL'):
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id

    gif_url = discord.ui.TextInput(
        label='GIF URL',
        placeholder='Enter URL for birthday announcement GIF...',
        default='',
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog.set_announcement_gif(interaction, self.guild_id, self.gif_url.value)

class SetAnnouncementTimeModal(discord.ui.Modal, title='Set Announcement Time'):
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id

    announcement_hour = discord.ui.TextInput(
        label='Announcement Hour (0-23)',
        placeholder='Enter hour for birthday announcements (0-23)...',
        default='12',
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            hour = int(self.announcement_hour.value)
            if 0 <= hour <= 23:
                await self.cog.set_announcement_hour(interaction, self.guild_id, hour)
            else:
                await interaction.response.send_message("❌ Hour must be between 0 and 23!", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("❌ Invalid hour! Please enter a number between 0 and 23.", ephemeral=True)

class ManualRoleIDModal(discord.ui.Modal, title='Enter Role ID'):
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id

    role_id = discord.ui.TextInput(
        label='Role ID',
        placeholder='Enter the role ID...',
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            role_id = int(self.role_id.value)
            await self.cog.set_birthday_role(interaction, self.guild_id, role_id)
        except ValueError:
            await interaction.response.send_message("❌ Invalid role ID!", ephemeral=True)

class ManualIDModal(discord.ui.Modal, title='Enter Channel ID'):
    def __init__(self, cog, guild_id, channel_type):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id
        self.channel_type = channel_type

    channel_id = discord.ui.TextInput(
        label='Channel ID',
        placeholder='Enter the channel ID...',
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            channel_id = int(self.channel_id.value)
            if self.channel_type == 'birthday':
                await self.cog.set_birthday_channel(interaction, self.guild_id, channel_id)
            elif self.channel_type == 'announcement':
                await self.cog.set_announcement_channel(interaction, self.guild_id, channel_id)
        except ValueError:
            await interaction.response.send_message("❌ Invalid channel ID!", ephemeral=True)

class BirthdayPaginationView(discord.ui.View):
    def __init__(self, cog, guild_id, birthdays, page=0):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id
        self.birthdays = birthdays
        self.page = page
        self.max_page = (len(birthdays) - 1) // MAX_BIRTHDAY_LIST_PAGE_SIZE

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.primary, emoji="⬅️")
    async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 0:
            self.page -= 1
            embed = await self.cog.create_birthday_list_embed(self.guild_id, self.birthdays, self.page)
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.defer()

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary, emoji="➡️")
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page < self.max_page:
            self.page += 1
            embed = await self.cog.create_birthday_list_embed(self.guild_id, self.birthdays, self.page)
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.defer()

class UpcomingBirthdaysView(discord.ui.View):
    def __init__(self, cog, guild_id, upcoming_birthdays, page=0):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id
        self.upcoming_birthdays = upcoming_birthdays
        self.page = page
        self.max_page = (len(upcoming_birthdays) - 1) // MAX_UPCOMING_BIRTHDAYS_DISPLAY

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.primary, emoji="⬅️")
    async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 0:
            self.page -= 1
            embed = await self.cog.create_upcoming_birthdays_embed(self.guild_id, self.upcoming_birthdays, self.page)
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.defer()

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary, emoji="➡️")
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page < self.max_page:
            self.page += 1
            embed = await self.cog.create_upcoming_birthdays_embed(self.guild_id, self.upcoming_birthdays, self.page)
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.defer()

class CalendarPaginationView(discord.ui.View):
    def __init__(self, cog, guild_id, year, month):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild_id = guild_id
        self.year = year
        self.month = month

    @discord.ui.button(label="Previous Month", style=discord.ButtonStyle.primary, emoji="⬅️")
    async def previous_month(self, interaction: discord.Interaction, button: discord.ui.Button):
        new_month = self.month - 1
        new_year = self.year
        if new_month < 1:
            new_month = 12
            new_year -= 1

        embed = await self.cog.create_calendar_embed(self.guild_id, new_year, new_month)
        view = CalendarPaginationView(self.cog, self.guild_id, new_year, new_month)
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Next Month", style=discord.ButtonStyle.primary, emoji="➡️")
    async def next_month(self, interaction: discord.Interaction, button: discord.ui.Button):
        new_month = self.month + 1
        new_year = self.year
        if new_month > 12:
            new_month = 1
            new_year += 1

        embed = await self.cog.create_calendar_embed(self.guild_id, new_year, new_month)
        view = CalendarPaginationView(self.cog, self.guild_id, new_year, new_month)
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Current Month", style=discord.ButtonStyle.secondary, emoji="📅")
    async def current_month(self, interaction: discord.Interaction, button: discord.ui.Button):
        now = datetime.datetime.now()
        embed = await self.cog.create_calendar_embed(self.guild_id, now.year, now.month)
        view = CalendarPaginationView(self.cog, self.guild_id, now.year, now.month)
        await interaction.response.edit_message(embed=embed, view=view)

class AnalyticsPaginationView(discord.ui.View):
    def __init__(self, cog, guild_id, page=0):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild_id = guild_id
        self.page = page

    @discord.ui.button(label="Overview", style=discord.ButtonStyle.primary)
    async def overview_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog.create_analytics_overview_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Monthly Stats", style=discord.ButtonStyle.primary)
    async def monthly_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog.create_monthly_stats_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="User Activity", style=discord.ButtonStyle.primary)
    async def user_activity(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog.create_user_activity_embed(self.guild_id)
        await interaction.response.edit_message(embed=embed, view=self)

class BirthdayCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.developer_id = DEVELOPER_ID
        self.rate_limits = defaultdict(dict)
        
        self.config_cache = TTLCache(maxsize=MAX_CACHE_SIZE_CONFIG, ttl=CACHE_TTL)
        self.birthday_cache = TTLCache(maxsize=MAX_CACHE_SIZE_BIRTHDAYS, ttl=CACHE_TTL)
        
        self.cache_ttl = CACHE_TTL
        self.disabled_guilds = set()
        self.persistent_views_added = set()

        self.timezone_map = {tz.lower(): tz for tz in pytz.all_timezones}

        self.bot.loop.create_task(self.setup_db())
        self.bot.loop.create_task(self.restore_all_persistent_views())
        
        self.cleanup_task.start()
        self.reminder_task.start()
        self.birthday_announcement_task.start()
        self.role_cleanup_task.start()
        self.cache_cleanup_task.start()
        self.announcement_cleanup_task.start()
        self.audit_cleanup_task.start()

    async def invalidate_birthday_cache(self, guild_id, user_id=None):
        """Invalidate birthday cache for a guild or specific user - INSTANT UPDATES!"""
        try:
            if user_id:
                # Invalidate specific user cache
                cache_key = f"{guild_id}_{user_id}"
                if cache_key in self.birthday_cache:
                    del self.birthday_cache[cache_key]
                    print(f"🧹 Invalidated user birthday cache: {cache_key}")
            
            # Always invalidate the "all birthdays" cache for this guild
            all_cache_key = f"all_{guild_id}"
            if all_cache_key in self.birthday_cache:
                del self.birthday_cache[all_cache_key]
                print(f"🧹 Invalidated all birthdays cache: {all_cache_key}")
                
        except Exception as e:
            await self.log_error(str(e), "invalidate_birthday_cache")

    async def invalidate_config_cache(self, guild_id):
        """Invalidate config cache for a guild - INSTANT UPDATES!"""
        try:
            if guild_id in self.config_cache:
                del self.config_cache[guild_id]
                print(f"🧹 Invalidated config cache: {guild_id}")
        except Exception as e:
            await self.log_error(str(e), "invalidate_config_cache")

    async def invalidate_all_guild_caches(self, guild_id):
        """Invalidate all caches for a guild - NUCLEAR OPTION"""
        try:
            await self.invalidate_config_cache(guild_id)
            await self.invalidate_birthday_cache(guild_id)
            print(f"💥 Invalidated ALL caches for guild: {guild_id}")
        except Exception as e:
            await self.log_error(str(e), "invalidate_all_guild_caches")

    async def setup_db(self):
        os.makedirs('data', exist_ok=True)
        async with aiosqlite.connect('data/birthdays.db') as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS guild_config (
                    guild_id INTEGER PRIMARY KEY,
                    birthday_channel_id INTEGER,
                    announcement_channel_id INTEGER,
                    default_message TEXT DEFAULT '🎉 Happy birthday to {user_mention}! {age_text}',
                    clean_toggle BOOLEAN DEFAULT 1,
                    reminder_roles TEXT DEFAULT '[]',
                    reminder_users TEXT DEFAULT '[]',
                    instructions_message_id INTEGER,
                    timezone TEXT DEFAULT 'UTC',
                    birthday_role_id INTEGER,
                    enabled BOOLEAN DEFAULT 1,
                    announcement_hour INTEGER DEFAULT 12,
                    announcement_image TEXT,
                    announcement_gif TEXT,
                    enable_age_milestones BOOLEAN DEFAULT 1,
                    enable_birthday_streaks BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_birthdays (
                    user_id INTEGER,
                    guild_id INTEGER,
                    birth_date TEXT,
                    has_year BOOLEAN,
                    custom_message TEXT,
                    timezone TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, guild_id)
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS recent_birthdays (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER,
                    user_id INTEGER,
                    birth_date TEXT,
                    set_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS developer_log (
                    guild_id INTEGER PRIMARY KEY,
                    log_channel_id INTEGER
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS birthday_roles (
                    guild_id INTEGER,
                    user_id INTEGER,
                    role_assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (guild_id, user_id)
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS birthday_stats (
                    guild_id INTEGER,
                    stat_date DATE,
                    total_birthdays INTEGER,
                    birthdays_with_year INTEGER,
                    birthdays_without_year INTEGER,
                    PRIMARY KEY (guild_id, stat_date)
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS birthday_announcements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER,
                    user_id INTEGER,
                    announcement_date DATE,
                    announced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(guild_id, user_id, announcement_date)
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS birthday_history (
                    guild_id INTEGER,
                    user_id INTEGER,
                    year INTEGER,
                    celebrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (guild_id, user_id, year)
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER,
                    admin_id INTEGER,
                    action TEXT,
                    description TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            await conn.execute('CREATE INDEX IF NOT EXISTS idx_birthdays_guild ON user_birthdays(guild_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_birthdays_date ON user_birthdays(birth_date)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_announcements_date ON birthday_announcements(announcement_date)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_recent_birthdays_guild_date ON recent_birthdays(guild_id, set_at)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_birthday_history_guild_user ON birthday_history(guild_id, user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_audit_log_guild ON audit_log(guild_id)')

            await conn.commit()

    async def restore_all_persistent_views(self):
        try:
            await self.bot.wait_until_ready()
            print("🔄 Restoring persistent views for all guilds...")
            
            for guild in self.bot.guilds:
                try:
                    config = await self.get_guild_config(guild.id)
                    if config and config.get('instructions_message_id') and config.get('birthday_channel_id'):
                        channel = self.bot.get_channel(config['birthday_channel_id'])
                        if channel:
                            try:
                                message = await channel.fetch_message(config['instructions_message_id'])
                                view = InstructionsView(self, guild.id)
                                self.bot.add_view(view, message_id=message.id)
                                self.persistent_views_added.add(guild.id)
                                print(f"✅ Restored persistent view for guild: {guild.name} ({guild.id})")
                            except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                                print(f"⚠️ Could not restore view for guild {guild.name}: {e}")
                except Exception as e:
                    print(f"❌ Error restoring view for guild {guild.id}: {e}")
                    
            print(f"✅ Persistent view restoration complete. Restored {len(self.persistent_views_added)} views.")
                    
        except Exception as e:
            print(f"❌ Critical error in restore_all_persistent_views: {e}")

    async def create_instructions_embed(self):
        embed = discord.Embed(
            title="🎂 Set Your Birthday",
            description="**💡Simply type your birthday in this channel!**\nYear and timezone are optional. But adding timezone helps us announce at the right time! Or the announcements may go out at midnight UTC which could be odd for your local time.To update your birthday, just type it again with the new information.\n\n",
            color=discord.Color.green()
        )

        embed.add_field(
            name="📅 **QUICK EXAMPLES**",
            value=(
                "**Basic formats:**\n"
                "• `28 3`\n"
                "• `28 mar`\n\n"
                
                "**With year:**\n"
                "• `28 3 2006`\n"
                "• `28 march 2006`\n\n"
                
                "**With timezone:**\n"
                "• `28 3 US/Eastern`\n"
                "• `28 march Europe/London`"
            ),
            inline=False
        )

        embed.set_footer(text="Need help? Contact server administrators.")

        return embed

    async def show_full_instructions(self, interaction):
        embed = discord.Embed(
            title="📖 Complete Birthday Guide",
            description="**Everything you need to know in one place**",
            color=discord.Color.blue()
        )

        # Section 1: Quick Start (Most Important)
        embed.add_field(
            name="🚀 **QUICK START**",
            value=(
                "**In Birthday Channel:**\n"
                "• Just type: `28 march` or `28 3 2006`\n\n"
                
                "**Anywhere Else:**\n"
                "• Use: `!bday 28 march`\n"
                "• Or: `!bday 28/3/2006`\n\n"
                
                "**Year & timezone are optional!**"
            ),
            inline=False
        )

        # Section 2: All Commands (Clean Format)
        embed.add_field(
            name="⚡ **ALL COMMANDS**",
            value=(
                "**For Everyone:**\n"
                "• `!bday 28 march` - Set birthday\n"
                "• `!bday remove` - Remove birthday\n"
                "• `!bday next` - Next birthday\n"
                "• `!bday streak` - View streaks\n"
                "• `!bday list` - All birthdays\n\n"
                
                "**Format Help:**\n"
                "• `!bday dmy 28/3/2006` - Force day/month/year\n"
                "• `!bday mdy 3/28/2006` - Force month/day/year\n"
                "• Add timezone: `!bday 28 march US/Eastern`\n\n"
                
                "**Admins Only:**\n"
                "• `!bday @User 28 march` - Set for others\n"
                "• `!bday @User remove` - Remove for others"
            ),
            inline=False
        )

        # Section 3: Date Examples (Clean)
        embed.add_field(
            name="📅 **DATE EXAMPLES**",
            value=(
                "**Simple:** 28 3, 28 march, 28/3\n"
                "**With Year:** 28 3 2006, 28 march 2006\n"
                "**With Timezone:** 28 march US/Eastern\n"
                "**Full Date:** March 28, 2006\n\n"
                
                "**All these work:**\n"
                "28-3, 28.3, 28th March, March 28th"
            ),
            inline=False
        )

        # Section 4: Key Features
        embed.add_field(
            name="🎯 **COOL FEATURES**",
            value=(
                "• **Age Milestones** - Special messages at 1, 5, 10, 13, 16, 18, 21, 30, 40, 50, 60, 70, 80, 90, 100!\n"
                "• **Birthday Streaks** - Track celebration years\n"
                "• **Birthday Roles** - Get a special role on your day\n"
                "• **Timezone Support** - Accurate announcements\n"
                "• **Reminders** - Staff get 3-day warnings"
            ),
            inline=False
        )

        # Section 5: Timezone Help
        embed.add_field(
            name="🌍 **TIMEZONES**",
            value=(
                "**Popular ones:** US/Eastern, Europe/London, Asia/Tokyo, UTC\n\n"
                "**How to use:** Add to end of date:\n"
                "`28 march US/Eastern` or `!bday 28/3 US/Eastern`\n\n"
                "**Not required** - uses server time if omitted"
            ),
            inline=False
        )

        # Section 6: Troubleshooting
        embed.add_field(
            name="🔧 **TROUBLESHOOTING**",
            value=(
                "**Date not working?**\n"
                "Try: `!bday dmy 28/3/2006` or `!bday mdy 3/28/2006`\n\n"
                "**No announcement?**\n"
                "Check announcement channel settings\n\n"
                "**Need admin help?**\n"
                "Use `/birthday` for bot configuration"
            ),
            inline=False
        )

        embed.set_footer(text="💡 Pro Tip: Use the birthday channel for easiest setup!")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

    def get_age_milestone_message(self, age):
        return AGE_MILESTONES.get(age)

    async def show_next_birthday(self, interaction, guild_id):
        try:
            stats = await self.get_birthday_statistics(guild_id)
            upcoming = stats['upcoming']

            if not upcoming:
                await interaction.response.send_message("❌ No upcoming birthdays found!", ephemeral=True)
                return

            next_bday = upcoming[0]
            user = self.bot.get_user(next_bday['user_id'])
            username = user.display_name if user else f"User {next_bday['user_id']}"
            display_date = self.format_birthday_display(next_bday['birth_date'])
            days_text = "tomorrow" if next_bday['days_until'] == 1 else f"in {next_bday['days_until']} days"

            embed = discord.Embed(
                title="⏰ Next Birthday",
                description=f"The next birthday is **{username}** on **{display_date}** ({days_text})!",
                color=discord.Color.gold()
            )

            if not next_bday['birth_date'].startswith('0000-'):
                age = self.calculate_age(next_bday['birth_date'])
                if age is not None:
                    next_age = age + 1 if (datetime.datetime.now().month, datetime.datetime.now().day) <= (int(next_bday['birth_date'][5:7]), int(next_bday['birth_date'][8:10])) else age
                    embed.add_field(name="Turning", value=f"{next_age} years old", inline=True)
                    
                    milestone = self.get_age_milestone_message(next_age)
                    if milestone:
                        embed.add_field(name="🎉 Milestone", value=milestone, inline=True)

            await interaction.response.send_message(embed=embed, ephemeral=True)

        except Exception as e:
            await self.log_error(str(e), "show_next_birthday")
            await interaction.response.send_message("❌ Error showing next birthday", ephemeral=True)

    async def set_birthday_channel(self, interaction, guild_id, channel_id):
        try:
            config = await self.get_guild_config(guild_id)
        
            # Remove old instructions panel if it exists
            if config and config.get('instructions_message_id') and config.get('birthday_channel_id'):
                try:
                    old_channel = self.bot.get_channel(config['birthday_channel_id'])
                    if old_channel:
                        try:
                            old_msg = await old_channel.fetch_message(config['instructions_message_id'])
                            await old_msg.delete()
                        except discord.NotFound:
                            pass  # Message already deleted
                        # Remove from persistent views tracking
                        if guild_id in self.persistent_views_added:
                            self.persistent_views_added.remove(guild_id)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass  # Old message doesn't exist or can't be accessed

            channel = interaction.guild.get_channel(channel_id)
            if not channel:
                if interaction.response.is_done():
                    await interaction.followup.send("❌ Channel not found!", ephemeral=True)
                else:
                    await interaction.response.send_message("❌ Channel not found!", ephemeral=True)
                return

            # Update config first
            await self.update_guild_config(guild_id, birthday_channel_id=channel_id)

            # Create new instructions panel
            instructions_embed = await self.create_instructions_embed()
            instructions_view = InstructionsView(self, guild_id)
        
            instructions_msg = await channel.send(embed=instructions_embed, view=instructions_view)
        
            try:
                self.bot.add_view(instructions_view, message_id=instructions_msg.id)
                self.persistent_views_added.add(guild_id)
            except Exception as e:
                print(f"⚠️ Could not add persistent view: {e}")
            await self.update_guild_config(guild_id, instructions_message_id=instructions_msg.id)

            await self.log_audit_action(guild_id, interaction.user.id, f"set_birthday_channel", f"Set birthday channel to #{channel.name}")

            # Update the configuration panel
            embed = await self.create_channel_setup_embed(guild_id)
            view = ChannelSetupView(self, guild_id, self.get_parent_view())
            view.update_dropdowns()
        
            # Only send one response - fix the double response issue
            if interaction.response.is_done():
                await interaction.edit_original_response(embed=embed, view=view)
                await interaction.followup.send(f"✅ Birthday channel set to {channel.mention}!", ephemeral=True)
            else:
                await interaction.response.edit_message(embed=embed, view=view)
                await interaction.followup.send(f"✅ Birthday channel set to {channel.mention}!", ephemeral=True)
            
        except Exception as e:
            await self.log_error(str(e), "set_birthday_channel")
            try:
                if interaction.response.is_done():
                    await interaction.followup.send("❌ Error setting birthday channel", ephemeral=True)
                else:
                    await interaction.response.send_message("❌ Error setting birthday channel", ephemeral=True)
            except discord.NotFound:
                pass


    async def set_announcement_channel(self, interaction, guild_id, channel_id):
        try:
            channel = interaction.guild.get_channel(channel_id)
            if not channel:
                await interaction.response.send_message("❌ Channel not found!", ephemeral=True)
                return
            await self.update_guild_config(guild_id, announcement_channel_id=channel_id)

            await self.log_audit_action(guild_id, interaction.user.id, f"set_announcement_channel", f"Set announcement channel to #{channel.name}")

            embed = await self.create_channel_setup_embed(guild_id)
            view = ChannelSetupView(self, guild_id, self.get_parent_view())
            view.update_dropdowns()
        
            # Only send one response - fix the double response issue
            if interaction.response.is_done():
                await interaction.edit_original_response(embed=embed, view=view)
                await interaction.followup.send(f"✅ Announcement channel set to {channel.mention}!", ephemeral=True)
            else:
                await interaction.response.edit_message(embed=embed, view=view)
                await interaction.followup.send(f"✅ Announcement channel set to {channel.mention}!", ephemeral=True)

        except Exception as e:
            await self.log_error(str(e), "set_announcement_channel")
            try:
                if interaction.response.is_done():
                    await interaction.followup.send("❌ Error setting announcement channel", ephemeral=True)
                else:
                    await interaction.response.send_message("❌ Error setting announcement channel", ephemeral=True)
            except discord.NotFound:
                pass

    async def set_birthday_role(self, interaction, guild_id, role_id):
        try:
            role = interaction.guild.get_role(role_id)
            if not role:
                await interaction.response.send_message("❌ Role not found!", ephemeral=True)
                return

            await self.update_guild_config(guild_id, birthday_role_id=role_id)

            await self.log_audit_action(guild_id, interaction.user.id, f"set_birthday_role", f"Set birthday role to @{role.name}")

            embed = await self.create_timezone_roles_embed(guild_id)
            view = TimezoneRolesView(self, guild_id, self.get_parent_view())
            view.update_role_dropdown()
            
            # Only send one response - fix the double response issue
            if interaction.response.is_done():
                await interaction.edit_original_response(embed=embed, view=view)
                await interaction.followup.send(f"✅ Birthday role set to {role.mention}!", ephemeral=True)
            else:
                await interaction.response.edit_message(embed=embed, view=view)
                await interaction.followup.send(f"✅ Birthday role set to {role.mention}!", ephemeral=True)
                            
        except Exception as e:
            await self.log_error(str(e), "set_birthday_role")
            try:
                if interaction.response.is_done():
                    await interaction.followup.send("❌ Error setting birthday role", ephemeral=True)
                else:
                    await interaction.response.send_message("❌ Error setting birthday role", ephemeral=True)
            except discord.NotFound:
                pass

    async def set_announcement_image(self, interaction, guild_id, image_url):
        try:
            if image_url and not image_url.startswith(('http://', 'https://')):
                await interaction.response.send_message("❌ Invalid image URL! Must start with http:// or https://", ephemeral=True)
                return

            await self.update_guild_config(guild_id, announcement_image=image_url)
            
            action_desc = "Set announcement image" if image_url else "Cleared announcement image"
            await self.log_audit_action(guild_id, interaction.user.id, "set_announcement_image", action_desc)

            if image_url:
                await interaction.response.send_message(f"✅ Announcement image set!", ephemeral=True)
            else:
                await interaction.response.send_message("✅ Announcement image cleared!", ephemeral=True)
                
        except Exception as e:
            await self.log_error(str(e), "set_announcement_image")
            await interaction.response.send_message("❌ Error setting announcement image", ephemeral=True)

    async def set_announcement_gif(self, interaction, guild_id, gif_url):
        try:
            if gif_url and not gif_url.startswith(('http://', 'https://')):
                await interaction.response.send_message("❌ Invalid GIF URL! Must start with http:// or https://", ephemeral=True)
                return

            await self.update_guild_config(guild_id, announcement_gif=gif_url)
            
            action_desc = "Set announcement GIF" if gif_url else "Cleared announcement GIF"
            await self.log_audit_action(guild_id, interaction.user.id, "set_announcement_gif", action_desc)

            if gif_url:
                await interaction.response.send_message(f"✅ Announcement GIF set!", ephemeral=True)
            else:
                await interaction.response.send_message("✅ Announcement GIF cleared!", ephemeral=True)
                
        except Exception as e:
            await self.log_error(str(e), "set_announcement_gif")
            await interaction.response.send_message("❌ Error setting announcement GIF", ephemeral=True)

    async def set_announcement_hour(self, interaction, guild_id, hour):
        try:
            await self.update_guild_config(guild_id, announcement_hour=hour)
            
            await self.log_audit_action(guild_id, interaction.user.id, "set_announcement_hour", f"Set announcement hour to {hour}:00")

            await interaction.response.send_message(f"✅ Announcement time set to {hour}:00!", ephemeral=True)
                
        except Exception as e:
            await self.log_error(str(e), "set_announcement_hour")
            await interaction.response.send_message("❌ Error setting announcement time", ephemeral=True)

    async def toggle_age_milestones(self, interaction, guild_id):
        try:
            config = await self.get_guild_config(guild_id)
            current = config.get('enable_age_milestones', True) if config else True
            new_value = not current

            await self.update_guild_config(guild_id, enable_age_milestones=new_value)
            
            action_desc = "Enabled age milestones" if new_value else "Disabled age milestones"
            await self.log_audit_action(guild_id, interaction.user.id, "toggle_age_milestones", action_desc)

            status = "enabled" if new_value else "disabled"
            await interaction.response.send_message(f"✅ Age milestones {status}!", ephemeral=True)
                
        except Exception as e:
            await self.log_error(str(e), "toggle_age_milestones")
            await interaction.response.send_message("❌ Error toggling age milestones", ephemeral=True)

    async def toggle_birthday_streaks(self, interaction, guild_id):
        try:
            config = await self.get_guild_config(guild_id)
            current = config.get('enable_birthday_streaks', True) if config else True
            new_value = not current

            await self.update_guild_config(guild_id, enable_birthday_streaks=new_value)
            
            action_desc = "Enabled birthday streaks" if new_value else "Disabled birthday streaks"
            await self.log_audit_action(guild_id, interaction.user.id, "toggle_birthday_streaks", action_desc)

            status = "enabled" if new_value else "disabled"
            await interaction.response.send_message(f"✅ Birthday streaks {status}!", ephemeral=True)
                
        except Exception as e:
            await self.log_error(str(e), "toggle_birthday_streaks")
            await interaction.response.send_message("❌ Error toggling birthday streaks", ephemeral=True)

    async def show_age_milestones_list(self, interaction):
        try:
            embed = discord.Embed(
                title="🎯 Age Milestones",
                description="Special messages for milestone birthdays:",
                color=discord.Color.gold()
            )

            milestone_list = ""
            for age, message in sorted(AGE_MILESTONES.items()):
                milestone_list += f"**{age}:** {message}\n"

            embed.add_field(name="Milestones", value=milestone_list, inline=False)
            embed.set_footer(text="These will be added to birthday announcements when enabled")

            await interaction.response.send_message(embed=embed, ephemeral=True)

        except Exception as e:
            await self.log_error(str(e), "show_age_milestones_list")
            await interaction.response.send_message("❌ Error showing age milestones", ephemeral=True)

    async def send_birthday_announcement(self, guild, channel, user, birthday_data, config):
        try:
            age = self.calculate_age(birthday_data['birth_date'])
            age_text = self.get_age_text(birthday_data['birth_date'])

            message_template = birthday_data.get('custom_message') or config['default_message']
            message = message_template.format(
                user_mention=user.mention,
                user_name=user.display_name,
                age=age or "",
                age_text=age_text
            )

            milestone_message = ""
            if config.get('enable_age_milestones', True) and age is not None:
                milestone = self.get_age_milestone_message(age)
                if milestone:
                    milestone_message = f"\n\n{milestone}"

            embed = discord.Embed(
                title="🎂 Happy Birthday!",
                description=f"{message}{milestone_message}",
                color=discord.Color.gold(),
                timestamp=datetime.datetime.utcnow()
            )
            embed.set_thumbnail(url=user.display_avatar.url)

            image_url = config.get('announcement_image')
            gif_url = config.get('announcement_gif')

            try:
                webhooks = await channel.webhooks()
                webhook = discord.utils.get(webhooks, name='Birthday Bot')

                if not webhook:
                    if channel.permissions_for(guild.me).manage_webhooks:
                        webhook = await channel.create_webhook(name='Birthday Bot')
                    else:
                        webhook = None

                if webhook:
                    files = []
                    if gif_url:
                        embed.set_image(url=gif_url)
                    elif image_url:
                        embed.set_image(url=image_url)

                    await webhook.send(
                        content=message,
                        embed=embed,
                        username=f"🎂 {user.display_name}'s Birthday",
                        avatar_url=user.display_avatar.url
                    )
                    
                    if config.get('enable_birthday_streaks', True):
                        await self.record_birthday_celebration(guild.id, user.id)
                    
                    return
            except Exception as e:
                await self.log_error(f"Webhook failed, falling back: {str(e)}", "send_birthday_announcement_webhook")

            if gif_url:
                embed.set_image(url=gif_url)
            elif image_url:
                embed.set_image(url=image_url)

            await channel.send(embed=embed)

            if config.get('enable_birthday_streaks', True):
                await self.record_birthday_celebration(guild.id, user.id)

            if config.get('birthday_role_id'):
                await self.assign_birthday_role(guild, user, config['birthday_role_id'])

        except Exception as e:
            await self.log_error(str(e), "send_birthday_announcement")

    async def record_birthday_celebration(self, guild_id, user_id):
        try:
            current_year = datetime.datetime.now().year
            async with aiosqlite.connect('data/birthdays.db') as conn:
                await conn.execute('''
                    INSERT OR IGNORE INTO birthday_history (guild_id, user_id, year, celebrated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ''', (guild_id, user_id, current_year))
                await conn.commit()
        except Exception as e:
            await self.log_error(str(e), "record_birthday_celebration")

    async def get_birthday_streak(self, guild_id, user_id):
        try:
            current_year = datetime.datetime.now().year
            async with aiosqlite.connect('data/birthdays.db') as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute('''
                    SELECT year FROM birthday_history 
                    WHERE guild_id = ? AND user_id = ? 
                    ORDER BY year DESC
                ''', (guild_id, user_id)) as cursor:
                    rows = await cursor.fetchall()
                    
                    if not rows:
                        return 0
                    
                    streak = 0
                    expected_year = current_year
                    
                    for row in rows:
                        if row['year'] == expected_year:
                            streak += 1
                            expected_year -= 1
                        else:
                            break
                    
                    return streak
        except Exception as e:
            await self.log_error(str(e), "get_birthday_streak")
            return 0

    async def get_birthday_statistics(self, guild_id):
        birthdays = await self.get_all_birthdays(guild_id)

        stats = {
            'total': len(birthdays),
            'with_year': 0,
            'without_year': 0,
            'by_month': defaultdict(int),
            'upcoming': [],
            'recent': await self.get_recent_birthdays(guild_id),
            'streaks': []
        }

        today = datetime.datetime.now()
        current_year = today.year

        for bday in birthdays:
            if bday['has_year']:
                stats['with_year'] += 1
            else:
                stats['without_year'] += 1

            if bday['birth_date'].startswith('0000-'):
                month = int(bday['birth_date'][5:7])
            else:
                month = int(bday['birth_date'][5:7])
            stats['by_month'][month] += 1

            if bday['birth_date'].startswith('0000-'):
                birth_month = int(bday['birth_date'][5:7])
                birth_day = int(bday['birth_date'][8:10])
                next_birthday = datetime.datetime(current_year, birth_month, birth_day)

                if next_birthday < today:
                    next_birthday = datetime.datetime(current_year + 1, birth_month, birth_day)

                days_until = (next_birthday - today).days
                stats['upcoming'].append({
                    'user_id': bday['user_id'],
                    'birth_date': bday['birth_date'],
                    'days_until': days_until,
                    'next_date': next_birthday
                })

            if bday['has_year']:
                streak = await self.get_birthday_streak(guild_id, bday['user_id'])
                if streak > 1:
                    stats['streaks'].append({
                        'user_id': bday['user_id'],
                        'streak': streak
                    })

        stats['upcoming'].sort(key=lambda x: x['days_until'])
        stats['upcoming'] = stats['upcoming'][:MAX_UPCOMING_BIRTHDAYS_DISPLAY]

        stats['streaks'].sort(key=lambda x: x['streak'], reverse=True)
        stats['streaks'] = stats['streaks'][:10]

        return stats

    async def show_birthday_analytics(self, interaction, guild_id):
        try:
            embed = await self.create_analytics_overview_embed(guild_id)
            view = AnalyticsPaginationView(self, guild_id)
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        except Exception as e:
            await self.log_error(str(e), "show_birthday_analytics")
            await interaction.response.send_message("❌ Error showing analytics", ephemeral=True)

    async def create_analytics_overview_embed(self, guild_id):
        stats = await self.get_birthday_statistics(guild_id)
        config = await self.get_guild_config(guild_id)

        embed = discord.Embed(
            title="📊 Birthday Analytics - Overview",
            color=discord.Color.purple()
        )

        embed.add_field(
            name="📈 Basic Statistics",
            value=f"**Total Birthdays:** {stats['total']}\n"
                  f"**With Year:** {stats['with_year']}\n"
                  f"**Without Year:** {stats['without_year']}\n"
                  f"**Completion Rate:** {(stats['with_year']/stats['total']*100) if stats['total'] > 0 else 0:.1f}%",
            inline=True
        )

        if stats['by_month']:
            busiest_month = max(stats['by_month'], key=stats['by_month'].get)
            quietest_month = min(stats['by_month'], key=stats['by_month'].get)
            
            embed.add_field(
                name="📅 Monthly Analysis",
                value=f"**Busiest Month:** {calendar.month_name[busiest_month]} ({stats['by_month'][busiest_month]})\n"
                      f"**Quietest Month:** {calendar.month_name[quietest_month]} ({stats['by_month'][quietest_month]})\n"
                      f"**Avg per Month:** {stats['total']/12:.1f}",
                inline=True
            )

        if stats['streaks']:
            longest_streak = max(stats['streaks'], key=lambda x: x['streak'])
            user = self.bot.get_user(longest_streak['user_id'])
            username = user.display_name if user else f"User {longest_streak['user_id']}"
            
            embed.add_field(
                name="🔥 Streak Leaders",
                value=f"**Longest Streak:** {username} ({longest_streak['streak']} years)\n"
                      f"**Active Streaks:** {len(stats['streaks'])}\n"
                      f"**Avg Streak:** {sum(s['streak'] for s in stats['streaks'])/len(stats['streaks']):.1f} years",
                inline=True
            )

        if config:
            features = []
            if config.get('enable_age_milestones', True):
                features.append("Age Milestones")
            if config.get('enable_birthday_streaks', True):
                features.append("Birthday Streaks")
            if config.get('announcement_image'):
                features.append("Custom Image")
            if config.get('announcement_gif'):
                features.append("Custom GIF")
            
            embed.add_field(
                name="⚙️ Active Features",
                value=", ".join(features) if features else "Basic features only",
                inline=False
            )

        return embed

    async def create_monthly_stats_embed(self, guild_id):
        stats = await self.get_birthday_statistics(guild_id)

        embed = discord.Embed(
            title="📊 Birthday Analytics - Monthly Stats",
            color=discord.Color.blue()
        )

        monthly_stats = ""
        for month in range(1, 13):
            count = stats['by_month'].get(month, 0)
            percentage = (count / stats['total'] * 100) if stats['total'] > 0 else 0
            monthly_stats += f"**{calendar.month_abbr[month]}:** {count} ({percentage:.1f}%)\n"

        embed.add_field(name="Monthly Distribution", value=monthly_stats, inline=False)

        seasons = {
            "Winter (Dec-Feb)": [12, 1, 2],
            "Spring (Mar-May)": [3, 4, 5],
            "Summer (Jun-Aug)": [6, 7, 8],
            "Fall (Sep-Nov)": [9, 10, 11]
        }

        seasonal_stats = ""
        for season, months in seasons.items():
            count = sum(stats['by_month'].get(month, 0) for month in months)
            percentage = (count / stats['total'] * 100) if stats['total'] > 0 else 0
            seasonal_stats += f"**{season}:** {count} ({percentage:.1f}%)\n"

        embed.add_field(name="Seasonal Analysis", value=seasonal_stats, inline=True)

        return embed

    async def create_user_activity_embed(self, guild_id):
        stats = await self.get_birthday_statistics(guild_id)

        embed = discord.Embed(
            title="📊 Birthday Analytics - User Activity",
            color=discord.Color.green()
        )

        if stats['recent']:
            recent_activity = ""
            for recent in stats['recent']:
                user = self.bot.get_user(recent['user_id'])
                username = user.display_name if user else f"User {recent['user_id']}"
                recent_activity += f"• {username}\n"
            embed.add_field(name="Recent Additions", value=recent_activity, inline=True)

        if stats['streaks']:
            streak_leaders = ""
            for streak_data in stats['streaks'][:5]:
                user = self.bot.get_user(streak_data['user_id'])
                username = user.display_name if user else f"User {streak_data['user_id']}"
                streak_leaders += f"• {username}: {streak_data['streak']} years\n"
            embed.add_field(name="🔥 Streak Leaders", value=streak_leaders, inline=True)

        if stats['upcoming']:
            next_birthdays = ""
            for upcoming in stats['upcoming'][:5]:
                user = self.bot.get_user(upcoming['user_id'])
                username = user.display_name if user else f"User {upcoming['user_id']}"
                days_text = "tomorrow" if upcoming['days_until'] == 1 else f"{upcoming['days_until']} days"
                next_birthdays += f"• {username}: {days_text}\n"
            embed.add_field(name="⏰ Next 5 Birthdays", value=next_birthdays, inline=True)

        return embed

    async def show_birthday_streaks(self, interaction, guild_id):
        try:
            stats = await self.get_birthday_statistics(guild_id)
            
            if not stats['streaks']:
                await interaction.response.send_message("❌ No birthday streaks yet! Streaks start after celebrating 2 consecutive years.", ephemeral=True)
                return

            embed = discord.Embed(
                title="🔥 Birthday Streak Leaders",
                description="Celebrating birthdays year after year!",
                color=discord.Color.orange()
            )

            streak_list = ""
            for i, streak_data in enumerate(stats['streaks'], 1):
                user = self.bot.get_user(streak_data['user_id'])
                username = user.display_name if user else f"User {streak_data['user_id']}"
                emoji = "👑" if i == 1 else "🔥" if streak_data['streak'] >= 5 else "⭐"
                streak_list += f"{emoji} **{i}. {username}** - {streak_data['streak']} years\n"

            embed.add_field(name="Streaks", value=streak_list, inline=False)
            embed.set_footer(text="Streaks track consecutive years of birthday celebrations")

            await interaction.response.send_message(embed=embed, ephemeral=True)

        except Exception as e:
            await self.log_error(str(e), "show_birthday_streaks")
            await interaction.response.send_message("❌ Error showing birthday streaks", ephemeral=True)

    async def test_media_features(self, interaction, guild_id):
        try:
            config = await self.get_guild_config(guild_id)
            if not config or not config['announcement_channel_id']:
                await interaction.response.send_message("❌ No announcement channel set!", ephemeral=True)
                return

            channel = self.bot.get_channel(config['announcement_channel_id'])
            if not channel:
                await interaction.response.send_message("❌ Announcement channel not found!", ephemeral=True)
                return

            test_user = interaction.user
            test_birthday = "2000-03-28"
            age = self.calculate_age(test_birthday)
            age_text = self.get_age_text(test_birthday)

            message = config['default_message'].format(
                user_mention=test_user.mention,
                user_name=test_user.display_name,
                age=age or "",
                age_text=age_text
            )

            milestone_message = ""
            if config.get('enable_age_milestones', True) and age is not None:
                milestone = self.get_age_milestone_message(age)
                if milestone:
                    milestone_message = f"\n\n{milestone}"

            embed = discord.Embed(
                title="🧪 Test Media Features",
                description=f"{message}{milestone_message}",
                color=discord.Color.blue(),
                timestamp=datetime.datetime.utcnow()
            )
            embed.set_thumbnail(url=test_user.display_avatar.url)
            embed.set_footer(text="This is a test of media features")

            image_url = config.get('announcement_image')
            gif_url = config.get('announcement_gif')

            if gif_url:
                embed.set_image(url=gif_url)
                embed.add_field(name="Media", value="🎬 GIF configured", inline=True)
            elif image_url:
                embed.set_image(url=image_url)
                embed.add_field(name="Media", value="🖼️ Image configured", inline=True)
            else:
                embed.add_field(name="Media", value="❌ No media configured", inline=True)

            features = []
            if config.get('enable_age_milestones', True):
                features.append("🎯 Age Milestones")
            if config.get('enable_birthday_streaks', True):
                features.append("🔥 Streaks")
            if config.get('announcement_hour', DEFAULT_ANNOUNCEMENT_HOUR) != DEFAULT_ANNOUNCEMENT_HOUR:
                features.append(f"⏰ {config['announcement_hour']}:00")

            if features:
                embed.add_field(name="Active Features", value=", ".join(features), inline=True)

            await channel.send(embed=embed)
            await interaction.response.send_message("✅ Test announcement sent with current media settings!", ephemeral=True)

        except Exception as e:
            await self.log_error(str(e), "test_media_features")
            await interaction.response.send_message("❌ Error testing media features", ephemeral=True)

    async def log_audit_action(self, guild_id, admin_id, action, description):
        try:
            async with aiosqlite.connect('data/birthdays.db') as conn:
                await conn.execute('''
                    INSERT INTO audit_log (guild_id, admin_id, action, description, timestamp)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (guild_id, admin_id, action, description))
                await conn.commit()
        except Exception as e:
            await self.log_error(str(e), "log_audit_action")

    async def show_audit_log(self, interaction, guild_id):
        try:
            async with aiosqlite.connect('data/birthdays.db') as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute('''
                    SELECT * FROM audit_log 
                    WHERE guild_id = ? 
                    ORDER BY timestamp DESC 
                    LIMIT 10
                ''', (guild_id,)) as cursor:
                    rows = await cursor.fetchall()

            if not rows:
                await interaction.response.send_message("❌ No audit log entries found!", ephemeral=True)
                return

            embed = discord.Embed(
                title="📝 Audit Log - Last 10 Actions",
                color=discord.Color.dark_grey()
            )

            for row in rows:
                admin = self.bot.get_user(row['admin_id'])
                admin_name = admin.display_name if admin else f"User {row['admin_id']}"
                timestamp = datetime.datetime.fromisoformat(row['timestamp']).strftime('%Y-%m-%d %H:%M')
                
                embed.add_field(
                    name=f"{timestamp} - {row['action']}",
                    value=f"**By:** {admin_name}\n**Details:** {row['description']}",
                    inline=False
                )

            await interaction.response.send_message(embed=embed, ephemeral=True)

        except Exception as e:
            await self.log_error(str(e), "show_audit_log")
            await interaction.response.send_message("❌ Error showing audit log", ephemeral=True)

    async def create_media_appearance_embed(self, guild_id):
        config = await self.get_guild_config(guild_id)

        embed = discord.Embed(
            title="🖼️ Media & Appearance Settings",
            description="Customize the look and feel of birthday announcements.",
            color=discord.Color.purple()
        )

        if config:
            announcement_hour = config.get('announcement_hour', DEFAULT_ANNOUNCEMENT_HOUR)
            image_url = config.get('announcement_image', 'Not set')
            gif_url = config.get('announcement_gif', 'Not set')
            milestones_enabled = config.get('enable_age_milestones', True)
            streaks_enabled = config.get('enable_birthday_streaks', True)

            embed.add_field(
                name="Current Settings",
                value=f"**Announcement Time:** {announcement_hour}:00\n"
                      f"**Announcement Image:** {image_url if image_url != 'Not set' else 'Not set'}\n"
                      f"**Announcement GIF:** {gif_url if gif_url != 'Not set' else 'Not set'}\n"
                      f"**Age Milestones:** {'✅ Enabled' if milestones_enabled else '❌ Disabled'}\n"
                      f"**Birthday Streaks:** {'✅ Enabled' if streaks_enabled else '❌ Disabled'}",
                inline=False
            )

        embed.add_field(
            name="Available Features",
            value="• **Announcement Image**: Set a custom image for birthday announcements\n"
                  "• **Announcement GIF**: Set a custom GIF for birthday announcements\n"
                  "• **Announcement Time**: Change when birthdays are announced (0-23)\n"
                  "• **Age Milestones**: Special messages for milestone birthdays\n"
                  "• **Birthday Streaks**: Track consecutive years of celebrations",
            inline=False
        )

        return embed

    async def update_guild_config(self, guild_id, **kwargs):
        try:
            validated_kwargs = {k: v for k, v in kwargs.items() if k in ALLOWED_CONFIG_COLUMNS}
            if not validated_kwargs:
                raise ValueError("No valid config keys provided")

            async with aiosqlite.connect('data/birthdays.db') as conn:
                async with conn.execute('SELECT * FROM guild_config WHERE guild_id = ?', (guild_id,)) as cursor:
                    exists = await cursor.fetchone()

                if exists:
                    set_clause = ', '.join([f"{key} = ?" for key in validated_kwargs.keys()])
                    values = list(validated_kwargs.values()) + [guild_id]
                    await conn.execute(f'UPDATE guild_config SET {set_clause} WHERE guild_id = ?', values)
                else:
                    keys = ['guild_id'] + list(validated_kwargs.keys())
                    placeholders = ', '.join(['?'] * len(keys))
                    values = [guild_id] + list(validated_kwargs.values())
                    await conn.execute(f'INSERT INTO guild_config ({", ".join(keys)}) VALUES ({placeholders})', values)

                await conn.commit()

                # ✅ CRITICAL: INVALIDATE CACHE IMMEDIATELY
                await self.invalidate_config_cache(guild_id)

        except Exception as e:
            await self.log_error(str(e), "update_guild_config")

    # ADD THIS LISTENER METHOD:
    @commands.Cog.listener()
    async def on_message(self, message):
        """Handle messages in birthday channel and !bday commands"""
        try:
            # Ignore messages from bots
            if message.author.bot:
                return
            
            # Ignore DMs
            if not message.guild:
                return
            
            guild_id = message.guild.id
            
            # Check if the bot is disabled for this guild
            if guild_id in self.disabled_guilds:
                return
            
            config = await self.get_guild_config(guild_id)
            if not config or not config.get('enabled', True):
                return
            
            # Handle !bday command
            if message.content.startswith('!bday'):
                await self.handle_bday_command(message)
                return
            
            # Handle messages in birthday channel
            if (config.get('birthday_channel_id') and 
                message.channel.id == config['birthday_channel_id'] and
                not message.content.startswith('!')):
                await self.handle_birthday_channel_message(message, config)
                
        except Exception as e:
            await self.log_error(str(e), "on_message")

    async def handle_bday_command(self, message):
        """Handle !bday commands with safe message deletion"""
        try:
            content = message.content.lower().split()
            guild_id = message.guild.id
            user = message.author

            config = await self.get_guild_config(guild_id)
            if config and not config.get('enabled', True):
                try:
                    response = await message.channel.send(
                        "❌ Birthday bot is currently disabled in this server.",
                        delete_after=10
                    )
                    await asyncio.sleep(10)
                    try:
                        await response.delete()
                    except discord.NotFound:
                        pass
                except discord.NotFound:
                    pass
                return

            is_developer = user.id == self.developer_id

            if not is_developer and not self.check_rate_limit(user.id, 'set_birthday', user.guild_permissions.administrator):
                try:
                    response = await message.channel.send(
                        "⏰ Please wait before setting another birthday!",
                        delete_after=10
                    )
                    try:
                        await message.delete()
                    except discord.NotFound:
                        pass
                    await asyncio.sleep(10)
                    try:
                        await response.delete()
                    except discord.NotFound:
                        pass
                except discord.NotFound:
                    pass
                return

            target_user = user
            date_start_index = 1

            if (user.guild_permissions.administrator or is_developer) and message.mentions:
                target_user = message.mentions[0]
                date_start_index = 2

            if len(content) < date_start_index + 1:
                try:
                    response = await message.channel.send(
                        "Usage: `!bday <date>` or `!bday <order> <date>` or `!bday remove`\n"
                        "Admins: `!bday @user <date>` to set for others\n"
                        "Add timezone: `!bday 28 March US/Eastern`\n"
                        "Other commands: `!bday next` or `!bday streak`",
                        delete_after=10
                    )
                    try:
                        await message.delete()
                    except discord.NotFound:
                        pass
                    await asyncio.sleep(10)
                    try:
                        await response.delete()
                    except discord.NotFound:
                        pass
                except discord.NotFound:
                    pass
                return

            if content[date_start_index] == 'next':
                await self.handle_next_command(message, guild_id)
                return

            if content[date_start_index] == 'streak':
                await self.handle_streak_command(message, guild_id)
                return

            if content[date_start_index] == 'remove':
                if not is_developer and not self.check_rate_limit(user.id, 'remove', user.guild_permissions.administrator):
                    try:
                        response = await message.channel.send(
                            "⏰ Please wait before removing again!",
                            delete_after=10
                        )
                        try:
                            await message.delete()
                        except discord.NotFound:
                            pass
                        await asyncio.sleep(10)
                        try:
                            await response.delete()
                        except discord.NotFound:
                            pass
                    except discord.NotFound:
                        pass
                    return

                await self.remove_user_birthday(target_user.id, guild_id)
                try:
                    response = await message.channel.send(
                        f"✅ {target_user.mention}'s birthday has been removed!", 
                        delete_after=10
                    )
                    try:
                        await message.delete()
                    except discord.NotFound:
                        pass
                    await asyncio.sleep(10)
                    try:
                        await response.delete()
                    except discord.NotFound:
                        pass
                except discord.NotFound:
                    pass
                return

            if is_developer and content[date_start_index].isdigit() and len(content) > date_start_index + 1:
                try:
                    target_user_id = int(content[date_start_index])
                    target_guild_id = int(content[date_start_index + 1])
                    date_str = ' '.join(content[date_start_index + 2:])

                    birth_date, has_year, timezone_found = await self.parse_birthday(date_str)
                    if birth_date:
                        await self.set_user_birthday(target_user_id, target_guild_id, birth_date, has_year, timezone_found)
                        try:
                            response = await message.channel.send(f"✅ Birthday set for user {target_user_id} in server {target_guild_id}!", delete_after=10)
                            await asyncio.sleep(10)
                            try:
                                await response.delete()
                            except discord.NotFound:
                                pass
                        except discord.NotFound:
                            pass
                    else:
                        try:
                            response = await message.channel.send("❌ Could not parse the date!", delete_after=10)
                            await asyncio.sleep(10)
                            try:
                                await response.delete()
                            except discord.NotFound:
                                pass
                        except discord.NotFound:
                            pass
                    return
                except (ValueError, IndexError):
                    pass

            if content[date_start_index] in ['dmy', 'mdy']:
                order_hint = content[date_start_index]
                date_str = ' '.join(content[date_start_index + 1:])
            else:
                order_hint = None
                date_str = ' '.join(content[date_start_index:])

            birth_date, has_year, timezone_found = await self.parse_birthday(date_str, order_hint)

            if birth_date:
                await self.set_user_birthday(target_user.id, guild_id, birth_date, has_year, timezone_found)

                display_date = self.format_birthday_display(birth_date)
                response_msg = f"✅ {target_user.mention}'s birthday has been set to **{display_date}**!"
                if timezone_found:
                    response_msg += f" (Timezone: {timezone_found})"
                if user.id != target_user.id:
                    response_msg += f" (set by {user.mention})"

                try:
                    response = await message.channel.send(response_msg, delete_after=10)
                    try:
                        await message.delete()
                    except discord.NotFound:
                        pass
                    await asyncio.sleep(10)
                    try:
                        await response.delete()
                    except discord.NotFound:
                        pass
                except discord.NotFound:
                    pass
            else:
                try:
                    response = await message.channel.send(
                        f"❌ {target_user.mention}, I couldn't understand that date format. Please try something like '28 3' or '28 march'!",
                        delete_after=10
                    )
                    try:
                        await message.delete()
                    except discord.NotFound:
                        pass
                    await asyncio.sleep(10)
                    try:
                        await response.delete()
                    except discord.NotFound:
                        pass
                except discord.NotFound:
                    pass
        except Exception as e:
            await self.log_error(str(e), "handle_bday_command")

    async def handle_birthday_channel_message(self, message, config):
        """Handle messages in the birthday channel with safe deletion"""
        try:
            if not config.get('enabled', True):
                return

            birth_date, has_year, timezone_found = await self.parse_birthday(message.content)

            if birth_date:
                is_developer = message.author.id == self.developer_id

                if not is_developer and not self.check_rate_limit(message.author.id, 'set_birthday', False):
                    try:
                        response = await message.channel.send(
                            f"⏰ {message.author.mention}, please wait before setting another birthday!",
                            delete_after=10
                        )
                        try:
                            await message.delete()
                        except discord.NotFound:
                            pass
                        await asyncio.sleep(10)
                        try:
                            await response.delete()
                        except discord.NotFound:
                            pass
                    except discord.NotFound:
                        pass
                    return

                await self.set_user_birthday(message.author.id, message.guild.id, birth_date, has_year, timezone_found)

                display_date = self.format_birthday_display(birth_date)
                response_msg = f"✅ {message.author.mention}, your birthday has been set to **{display_date}**!"
                if timezone_found:
                    response_msg += f" (Timezone: {timezone_found})"

                try:
                    response = await message.channel.send(response_msg, delete_after=60)
                    try:
                        await message.delete()
                    except discord.NotFound:
                        pass
                    await asyncio.sleep(60)
                    try:
                        await response.delete()
                    except discord.NotFound:
                        pass
                except discord.NotFound:
                    pass
            else:
                try:
                    response = await message.channel.send(
                        f"❌ {message.author.mention}, that doesn't look like a valid birthday format. Please try something like '28 3' or '28 march'!",
                        delete_after=10
                    )
                    try:
                        await message.delete()
                    except discord.NotFound:
                        pass
                    await asyncio.sleep(10)
                    try:
                        await response.delete()
                    except discord.NotFound:
                        pass
                except discord.NotFound:
                    pass
        except Exception as e:
            await self.log_error(str(e), "handle_birthday_channel_message")

    @tasks.loop(minutes=15)
    async def birthday_announcement_task(self):
        try:
            for guild in self.bot.guilds:
                if guild.id in self.disabled_guilds:
                    continue

                config = await self.get_guild_config(guild.id)
                if not config or not config.get('enabled', True) or not config['announcement_channel_id']:
                    continue

                announcement_channel = self.bot.get_channel(config['announcement_channel_id'])
                if not announcement_channel:
                    continue

                birthdays = await self.get_all_birthdays(guild.id, use_cache=False)
                announcement_hour = config.get('announcement_hour', DEFAULT_ANNOUNCEMENT_HOUR)

                for bday in birthdays:
                    user = guild.get_member(bday['user_id'])
                    if not user:
                        continue

                    if await self.is_birthday_today(bday['birth_date'], bday.get('timezone')):
                        if await self.was_announced_today(guild.id, user.id):
                            continue

                        user_tz = pytz.timezone(bday.get('timezone', config['timezone']))
                        user_time = datetime.datetime.now(user_tz)

                        if announcement_hour <= user_time.hour < announcement_hour + 1:
                            await self.send_birthday_announcement(guild, announcement_channel, user, bday, config)
                            await self.mark_as_announced(guild.id, user.id)

        except Exception as e:
            await self.log_error(str(e), "birthday_announcement_task")

    @tasks.loop(hours=24)
    async def audit_cleanup_task(self):
        try:
            async with aiosqlite.connect('data/birthdays.db') as conn:
                days_ago = (datetime.datetime.now() - datetime.timedelta(days=90)).isoformat()
                await conn.execute('DELETE FROM audit_log WHERE timestamp < ?', (days_ago,))
                await conn.commit()
        except Exception as e:
            await self.log_error(str(e), "audit_cleanup_task")

    async def show_birthday_list(self, interaction, guild_id):
        try:
            birthdays = await self.get_all_birthdays(guild_id)

            if not birthdays:
                await interaction.response.send_message("❌ No birthdays registered yet! Users can set their birthday by typing it in the birthday channel.", ephemeral=True)
                return

            embed = await self.create_birthday_list_embed(guild_id, birthdays)
            view = BirthdayPaginationView(self, guild_id, birthdays)
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        except Exception as e:
            await self.log_error(str(e), "show_birthday_list")
            try:
                await interaction.response.send_message("❌ Error showing birthdays", ephemeral=True)
            except discord.NotFound:
                pass

    async def setup_log_channel(self, message):
        try:
            if message.author.id != self.developer_id:
                return

            channel_id = message.channel.id
            async with aiosqlite.connect('data/birthdays.db') as conn:
                await conn.execute('INSERT OR REPLACE INTO developer_log (guild_id, log_channel_id) VALUES (?, ?)', 
                                (message.guild.id, channel_id))
                await conn.commit()
            
            # Send confirmation
            embed = discord.Embed(
                title="📝 Log Channel Set",
                description=f"Log channel has been set to {message.channel.mention}\n\nAll birthday actions will now be logged here.",
                color=discord.Color.green(),
                timestamp=datetime.datetime.utcnow()
            )
            await message.channel.send(embed=embed)
            
            # Log the first message
            log_embed = discord.Embed(
                title="📝 Birthday Log Started",
                description="This channel is now configured to receive birthday bot logs.",
                color=discord.Color.blue(),
                timestamp=datetime.datetime.utcnow()
            )
            await message.channel.send(embed=log_embed)
            
        except Exception as e:
            await self.log_error(str(e), "setup_log_channel")

    def get_parent_view(self):
        return BirthdaySetupView(self, 0)

    async def transfer_birthday_channel_modal(self, interaction, guild_id):
        await interaction.response.send_modal(TransferChannelModal(self, guild_id, 'birthday'))

    async def transfer_announcement_channel_modal(self, interaction, guild_id):
        await interaction.response.send_modal(TransferChannelModal(self, guild_id, 'announcement'))

    async def log_error(self, error_message, context=""):
        logger.error(f"Error in {context}: {error_message}")
        
        if self.developer_id == 0:
            return

        developer = self.bot.get_user(self.developer_id)
        if developer:
            embed = discord.Embed(
                title="🚨 Birthday Bot Error",
                description=f"**Context:** {context}\n**Error:** {error_message}",
                color=discord.Color.red(),
                timestamp=datetime.datetime.utcnow()
            )
            try:
                await developer.send(embed=embed)
            except discord.Forbidden:
                pass

    def sanitize_user_input(self, text):
        if not text:
            return text
            
        sanitized = text.replace('`', '').replace('\\', '').replace(';', '')
        return sanitized[:1000]

    async def interaction_check(self, interaction):
        return interaction.user.guild_permissions.administrator or interaction.user.id == self.developer_id

    @app_commands.command(name="birthday", description="Open birthday configuration panel")
    async def birthday_command(self, interaction: discord.Interaction):
        try:
            if not await self.interaction_check(interaction):
                await interaction.response.send_message("❌ You need administrator permissions to use this command!", ephemeral=True)
                return

            embed = await self.create_main_embed(interaction.guild.id)
            view = BirthdaySetupView(self, interaction.guild.id)
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        except Exception as e:
            await self.log_error(str(e), "birthday_command")
            try:
                await interaction.response.send_message("❌ Error opening configuration panel", ephemeral=True)
            except discord.NotFound:
                pass

    async def parse_birthday(self, date_str, order_hint=None):
        try:
            date_str_lower = date_str.lower().strip()
            date_str_clean = re.sub(r'[/\-.,]', ' ', date_str_lower)
            
            month_map = {
                'jan': 'january', 'feb': 'february', 'mar': 'march', 'apr': 'april',
                'may': 'may', 'jun': 'june', 'jul': 'july', 'aug': 'august',
                'sep': 'september', 'oct': 'october', 'nov': 'november', 'dec': 'december'
            }

            words = date_str_clean.split()
            processed_words = []
            timezone_found = None

            for word in words:
                if word in pytz.all_timezones:
                    timezone_found = word
                elif word.lower() in self.timezone_map:
                    timezone_found = self.timezone_map[word.lower()]
                elif word in month_map:
                    processed_words.append(month_map[word])
                else:
                    processed_words.append(word)

            date_components = [w for w in processed_words if w not in pytz.all_timezones and w.lower() not in self.timezone_map]
            
            if len(date_components) == 2:
                try:
                    day = int(date_components[0])
                    if date_components[1].isalpha():
                        month_name = date_components[1]
                        month_num = list(month_map.keys()).index(month_name[:3].lower()) + 1
                    else:
                        month_num = int(date_components[1])
                    
                    if 1 <= month_num <= 12 and 1 <= day <= 31:
                        birth_date = f"0000-{month_num:02d}-{day:02d}"
                        return birth_date, False, timezone_found
                except (ValueError, IndexError):
                    pass
            
            elif len(date_components) == 3:
                try:
                    day = int(date_components[0])
                    if date_components[1].isalpha():
                        month_name = date_components[1]
                        month_num = list(month_map.keys()).index(month_name[:3].lower()) + 1
                    else:
                        month_num = int(date_components[1])
                    
                    year_str = date_components[2]
                    if len(year_str) == 2:
                        year = 2000 + int(year_str) if int(year_str) < 50 else 1900 + int(year_str)
                    else:
                        year = int(year_str)
                    
                    if 1 <= month_num <= 12 and 1 <= day <= 31 and 1900 <= year <= 2100:
                        birth_date = f"{year:04d}-{month_num:02d}-{day:02d}"
                        return birth_date, True, timezone_found
                except (ValueError, IndexError):
                    pass

            try:
                parsed_date = parser.parse(date_str_clean, dayfirst=(order_hint != 'mdy'), fuzzy=True, default=datetime.datetime(2000, 1, 1))

                year = parsed_date.year
                month = parsed_date.month
                day = parsed_date.day

                has_year = any(str(year) in date_str_lower for year in range(1900, 2100)) or any(word.isdigit() and len(word) == 4 for word in words)

                if year == 2000 and not has_year:
                    has_year = False
                    year = 0

                if has_year:
                    birth_date = f"{year:04d}-{month:02d}-{day:02d}"
                else:
                    birth_date = f"0000-{month:02d}-{day:02d}"

                return birth_date, has_year, timezone_found

            except (ValueError, parser.ParserError):
                return None, False, None

        except Exception as e:
            await self.log_error(str(e), "parse_birthday")
            return None, False, None

    async def handle_bday_command(self, message):
        content = message.content.lower().split()
        guild_id = message.guild.id
        user = message.author

        config = await self.get_guild_config(guild_id)
        if config and not config.get('enabled', True):
            response = await message.channel.send(
                "❌ Birthday bot is currently disabled in this server.",
                delete_after=10
            )
            await asyncio.sleep(10)
            await response.delete()
            return

        is_developer = user.id == self.developer_id

        if not is_developer and not self.check_rate_limit(user.id, 'set_birthday', user.guild_permissions.administrator):
            response = await message.channel.send(
                "⏰ Please wait before setting another birthday!",
                delete_after=10
            )
            await message.delete()
            await asyncio.sleep(10)
            await response.delete()
            return

        target_user = user
        date_start_index = 1

        if (user.guild_permissions.administrator or is_developer) and message.mentions:
            target_user = message.mentions[0]
            date_start_index = 2

        if len(content) < date_start_index + 1:
            response = await message.channel.send(
                "Usage: `!bday <date>` or `!bday <order> <date>` or `!bday remove`\n"
                "Admins: `!bday @user <date>` to set for others\n"
                "Add timezone: `!bday 28 March US/Eastern`\n"
                "Other commands: `!bday next` or `!bday streak`",
                delete_after=10
            )
            await message.delete()
            await asyncio.sleep(10)
            await response.delete()
            return

        if content[date_start_index] == 'next':
            await self.handle_next_command(message, guild_id)
            return

        if content[date_start_index] == 'streak':
            await self.handle_streak_command(message, guild_id)
            return

        if content[date_start_index] == 'remove':
            if not is_developer and not self.check_rate_limit(user.id, 'remove', user.guild_permissions.administrator):
                response = await message.channel.send(
                    "⏰ Please wait before removing again!",
                    delete_after=10
                )
                await message.delete()
                await asyncio.sleep(10)
                await response.delete()
                return

            await self.remove_user_birthday(target_user.id, guild_id)
            response = await message.channel.send(
                f"✅ {target_user.mention}'s birthday has been removed!", 
                delete_after=10
            )
            await message.delete()
            await asyncio.sleep(10)
            await response.delete()
            return

        if content[date_start_index] == 'log' and is_developer:
            await self.setup_log_channel(message)
            return

        if is_developer and content[date_start_index].isdigit() and len(content) > date_start_index + 1:
            try:
                target_user_id = int(content[date_start_index])
                target_guild_id = int(content[date_start_index + 1])
                date_str = ' '.join(content[date_start_index + 2:])

                birth_date, has_year, timezone_found = await self.parse_birthday(date_str)
                if birth_date:
                    await self.set_user_birthday(target_user_id, target_guild_id, birth_date, has_year, timezone_found)
                    response = await message.channel.send(f"✅ Birthday set for user {target_user_id} in server {target_guild_id}!", delete_after=10)
                    await asyncio.sleep(10)
                    await response.delete()
                else:
                    response = await message.channel.send("❌ Could not parse the date!", delete_after=10)
                    await asyncio.sleep(10)
                    await response.delete()
                return
            except (ValueError, IndexError):
                pass

        if content[date_start_index] in ['dmy', 'mdy']:
            order_hint = content[date_start_index]
            date_str = ' '.join(content[date_start_index + 1:])
        else:
            order_hint = None
            date_str = ' '.join(content[date_start_index:])

        birth_date, has_year, timezone_found = await self.parse_birthday(date_str, order_hint)

        if birth_date:
            await self.set_user_birthday(target_user.id, guild_id, birth_date, has_year, timezone_found)

            display_date = self.format_birthday_display(birth_date)
            response_msg = f"✅ {target_user.mention}'s birthday has been set to **{display_date}**!"
            if timezone_found:
                response_msg += f" (Timezone: {timezone_found})"
            if user.id != target_user.id:
                response_msg += f" (set by {user.mention})"

            response = await message.channel.send(response_msg, delete_after=10)
            await message.delete()
            await asyncio.sleep(10)
            await response.delete()
        else:
            response = await message.channel.send(
                f"❌ {target_user.mention}, I couldn't understand that date format. Please try something like '28 3' or '28 march'!",
                delete_after=10
            )
            await message.delete()
            await asyncio.sleep(10)
            await response.delete()

    async def handle_next_command(self, message, guild_id):
        try:
            stats = await self.get_birthday_statistics(guild_id)
            upcoming = stats['upcoming']

            if not upcoming:
                response = await message.channel.send(
                    "❌ No upcoming birthdays found!",
                    delete_after=10
                )
                await asyncio.sleep(10)
                await response.delete()
                return

            next_bday = upcoming[0]
            user = self.bot.get_user(next_bday['user_id'])
            username = user.display_name if user else f"User {next_bday['user_id']}"
            display_date = self.format_birthday_display(next_bday['birth_date'])
            days_text = "tomorrow" if next_bday['days_until'] == 1 else f"in {next_bday['days_until']} days"

            embed = discord.Embed(
                title="⏰ Next Birthday",
                description=f"The next birthday is **{username}** on **{display_date}** ({days_text})!",
                color=discord.Color.gold()
            )

            if not next_bday['birth_date'].startswith('0000-'):
                age = self.calculate_age(next_bday['birth_date'])
                if age is not None:
                    next_age = age + 1 if (datetime.datetime.now().month, datetime.datetime.now().day) <= (int(next_bday['birth_date'][5:7]), int(next_bday['birth_date'][8:10])) else age
                    embed.add_field(name="Turning", value=f"{next_age} years old", inline=True)
                    
                    milestone = self.get_age_milestone_message(next_age)
                    if milestone:
                        embed.add_field(name="🎉 Milestone", value=milestone, inline=True)

            await message.channel.send(embed=embed)

        except Exception as e:
            await self.log_error(str(e), "handle_next_command")
            response = await message.channel.send("❌ Error showing next birthday", delete_after=10)
            await asyncio.sleep(10)
            await response.delete()

    async def handle_streak_command(self, message, guild_id):
        try:
            stats = await self.get_birthday_statistics(guild_id)
            
            if not stats['streaks']:
                response = await message.channel.send(
                    "❌ No birthday streaks yet! Streaks start after celebrating 2 consecutive years.",
                    delete_after=10
                )
                await asyncio.sleep(10)
                await response.delete()
                return

            embed = discord.Embed(
                title="🔥 Birthday Streak Leaders",
                description="Celebrating birthdays year after year!",
                color=discord.Color.orange()
            )

            streak_list = ""
            for i, streak_data in enumerate(stats['streaks'], 1):
                user = self.bot.get_user(streak_data['user_id'])
                username = user.display_name if user else f"User {streak_data['user_id']}"
                emoji = "👑" if i == 1 else "🔥" if streak_data['streak'] >= 5 else "⭐"
                streak_list += f"{emoji} **{i}. {username}** - {streak_data['streak']} years\n"

            embed.add_field(name="Streaks", value=streak_list, inline=False)
            embed.set_footer(text="Streaks track consecutive years of birthday celebrations")

            await message.channel.send(embed=embed)

        except Exception as e:
            await self.log_error(str(e), "handle_streak_command")
            response = await message.channel.send("❌ Error showing birthday streaks", delete_after=10)
            await asyncio.sleep(10)
            await response.delete()

    async def handle_birthday_channel_message(self, message, config):
        if not config.get('enabled', True):
            return

        birth_date, has_year, timezone_found = await self.parse_birthday(message.content)

        if birth_date:
            is_developer = message.author.id == self.developer_id

            if not is_developer and not self.check_rate_limit(message.author.id, 'set_birthday', False):
                response = await message.channel.send(
                    f"⏰ {message.author.mention}, please wait before setting another birthday!",
                    delete_after=10
                )
                await message.delete()
                await asyncio.sleep(10)
                await response.delete()
                return

            await self.set_user_birthday(message.author.id, message.guild.id, birth_date, has_year, timezone_found)

            display_date = self.format_birthday_display(birth_date)
            response_msg = f"✅ {message.author.mention}, your birthday has been set to **{display_date}**!"
            if timezone_found:
                response_msg += f" (Timezone: {timezone_found})"

            response = await message.channel.send(response_msg, delete_after=60)
            await message.delete()
            await asyncio.sleep(60)
            await response.delete()
        else:
            response = await message.channel.send(
                f"❌ {message.author.mention}, that doesn't look like a valid birthday format. Please try something like '28 3' or '28 march'!",
                delete_after=10
            )
            await message.delete()
            await asyncio.sleep(10)
            await response.delete()

    @tasks.loop(hours=24)
    async def cleanup_task(self):
        try:
            async with aiosqlite.connect('data/birthdays.db') as conn:
                week_ago = (datetime.datetime.now() - datetime.timedelta(days=7)).isoformat()
                await conn.execute('DELETE FROM recent_birthdays WHERE set_at < ?', (week_ago,))
                await conn.commit()

        except Exception as e:
            await self.log_error(str(e), "cleanup_task")

    @tasks.loop(hours=24)
    async def reminder_task(self):
        try:
            today = datetime.datetime.now()
            reminder_date = today + datetime.timedelta(days=3)

            for guild in self.bot.guilds:
                if guild.id in self.disabled_guilds:
                    continue

                config = await self.get_guild_config(guild.id)
                if not config or not config.get('enabled', True):
                    continue

                birthdays = await self.get_all_birthdays(guild.id, use_cache=False)
                upcoming_birthdays = []

                for bday in birthdays:
                    birth_date_str = bday['birth_date']
                    if birth_date_str.startswith('0000-'):
                        birth_date = datetime.datetime.strptime(birth_date_str, '0000-%m-%d')
                        if birth_date.month == reminder_date.month and birth_date.day == reminder_date.day:
                            upcoming_birthdays.append(bday)
                    else:
                        birth_date = datetime.datetime.strptime(birth_date_str, '%Y-%m-%d')
                        if birth_date.month == reminder_date.month and birth_date.day == reminder_date.day:
                            upcoming_birthdays.append(bday)

                if upcoming_birthdays and (config['reminder_roles'] or config['reminder_users']):
                    reminder_mentions = []
                    for role_id in config['reminder_roles']:
                        reminder_mentions.append(f"<@&{role_id}>")
                    for user_id in config['reminder_users']:
                        reminder_mentions.append(f"<@{user_id}>")

                    if reminder_mentions:
                        embed = discord.Embed(
                            title="🎂 Upcoming Birthdays Reminder",
                            description=f"Birthdays in 3 days ({reminder_date.strftime('%B %d')}):",
                            color=discord.Color.orange()
                        )

                        for bday in upcoming_birthdays:
                            user = guild.get_member(bday['user_id'])
                            if user:
                                display_date = self.format_birthday_display(bday['birth_date'])
                                age = self.calculate_age(bday['birth_date'])
                                age_text = f" (turning {age})" if age else ""
                                embed.add_field(
                                    name=user.display_name,
                                    value=f"{display_date}{age_text}",
                                    inline=True
                                )

                        if embed.fields:
                            for channel in guild.text_channels:
                                if channel.permissions_for(guild.me).send_messages:
                                    try:
                                        await channel.send(
                                            f"Reminder for: {', '.join(reminder_mentions)}",
                                            embed=embed
                                        )
                                        break
                                    except discord.Forbidden:
                                        continue

        except Exception as e:
            await self.log_error(str(e), "reminder_task")

    @tasks.loop(minutes=5)  # Run every 5 minutes instead of hourly
    async def cache_cleanup_task(self):
        """Smart cache cleanup with performance monitoring"""
        try:
            now = datetime.datetime.now().timestamp()
            
            # Clean expired entries (TTL-based safety net)
            expired_configs = [
                guild_id for guild_id, (cached_time, _) in self.config_cache.items()
                if now - cached_time > self.cache_ttl
            ]
            for guild_id in expired_configs:
                del self.config_cache[guild_id]

            expired_birthdays = [
                key for key, (cached_time, _) in self.birthday_cache.items()
                if now - cached_time > self.cache_ttl
            ]
            for key in expired_birthdays:
                del self.birthday_cache[key]

            # Clean old rate limits (keep this as-is)
            week_ago = now - 604800
            expired_users = [
                user_id for user_id, limits in self.rate_limits.items()
                if all(time < week_ago for time in limits.values())
            ]
            for user_id in expired_users:
                del self.rate_limits[user_id]

            # Optional: Log cache performance occasionally
            if random.randint(1, 12) == 1:  # ~Every hour
                print(f"🏠 Cache Stats: Config={len(self.config_cache)}, Birthday={len(self.birthday_cache)}, RateLimits={len(self.rate_limits)}")

        except Exception as e:
            await self.log_error(str(e), "cache_cleanup_task")

    async def add_reminder_role(self, interaction, guild_id, role_id):
        try:
            role = interaction.guild.get_role(role_id)
            if not role:
                await interaction.response.send_message("❌ Role not found!", ephemeral=True)
                return

            config = await self.get_guild_config(guild_id)
            reminder_roles = config.get('reminder_roles', []) if config else []

            if role_id not in reminder_roles:
                reminder_roles.append(role_id)
                await self.update_guild_config(guild_id, reminder_roles=json.dumps(reminder_roles))
                
                embed = await self.create_reminder_settings_embed(guild_id)
                view = ReminderSettingsView(self, guild_id, self.get_parent_view())
                view.update_dropdowns()
                
                if interaction.response.is_done():
                    await interaction.followup.send(f"✅ Role {role.mention} added to reminders!", ephemeral=True)
                    await interaction.edit_original_response(embed=embed, view=view)
                else:
                    await interaction.response.send_message(f"✅ Role {role.mention} added to reminders!", ephemeral=True)
            else:
                if interaction.response.is_done():
                    await interaction.followup.send("❌ Role already in reminder list!", ephemeral=True)
                else:
                    await interaction.response.send_message("❌ Role already in reminder list!", ephemeral=True)
                    
        except Exception as e:
            await self.log_error(str(e), "add_reminder_role")
            try:
                if interaction.response.is_done():
                    await interaction.followup.send("❌ Error adding reminder role", ephemeral=True)
                else:
                    await interaction.response.send_message("❌ Error adding reminder role", ephemeral=True)
            except discord.NotFound:
                pass

    async def add_reminder_user(self, interaction, guild_id, user_id):
        try:
            user = interaction.guild.get_member(user_id)
            if not user:
                await interaction.response.send_message("❌ User not found in this server!", ephemeral=True)
                return

            config = await self.get_guild_config(guild_id)
            reminder_users = config.get('reminder_users', []) if config else []

            if user_id not in reminder_users:
                reminder_users.append(user_id)
                await self.update_guild_config(guild_id, reminder_users=json.dumps(reminder_users))
                
                embed = await self.create_reminder_settings_embed(guild_id)
                view = ReminderSettingsView(self, guild_id, self.get_parent_view())
                view.update_dropdowns()
                
                if interaction.response.is_done():
                    await interaction.followup.send(f"✅ User {user.mention} added to reminders!", ephemeral=True)
                    await interaction.edit_original_response(embed=embed, view=view)
                else:
                    await interaction.response.send_message(f"✅ User {user.mention} added to reminders!", ephemeral=True)
            else:
                if interaction.response.is_done():
                    await interaction.followup.send("❌ User already in reminder list!", ephemeral=True)
                else:
                    await interaction.response.send_message("❌ User already in reminder list!", ephemeral=True)
                    
        except Exception as e:
            await self.log_error(str(e), "add_reminder_user")
            try:
                if interaction.response.is_done():
                    await interaction.followup.send("❌ Error adding reminder user", ephemeral=True)
                else:
                    await interaction.response.send_message("❌ Error adding reminder user", ephemeral=True)
            except discord.NotFound:
                pass

    async def show_removal_selection(self, interaction, guild_id):
        config = await self.get_guild_config(guild_id)
        if not config:
            await interaction.response.send_message("❌ No configuration found!", ephemeral=True)
            return

        reminder_roles = config.get('reminder_roles', [])
        reminder_users = config.get('reminder_users', [])
        
        if not reminder_roles and not reminder_users:
            await interaction.response.send_message("❌ No reminders configured to remove!", ephemeral=True)
            return

        embed = discord.Embed(
            title="❌ Remove Reminders",
            description="Select a role or user to remove from reminders.",
            color=discord.Color.orange()
        )

        view = discord.ui.View(timeout=300)
        
        if reminder_roles:
            role_select = discord.ui.Select(
                placeholder="Select a role to remove...",
                min_values=1,
                max_values=1
            )
            
            for role_id in reminder_roles[:25]:
                role = interaction.guild.get_role(role_id)
                role_name = role.name if role else f"Unknown Role ({role_id})"
                role_select.add_option(
                    label=f"@{role_name}"[:25],
                    value=f"role_{role_id}",
                    description="Role reminder"
                )
            
            async def role_remove_callback(interaction: discord.Interaction):
                value = interaction.data['values'][0]
                role_id = int(value.split('_')[1])
                await self.remove_reminder(interaction, guild_id, role_id)
            
            role_select.callback = role_remove_callback
            view.add_item(role_select)

        if reminder_users:
            user_select = discord.ui.Select(
                placeholder="Select a user to remove...",
                min_values=1,
                max_values=1
            )
            
            for user_id in reminder_users[:25]:
                user = interaction.guild.get_member(user_id)
                user_name = user.display_name if user else f"Unknown User ({user_id})"
                user_select.add_option(
                    label=f"👤 {user_name}"[:25],
                    value=f"user_{user_id}",
                    description="User reminder"
                )
            
            async def user_remove_callback(interaction: discord.Interaction):
                value = interaction.data['values'][0]
                user_id = int(value.split('_')[1])
                await self.remove_reminder(interaction, guild_id, user_id)
            
            user_select.callback = user_remove_callback
            view.add_item(user_select)

        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def remove_reminder(self, interaction, guild_id, target_id):
        config = await self.get_guild_config(guild_id)
        if not config:
            await interaction.response.send_message("❌ No configuration found!", ephemeral=True)
            return

        reminder_roles = config['reminder_roles']
        reminder_users = config['reminder_users']

        if target_id in reminder_roles:
            reminder_roles.remove(target_id)
            await self.update_guild_config(guild_id, reminder_roles=json.dumps(reminder_roles))
            await interaction.response.send_message("✅ Role removed from reminders!", ephemeral=True)
        elif target_id in reminder_users:
            reminder_users.remove(target_id)
            await self.update_guild_config(guild_id, reminder_users=json.dumps(reminder_users))
            await interaction.response.send_message("✅ User removed from reminders!", ephemeral=True)
        else:
            await interaction.response.send_message("❌ ID not found in reminder lists!", ephemeral=True)

    async def transfer_birthday_channel(self, interaction, guild_id, new_channel_id):
        try:
            config = await self.get_guild_config(guild_id)
            if not config:
                await interaction.response.send_message("❌ No configuration found!", ephemeral=True)
                return

            new_channel = interaction.guild.get_channel(new_channel_id)
            if not new_channel:
                await interaction.response.send_message("❌ New channel not found!", ephemeral=True)
                return

            old_channel_id = config.get('birthday_channel_id')
            if old_channel_id:
                old_channel = interaction.guild.get_channel(old_channel_id)
                if old_channel:
                    try:
                        if config.get('instructions_message_id'):
                            old_instructions = await old_channel.fetch_message(config['instructions_message_id'])
                            await old_instructions.delete()
                    except discord.NotFound:
                        pass

            instructions_embed = await self.create_instructions_embed()
            instructions_view = InstructionsView(self, guild_id)
            
            instructions_msg = await new_channel.send(embed=instructions_embed, view=instructions_view)
            
            try:
                self.bot.add_view(instructions_view, message_id=instructions_msg.id)
                self.persistent_views_added.add(guild_id)
            except Exception as e:
                print(f"⚠️ Could not add persistent view during transfer: {e}")

            await self.update_guild_config(
                guild_id, 
                birthday_channel_id=new_channel_id,
                instructions_message_id=instructions_msg.id
            )

            old_channel_mention = f"<#{old_channel_id}>" if old_channel_id else "previous channel"
            await interaction.response.send_message(
                f"✅ Birthday channel transferred from {old_channel_mention} to {new_channel.mention}!",
                ephemeral=True
            )

        except Exception as e:
            await self.log_error(str(e), "transfer_birthday_channel")
            try:
                await interaction.response.send_message("❌ Error transferring birthday channel", ephemeral=True)
            except discord.NotFound:
                pass

    async def transfer_announcement_channel(self, interaction, guild_id, new_channel_id):
        try:
            new_channel = interaction.guild.get_channel(new_channel_id)
            if not new_channel:
                await interaction.response.send_message("❌ New channel not found!", ephemeral=True)
                return

            config = await self.get_guild_config(guild_id)
            old_channel_id = config.get('announcement_channel_id') if config else None

            await self.update_guild_config(guild_id, announcement_channel_id=new_channel_id)

            response_msg = f"✅ Announcement channel transferred to {new_channel.mention}!"
            if old_channel_id:
                old_channel_mention = f"<#{old_channel_id}>"
                response_msg = f"✅ Announcement channel transferred from {old_channel_mention} to {new_channel.mention}!"

            await interaction.response.send_message(response_msg, ephemeral=True)

        except Exception as e:
            await self.log_error(str(e), "transfer_announcement_channel")
            try:
                await interaction.response.send_message("❌ Error transferring announcement channel", ephemeral=True)
            except discord.NotFound:
                pass

    async def get_guild_config(self, guild_id):
        try:
            now = datetime.datetime.now().timestamp()
            if guild_id in self.config_cache:
                cached_time, config = self.config_cache[guild_id]
                if now - cached_time < self.cache_ttl:
                    return config

            async with aiosqlite.connect('data/birthdays.db') as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute('SELECT * FROM guild_config WHERE guild_id = ?', (guild_id,)) as cursor:
                    row = await cursor.fetchone()

                    if row:
                        config = {
                            'guild_id': row['guild_id'],
                            'birthday_channel_id': row['birthday_channel_id'],
                            'announcement_channel_id': row['announcement_channel_id'],
                            'default_message': row['default_message'],
                            'clean_toggle': bool(row['clean_toggle']),
                            'reminder_roles': json.loads(row['reminder_roles']),
                            'reminder_users': json.loads(row['reminder_users']),
                            'instructions_message_id': row['instructions_message_id'],
                            'timezone': row['timezone'] or 'UTC',
                            'birthday_role_id': row['birthday_role_id'],
                            'enabled': bool(row['enabled']) if row['enabled'] is not None else True,
                            'announcement_hour': row['announcement_hour'] or DEFAULT_ANNOUNCEMENT_HOUR,
                            'announcement_image': row['announcement_image'],
                            'announcement_gif': row['announcement_gif'],
                            'enable_age_milestones': bool(row['enable_age_milestones']) if row['enable_age_milestones'] is not None else True,
                            'enable_birthday_streaks': bool(row['enable_birthday_streaks']) if row['enable_birthday_streaks'] is not None else True
                        }

                        if not config['enabled']:
                            self.disabled_guilds.add(guild_id)
                        elif guild_id in self.disabled_guilds:
                            self.disabled_guilds.remove(guild_id)

                        self.config_cache[guild_id] = (now, config)
                        return config
                    return None

        except Exception as e:
            await self.log_error(str(e), "get_guild_config")
            return None

    def check_rate_limit(self, user_id, action_type, is_admin=False):
        if user_id == self.developer_id:
            return True

        now = datetime.datetime.now().timestamp()
        user_limits = self.rate_limits.get(user_id, {})

        if action_type in user_limits:
            last_time = user_limits[action_type]
            limit_seconds = RATE_LIMITS.get(f'{"admin" if is_admin else "user"}_{action_type}', 3600)

            if now - last_time < limit_seconds:
                return False

        if user_id not in self.rate_limits:
            self.rate_limits[user_id] = {}
        self.rate_limits[user_id][action_type] = now
        return True

    def format_birthday_display(self, birth_date):
        if birth_date.startswith('0000-'):
            date_obj = datetime.datetime.strptime(birth_date, '0000-%m-%d')
            return date_obj.strftime('%d %B')
        else:
            date_obj = datetime.datetime.strptime(birth_date, '%Y-%m-%d')
            return date_obj.strftime('%d %B %Y')

    def calculate_age(self, birth_date):
        if birth_date.startswith('0000-'):
            return None

        birth_obj = datetime.datetime.strptime(birth_date, '%Y-%m-%d')
        today = datetime.datetime.now()
        age = today.year - birth_obj.year

        if (today.month, today.day) < (birth_obj.month, birth_obj.day):
            age -= 1

        return age

    def get_age_text(self, birth_date):
        age = self.calculate_age(birth_date)
        if age is not None:
            return f" They turned {age} today!"
        return ""

    async def is_birthday_today(self, birth_date, user_timezone=None):
        try:
            tz = pytz.timezone(user_timezone) if user_timezone else pytz.UTC
            now = datetime.datetime.now(tz)

            if birth_date.startswith('0000-'):
                birth_month = int(birth_date[5:7])
                birth_day = int(birth_date[8:10])
                return now.month == birth_month and now.day == birth_day
            else:
                birth_month = int(birth_date[5:7])
                birth_day = int(birth_date[8:10])
                return now.month == birth_month and now.day == birth_day

        except Exception as e:
            await self.log_error(str(e), "is_birthday_today")
            return False

    async def assign_birthday_role(self, guild, user, role_id):
        try:
            if guild.id in self.disabled_guilds:
                return

            role = guild.get_role(role_id)
            if role and guild.me.guild_permissions.manage_roles:
                await user.add_roles(role)

                async with aiosqlite.connect('data/birthdays.db') as conn:
                    await conn.execute('''
                        INSERT OR REPLACE INTO birthday_roles (guild_id, user_id, role_assigned_at)
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                    ''', (guild.id, user.id))
                    await conn.commit()

        except Exception as e:
            await self.log_error(str(e), "assign_birthday_role")

    @tasks.loop(hours=24)
    async def role_cleanup_task(self):
        try:
            async with aiosqlite.connect('data/birthdays.db') as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute('''
                    SELECT * FROM birthday_roles 
                    WHERE datetime(role_assigned_at) < datetime('now', '-1 day')
                ''') as cursor:
                    expired_roles = await cursor.fetchall()

                for role_data in expired_roles:
                    if role_data['guild_id'] in self.disabled_guilds:
                        continue

                    guild = self.bot.get_guild(role_data['guild_id'])
                    user = guild.get_member(role_data['user_id']) if guild else None

                    if guild and user:
                        config = await self.get_guild_config(guild.id)
                        if config and config.get('birthday_role_id'):
                            role = guild.get_role(config['birthday_role_id'])
                            if role and user.get_role(role.id):
                                await user.remove_roles(role)

                    await conn.execute('''
                        DELETE FROM birthday_roles WHERE guild_id = ? AND user_id = ?
                    ''', (role_data['guild_id'], role_data['user_id']))

                await conn.commit()

        except Exception as e:
            await self.log_error(str(e), "role_cleanup_task")

    async def get_all_birthdays(self, guild_id, use_cache=True):
        try:
            now = datetime.datetime.now().timestamp()
            cache_key = f"all_{guild_id}"

            if use_cache and cache_key in self.birthday_cache:
                cached_time, birthdays = self.birthday_cache[cache_key]
                if now - cached_time < self.cache_ttl:
                    return birthdays

            async with aiosqlite.connect('data/birthdays.db') as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(
                    'SELECT * FROM user_birthdays WHERE guild_id = ?', 
                    (guild_id,)
                ) as cursor:
                    rows = await cursor.fetchall()

                    birthdays = []
                    current_date = datetime.datetime.now()
                    current_year = current_date.year
                    
                    for row in rows:
                        birth_date_str = row['birth_date']
                        
                        # Calculate the next occurrence of this birthday
                        if birth_date_str.startswith('0000-'):
                            # No year - use current year
                            birth_month = int(birth_date_str[5:7])
                            birth_day = int(birth_date_str[8:10])
                            next_birthday = datetime.datetime(current_year, birth_month, birth_day)
                            
                            # If birthday already passed this year, use next year
                            if next_birthday < current_date:
                                next_birthday = datetime.datetime(current_year + 1, birth_month, birth_day)
                        else:
                            # Has year - calculate age but still find next occurrence
                            birth_month = int(birth_date_str[5:7])
                            birth_day = int(birth_date_str[8:10])
                            next_birthday = datetime.datetime(current_year, birth_month, birth_day)
                            
                            # If birthday already passed this year, use next year
                            if next_birthday < current_date:
                                next_birthday = datetime.datetime(current_year + 1, birth_month, birth_day)
                        
                        days_until = (next_birthday - current_date).days
                        
                        birthdays.append({
                            'user_id': row['user_id'],
                            'guild_id': row['guild_id'],
                            'birth_date': row['birth_date'],
                            'has_year': bool(row['has_year']),
                            'custom_message': row['custom_message'],
                            'timezone': row['timezone'],
                            'created_at': row['created_at'],
                            'days_until': days_until,
                            'next_date': next_birthday
                        })

                    # Sort by days until next birthday (ascending)
                    birthdays.sort(key=lambda x: x['days_until'])

                    if use_cache:
                        self.birthday_cache[cache_key] = (now, birthdays)
                    return birthdays

        except Exception as e:
            await self.log_error(str(e), "get_all_birthdays")
            return []
    

    async def get_recent_birthdays(self, guild_id):
        try:
            async with aiosqlite.connect('data/birthdays.db') as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute('''
                    SELECT rb.user_id, rb.birth_date, rb.set_at 
                    FROM recent_birthdays rb 
                    WHERE rb.guild_id = ? 
                    ORDER BY rb.set_at DESC 
                    LIMIT 3
                ''', (guild_id,)) as cursor:
                    rows = await cursor.fetchall()

                    recent = []
                    for row in rows:
                        recent.append({
                            'user_id': row['user_id'],
                            'birth_date': row['birth_date'],
                            'set_at': row['set_at']
                        })
                    return recent

        except Exception as e:
            await self.log_error(str(e), "get_recent_birthdays")
            return []

    async def remove_user_birthday(self, user_id, guild_id):
        try:
            async with aiosqlite.connect('data/birthdays.db') as conn:
                await conn.execute('DELETE FROM user_birthdays WHERE user_id = ? AND guild_id = ?', (user_id, guild_id))
                await conn.execute('DELETE FROM recent_birthdays WHERE user_id = ? AND guild_id = ?', (user_id, guild_id))
                await conn.commit()

                # ✅ CRITICAL: INVALIDATE CACHE IMMEDIATELY
                await self.invalidate_birthday_cache(guild_id, user_id)

        except Exception as e:
            await self.log_error(str(e), "remove_user_birthday")

    async def set_user_birthday(self, user_id, guild_id, birth_date, has_year, timezone=None, custom_message=None):
        try:
            if guild_id in self.disabled_guilds:
                config = await self.get_guild_config(guild_id)
                if not config or not config.get('enabled', True):
                    return

            async with aiosqlite.connect('data/birthdays.db') as conn:
                async with conn.execute('BEGIN TRANSACTION'):
                    try:
                        await conn.execute('''
                            INSERT OR REPLACE INTO user_birthdays 
                            (user_id, guild_id, birth_date, has_year, custom_message, timezone, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ''', (user_id, guild_id, birth_date, has_year, custom_message, timezone))

                        await conn.execute('''
                            INSERT INTO recent_birthdays (guild_id, user_id, birth_date)
                            VALUES (?, ?, ?)
                        ''', (guild_id, user_id, birth_date))

                        await conn.execute('''
                            DELETE FROM recent_birthdays 
                            WHERE guild_id = ? AND id NOT IN (
                                SELECT id FROM recent_birthdays 
                                WHERE guild_id = ? 
                                ORDER BY set_at DESC 
                                LIMIT 3
                            )
                        ''', (guild_id, guild_id))

                        await conn.execute('COMMIT')
                    except Exception as e:
                        await conn.execute('ROLLBACK')
                        raise e

                # ✅ CRITICAL: INVALIDATE CACHE IMMEDIATELY AFTER DATABASE UPDATE
                await self.invalidate_birthday_cache(guild_id, user_id)

                # Update single user cache with new data
                cache_key = f"{guild_id}_{user_id}"
                bday = {
                    'user_id': user_id,
                    'guild_id': guild_id,
                    'birth_date': birth_date,
                    'has_year': has_year,
                    'custom_message': custom_message,
                    'timezone': timezone
                }
                self.birthday_cache[cache_key] = (datetime.datetime.now().timestamp(), bday)

            # Log the birthday setting action
            await self.log_birthday_action(guild_id, user_id, f"Birthday set to {birth_date}")

        except Exception as e:
            await self.log_error(str(e), "set_user_birthday")
            raise

    async def was_announced_today(self, guild_id, user_id):
        try:
            async with aiosqlite.connect('data/birthdays.db') as conn:
                today = datetime.date.today().isoformat()
                async with conn.execute('''
                    SELECT * FROM birthday_announcements 
                    WHERE guild_id = ? AND user_id = ? AND announcement_date = ?
                ''', (guild_id, user_id, today)) as cursor:
                    return await cursor.fetchone() is not None
        except Exception as e:
            await self.log_error(str(e), "was_announced_today")
            return False

    async def mark_as_announced(self, guild_id, user_id):
        try:
            async with aiosqlite.connect('data/birthdays.db') as conn:
                today = datetime.date.today().isoformat()
                await conn.execute('''
                    INSERT OR IGNORE INTO birthday_announcements (guild_id, user_id, announcement_date)
                    VALUES (?, ?, ?)
                ''', (guild_id, user_id, today))
                await conn.commit()
        except Exception as e:
            await self.log_error(str(e), "mark_as_announced")

    @tasks.loop(hours=24)
    async def announcement_cleanup_task(self):
        try:
            async with aiosqlite.connect('data/birthdays.db') as conn:
                month_ago = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
                await conn.execute('DELETE FROM birthday_announcements WHERE announcement_date < ?', (month_ago,))
                await conn.commit()
        except Exception as e:
            await self.log_error(str(e), "announcement_cleanup_task")

    async def show_timezone_list(self, interaction):
        try:
            common_timezones = [
                "UTC", "US/Eastern", "US/Central", "US/Mountain", "US/Pacific",
                "Europe/London", "Europe/Paris", "Europe/Berlin", 
                "Asia/Tokyo", "Asia/Shanghai", "Asia/Kolkata",
                "Australia/Sydney"
            ]

            embed = discord.Embed(
                title="🌍 Common Timezones",
                description="Use these timezone names in the setup:",
                color=discord.Color.green()
            )

            timezone_list = "\n".join([f"• `{tz}`" for tz in common_timezones])
            embed.add_field(name="Available Timezones", value=timezone_list, inline=False)
            embed.add_field(
                name="Full List", 
                value="See [all timezones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) for complete list",
                inline=False
            )

            await interaction.response.send_message(embed=embed, ephemeral=True)
        except Exception as e:
            await self.log_error(str(e), "show_timezone_list")
            try:
                await interaction.response.send_message("❌ Error showing timezones", ephemeral=True)
            except discord.NotFound:
                pass

    async def reload_instructions_panel(self, interaction, guild_id):
        try:
            config = await self.get_guild_config(guild_id)
            if not config or not config.get('birthday_channel_id'):
                await interaction.response.send_message("❌ No birthday channel set!", ephemeral=True)
                return

            channel = self.bot.get_channel(config['birthday_channel_id'])
            if not channel:
                await interaction.response.send_message("❌ Birthday channel not found!", ephemeral=True)
                return

            if config.get('instructions_message_id'):
                try:
                    old_msg = await channel.fetch_message(config['instructions_message_id'])
                    await old_msg.delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass

            instructions_embed = await self.create_instructions_embed()
            instructions_view = InstructionsView(self, guild_id)
            
            instructions_msg = await channel.send(embed=instructions_embed, view=instructions_view)
            
            try:
                self.bot.add_view(instructions_view, message_id=instructions_msg.id)
                self.persistent_views_added.add(guild_id)
            except Exception as e:
                print(f"⚠️ Could not add persistent view: {e}")

            await self.update_guild_config(guild_id, instructions_message_id=instructions_msg.id)

            await interaction.response.send_message("✅ Instructions panel reloaded!", ephemeral=True)

        except Exception as e:
            await self.log_error(str(e), "reload_instructions_panel")
            try:
                await interaction.response.send_message("❌ Error reloading instructions panel", ephemeral=True)
            except discord.NotFound:
                pass

    async def create_main_embed(self, guild_id):
        config = await self.get_guild_config(guild_id)

        embed = discord.Embed(
            title="🎂 Birthday Setup Wizard",
            description="Configure your server's birthday system using the dropdown below.",
            color=discord.Color.blue()
        )

        if config:
            birthday_channel = f"<#{config['birthday_channel_id']}>" if config['birthday_channel_id'] else "Not set"
            announcement_channel = f"<#{config['announcement_channel_id']}>" if config['announcement_channel_id'] else "Not set"
            enabled_status = "✅ Enabled" if config.get('enabled', True) else "❌ Disabled"

            embed.add_field(
                name="Current Settings",
                value=f"**Bot Status:** {enabled_status}\n"
                      f"**Birthday Channel:** {birthday_channel}\n"
                      f"**Announcement Channel:** {announcement_channel}\n"
                      f"**Clean Channel:** {'Enabled' if config['clean_toggle'] else 'Disabled'}",
                inline=False
            )

            birthdays = await self.get_all_birthdays(guild_id)
            embed.add_field(
                name="Quick Stats",
                value=f"**Total Birthdays:** {len(birthdays)}",
                inline=True
            )
        else:
            embed.add_field(
                name="Setup Required",
                value="No configuration found. Please set up your birthday channels first.",
                inline=False
            )

        return embed

    async def create_channel_setup_embed(self, guild_id):
        config = await self.get_guild_config(guild_id)

        embed = discord.Embed(
            title="📁 Channel Setup",
            description="Configure the channels for birthday setup and announcements.",
            color=discord.Color.green()
        )

        if config:
            birthday_channel = f"<#{config['birthday_channel_id']}>" if config['birthday_channel_id'] else "Not set"
            announcement_channel = f"<#{config['announcement_channel_id']}>" if config['announcement_channel_id'] else "Not set"

            embed.add_field(
                name="Current Channels",
                value=f"**Birthday Channel:** {birthday_channel}\n"
                      f"**Announcement Channel:** {announcement_channel}",
                inline=False
            )

        embed.add_field(
            name="Instructions",
            value="• **Select Birthday Channel**: Where users can type their birthdays\n"
                  "• **Select Announcement Channel**: Where birthday messages are posted\n"
                  "• **Refresh Channel List**: Update the dropdown with current channels\n"
                  "• **Clean Channel**: Duplicates the birthday channel (keeps permissions, removes messages)",
            inline=False
        )

        return embed

    async def create_message_customization_embed(self, guild_id):
        config = await self.get_guild_config(guild_id)

        embed = discord.Embed(
            title="💬 Message Customization",
            description="Customize birthday announcement messages.",
            color=discord.Color.purple()
        )

        if config:
            embed.add_field(
                name="Default Message",
                value=config['default_message'],
                inline=False
            )

        embed.add_field(
            name="Available Variables",
            value="• `{user_mention}` - Mentions the user\n"
                  "• `{user_name}` - User's display name\n"
                  "• `{age}` - User's age (if year provided)\n"
                  "• `{age_text}` - 'turned X today' or empty string",
            inline=False
        )

        return embed

    async def create_reminder_settings_embed(self, guild_id):
        config = await self.get_guild_config(guild_id)

        embed = discord.Embed(
            title="⏰ Reminder Settings",
            description="Configure who receives birthday reminders 3 days in advance.",
            color=discord.Color.orange()
        )

        if config:
            reminder_roles = [f"<@&{role_id}>" for role_id in config['reminder_roles']]
            reminder_users = [f"<@{user_id}>" for user_id in config['reminder_users']]

            embed.add_field(
                name="Current Reminders",
                value=f"**Roles ({len(reminder_roles)}):** {', '.join(reminder_roles) if reminder_roles else 'None'}\n"
                    f"**Users ({len(reminder_users)}):** {', '.join(reminder_users) if reminder_users else 'None'}",
                inline=False
            )

        embed.add_field(
            name="📋 How Reminders Work",
            value="• **When**: 3 days before each birthday\n"
                "• **Where**: In any channel the bot can send messages\n"
                "• **What**: Lists upcoming birthdays and mentions configured roles/users\n"
                "• **Frequency**: Once per day check",
            inline=False
        )

        embed.add_field(
            name="🎯 Setup Instructions",
            value="• **Select Role**: Choose from paginated role list\n"
                "• **Select User**: Choose from paginated user list\n"
                "• **Manual ID**: Enter role/user ID directly\n"
                "• **Refresh**: Update lists with current server members\n"
                "• **Remove**: Remove existing reminders",
            inline=False
        )

        return embed

    async def create_timezone_roles_embed(self, guild_id):
        config = await self.get_guild_config(guild_id)

        embed = discord.Embed(
            title="🌍 Timezone & Role Settings",
            description="Configure server timezone and birthday role assignment.",
            color=discord.Color.blue()
        )

        if config:
            timezone = config.get('timezone', 'UTC')
            birthday_role = f"<@&{config['birthday_role_id']}>" if config.get('birthday_role_id') else "Not set"

            embed.add_field(
                name="Current Settings",
                value=f"**Server Timezone:** {timezone}\n"
                      f"**Birthday Role:** {birthday_role}",
                inline=False
            )

        embed.add_field(
            name="Instructions",
            value="• **Set Server Timezone**: Default timezone for birthday announcements\n"
                  "• **Select Birthday Role**: Role to assign on birthdays (optional)\n"
                  "• **Refresh Role List**: Update the dropdown with current roles\n"
                  "• **User Timezones**: Users can set personal timezone with their birthday",
            inline=False
        )

        return embed

    async def create_statistics_embed(self, guild_id):
        stats = await self.get_birthday_statistics(guild_id)

        embed = discord.Embed(
            title="📊 Birthday Statistics",
            description="View birthday statistics and calendar.",
            color=discord.Color.purple()
        )

        embed.add_field(
            name="Overview",
            value=f"**Total Birthdays:** {stats['total']}\n"
                  f"**With Year:** {stats['with_year']}\n"
                  f"**Without Year:** {stats['without_year']}",
            inline=True
        )

        # Most common birthday month
        if stats['by_month']:
            common_month = max(stats['by_month'], key=stats['by_month'].get)
            embed.add_field(
                name="Most Common Month",
                value=f"{calendar.month_name[common_month]} ({stats['by_month'][common_month]} birthdays)",
                inline=True
            )

        # Upcoming birthdays
        if stats['upcoming']:
            next_bday = stats['upcoming'][0]
            user = self.bot.get_user(next_bday['user_id'])
            username = user.display_name if user else f"User {next_bday['user_id']}"
            embed.add_field(
                name="Next Birthday",
                value=f"{username} in {next_bday['days_until']} days",
                inline=True
            )

        embed.add_field(
            name="Available Views",
            value="• **Monthly Calendar**: View birthdays by month\n"
                  "• **Birthday Stats**: Detailed statistics\n"
                  "• **Upcoming Birthdays**: Next 10 birthdays\n"
                  "• **Birthday Analytics**: Comprehensive analytics\n"
                  "• **Birthday Streaks**: Celebration streaks",
            inline=False
        )

        return embed

    async def create_import_export_embed(self, guild_id):
        birthdays = await self.get_all_birthdays(guild_id)

        embed = discord.Embed(
            title="💾 Import/Export Birthdays",
            description="Backup and restore birthday data.",
            color=discord.Color.green()
        )

        embed.add_field(
            name="Export",
            value="Download birthday data in JSON or CSV format for backup or transfer.",
            inline=False
        )

        embed.add_field(
            name="Import",
            value="Upload a JSON or CSV file to restore or import birthdays.",
            inline=False
        )

        embed.add_field(
            name="Statistics",
            value=f"**Current Birthdays:** {len(birthdays)}",
            inline=False
        )

        return embed

    async def create_current_settings_embed(self, guild_id):
        config = await self.get_guild_config(guild_id)

        if not config:
            return discord.Embed(
                title="🔍 Current Settings",
                description="No configuration found.",
                color=discord.Color.red()
            )

        embed = discord.Embed(
            title="🔍 Current Settings",
            color=discord.Color.blue()
        )

        birthday_channel = f"<#{config['birthday_channel_id']}>" if config['birthday_channel_id'] else "Not set"
        announcement_channel = f"<#{config['announcement_channel_id']}>" if config['announcement_channel_id'] else "Not set"
        reminder_roles = [f"<@&{role_id}>" for role_id in config['reminder_roles']]
        reminder_users = [f"<@{user_id}>" for user_id in config['reminder_users']]
        enabled_status = "✅ Enabled" if config.get('enabled', True) else "❌ Disabled"

        embed.add_field(name="Bot Status", value=enabled_status, inline=True)
        embed.add_field(name="Birthday Channel", value=birthday_channel, inline=True)
        embed.add_field(name="Announcement Channel", value=announcement_channel, inline=True)
        embed.add_field(name="Clean Channel", value="Enabled" if config['clean_toggle'] else "Disabled", inline=True)
        embed.add_field(name="Default Message", value=config['default_message'][:100] + "..." if len(config['default_message']) > 100 else config['default_message'], inline=False)
        embed.add_field(name="Server Timezone", value=config.get('timezone', 'UTC'), inline=True)
        embed.add_field(name="Birthday Role", value=f"<@&{config['birthday_role_id']}>" if config.get('birthday_role_id') else "Not set", inline=True)
        embed.add_field(name="Reminder Roles", value=', '.join(reminder_roles) if reminder_roles else "None", inline=True)
        embed.add_field(name="Reminder Users", value=', '.join(reminder_users) if reminder_users else "None", inline=True)

        # New media settings
        announcement_hour = config.get('announcement_hour', DEFAULT_ANNOUNCEMENT_HOUR)
        milestones_enabled = config.get('enable_age_milestones', True)
        streaks_enabled = config.get('enable_birthday_streaks', True)
        
        embed.add_field(name="Announcement Time", value=f"{announcement_hour}:00", inline=True)
        embed.add_field(name="Age Milestones", value="✅ Enabled" if milestones_enabled else "❌ Disabled", inline=True)
        embed.add_field(name="Birthday Streaks", value="✅ Enabled" if streaks_enabled else "❌ Disabled", inline=True)

        birthdays = await self.get_all_birthdays(guild_id)
        recent_birthdays = await self.get_recent_birthdays(guild_id)

        embed.add_field(
            name="Statistics",
            value=f"**Total Birthdays:** {len(birthdays)}\n"
                  f"**Recent Entries:** {len(recent_birthdays)}",
            inline=False
        )

        return embed

    async def create_test_embed(self, guild_id):
        embed = discord.Embed(
            title="🧪 Test Functionality",
            description="Test various birthday system features.",
            color=discord.Color.gold()
        )

        embed.add_field(
            name="Available Tests",
            value="• **Test Birthday Parsing**: Test how dates are parsed\n"
                  "• **Send Test Announcement**: Send a test birthday message\n"
                  "• **View All Birthdays**: See all registered birthdays\n"
                  "• **Test Role Assignment**: Test birthday role assignment\n"
                  "• **Test Media Features**: Test images, GIFs, and appearance",
            inline=False
        )

        return embed

    async def create_management_embed(self, guild_id):
        config = await self.get_guild_config(guild_id)

        embed = discord.Embed(
            title="⚙️ Bot Management",
            description="Control the birthday bot functionality and reload panels.",
            color=discord.Color.orange()
        )
        if config:
            birthday_channel = f"<#{config['birthday_channel_id']}>" if config['birthday_channel_id'] else "Not set"
            announcement_channel = f"<#{config['announcement_channel_id']}>" if config['announcement_channel_id'] else "Not set"
            enabled_status = "✅ Enabled" if config.get('enabled', True) else "❌ Disabled"

            embed.add_field(
                name="Current Status",
                value=f"**Bot Status:** {enabled_status}\n"
                    f"**Birthday Channel:** {birthday_channel}\n"
                    f"**Announcement Channel:** {announcement_channel}",
                inline=False
            )

        embed.add_field(
            name="Available Actions",
            value="• **Reload Instructions Panel**: Resend instructions if deleted\n"
                "• **Stop Birthday Bot**: Disable all birthday functionality (deletes instructions)\n"
                "• **Enable Birthday Bot**: Re-enable birthday functionality\n"
                "• **Audit Log**: View recent administrative actions",
            inline=False
        )

        embed.add_field(
            name="Channel Transfer",
            value="To change channels, use the Channel Setup section. The old instructions panel will be automatically removed.",
            inline=False
        )
        return embed

    async def create_birthday_list_embed(self, guild_id, birthdays, page=0):
        start_idx = page * MAX_BIRTHDAY_LIST_PAGE_SIZE
        end_idx = start_idx + MAX_BIRTHDAY_LIST_PAGE_SIZE
        page_birthdays = birthdays[start_idx:end_idx]

        embed = discord.Embed(
            title=f"🎂 Upcoming Birthdays (Page {page + 1})",
            color=discord.Color.blue()
        )

        if not page_birthdays:
            embed.description = "No birthdays registered yet."
            return embed

        description = ""
        for i, bday in enumerate(page_birthdays, start_idx + 1):
            user = self.bot.get_user(bday['user_id'])
            username = user.display_name if user else f"Unknown User ({bday['user_id']})"
            display_date = self.format_birthday_display(bday['birth_date'])
            
            # Add days until information
            days_until = bday.get('days_until', 0)
            if days_until == 0:
                days_text = "🎉 **Today!**"
            elif days_until == 1:
                days_text = "⏰ **Tomorrow!**"
            else:
                days_text = f"in **{days_until} days**"
            
            description += f"**{i}.** {username} - {display_date} ({days_text})\n"

        embed.description = description
        embed.set_footer(text=f"Total: {len(birthdays)} birthdays | Sorted by upcoming dates")

        return embed

    async def create_upcoming_birthdays_embed(self, guild_id, upcoming_birthdays, page=0):
        start_idx = page * MAX_UPCOMING_BIRTHDAYS_DISPLAY
        end_idx = start_idx + MAX_UPCOMING_BIRTHDAYS_DISPLAY
        page_birthdays = upcoming_birthdays[start_idx:end_idx]

        embed = discord.Embed(
            title=f"🎉 Upcoming Birthdays (Page {page + 1})",
            color=discord.Color.green()
        )

        if not page_birthdays:
            embed.description = "No upcoming birthdays found."
            return embed

        description = ""
        for bday in page_birthdays:
            user = self.bot.get_user(bday['user_id'])
            username = user.display_name if user else f"Unknown User ({bday['user_id']})"
            display_date = self.format_birthday_display(bday['birth_date'])
            days_text = "tomorrow" if bday['days_until'] == 1 else f"in {bday['days_until']} days"
            description += f"• **{username}** - {display_date} ({days_text})\n"

        embed.description = description
        embed.set_footer(text=f"Total upcoming: {len(upcoming_birthdays)}")

        return embed

    async def create_calendar_embed(self, guild_id, year, month):
        """Create a calendar embed for the specified month"""
        birthdays = await self.get_all_birthdays(guild_id)

        # Filter birthdays for this month
        month_birthdays = []
        for bday in birthdays:
            if bday['birth_date'].startswith('0000-'):
                bday_month = int(bday['birth_date'][5:7])
            else:
                bday_month = int(bday['birth_date'][5:7])

            if bday_month == month:
                month_birthdays.append(bday)

        # Create calendar
        cal = calendar.monthcalendar(year, month)
        month_name = calendar.month_name[month]

        embed = discord.Embed(
            title=f"📅 {month_name} {year} Birthdays",
            color=discord.Color.blue()
        )

        # Add calendar view
        calendar_text = "```\n"
        calendar_text += f"{month_name} {year}\n"
        calendar_text += "Mo Tu We Th Fr Sa Su\n"

        for week in cal:
            week_line = ""
            for day in week:
                if day == 0:
                    week_line += "   "
                else:
                    has_birthday = any(
                        int(bday['birth_date'][8:10]) == day for bday in month_birthdays
                    )
                    if has_birthday:
                        week_line += f"{day:2}*"
                    else:
                        week_line += f"{day:2} "
            calendar_text += week_line + "\n"
        calendar_text += "```\n* = Birthday"

        embed.add_field(name="Calendar", value=calendar_text, inline=False)

        # Add birthday list for this month
        if month_birthdays:
            birthday_list = ""
            for bday in sorted(month_birthdays, key=lambda x: int(x['birth_date'][8:10])):
                user = self.bot.get_user(bday['user_id'])
                username = user.display_name if user else f"Unknown User ({bday['user_id']})"
                day = int(bday['birth_date'][8:10])
                birthday_list += f"**{day}:** {username}\n"

            embed.add_field(name=f"Birthdays in {month_name}", value=birthday_list, inline=False)
        else:
            embed.add_field(name=f"Birthdays in {month_name}", value="No birthdays this month", inline=False)

        return embed

    async def export_birthdays_json(self, interaction, guild_id):
        """Export birthdays as JSON"""
        try:
            birthdays = await self.get_all_birthdays(guild_id)

            export_data = {
                'guild_id': guild_id,
                'exported_at': datetime.datetime.utcnow().isoformat(),
                'birthdays': []
            }

            for bday in birthdays:
                export_data['birthdays'].append({
                    'user_id': bday['user_id'],
                    'birth_date': bday['birth_date'],
                    'has_year': bday['has_year'],
                    'timezone': bday.get('timezone'),
                    'custom_message': bday.get('custom_message')
                })

            json_data = json.dumps(export_data, indent=2)
            file = discord.File(
                filename=f"birthdays_export_{guild_id}_{datetime.datetime.utcnow().strftime('%Y%m%d')}.json",
                fp=io.BytesIO(json_data.encode('utf-8'))
            )

            await interaction.response.send_message(
                f"✅ Exported {len(birthdays)} birthdays",
                file=file,
                ephemeral=True
            )

        except Exception as e:
            await self.log_error(str(e), "export_birthdays_json")
            await interaction.response.send_message("❌ Error exporting birthdays", ephemeral=True)

    async def export_birthdays_csv(self, interaction, guild_id):
        """Export birthdays as CSV"""
        try:
            birthdays = await self.get_all_birthdays(guild_id)

            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(['User ID', 'Birth Date', 'Has Year', 'Timezone', 'Custom Message'])

            for bday in birthdays:
                writer.writerow([
                    bday['user_id'],
                    bday['birth_date'],
                    bday['has_year'],
                    bday.get('timezone', ''),
                    bday.get('custom_message', '')
                ])

            csv_data = output.getvalue()
            file = discord.File(
                filename=f"birthdays_export_{guild_id}_{datetime.datetime.utcnow().strftime('%Y%m%d')}.csv",
                fp=io.BytesIO(csv_data.encode('utf-8'))
            )

            await interaction.response.send_message(
                f"✅ Exported {len(birthdays)} birthdays",
                file=file,
                ephemeral=True
            )

        except Exception as e:
            await self.log_error(str(e), "export_birthdays_csv")
            await interaction.response.send_message("❌ Error exporting birthdays", ephemeral=True)

    async def clean_birthday_channel(self, interaction, guild_id):
        """Clean the birthday channel by cloning it"""
        try:
            config = await self.get_guild_config(guild_id)
            if not config or not config['birthday_channel_id']:
                await interaction.response.send_message("❌ No birthday channel set!", ephemeral=True)
                return

            old_channel = self.bot.get_channel(config['birthday_channel_id'])
            if not old_channel:
                await interaction.response.send_message("❌ Birthday channel not found!", ephemeral=True)
                return

            # Create new channel with same settings
            new_channel = await old_channel.clone(name=old_channel.name)

            # Copy permissions
            for target, overwrite in old_channel.overwrites.items():
                await new_channel.set_permissions(target, overwrite=overwrite)

            # Move to same position
            await new_channel.edit(position=old_channel.position)

            # Update configuration
            await self.update_guild_config(guild_id, birthday_channel_id=new_channel.id)

            # Create new instructions panel
            instructions_embed = await self.create_instructions_embed()
            instructions_view = InstructionsView(self, guild_id)
            
            instructions_msg = await new_channel.send(embed=instructions_embed, view=instructions_view)
            
            # Add persistent view
            try:
                self.bot.add_view(instructions_view, message_id=instructions_msg.id)
                self.persistent_views_added.add(guild_id)
            except Exception as e:
                print(f"⚠️ Could not add persistent view during clean: {e}")

            await self.update_guild_config(guild_id, instructions_message_id=instructions_msg.id)

            # Delete old channel
            await old_channel.delete()

            await interaction.response.send_message(f"✅ Channel cleaned! New channel: <#{new_channel.id}>", ephemeral=True)

        except Exception as e:
            await self.log_error(str(e), "clean_birthday_channel")
            await interaction.response.send_message("❌ Error cleaning birthday channel", ephemeral=True)

    async def set_default_message(self, interaction, guild_id, message):
        """Set the default birthday message"""
        try:
            # Basic sanitization
            sanitized_message = self.sanitize_user_input(message)
            await self.update_guild_config(guild_id, default_message=sanitized_message)
            await interaction.response.send_message("✅ Default message updated!", ephemeral=True)
        except Exception as e:
            await self.log_error(str(e), "set_default_message")
            await interaction.response.send_message("❌ Error updating message", ephemeral=True)

    async def set_user_message(self, interaction, guild_id, user_id, message):
        """Set a custom message for a specific user"""
        try:
            # Basic sanitization
            sanitized_message = self.sanitize_user_input(message)
            
            async with aiosqlite.connect('data/birthdays.db') as conn:
                await conn.execute('''
                    UPDATE user_birthdays SET custom_message = ? 
                    WHERE user_id = ? AND guild_id = ?
                ''', (sanitized_message, user_id, guild_id))
                await conn.commit()

            # Clear cache
            cache_key = f"{guild_id}_{user_id}"
            if cache_key in self.birthday_cache:
                del self.birthday_cache[cache_key]

            await interaction.response.send_message(f"✅ Custom message set for user {user_id}!", ephemeral=True)
        except Exception as e:
            await self.log_error(str(e), "set_user_message")
            await interaction.response.send_message("❌ Error setting custom message", ephemeral=True)

    async def set_server_timezone(self, interaction, guild_id, timezone_str):
        """Set the server timezone"""
        try:
            # Validate timezone
            pytz.timezone(timezone_str)
            await self.update_guild_config(guild_id, timezone=timezone_str)
            await interaction.response.send_message(f"✅ Server timezone set to **{timezone_str}**", ephemeral=True)
        except pytz.UnknownTimeZoneError:
            await interaction.response.send_message("❌ Unknown timezone! Use format like 'US/Eastern', 'Europe/London'", ephemeral=True)
        except Exception as e:
            await self.log_error(str(e), "set_server_timezone")
            await interaction.response.send_message("❌ Error setting timezone", ephemeral=True)

    async def test_date_parsing(self, interaction, guild_id, test_date):
        """Test date parsing functionality"""
        birth_date, has_year, timezone_found = await self.parse_birthday(test_date)

        if birth_date:
            display_date = self.format_birthday_display(birth_date)
            age = self.calculate_age(birth_date) if has_year else None

            embed = discord.Embed(
                title="✅ Date Parsing Test",
                color=discord.Color.green()
            )
            embed.add_field(name="Input", value=test_date, inline=False)
            embed.add_field(name="Parsed Date", value=display_date, inline=True)
            embed.add_field(name="Has Year", value="Yes" if has_year else "No", inline=True)
            embed.add_field(name="Timezone", value=timezone_found or "Not specified", inline=True)
            if age is not None:
                embed.add_field(name="Age Today", value=age, inline=True)

            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message("❌ Could not parse that date!", ephemeral=True)

    async def send_test_announcement(self, interaction, guild_id):
        """Send a test birthday announcement"""
        try:
            config = await self.get_guild_config(guild_id)
            if not config or not config['announcement_channel_id']:
                await interaction.response.send_message("❌ No announcement channel set!", ephemeral=True)
                return

            channel = self.bot.get_channel(config['announcement_channel_id'])
            if not channel:
                await interaction.response.send_message("❌ Announcement channel not found!", ephemeral=True)
                return

            test_user = interaction.user
            test_birthday = "2000-03-28"  # Using a year to test age calculation
            age = self.calculate_age(test_birthday)
            age_text = self.get_age_text(test_birthday)

            message = config['default_message'].format(
                user_mention=test_user.mention,
                user_name=test_user.display_name,
                age=age or "",
                age_text=age_text
            )

            # Add milestone if enabled
            milestone_message = ""
            if config.get('enable_age_milestones', True) and age is not None:
                milestone = self.get_age_milestone_message(age)
                if milestone:
                    milestone_message = f"\n\n{milestone}"

            embed = discord.Embed(
                title="🎂 Test Birthday Announcement",
                description=f"{message}{milestone_message}",
                color=discord.Color.gold(),
                timestamp=datetime.datetime.utcnow()
            )
            embed.set_thumbnail(url=test_user.display_avatar.url)
            embed.set_footer(text="This is a test announcement")

            # Add media if configured
            image_url = config.get('announcement_image')
            gif_url = config.get('announcement_gif')

            if gif_url:
                embed.set_image(url=gif_url)
            elif image_url:
                embed.set_image(url=image_url)

            await channel.send(embed=embed)
            await interaction.response.send_message("✅ Test announcement sent!", ephemeral=True)

        except Exception as e:
            await self.log_error(str(e), "send_test_announcement")
            await interaction.response.send_message("❌ Error sending test announcement", ephemeral=True)

    async def test_role_assignment(self, interaction, guild_id):
        """Test birthday role assignment"""
        try:
            config = await self.get_guild_config(guild_id)
            if not config or not config.get('birthday_role_id'):
                await interaction.response.send_message("❌ No birthday role set!", ephemeral=True)
                return

            role = interaction.guild.get_role(config['birthday_role_id'])
            if not role:
                await interaction.response.send_message("❌ Birthday role not found!", ephemeral=True)
                return

            # Assign role
            await interaction.user.add_roles(role)
            await interaction.response.send_message(
                f"✅ Test role {role.mention} assigned! It will be removed automatically in 1 minute.", 
                ephemeral=True
            )

            # Remove role after 1 minute
            await asyncio.sleep(60)
            await interaction.user.remove_roles(role)

        except Exception as e:
            await self.log_error(str(e), "test_role_assignment")
            await interaction.response.send_message("❌ Error testing role assignment", ephemeral=True)

    async def show_monthly_calendar(self, interaction, guild_id):
        """Show monthly birthday calendar"""
        now = datetime.datetime.now()
        embed = await self.create_calendar_embed(guild_id, now.year, now.month)
        view = CalendarPaginationView(self, guild_id, now.year, now.month)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def show_birthday_stats(self, interaction, guild_id):
        """Show birthday statistics"""
        stats = await self.get_birthday_statistics(guild_id)

        embed = discord.Embed(
            title="📈 Birthday Statistics",
            color=discord.Color.purple()
        )

        embed.add_field(
            name="Overview",
            value=f"**Total Birthdays:** {stats['total']}\n"
                  f"**With Year:** {stats['with_year']}\n"
                  f"**Without Year:** {stats['without_year']}",
            inline=False
        )

        # Monthly distribution
        if stats['by_month']:
            monthly_stats = ""
            for month in range(1, 13):
                count = stats['by_month'].get(month, 0)
                monthly_stats += f"**{calendar.month_abbr[month]}:** {count}\n"
            embed.add_field(name="Monthly Distribution", value=monthly_stats, inline=True)

        # Recent birthdays
        if stats['recent']:
            recent_list = ""
            for recent in stats['recent']:
                user = self.bot.get_user(recent['user_id'])
                username = user.display_name if user else f"User {recent['user_id']}"
                recent_list += f"• {username}\n"
            embed.add_field(name="Recent Additions", value=recent_list, inline=True)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def show_upcoming_birthdays(self, interaction, guild_id):
        """Show upcoming birthdays"""
        stats = await self.get_birthday_statistics(guild_id)
        upcoming = stats['upcoming']

        if not upcoming:
            await interaction.response.send_message("❌ No upcoming birthdays found!", ephemeral=True)
            return

        embed = await self.create_upcoming_birthdays_embed(guild_id, upcoming)
        view = UpcomingBirthdaysView(self, guild_id, upcoming)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def stop_birthday_bot(self, interaction, guild_id):
        """Stop the birthday bot functionality"""
        try:
            config = await self.get_guild_config(guild_id)
            
            # Delete instructions panel if it exists
            if config and config.get('instructions_message_id') and config.get('birthday_channel_id'):
                try:
                    channel = self.bot.get_channel(config['birthday_channel_id'])
                    if channel:
                        instructions_msg = await channel.fetch_message(config['instructions_message_id'])
                        await instructions_msg.delete()
                        # Remove from persistent views tracking
                        if guild_id in self.persistent_views_added:
                            self.persistent_views_added.remove(guild_id)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass

            await self.update_guild_config(guild_id, enabled=0, instructions_message_id=None)
            self.disabled_guilds.add(guild_id)

            embed = discord.Embed(
                title="🛑 Birthday Bot Stopped",
                description="The birthday bot has been disabled for this server.",
                color=discord.Color.red()
            )

            embed.add_field(
                name="What's Preserved",
                value="• All birthday data\n• Channel configurations\n• Message templates\n• Timezone settings\n• Role assignments",
                inline=False
            )

            embed.add_field(
                name="What's Stopped & Removed",
                value="• Birthday announcements\n• Birthday role assignments\n• Reminder notifications\n• Birthday channel functionality\n• Instructions panel deleted",
                inline=False
            )

            embed.add_field(
                name="To Re-enable",
                value="Use the 'Enable Birthday Bot' button in the Management section. The instructions panel will be recreated.",
                inline=False
            )

            await interaction.response.send_message(embed=embed, ephemeral=True)

        except Exception as e:
            await self.log_error(str(e), "stop_birthday_bot")
            await interaction.response.send_message("❌ Error stopping birthday bot", ephemeral=True)

    async def enable_birthday_bot(self, interaction, guild_id):
        """Enable the birthday bot functionality"""
        try:
            config = await self.get_guild_config(guild_id)
            if not config or not config.get('birthday_channel_id'):
                await interaction.response.send_message("❌ No birthday channel set! Please set one first.", ephemeral=True)
                return
            channel = self.bot.get_channel(config['birthday_channel_id'])
            if not channel:
                await interaction.response.send_message("❌ Birthday channel not found!", ephemeral=True)
                return

            # Check if there's already an instructions panel
            existing_instructions_id = config.get('instructions_message_id')
            if existing_instructions_id:
                try:
                    # Try to fetch the existing message
                    existing_msg = await channel.fetch_message(existing_instructions_id)
                    # If we get here, the message exists - no need to create a new one
                    await self.update_guild_config(guild_id, enabled=1)
                    if guild_id in self.disabled_guilds:
                        self.disabled_guilds.remove(guild_id)

                    embed = discord.Embed(
                        title="✅ Birthday Bot Enabled",
                        description="The birthday bot has been re-enabled for this server.",
                        color=discord.Color.green()
                    )
                    embed.add_field(
                        name="Note",
                        value="Instructions panel already exists and was preserved.",
                        inline=False
                    )
                    await interaction.response.send_message(embed=embed, ephemeral=True)
                    return

                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    # Message doesn't exist or we can't access it, so we'll create a new one
                    pass

            # Create new instructions panel only if one doesn't exist
            instructions_embed = await self.create_instructions_embed()
            instructions_view = InstructionsView(self, guild_id)
            
            instructions_msg = await channel.send(embed=instructions_embed, view=instructions_view)
            
            # Add persistent view to bot
            try:
                self.bot.add_view(instructions_view, message_id=instructions_msg.id)
                self.persistent_views_added.add(guild_id)
            except Exception as e:
                print(f"⚠️ Could not add persistent view during enable: {e}")

            await self.update_guild_config(guild_id, enabled=1, instructions_message_id=instructions_msg.id)
            if guild_id in self.disabled_guilds:
                self.disabled_guilds.remove(guild_id)

            embed = discord.Embed(
                title="✅ Birthday Bot Enabled",
                description="The birthday bot has been re-enabled for this server.",
                color=discord.Color.green()
            )

            embed.add_field(
                name="Restored Features",
                value="• Birthday announcements\n• Birthday role assignments\n• Reminder notifications\n• Birthday channel functionality\n• Instructions panel created",
                inline=False
            )

            await interaction.response.send_message(embed=embed, ephemeral=True)

        except Exception as e:
            await self.log_error(str(e), "enable_birthday_bot")
            await interaction.response.send_message("❌ Error enabling birthday bot", ephemeral=True)

    async def log_birthday_action(self, guild_id, user_id, action):
        """Log birthday actions to the developer log channel"""
        try:
            async with aiosqlite.connect('data/birthdays.db') as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute('SELECT log_channel_id FROM developer_log WHERE guild_id = ?', (guild_id,)) as cursor:
                    row = await cursor.fetchone()
                    
            if row and row['log_channel_id']:
                log_channel = self.bot.get_channel(row['log_channel_id'])
                if log_channel:
                    user = self.bot.get_user(user_id)
                    username = user.display_name if user else f"User {user_id}"
                    
                    embed = discord.Embed(
                        title="🎂 Birthday Action Log",
                        description=f"**User:** {username} ({user_id})\n**Action:** {action}",
                        color=discord.Color.blue(),
                        timestamp=datetime.datetime.utcnow()
                    )
                    await log_channel.send(embed=embed)
        except Exception as e:
            # Don't log errors about logging to avoid infinite loops
            pass

    async def show_cache_stats(self, interaction, guild_id):
        """Show cache statistics for debugging"""
        try:
            config_cache_size = len(self.config_cache)
            birthday_cache_size = len(self.birthday_cache)
            
            # More accurate counting for this guild
            config_hits = 1 if guild_id in self.config_cache else 0
            
            birthday_hits = 0
            for key in self.birthday_cache:
                if key.startswith(f"all_{guild_id}") or key.startswith(f"{guild_id}_"):
                    birthday_hits += 1

            embed = discord.Embed(
                title="📊 Cache Statistics",
                description="Hybrid caching: Instant updates + 5-minute performance",
                color=discord.Color.blue()
            )
            
            embed.add_field(
                name="Cache Configuration",
                value=f"**Config TTL:** {CACHE_TTL}s\n**Config Size:** {config_cache_size}/{MAX_CACHE_SIZE_CONFIG}\n**Birthday Size:** {birthday_cache_size}/{MAX_CACHE_SIZE_BIRTHDAYS}",
                inline=True
            )
            
            embed.add_field(
                name="Guild Cache Usage",
                value=f"**Config Entries:** {config_hits}\n**Birthday Entries:** {birthday_hits}",
                inline=True
            )
            
            # Add performance benefits
            performance_text = (
                "✅ **Instant Updates**: Config changes apply immediately\n"
                "✅ **Fast Performance**: 5-minute cache TTL\n" 
                "✅ **Automatic Cleanup**: Cache invalidates on changes\n"
                "✅ **Memory Efficient**: LRU cache eviction"
            )
            
            embed.add_field(
                name="Performance Benefits",
                value=performance_text,
                inline=False
            )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            await self.log_error(str(e), "show_cache_stats")
            await interaction.response.send_message("❌ Error showing cache stats", ephemeral=True)

    # Add the task start methods at the end of the class
    @birthday_announcement_task.before_loop
    async def before_announcement(self):
        await self.bot.wait_until_ready()

    @role_cleanup_task.before_loop
    async def before_role_cleanup(self):
        await self.bot.wait_until_ready()

    @cleanup_task.before_loop
    async def before_cleanup(self):
        await self.bot.wait_until_ready()

    @reminder_task.before_loop
    async def before_reminder(self):
        await self.bot.wait_until_ready()

    @cache_cleanup_task.before_loop
    async def before_cache_cleanup(self):
        await self.bot.wait_until_ready()

    @announcement_cleanup_task.before_loop
    async def before_announcement_cleanup(self):
        await self.bot.wait_until_ready()

    @audit_cleanup_task.before_loop
    async def before_audit_cleanup(self):
        await self.bot.wait_until_ready()

# This should be OUTSIDE the class (no indentation)
async def setup(bot):
    await bot.add_cog(BirthdayCog(bot))
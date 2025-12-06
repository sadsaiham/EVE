import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
import os
import re
import random
from typing import List, Optional
import asyncio

# Default word lists
DEFAULT_POSITIVE_WORDS = [
    "yes", "yep", "yeah", "yea", "yah", "ya", "yup", "yessir", 
    "affirmative", "aye", "sure", "ok", "okay", "alright", "fine", 
    "agreed", "roger", "correct", "true", "right"
]

DEFAULT_NEGATIVE_WORDS = [
    "no", "nope", "nah", "nay", "negative", "never", "nuhuh", 
    "noway", "wrong", "false", "incorrect", "disagree"
]

# Default sincerity triggers
DEFAULT_SINCERITY_TRIGGERS = [
    "fuck off", "stop", "shut up", "leave me alone", "go away",
    "enough", "quit it", "cut it out", "knock it off"
]

# Default change detection responses
DEFAULT_CHANGE_DETECTION_RESPONSES = [
    "I know I'm right",
    "I win then",
    "you agree with me now then?",
    "so you've changed your mind?",
    "I knew you'd come around",
    "see, I told you",
    "so now you see it my way?",
    "I'm glad you've seen the light"
]

DEVELOPER_IDS = [1313333441525448704]

def is_developer(user_id):
    return user_id in DEVELOPER_IDS

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
        main_view = ArgueTrackerView(self.cog, self.guild_id)
        embed = main_view.create_status_embed()
        await interaction.response.edit_message(embed=embed, view=main_view)
        main_view.message = await interaction.original_response()

class AddPositiveWordModal(discord.ui.Modal, title="Add Positive Word"):
    word_input = discord.ui.TextInput(
        label="Positive word to add",
        placeholder="Enter a positive word (will trigger 'no' response)...",
        max_length=50
    )
    
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id
    
    async def on_submit(self, interaction: discord.Interaction):
        word = self.word_input.value.strip().lower()
        if not word:
            await interaction.response.send_message("❌ Please enter a valid word.", ephemeral=True)
            return
            
        settings = self.cog.get_guild_settings(self.guild_id)
        
        if word in settings["positive_words"]:
            await interaction.response.send_message(f"❌ **{word}** is already in the positive words list!", ephemeral=True)
            return
        
        settings["positive_words"].append(word)
        self.cog.save_guild_settings(self.guild_id, positive_words=settings["positive_words"])
        
        await interaction.response.send_message(f"✅ Added **{word}** to positive words list! (Will respond with 'no')", ephemeral=True)

class AddNegativeWordModal(discord.ui.Modal, title="Add Negative Word"):
    word_input = discord.ui.TextInput(
        label="Negative word to add",
        placeholder="Enter a negative word (will trigger 'yes' response)...",
        max_length=50
    )
    
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id
    
    async def on_submit(self, interaction: discord.Interaction):
        word = self.word_input.value.strip().lower()
        if not word:
            await interaction.response.send_message("❌ Please enter a valid word.", ephemeral=True)
            return
            
        settings = self.cog.get_guild_settings(self.guild_id)
        
        if word in settings["negative_words"]:
            await interaction.response.send_message(f"❌ **{word}** is already in the negative words list!", ephemeral=True)
            return
        
        settings["negative_words"].append(word)
        self.cog.save_guild_settings(self.guild_id, negative_words=settings["negative_words"])
        
        await interaction.response.send_message(f"✅ Added **{word}** to negative words list! (Will respond with 'yes')", ephemeral=True)

class RemoveWordModal(discord.ui.Modal, title="Remove Word"):
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
        word = self.word_input.value.strip().lower()
        if not word:
            await interaction.response.send_message("❌ Please enter a valid word.", ephemeral=True)
            return
            
        settings = self.cog.get_guild_settings(self.guild_id)
        
        removed_from = []
        if word in settings["positive_words"]:
            settings["positive_words"].remove(word)
            removed_from.append("positive")
        if word in settings["negative_words"]:
            settings["negative_words"].remove(word)
            removed_from.append("negative")
        
        if not removed_from:
            await interaction.response.send_message(f"❌ **{word}** is not in any word list!", ephemeral=True)
            return
        
        self.cog.save_guild_settings(
            self.guild_id, 
            positive_words=settings["positive_words"],
            negative_words=settings["negative_words"]
        )
        
        await interaction.response.send_message(f"✅ Removed **{word}** from {', '.join(removed_from)} word list(s)!", ephemeral=True)

class RagequitSettingsModal(discord.ui.Modal, title="Ragequit Settings"):
    min_args = discord.ui.TextInput(
        label="Min arguments before ragequit",
        placeholder="Enter minimum number (e.g., 3)...",
        default="3",
        max_length=2
    )
    
    max_args = discord.ui.TextInput(
        label="Max arguments before ragequit",
        placeholder="Enter maximum number (e.g., 5)...",
        default="5",
        max_length=2
    )
    
    ragequit_message = discord.ui.TextInput(
        label="Ragequit message",
        placeholder="Enter message when bot gives up...",
        default="Fine, I give up. For now.",
        max_length=100
    )
    
    ragequit_cooldown = discord.ui.TextInput(
        label="Ragequit cooldown (seconds)",
        placeholder="Enter cooldown after ragequit...",
        default="30",
        max_length=4
    )
    
    argument_timeout = discord.ui.TextInput(
        label="Argument timeout (seconds)",
        placeholder="Enter timeout to reset argument count...",
        default="60",
        max_length=5
    )
    
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            min_val = int(self.min_args.value)
            max_val = int(self.max_args.value)
            cooldown = int(self.ragequit_cooldown.value)
            timeout = int(self.argument_timeout.value)
            
            if min_val < 1 or max_val < 1 or cooldown < 0 or timeout < 1:
                await interaction.response.send_message("❌ Values must be positive numbers.", ephemeral=True)
                return
                
            if min_val > max_val:
                await interaction.response.send_message("❌ Minimum cannot be greater than maximum.", ephemeral=True)
                return
                
            settings = self.cog.get_guild_settings(self.guild_id)
            settings["ragequit_min"] = min_val
            settings["ragequit_max"] = max_val
            settings["ragequit_message"] = self.ragequit_message.value
            settings["ragequit_cooldown"] = cooldown
            settings["argument_timeout"] = timeout
            
            self.cog.save_guild_settings(self.guild_id, **settings)
            
            await interaction.response.send_message(
                f"✅ Ragequit settings updated!\n"
                f"**Range:** {min_val}-{max_val} arguments\n"
                f"**Message:** {self.ragequit_message.value}\n"
                f"**Cooldown:** {cooldown} seconds\n"
                f"**Timeout:** {timeout} seconds",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("❌ Please enter valid numbers.", ephemeral=True)

class SinceritySettingsModal(discord.ui.Modal, title="Sincerity Settings"):
    sincerity_message = discord.ui.TextInput(
        label="Sincerity response message",
        placeholder="Enter message when user tells bot to stop...",
        default="Okay, I'm sorry",
        max_length=100
    )
    
    cooldown_time = discord.ui.TextInput(
        label="Cooldown time (seconds)",
        placeholder="Enter cooldown duration...",
        default="60",
        max_length=4
    )
    
    sincerity_timeout = discord.ui.TextInput(
        label="Sincerity timeout (seconds)",
        placeholder="Enter timeout after bot's last message...",
        default="5",
        max_length=4
    )
    
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            cooldown = int(self.cooldown_time.value)
            timeout = int(self.sincerity_timeout.value)
            
            if cooldown < 0 or timeout < 1:
                await interaction.response.send_message("❌ Values must be positive numbers.", ephemeral=True)
                return
                
            settings = self.cog.get_guild_settings(self.guild_id)
            settings["sincerity_message"] = self.sincerity_message.value
            settings["sincerity_cooldown"] = cooldown
            settings["sincerity_timeout"] = timeout
            
            self.cog.save_guild_settings(self.guild_id, **settings)
            
            await interaction.response.send_message(
                f"✅ Sincerity settings updated!\n"
                f"**Message:** {self.sincerity_message.value}\n"
                f"**Cooldown:** {cooldown} seconds\n"
                f"**Timeout:** {timeout} seconds",
                ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message("❌ Please enter valid numbers.", ephemeral=True)

class AddSincerityTriggerModal(discord.ui.Modal, title="Add Sincerity Trigger"):
    trigger_input = discord.ui.TextInput(
        label="Sincerity trigger phrase",
        placeholder="Enter phrase that makes bot apologize...",
        max_length=50
    )
    
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id
    
    async def on_submit(self, interaction: discord.Interaction):
        trigger = self.trigger_input.value.strip().lower()
        if not trigger:
            await interaction.response.send_message("❌ Please enter a valid trigger phrase.", ephemeral=True)
            return
            
        settings = self.cog.get_guild_settings(self.guild_id)
        
        if trigger in settings["sincerity_triggers"]:
            await interaction.response.send_message(f"❌ **{trigger}** is already in the sincerity triggers list!", ephemeral=True)
            return
        
        settings["sincerity_triggers"].append(trigger)
        self.cog.save_guild_settings(self.guild_id, sincerity_triggers=settings["sincerity_triggers"])
        
        await interaction.response.send_message(f"✅ Added **{trigger}** to sincerity triggers!", ephemeral=True)

class RemoveSincerityTriggerModal(discord.ui.Modal, title="Remove Sincerity Trigger"):
    trigger_input = discord.ui.TextInput(
        label="Sincerity trigger to remove",
        placeholder="Enter trigger phrase to remove...",
        max_length=50
    )
    
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id
    
    async def on_submit(self, interaction: discord.Interaction):
        trigger = self.trigger_input.value.strip().lower()
        if not trigger:
            await interaction.response.send_message("❌ Please enter a valid trigger phrase.", ephemeral=True)
            return
            
        settings = self.cog.get_guild_settings(self.guild_id)
        
        if trigger not in settings["sincerity_triggers"]:
            await interaction.response.send_message(f"❌ **{trigger}** is not in the sincerity triggers list!", ephemeral=True)
            return
        
        settings["sincerity_triggers"].remove(trigger)
        self.cog.save_guild_settings(self.guild_id, sincerity_triggers=settings["sincerity_triggers"])
        
        await interaction.response.send_message(f"✅ Removed **{trigger}** from sincerity triggers!", ephemeral=True)

class AddChangeResponseModal(discord.ui.Modal, title="Add Change Response"):
    response_input = discord.ui.TextInput(
        label="Change response to add",
        placeholder="Enter response when user changes answer...",
        max_length=100
    )
    
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id
    
    async def on_submit(self, interaction: discord.Interaction):
        response = self.response_input.value.strip()
        if not response:
            await interaction.response.send_message("❌ Please enter a valid response.", ephemeral=True)
            return
            
        settings = self.cog.get_guild_settings(self.guild_id)
        change_responses = settings.get("change_detection_responses", DEFAULT_CHANGE_DETECTION_RESPONSES.copy())
        
        if response in change_responses:
            await interaction.response.send_message(f"❌ **{response}** is already in the change responses list!", ephemeral=True)
            return
        
        change_responses.append(response)
        self.cog.save_guild_settings(self.guild_id, change_detection_responses=change_responses)
        
        await interaction.response.send_message(f"✅ Added **{response}** to change detection responses!", ephemeral=True)

class RemoveChangeResponseModal(discord.ui.Modal, title="Remove Change Response"):
    response_input = discord.ui.TextInput(
        label="Change response to remove",
        placeholder="Enter response to remove...",
        max_length=100
    )
    
    def __init__(self, cog, guild_id):
        super().__init__()
        self.cog = cog
        self.guild_id = guild_id
    
    async def on_submit(self, interaction: discord.Interaction):
        response = self.response_input.value.strip()
        if not response:
            await interaction.response.send_message("❌ Please enter a valid response.", ephemeral=True)
            return
            
        settings = self.cog.get_guild_settings(self.guild_id)
        change_responses = settings.get("change_detection_responses", DEFAULT_CHANGE_DETECTION_RESPONSES.copy())
        
        if response not in change_responses:
            await interaction.response.send_message(f"❌ **{response}** is not in the change responses list!", ephemeral=True)
            return
        
        change_responses.remove(response)
        self.cog.save_guild_settings(self.guild_id, change_detection_responses=change_responses)
        
        await interaction.response.send_message(f"✅ Removed **{response}** from change detection responses!", ephemeral=True)

class ArgueTrackerView(discord.ui.View):
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
            title="🤖 Argue Tracker Control Panel",
            color=0x00ff00 if settings["enabled"] else 0xff0000,
            description="Select an option from the dropdown menu below to manage the automatic argument feature."
        )
        
        # Add status fields
        embed.add_field(
            name="Quick Status", 
            value=f"**Tracker:** {status}\n**Positive Words:** {len(settings['positive_words'])}\n**Negative Words:** {len(settings['negative_words'])}",
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
        
        # Add optional settings status
        ragequit_status = "✅" if settings.get("ragequit_enabled", False) else "❌"
        sincerity_status = "✅" if settings.get("sincerity_enabled", False) else "❌"
        change_detection_status = "✅" if settings.get("change_detection_enabled", False) else "❌"
        
        embed.add_field(
            name="Optional Features",
            value=f"**Ragequit:** {ragequit_status}\n**Sincerity:** {sincerity_status}\n**Change Detection:** {change_detection_status}",
            inline=False
        )
        
        embed.set_footer(text="All interactions are private • Use dropdown to navigate")
        
        return embed

    @discord.ui.select(
        placeholder="Choose an option...",
        options=[
            discord.SelectOption(label="Toggle On/Off", description="Enable or disable argument feature", emoji="⚡"),
            discord.SelectOption(label="Setup", description="Configure channels, roles, and users", emoji="⚙️"),
            discord.SelectOption(label="Manage Words", description="Add, remove, or reset words", emoji="📝"),
            discord.SelectOption(label="Optional Settings", description="Ragequit, sincerity, and cooldown settings", emoji="🎛️"),
            discord.SelectOption(label="Leaderboard", description="View argument leaderboard", emoji="🏆"),
            discord.SelectOption(label="Word List", description="View current word lists", emoji="📋")
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
            await interaction.followup.send(f"✅ Argument feature turned **{'on' if new_state else 'off'}**!", ephemeral=True)
            
        elif choice == "Setup":
            view = SetupView(self.cog, self.guild_id, self.current_settings)
            embed = discord.Embed(
                title="⚙️ Setup Configuration",
                description="Configure which channels, roles, and users can trigger arguments.",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()
            
        elif choice == "Manage Words":
            view = ManageWordsView(self.cog, self.guild_id, self.current_settings)
            embed = discord.Embed(
                title="📝 Manage Argument Words",
                description="Add, remove, or reset positive and negative words.",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()
            
        elif choice == "Optional Settings":
            view = OptionalSettingsView(self.cog, self.guild_id, self.current_settings)
            embed = discord.Embed(
                title="🎛️ Optional Settings",
                description="Configure ragequit, sincerity, and cooldown settings.",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()
            
        elif choice == "Leaderboard":
            # Create leaderboard embed in the current message with a back button
            leaderboard_embed = await self.cog.create_leaderboard_embed(self.guild_id)
            
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
            
            embed = discord.Embed(
                title="📋 Argument Word Lists",
                color=discord.Color.blue()
            )
            
            # Positive words
            positive_text = ", ".join(sorted(settings["positive_words"]))
            if len(positive_text) > 500:
                positive_text = positive_text[:500] + "..."
            embed.add_field(
                name=f"✅ Positive Words ({len(settings['positive_words'])} - triggers 'no' response)",
                value=positive_text or "No positive words configured",
                inline=False
            )
            
            # Negative words
            negative_text = ", ".join(sorted(settings["negative_words"]))
            if len(negative_text) > 500:
                negative_text = negative_text[:500] + "..."
            embed.add_field(
                name=f"❌ Negative Words ({len(settings['negative_words'])} - triggers 'yes' response)",
                value=negative_text or "No negative words configured",
                inline=False
            )
            
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
                description="Configure which channels can trigger arguments.",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()
            
        elif choice == "Role Settings":
            view = RoleSettingsView(self.cog, self.guild_id, self.current_settings)
            embed = discord.Embed(
                title="👥 Role Settings",
                description="Configure which roles can trigger arguments.",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()
            
        elif choice == "User Settings":
            view = UserSettingsView(self.cog, self.guild_id, self.current_settings)
            embed = discord.Embed(
                title="👤 User Settings",
                description="Configure which users can trigger arguments.",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        main_view = ArgueTrackerView(self.cog, self.guild_id)
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
        placeholder="Manage argument words...",
        options=[
            discord.SelectOption(label="Add Positive Word", description="Add word that triggers 'no' response", emoji="✅"),
            discord.SelectOption(label="Add Negative Word", description="Add word that triggers 'yes' response", emoji="❌"),
            discord.SelectOption(label="Remove Word", description="Remove word from any list", emoji="➖"),
            discord.SelectOption(label="Reset Words", description="Reset to default words", emoji="🔄")
        ]
    )
    async def words_select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
        choice = select.values[0]
        
        if choice == "Add Positive Word":
            modal = AddPositiveWordModal(self.cog, self.guild_id)
            await interaction.response.send_modal(modal)
            
        elif choice == "Add Negative Word":
            modal = AddNegativeWordModal(self.cog, self.guild_id)
            await interaction.response.send_modal(modal)
            
        elif choice == "Remove Word":
            modal = RemoveWordModal(self.cog, self.guild_id)
            await interaction.response.send_modal(modal)
            
        elif choice == "Reset Words":
            confirm_view = ConfirmResetView(self.cog, self.guild_id, self)
            embed = discord.Embed(
                title="⚠️ Reset Confirmation",
                description="**Are you sure you want to reset all custom words?**\nThis will remove all custom words and keep only the default ones.",
                color=discord.Color.orange()
            )
            await interaction.response.edit_message(embed=embed, view=confirm_view)
            confirm_view.message = await interaction.original_response()

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        main_view = ArgueTrackerView(self.cog, self.guild_id)
        embed = main_view.create_status_embed()
        await interaction.response.edit_message(embed=embed, view=main_view)
        main_view.message = await interaction.original_response()

class OptionalSettingsView(discord.ui.View):
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
        placeholder="Configure optional features...",
        options=[
            discord.SelectOption(label="Toggle Ragequit", description="Enable/disable bot giving up", emoji="😤"),
            discord.SelectOption(label="Toggle Sincerity", description="Enable/disable bot apologizing", emoji="🙏"),
            discord.SelectOption(label="Toggle Change Detection", description="Enable/disable change detection", emoji="🔄"),
            discord.SelectOption(label="Ragequit Settings", description="Configure ragequit behavior", emoji="⚙️"),
            discord.SelectOption(label="Sincerity Settings", description="Configure sincerity behavior", emoji="⚙️"),
            discord.SelectOption(label="Manage Sincerity Triggers", description="Add/remove sincerity triggers", emoji="📝"),
            discord.SelectOption(label="Manage Change Responses", description="Add/remove change detection responses", emoji="💬")
        ]
    )
    async def optional_select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
        choice = select.values[0]
        settings = self.cog.get_guild_settings(self.guild_id)
        
        if choice == "Toggle Ragequit":
            new_state = not settings.get("ragequit_enabled", False)
            self.cog.save_guild_settings(self.guild_id, ragequit_enabled=new_state)
            self.current_settings["ragequit_enabled"] = new_state
            
            status = "enabled" if new_state else "disabled"
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="✅ Ragequit Toggled",
                    description=f"Ragequit feature has been **{status}**.",
                    color=discord.Color.green()
                ),
                view=self
            )
            
        elif choice == "Toggle Sincerity":
            new_state = not settings.get("sincerity_enabled", False)
            self.cog.save_guild_settings(self.guild_id, sincerity_enabled=new_state)
            self.current_settings["sincerity_enabled"] = new_state
            
            status = "enabled" if new_state else "disabled"
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="✅ Sincerity Toggled",
                    description=f"Sincerity feature has been **{status}**.",
                    color=discord.Color.green()
                ),
                view=self
            )
            
        elif choice == "Toggle Change Detection":
            new_state = not settings.get("change_detection_enabled", False)
            self.cog.save_guild_settings(self.guild_id, change_detection_enabled=new_state)
            self.current_settings["change_detection_enabled"] = new_state
            
            status = "enabled" if new_state else "disabled"
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="✅ Change Detection Toggled",
                    description=f"Change detection has been **{status}**.",
                    color=discord.Color.green()
                ),
                view=self
            )
            
        elif choice == "Ragequit Settings":
            modal = RagequitSettingsModal(self.cog, self.guild_id)
            await interaction.response.send_modal(modal)
            
        elif choice == "Sincerity Settings":
            modal = SinceritySettingsModal(self.cog, self.guild_id)
            await interaction.response.send_modal(modal)
            
        elif choice == "Manage Sincerity Triggers":
            view = ManageSincerityTriggersView(self.cog, self.guild_id, self.current_settings)
            embed = discord.Embed(
                title="📝 Manage Sincerity Triggers",
                description="Add or remove phrases that make the bot apologize.",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()
            
        elif choice == "Manage Change Responses":
            view = ManageChangeResponsesView(self.cog, self.guild_id, self.current_settings)
            embed = discord.Embed(
                title="💬 Manage Change Detection Responses",
                description="Add or remove responses when users change their answer.",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
            view.message = await interaction.original_response()

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        main_view = ArgueTrackerView(self.cog, self.guild_id)
        embed = main_view.create_status_embed()
        await interaction.response.edit_message(embed=embed, view=main_view)
        main_view.message = await interaction.original_response()

class ManageSincerityTriggersView(discord.ui.View):
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
        placeholder="Manage sincerity triggers...",
        options=[
            discord.SelectOption(label="Add Sincerity Trigger", description="Add phrase that makes bot apologize", emoji="➕"),
            discord.SelectOption(label="Remove Sincerity Trigger", description="Remove sincerity trigger phrase", emoji="➖"),
            discord.SelectOption(label="View Sincerity Triggers", description="View current sincerity triggers", emoji="👀"),
            discord.SelectOption(label="Reset Sincerity Triggers", description="Reset to default sincerity triggers", emoji="🔄")
        ]
    )
    async def triggers_select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
        choice = select.values[0]
        
        if choice == "Add Sincerity Trigger":
            modal = AddSincerityTriggerModal(self.cog, self.guild_id)
            await interaction.response.send_modal(modal)
            
        elif choice == "Remove Sincerity Trigger":
            modal = RemoveSincerityTriggerModal(self.cog, self.guild_id)
            await interaction.response.send_modal(modal)
            
        elif choice == "View Sincerity Triggers":
            settings = self.cog.get_guild_settings(self.guild_id)
            triggers = settings.get("sincerity_triggers", DEFAULT_SINCERITY_TRIGGERS.copy())
            
            embed = discord.Embed(
                title="👀 Sincerity Triggers",
                description="Phrases that make the bot apologize when sincerity is enabled.",
                color=discord.Color.blue()
            )
            
            if triggers:
                triggers_text = "\n".join([f"• {trigger}" for trigger in sorted(triggers)])
                embed.add_field(name="Current Triggers", value=triggers_text, inline=False)
            else:
                embed.add_field(name="Current Triggers", value="No sincerity triggers configured.", inline=False)
            
            await interaction.response.edit_message(embed=embed, view=self)
            
        elif choice == "Reset Sincerity Triggers":
            confirm_view = ConfirmResetSincerityTriggersView(self.cog, self.guild_id, self)
            embed = discord.Embed(
                title="⚠️ Reset Sincerity Triggers Confirmation",
                description="**Are you sure you want to reset all custom sincerity triggers?**\nThis will remove all custom triggers and reset to the default ones.",
                color=discord.Color.orange()
            )
            await interaction.response.edit_message(embed=embed, view=confirm_view)
            confirm_view.message = await interaction.original_response()

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = OptionalSettingsView(self.cog, self.guild_id, self.current_settings)
        embed = discord.Embed(
            title="🎛️ Optional Settings",
            description="Configure ragequit, sincerity, and cooldown settings.",
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)
        view.message = await interaction.original_response()

class ManageChangeResponsesView(discord.ui.View):
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
        placeholder="Manage change detection responses...",
        options=[
            discord.SelectOption(label="Add Change Response", description="Add response when user changes answer", emoji="➕"),
            discord.SelectOption(label="Remove Change Response", description="Remove change response", emoji="➖"),
            discord.SelectOption(label="View Change Responses", description="View current change responses", emoji="👀"),
            discord.SelectOption(label="Reset Change Responses", description="Reset to default change responses", emoji="🔄")
        ]
    )
    async def change_responses_select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
        choice = select.values[0]
        
        if choice == "Add Change Response":
            modal = AddChangeResponseModal(self.cog, self.guild_id)
            await interaction.response.send_modal(modal)
            
        elif choice == "Remove Change Response":
            modal = RemoveChangeResponseModal(self.cog, self.guild_id)
            await interaction.response.send_modal(modal)
            
        elif choice == "View Change Responses":
            settings = self.cog.get_guild_settings(self.guild_id)
            change_responses = settings.get("change_detection_responses", DEFAULT_CHANGE_DETECTION_RESPONSES.copy())
            
            embed = discord.Embed(
                title="👀 Change Detection Responses",
                description="Responses when users change their answer after the bot argues.",
                color=discord.Color.blue()
            )
            
            if change_responses:
                responses_text = "\n".join([f"• {response}" for response in sorted(change_responses)])
                embed.add_field(name="Current Responses", value=responses_text, inline=False)
            else:
                embed.add_field(name="Current Responses", value="No change detection responses configured.", inline=False)
            
            await interaction.response.edit_message(embed=embed, view=self)
            
        elif choice == "Reset Change Responses":
            confirm_view = ConfirmResetChangeResponsesView(self.cog, self.guild_id, self)
            embed = discord.Embed(
                title="⚠️ Reset Change Responses Confirmation",
                description="**Are you sure you want to reset all custom change responses?**\nThis will remove all custom responses and reset to the default ones.",
                color=discord.Color.orange()
            )
            await interaction.response.edit_message(embed=embed, view=confirm_view)
            confirm_view.message = await interaction.original_response()

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = OptionalSettingsView(self.cog, self.guild_id, self.current_settings)
        embed = discord.Embed(
            title="🎛️ Optional Settings",
            description="Configure ragequit, sincerity, and cooldown settings.",
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)
        view.message = await interaction.original_response()

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
            description="Configure which channels, roles, and users can trigger arguments.",
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
            description="Configure which channels, roles, and users can trigger arguments.",
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
            description="Configure which channels, roles, and users can trigger arguments.",
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
        self.cog.save_guild_settings(
            self.guild_id, 
            positive_words=DEFAULT_POSITIVE_WORDS.copy(),
            negative_words=DEFAULT_NEGATIVE_WORDS.copy()
        )
        
        main_view = ArgueTrackerView(self.cog, self.guild_id)
        embed = main_view.create_status_embed()
        await interaction.response.edit_message(
            embed=discord.Embed(
                title="✅ Words Reset",
                description="All custom words have been reset to default!",
                color=discord.Color.green()
            ),
            view=main_view
        )
        main_view.message = await interaction.original_response()
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_reset(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ManageWordsView(self.cog, self.guild_id, self.parent_view.current_settings)
        embed = discord.Embed(
            title="📝 Manage Argument Words",
            description="Add, remove, or reset positive and negative words.",
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)
        view.message = await interaction.original_response()

class ConfirmResetChangeResponsesView(discord.ui.View):
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
        self.cog.save_guild_settings(
            self.guild_id, 
            change_detection_responses=DEFAULT_CHANGE_DETECTION_RESPONSES.copy()
        )
        
        view = ManageChangeResponsesView(self.cog, self.guild_id, self.parent_view.current_settings)
        embed = discord.Embed(
            title="✅ Change Responses Reset",
            description="All custom change responses have been reset to default!",
            color=discord.Color.green()
        )
        await interaction.response.edit_message(embed=embed, view=view)
        view.message = await interaction.original_response()
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_reset(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ManageChangeResponsesView(self.cog, self.guild_id, self.parent_view.current_settings)
        embed = discord.Embed(
            title="💬 Manage Change Detection Responses",
            description="Add or remove responses when users change their answer.",
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)
        view.message = await interaction.original_response()

class ConfirmResetSincerityTriggersView(discord.ui.View):
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
        self.cog.save_guild_settings(
            self.guild_id, 
            sincerity_triggers=DEFAULT_SINCERITY_TRIGGERS.copy()
        )
        
        view = ManageSincerityTriggersView(self.cog, self.guild_id, self.parent_view.current_settings)
        embed = discord.Embed(
            title="✅ Sincerity Triggers Reset",
            description="All custom sincerity triggers have been reset to default!",
            color=discord.Color.green()
        )
        await interaction.response.edit_message(embed=embed, view=view)
        view.message = await interaction.original_response()
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_reset(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ManageSincerityTriggersView(self.cog, self.guild_id, self.parent_view.current_settings)
        embed = discord.Embed(
            title="📝 Manage Sincerity Triggers",
            description="Add or remove phrases that make the bot apologize.",
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)
        view.message = await interaction.original_response()

class Argue(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_path = "data/argue.db"
        self.user_argument_count = {}  # Track consecutive arguments per user
        self.user_cooldowns = {}  # Track user cooldowns
        self.user_last_response = {}  # Track last response type per user for change detection
        self.user_last_argument_time = {}  # Track last argument time per user for timeout
        self.bot_last_message_time = {}  # Track last bot message time per channel for sincerity timeout
        self.init_database()

    def init_database(self):
        """Initialize the SQLite database with per-guild support - FIXED TO PRESERVE DATA"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Guild-specific settings - ONLY CREATE IF NOT EXISTS
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS argue_settings (
                guild_id TEXT PRIMARY KEY,
                enabled BOOLEAN NOT NULL DEFAULT 0,
                positive_words TEXT NOT NULL,
                negative_words TEXT NOT NULL,
                included_channels TEXT,
                excluded_channels TEXT,
                included_roles TEXT,
                excluded_roles TEXT,
                included_users TEXT,
                excluded_users TEXT,
                ragequit_enabled BOOLEAN DEFAULT 0,
                ragequit_min INTEGER DEFAULT 3,
                ragequit_max INTEGER DEFAULT 5,
                ragequit_message TEXT DEFAULT 'Fine, I give up. For now.',
                ragequit_cooldown INTEGER DEFAULT 30,
                sincerity_enabled BOOLEAN DEFAULT 0,
                sincerity_cooldown INTEGER DEFAULT 60,
                sincerity_message TEXT DEFAULT 'Okay, I''m sorry',
                sincerity_triggers TEXT,
                sincerity_timeout INTEGER DEFAULT 5,
                change_detection_enabled BOOLEAN DEFAULT 0,
                change_detection_responses TEXT,
                argument_timeout INTEGER DEFAULT 60,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Guild-specific user stats - ONLY CREATE IF NOT EXISTS
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS argue_stats (
                guild_id TEXT,
                user_id TEXT,
                username TEXT,
                argument_count INTEGER NOT NULL DEFAULT 0,
                last_argued TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        
        conn.commit()
        conn.close()
        print("✅ Argue database initialized - existing data preserved")

    def get_guild_settings(self, guild_id):
        """Get settings for a specific guild"""
        import json
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT enabled, positive_words, negative_words, included_channels, excluded_channels, included_roles, excluded_roles, included_users, excluded_users, ragequit_enabled, ragequit_min, ragequit_max, ragequit_message, ragequit_cooldown, sincerity_enabled, sincerity_cooldown, sincerity_message, sincerity_triggers, sincerity_timeout, change_detection_enabled, change_detection_responses, argument_timeout FROM argue_settings WHERE guild_id = ?', (str(guild_id),))
        result = cursor.fetchone()
        
        if result:
            enabled = bool(result[0])
            positive_words = json.loads(result[1]) if result[1] else DEFAULT_POSITIVE_WORDS.copy()
            negative_words = json.loads(result[2]) if result[2] else DEFAULT_NEGATIVE_WORDS.copy()
            included_channels = json.loads(result[3]) if result[3] else []
            excluded_channels = json.loads(result[4]) if result[4] else []
            included_roles = json.loads(result[5]) if result[5] else []
            excluded_roles = json.loads(result[6]) if result[6] else []
            included_users = json.loads(result[7]) if result[7] else []
            excluded_users = json.loads(result[8]) if result[8] else []
            ragequit_enabled = bool(result[9])
            ragequit_min = result[10] if result[10] is not None else 3
            ragequit_max = result[11] if result[11] is not None else 5
            ragequit_message = result[12] if result[12] is not None else "Fine, I give up. For now."
            ragequit_cooldown = result[13] if result[13] is not None else 30
            sincerity_enabled = bool(result[14])
            sincerity_cooldown = result[15] if result[15] is not None else 60
            sincerity_message = result[16] if result[16] is not None else "Okay, I'm sorry"
            sincerity_triggers = json.loads(result[17]) if result[17] else DEFAULT_SINCERITY_TRIGGERS.copy()
            sincerity_timeout = result[18] if result[18] is not None else 5
            change_detection_enabled = bool(result[19])
            change_detection_responses = json.loads(result[20]) if result[20] else DEFAULT_CHANGE_DETECTION_RESPONSES.copy()
            argument_timeout = result[21] if result[21] is not None else 60
        else:
            # Create default settings for this guild
            enabled = False
            positive_words = DEFAULT_POSITIVE_WORDS.copy()
            negative_words = DEFAULT_NEGATIVE_WORDS.copy()
            included_channels = []
            excluded_channels = []
            included_roles = []
            excluded_roles = []
            included_users = []
            excluded_users = []
            ragequit_enabled = False
            ragequit_min = 3
            ragequit_max = 5
            ragequit_message = "Fine, I give up. For now."
            ragequit_cooldown = 30
            sincerity_enabled = False
            sincerity_cooldown = 60
            sincerity_message = "Okay, I'm sorry"
            sincerity_triggers = DEFAULT_SINCERITY_TRIGGERS.copy()
            sincerity_timeout = 5
            change_detection_enabled = False
            change_detection_responses = DEFAULT_CHANGE_DETECTION_RESPONSES.copy()
            argument_timeout = 60
            
            cursor.execute('''
                INSERT INTO argue_settings (guild_id, enabled, positive_words, negative_words, included_channels, excluded_channels, included_roles, excluded_roles, included_users, excluded_users, ragequit_enabled, ragequit_min, ragequit_max, ragequit_message, ragequit_cooldown, sincerity_enabled, sincerity_cooldown, sincerity_message, sincerity_triggers, sincerity_timeout, change_detection_enabled, change_detection_responses, argument_timeout) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(guild_id), enabled, json.dumps(positive_words), json.dumps(negative_words), 
                json.dumps(included_channels), json.dumps(excluded_channels),
                json.dumps(included_roles), json.dumps(excluded_roles),
                json.dumps(included_users), json.dumps(excluded_users),
                ragequit_enabled, ragequit_min, ragequit_max, ragequit_message, ragequit_cooldown,
                sincerity_enabled, sincerity_cooldown, sincerity_message,
                json.dumps(sincerity_triggers), sincerity_timeout,
                change_detection_enabled, json.dumps(change_detection_responses), argument_timeout
            ))
            conn.commit()
        
        conn.close()
        
        return {
            "enabled": enabled, 
            "positive_words": positive_words,
            "negative_words": negative_words,
            "included_channels": included_channels,
            "excluded_channels": excluded_channels,
            "included_roles": included_roles,
            "excluded_roles": excluded_roles,
            "included_users": included_users,
            "excluded_users": excluded_users,
            "ragequit_enabled": ragequit_enabled,
            "ragequit_min": ragequit_min,
            "ragequit_max": ragequit_max,
            "ragequit_message": ragequit_message,
            "ragequit_cooldown": ragequit_cooldown,
            "sincerity_enabled": sincerity_enabled,
            "sincerity_cooldown": sincerity_cooldown,
            "sincerity_message": sincerity_message,
            "sincerity_triggers": sincerity_triggers,
            "sincerity_timeout": sincerity_timeout,
            "change_detection_enabled": change_detection_enabled,
            "change_detection_responses": change_detection_responses,
            "argument_timeout": argument_timeout
        }

    def save_guild_settings(self, guild_id, enabled=None, positive_words=None, negative_words=None, **kwargs):
        """Save settings for a specific guild"""
        import json
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        current = self.get_guild_settings(guild_id)
        
        if enabled is not None:
            current["enabled"] = enabled
        if positive_words is not None:
            current["positive_words"] = positive_words
        if negative_words is not None:
            current["negative_words"] = negative_words
            
        # Update any additional settings
        for key, value in kwargs.items():
            if key in current:
                current[key] = value
        
        cursor.execute('''
            INSERT OR REPLACE INTO argue_settings 
            (guild_id, enabled, positive_words, negative_words, included_channels, excluded_channels, included_roles, excluded_roles, included_users, excluded_users, ragequit_enabled, ragequit_min, ragequit_max, ragequit_message, ragequit_cooldown, sincerity_enabled, sincerity_cooldown, sincerity_message, sincerity_triggers, sincerity_timeout, change_detection_enabled, change_detection_responses, argument_timeout, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            str(guild_id), 
            current["enabled"], 
            json.dumps(current["positive_words"]),
            json.dumps(current["negative_words"]),
            json.dumps(current["included_channels"]),
            json.dumps(current["excluded_channels"]),
            json.dumps(current["included_roles"]),
            json.dumps(current["excluded_roles"]),
            json.dumps(current["included_users"]),
            json.dumps(current["excluded_users"]),
            current["ragequit_enabled"],
            current["ragequit_min"],
            current["ragequit_max"],
            current["ragequit_message"],
            current["ragequit_cooldown"],
            current["sincerity_enabled"],
            current["sincerity_cooldown"],
            current["sincerity_message"],
            json.dumps(current["sincerity_triggers"]),
            current["sincerity_timeout"],
            current["change_detection_enabled"],
            json.dumps(current["change_detection_responses"]),
            current["argument_timeout"]
        ))
        
        conn.commit()
        conn.close()

    def normalize_word(self, word):
        """Normalize a word"""
        clean_word = re.sub(r'[^\w\s]', '', word.lower())
        normalized = re.sub(r'(.)\1{2,}', r'\1', clean_word)
        return normalized

    def is_single_word_message(self, text):
        """Check if message is a single word"""
        if not text or not text.strip():
            return False
            
        # Remove punctuation and check word count
        clean_text = re.sub(r'[^\w\s]', '', text.strip())
        words = clean_text.split()
        return len(words) == 1

    def get_word_type(self, text, settings):
        """Check if message contains positive or negative words"""
        if not self.is_single_word_message(text):
            return None
            
        word = text.strip().lower()
        normalized_word = self.normalize_word(word)
        
        # Check positive words
        for positive_word in settings["positive_words"]:
            if normalized_word == self.normalize_word(positive_word):
                return "positive"
        
        # Check negative words
        for negative_word in settings["negative_words"]:
            if normalized_word == self.normalize_word(negative_word):
                return "negative"
        
        return None

    def is_sincerity_trigger(self, text, settings):
        """Check if message contains sincerity triggers"""
        if not text or not text.strip():
            return False
            
        message = text.strip().lower()
        
        for trigger in settings["sincerity_triggers"]:
            if trigger.lower() in message:
                return True
                
        return False

    def is_user_on_cooldown(self, guild_id, user_id):
        """Check if user is on cooldown"""
        key = f"{guild_id}_{user_id}"
        current_time = discord.utils.utcnow().timestamp()
        
        if key in self.user_cooldowns:
            cooldown_end = self.user_cooldowns[key]
            if current_time < cooldown_end:
                return True
        
        return False

    def set_user_cooldown(self, guild_id, user_id, cooldown_seconds):
        """Set cooldown for user"""
        key = f"{guild_id}_{user_id}"
        current_time = discord.utils.utcnow().timestamp()
        self.user_cooldowns[key] = current_time + cooldown_seconds

    def increment_argument_count(self, guild_id, user_id):
        """Increment argument count for user and check for ragequit"""
        key = f"{guild_id}_{user_id}"
        
        if key not in self.user_argument_count:
            self.user_argument_count[key] = 0
            
        self.user_argument_count[key] += 1
        return self.user_argument_count[key]

    def reset_argument_count(self, guild_id, user_id):
        """Reset argument count for user"""
        key = f"{guild_id}_{user_id}"
        if key in self.user_argument_count:
            self.user_argument_count[key] = 0

    def should_ragequit(self, guild_id, user_id, settings):
        """Check if bot should ragequit based on argument count"""
        if not settings.get("ragequit_enabled", False):
            return False
            
        key = f"{guild_id}_{user_id}"
        current_count = self.user_argument_count.get(key, 0)
        
        min_args = settings.get("ragequit_min", 3)
        max_args = settings.get("ragequit_max", 5)
        
        # Pick random threshold between min and max
        threshold = random.randint(min_args, max_args)
        
        return current_count >= threshold

    def check_argument_timeout(self, guild_id, user_id, settings):
        """Check if argument count should be reset due to timeout"""
        key = f"{guild_id}_{user_id}"
        current_time = discord.utils.utcnow().timestamp()
        
        # If user has no last argument time, set it now
        if key not in self.user_last_argument_time:
            self.user_last_argument_time[key] = current_time
            return False
        
        # Check if timeout has passed
        timeout_seconds = settings.get("argument_timeout", 60)
        last_time = self.user_last_argument_time[key]
        
        if current_time - last_time > timeout_seconds:
            # Reset argument count due to timeout
            self.user_argument_count[key] = 0
            return True
        
        return False

    def update_last_argument_time(self, guild_id, user_id):
        """Update the last argument time for a user"""
        key = f"{guild_id}_{user_id}"
        self.user_last_argument_time[key] = discord.utils.utcnow().timestamp()

    def check_sincerity_timeout(self, channel_id, settings):
        """Check if sincerity trigger should be ignored due to timeout"""
        current_time = discord.utils.utcnow().timestamp()
        
        # If bot hasn't sent a message in this channel, ignore sincerity
        if channel_id not in self.bot_last_message_time:
            return True
        
        # Check if timeout has passed
        timeout_seconds = settings.get("sincerity_timeout", 5)
        last_time = self.bot_last_message_time[channel_id]
        
        return current_time - last_time > timeout_seconds

    def update_bot_message_time(self, channel_id):
        """Update the last bot message time for a channel"""
        self.bot_last_message_time[channel_id] = discord.utils.utcnow().timestamp()

    def update_stats(self, guild_id, user_id, username, count):
        """Update argument statistics for specific guild"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO argue_stats (guild_id, user_id, username, argument_count, last_argued)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (str(guild_id), str(user_id), username, count))
        
        conn.commit()
        conn.close()

    def get_user_count(self, guild_id, user_id):
        """Get argument count for a user in specific guild"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT argument_count FROM argue_stats WHERE guild_id = ? AND user_id = ?', 
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
        """Listen for argument triggers in messages - FIXED CHANGE DETECTION RESET"""
        if message.author == self.bot.user or not message.content:
            return
        
        # Don't ignore commands - but skip bot commands
        if message.content.startswith(('!', '/')) or message.author.bot:
            return
            
        if not message.guild:  # Skip DMs
            return
            
        guild_settings = self.get_guild_settings(message.guild.id)
        
        if not guild_settings["enabled"]:
            return
            
        # Check if user is on cooldown
        if self.is_user_on_cooldown(message.guild.id, message.author.id):
            return
            
        # Check if message should be tracked based on settings
        if not self.should_track_message(message, guild_settings):
            return
        
        # Check for sincerity triggers first
        if (guild_settings["sincerity_enabled"] and 
            self.is_sincerity_trigger(message.content, guild_settings) and
            not self.check_sincerity_timeout(message.channel.id, guild_settings)):
            
            # Send sincerity response and set cooldown
            await message.channel.send(guild_settings["sincerity_message"])
            self.set_user_cooldown(message.guild.id, message.author.id, guild_settings["sincerity_cooldown"])
            self.reset_argument_count(message.guild.id, message.author.id)
            # Reset last response for change detection
            user_key = f"{message.guild.id}_{message.author.id}"
            if user_key in self.user_last_response:
                del self.user_last_response[user_key]
            return
        
        found_type = self.get_word_type(message.content, guild_settings)
        
        if found_type:
            # Check for timeout and reset argument count if needed
            timeout_reset = self.check_argument_timeout(message.guild.id, message.author.id, guild_settings)
            
            # Update last argument time
            self.update_last_argument_time(message.guild.id, message.author.id)
            
            # Check for change detection
            user_key = f"{message.guild.id}_{message.author.id}"
            current_response = "yes" if found_type == "negative" else "no"
            
            # FIXED: Check if user has changed their answer from the last one
            if (guild_settings["change_detection_enabled"] and 
                user_key in self.user_last_response and 
                self.user_last_response[user_key] != found_type):
                
                # User changed their answer - send change detection response
                change_responses = guild_settings.get("change_detection_responses", DEFAULT_CHANGE_DETECTION_RESPONSES.copy())
                if change_responses:
                    response = random.choice(change_responses)
                    await message.channel.send(response)
                    # Update bot message time for sincerity timeout
                    self.update_bot_message_time(message.channel.id)
                    
                    # Update stats
                    current_count = self.get_user_count(message.guild.id, message.author.id)
                    new_count = current_count + 1
                    self.update_stats(message.guild.id, message.author.id, message.author.display_name, new_count)
                    
                    # FIXED: Reset the argument count after change detection
                    self.reset_argument_count(message.guild.id, message.author.id)
                    
                    # Update last response type
                    self.user_last_response[user_key] = found_type
                    return
            
            # Update stats
            current_count = self.get_user_count(message.guild.id, message.author.id)
            new_count = current_count + 1
            self.update_stats(message.guild.id, message.author.id, message.author.display_name, new_count)
            
            # Increment argument count and check for ragequit
            argument_count = self.increment_argument_count(message.guild.id, message.author.id)
            
            if self.should_ragequit(message.guild.id, message.author.id, guild_settings):
                # Send ragequit message, set cooldown, and reset count
                await message.channel.send(guild_settings["ragequit_message"])
                self.set_user_cooldown(message.guild.id, message.author.id, guild_settings["ragequit_cooldown"])
                self.reset_argument_count(message.guild.id, message.author.id)
                # Update bot message time for sincerity timeout
                self.update_bot_message_time(message.channel.id)
                # Reset last response for change detection
                if user_key in self.user_last_response:
                    del self.user_last_response[user_key]
                return
            
            # Send normal response
            await message.channel.send(current_response)
            # Update bot message time for sincerity timeout
            self.update_bot_message_time(message.channel.id)
            
            # Update last response type for change detection
            self.user_last_response[user_key] = found_type

    @commands.Cog.listener()
    async def on_member_remove(self, member):
        """Reset argument count when a user leaves the server"""
        guild_id = member.guild.id
        user_id = member.id
        
        # Reset user count to 0
        self.update_stats(guild_id, user_id, member.display_name, 0)
        self.reset_argument_count(guild_id, user_id)
        
        # Remove from change detection tracking
        user_key = f"{guild_id}_{user_id}"
        if user_key in self.user_last_response:
            del self.user_last_response[user_key]
        
        # Remove from argument time tracking
        if user_key in self.user_last_argument_time:
            del self.user_last_argument_time[user_key]

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

    async def create_leaderboard_embed(self, guild_id):
        """Create leaderboard embed without sending it"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Most arguments in this guild
        cursor.execute('''
            SELECT username, argument_count 
            FROM argue_stats 
            WHERE guild_id = ? AND argument_count > 0 
            ORDER BY argument_count DESC 
            LIMIT 10
        ''', (str(guild_id),))
        most_results = cursor.fetchall()
        
        conn.close()
        
        embed = discord.Embed(title="🏆 Argument Leaderboard", color=discord.Color.gold())
        
        if most_results:
            most_text = "\n".join([f"{i}. **{name}**: {count} argument(s)" for i, (name, count) in enumerate(most_results, 1)])
            embed.add_field(name="🔴 Most Argumentative", value=most_text, inline=False)
        else:
            embed.add_field(name="🔴 Most Argumentative", value="No arguments recorded yet!", inline=False)
        
        return embed

    async def show_leaderboard(self, interaction: discord.Interaction = None, ctx: commands.Context = None):
        """Show leaderboard (works for both interaction and context)"""
        guild_id = interaction.guild.id if interaction else ctx.guild.id
        
        embed = await self.create_leaderboard_embed(guild_id)
        
        if interaction:
            await interaction.response.send_message(embed=embed)
        else:
            await ctx.send(embed=embed)

    # Text Commands
    @commands.group(name="arg", invoke_without_command=True)
    async def arg(self, ctx):
        """Argue Tracker main command"""
        if ctx.invoked_subcommand is None:
            await ctx.send("**Argue Tracker Commands:**\n"
                          "`!arg on` - Enable tracking\n"
                          "`!arg off` - Disable tracking\n"
                          "`!arg lb` - Show leaderboard\n"
                          "`!arg pos <word>` - Add positive word\n"
                          "`!arg neg <word>` - Add negative word\n"
                          "`!arg remove <word>` - Remove word\n"
                          "`!arg list` - Show word list\n"
                          "`!arg reset list` - Reset to default words")

    @arg.command(name="on")
    async def arg_on(self, ctx):
        """Enable argument tracking"""
        if not await self.check_permissions_ctx(ctx):
            await ctx.send("❌ You need administrator permissions or developer access.")
            return
            
        self.save_guild_settings(ctx.guild.id, enabled=True)
        await ctx.send("✅ Argument feature turned **on**!")

    @arg.command(name="off")
    async def arg_off(self, ctx):
        """Disable argument tracking"""
        if not await self.check_permissions_ctx(ctx):
            await ctx.send("❌ You need administrator permissions or developer access.")
            return
            
        self.save_guild_settings(ctx.guild.id, enabled=False)
        await ctx.send("✅ Argument feature turned **off**!")

    @arg.command(name="lb")
    async def arg_lb(self, ctx):
        """Show leaderboard"""
        await self.show_leaderboard(ctx=ctx)

    @arg.command(name="pos")
    async def arg_pos(self, ctx, *, word: str):
        """Add a word to the positive words list"""
        if not await self.check_permissions_ctx(ctx):
            await ctx.send("❌ You need administrator permissions or developer access.")
            return
            
        word = word.strip().lower()
        if not word:
            await ctx.send("❌ Please specify a word to add.")
            return
            
        settings = self.get_guild_settings(ctx.guild.id)
        
        if word in settings["positive_words"]:
            await ctx.send(f"❌ **{word}** is already in the positive words list!")
            return
        
        settings["positive_words"].append(word)
        self.save_guild_settings(ctx.guild.id, positive_words=settings["positive_words"])
        
        await ctx.send(f"✅ Added **{word}** to positive words list! (Will respond with 'no')")

    @arg.command(name="neg")
    async def arg_neg(self, ctx, *, word: str):
        """Add a word to the negative words list"""
        if not await self.check_permissions_ctx(ctx):
            await ctx.send("❌ You need administrator permissions or developer access.")
            return
            
        word = word.strip().lower()
        if not word:
            await ctx.send("❌ Please specify a word to add.")
            return
            
        settings = self.get_guild_settings(ctx.guild.id)
        
        if word in settings["negative_words"]:
            await ctx.send(f"❌ **{word}** is already in the negative words list!")
            return
        
        settings["negative_words"].append(word)
        self.save_guild_settings(ctx.guild.id, negative_words=settings["negative_words"])
        
        await ctx.send(f"✅ Added **{word}** to negative words list! (Will respond with 'yes')")

    @arg.command(name="remove")
    async def arg_remove(self, ctx, *, word: str):
        """Remove a word from any word list"""
        if not await self.check_permissions_ctx(ctx):
            await ctx.send("❌ You need administrator permissions or developer access.")
            return
            
        word = word.strip().lower()
        if not word:
            await ctx.send("❌ Please specify a word to remove.")
            return
            
        settings = self.get_guild_settings(ctx.guild.id)
        
        removed_from = []
        if word in settings["positive_words"]:
            settings["positive_words"].remove(word)
            removed_from.append("positive")
        if word in settings["negative_words"]:
            settings["negative_words"].remove(word)
            removed_from.append("negative")
        
        if not removed_from:
            await ctx.send(f"❌ **{word}** is not in any word list!")
            return
        
        self.save_guild_settings(
            ctx.guild.id, 
            positive_words=settings["positive_words"],
            negative_words=settings["negative_words"]
        )
        
        await ctx.send(f"✅ Removed **{word}** from {', '.join(removed_from)} word list(s)!")

    @arg.command(name="list")
    async def arg_list(self, ctx):
        """Show the word lists"""
        if not await self.check_permissions_ctx(ctx):
            await ctx.send("❌ You need administrator permissions or developer access.")
            return
            
        settings = self.get_guild_settings(ctx.guild.id)
        
        embed = discord.Embed(
            title="📋 Argument Word Lists",
            color=discord.Color.blue()
        )
        
        # Positive words
        positive_text = ", ".join(sorted(settings["positive_words"]))
        if len(positive_text) > 500:
            positive_text = positive_text[:500] + "..."
        embed.add_field(
            name=f"✅ Positive Words ({len(settings['positive_words'])} - triggers 'no' response)",
            value=positive_text or "No positive words configured",
            inline=False
        )
        
        # Negative words
        negative_text = ", ".join(sorted(settings["negative_words"]))
        if len(negative_text) > 500:
            negative_text = negative_text[:500] + "..."
        embed.add_field(
            name=f"❌ Negative Words ({len(settings['negative_words'])} - triggers 'yes' response)",
            value=negative_text or "No negative words configured",
            inline=False
        )
        
        await ctx.send(embed=embed)

    @arg.command(name="reset")
    async def arg_reset(self, ctx, *, target: str = None):
        """Reset word lists to default"""
        if not await self.check_permissions_ctx(ctx):
            await ctx.send("❌ You need administrator permissions or developer access.")
            return
            
        if not target or target.lower() != "list":
            await ctx.send("❌ Please specify 'list' to reset word lists.")
            return
            
        # Reset to default words only
        self.save_guild_settings(
            ctx.guild.id, 
            positive_words=DEFAULT_POSITIVE_WORDS.copy(),
            negative_words=DEFAULT_NEGATIVE_WORDS.copy()
        )
        await ctx.send("✅ All custom words have been reset to default!")

    # Single Slash Command - Interactive Setup Wizard
    @app_commands.command(name="argue", description="Manage the automatic argument feature with an interactive menu")
    async def argue(self, interaction: discord.Interaction):
        """Main command to open the interactive setup wizard"""
        if not await self.check_permissions(interaction):
            await interaction.response.send_message("❌ You need administrator permissions or developer access.", ephemeral=True)
            return
            
        view = ArgueTrackerView(self, interaction.guild.id)
        embed = view.create_status_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()


async def setup(bot):
    cog = Argue(bot)
    await bot.add_cog(cog)
    print("✅ Argue cog loaded successfully - settings preserved")
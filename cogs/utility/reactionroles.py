import discord
from discord import app_commands
from discord.ext import commands
import sqlite3
import json
import os
from typing import Optional, Literal
import traceback

# Add your user ID here to bypass permission checks
BOT_DEVELOPER_IDS = [1313333441525448704]  # Replace with your actual Discord user ID

def is_bot_developer():
    """Check if the user is a bot developer"""
    async def predicate(interaction: discord.Interaction):
        return interaction.user.id in BOT_DEVELOPER_IDS
    return app_commands.check(predicate)

class ReactionRoleButton(discord.ui.Button):
    def __init__(self, role_id: int, emoji: str, label: str, style: discord.ButtonStyle):
        super().__init__(style=style, emoji=emoji, label=label, custom_id=f"rr_{role_id}")
        self.role_id = role_id

    async def callback(self, interaction: discord.Interaction):
        try:
            role = interaction.guild.get_role(self.role_id)
            if not role:
                await interaction.response.send_message("❌ Role not found!", ephemeral=True)
                return

            member = interaction.user
            if role in member.roles:
                await member.remove_roles(role)
                await interaction.response.send_message(f"✅ Removed role: {role.mention}", ephemeral=True)
            else:
                await member.add_roles(role)
                await interaction.response.send_message(f"✅ Added role: {role.mention}", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class ReactionRoleView(discord.ui.View):
    def __init__(self, roles_data: list):
        super().__init__(timeout=None)
        for role_data in roles_data:
            button = ReactionRoleButton(
                role_id=role_data['role_id'],
                emoji=role_data['emoji'],
                label=role_data['label'],
                style=discord.ButtonStyle.primary
            )
            self.add_item(button)

class ChannelSelect(discord.ui.ChannelSelect):
    def __init__(self):
        super().__init__(
            placeholder="Select a channel...",
            channel_types=[discord.ChannelType.text]
        )

    async def callback(self, interaction: discord.Interaction):
        try:
            self.view.channel = self.values[0]
            await interaction.response.edit_message(
                content=f"📝 Channel selected: {self.values[0].mention}\n\nNow choose the mode:",
                view=ModeSelectView(self.view)
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class ModeSelectView(discord.ui.View):
    def __init__(self, parent_view):
        super().__init__(timeout=300)
        self.parent_view = parent_view

    @discord.ui.button(label="Stackable", style=discord.ButtonStyle.green, emoji="📚")
    async def stackable(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self.parent_view.mode = "stackable"
            await interaction.response.edit_message(
                content=f"📝 Channel: {self.parent_view.channel.mention}\n⚙️ Mode: **Stackable**\n\nNow choose the type:",
                view=TypeSelectView(self.parent_view)
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    @discord.ui.button(label="Non-Stackable", style=discord.ButtonStyle.red, emoji="🚫")
    async def non_stackable(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self.parent_view.mode = "non-stackable"
            await interaction.response.edit_message(
                content=f"📝 Channel: {self.parent_view.channel.mention}\n⚙️ Mode: **Non-Stackable**\n\nNow choose the type:",
                view=TypeSelectView(self.parent_view)
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class TypeSelectView(discord.ui.View):
    def __init__(self, parent_view):
        super().__init__(timeout=300)
        self.parent_view = parent_view

    @discord.ui.button(label="Buttons", style=discord.ButtonStyle.blurple, emoji="🔘")
    async def button_type(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self.parent_view.msg_type = "button"
            await interaction.response.send_modal(CreateMessageModal(self.parent_view))
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    @discord.ui.button(label="Reactions", style=discord.ButtonStyle.gray, emoji="👍")
    async def reaction_type(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self.parent_view.msg_type = "reaction"
            await interaction.response.send_modal(CreateMessageModal(self.parent_view))
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class CreateMessageModal(discord.ui.Modal, title="🎯 Create Reaction Role Message"):
    title_input = discord.ui.TextInput(
        label="📝 Embed Title",
        placeholder="Choose Your Roles",
        required=True,
        max_length=256
    )
    
    description = discord.ui.TextInput(
        label="📄 Embed Description",
        placeholder="Click the buttons below to get roles!\n\nPress ENTER for line breaks\nNot \\n characters",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=2000
    )

    footer = discord.ui.TextInput(
        label="👣 Embed Footer (Optional)",
        placeholder="Server Roles • Updated regularly",
        required=False,
        max_length=2048
    )

    def __init__(self, parent_view):
        super().__init__()
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)

            # Get the proper TextChannel object from the guild
            try:
                channel = interaction.guild.get_channel(self.parent_view.channel.id)
                if not channel:
                    channel = await interaction.guild.fetch_channel(self.parent_view.channel.id)
            except Exception as e:
                await interaction.followup.send(f"❌ Error getting channel: {str(e)}", ephemeral=True)
                return

            if not channel or not isinstance(channel, discord.TextChannel):
                await interaction.followup.send("❌ Invalid channel selected!", ephemeral=True)
                return

            # Check permissions for non-developers
            if interaction.user.id not in BOT_DEVELOPER_IDS:
                # Check if user has permission to send messages in the channel
                if not channel.permissions_for(interaction.user).send_messages:
                    await interaction.followup.send("❌ You don't have permission to send messages in that channel!", ephemeral=True)
                    return
                
                # Check if user has permission to manage messages in the channel
                if not channel.permissions_for(interaction.user).manage_messages:
                    await interaction.followup.send("❌ You need 'Manage Messages' permission in that channel!", ephemeral=True)
                    return

            # Check bot permissions
            bot_member = interaction.guild.get_member(self.parent_view.cog.bot.user.id)
            if not channel.permissions_for(bot_member).send_messages:
                await interaction.followup.send("❌ I don't have permission to send messages in that channel!", ephemeral=True)
                return
            if not channel.permissions_for(bot_member).embed_links:
                await interaction.followup.send("❌ I need 'Embed Links' permission in that channel!", ephemeral=True)
                return
            if not channel.permissions_for(bot_member).manage_messages and self.parent_view.msg_type == "reaction":
                await interaction.followup.send("❌ I need 'Manage Messages' permission for reaction roles!", ephemeral=True)
                return

            # Create embed - the description will preserve the actual line breaks from user input
            embed = discord.Embed(
                title=self.title_input.value,
                description=self.description.value,  # This preserves actual line breaks
                color=discord.Color.blue()
            )

            # Add footer if provided
            if self.footer.value:
                embed.set_footer(text=self.footer.value)

            # Send message
            try:
                message = await channel.send(embed=embed)
            except Exception as e:
                await interaction.followup.send(f"❌ Error sending message: {str(e)}", ephemeral=True)
                return

            # Store in database
            conn = sqlite3.connect(self.parent_view.cog.db_path)
            cursor = conn.cursor()
            embed_data = {
                "title": self.title_input.value,
                "description": self.description.value,
                "color": None,
                "thumbnail": None,
                "image": None,
                "footer": self.footer.value if self.footer.value else None
            }
            cursor.execute("""
                INSERT INTO rr_embeds (message_id, guild_id, channel_id, embed_data, mode, type)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (message.id, interaction.guild.id, channel.id, json.dumps(embed_data), self.parent_view.mode, self.parent_view.msg_type))
            conn.commit()
            conn.close()

            # Show management view
            view = ReactionRoleManagementView(self.parent_view.cog, message.id)
            await interaction.followup.send(
                f"✅ Created reaction role message in {channel.mention}!\n"
                f"📝 Message ID: `{message.id}`\n"
                f"⚙️ Mode: **{self.parent_view.mode}** | Type: **{self.parent_view.msg_type}**\n\n"
                f"Use the buttons below to manage this message:",
                view=view,
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(f"❌ Unexpected error: {str(e)}", ephemeral=True)

class CreateMessageView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=300)
        self.cog = cog
        self.channel = None
        self.mode = None
        self.msg_type = None

    @discord.ui.button(label="Select Channel", style=discord.ButtonStyle.blurple, emoji="📝")
    async def select_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            view = discord.ui.View(timeout=300)
            view.add_item(ChannelSelect())
            view.channel = None
            view.cog = self.cog
            await interaction.response.edit_message(
                content="📝 **Step 1/3**: Select a channel for the reaction role message:",
                view=view
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class RoleSelect(discord.ui.RoleSelect):
    def __init__(self):
        super().__init__(placeholder="Select a role...")

    async def callback(self, interaction: discord.Interaction):
        try:
            self.view.role = self.values[0]
            
            # Check if user can manage this role (unless they're a developer)
            if interaction.user.id not in BOT_DEVELOPER_IDS:
                if self.view.role.position >= interaction.guild.me.top_role.position:
                    await interaction.response.send_message("❌ I cannot assign roles higher than my own!", ephemeral=True)
                    return
                if not interaction.user.guild_permissions.manage_roles and self.view.role.position >= interaction.user.top_role.position:
                    await interaction.response.send_message("❌ You cannot manage roles higher than your own!", ephemeral=True)
                    return

            await interaction.response.edit_message(
                content=f"✅ Role selected: {self.values[0].mention}\n\nNow provide an emoji:",
                view=EmojiInputView(self.view)
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class EmojiInputView(discord.ui.View):
    def __init__(self, parent_view):
        super().__init__(timeout=300)
        self.parent_view = parent_view

    @discord.ui.button(label="Enter Emoji", style=discord.ButtonStyle.green, emoji="😀")
    async def enter_emoji(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.send_modal(EmojiModal(self.parent_view))
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class EmojiModal(discord.ui.Modal, title="🎭 Add Emoji"):
    emoji = discord.ui.TextInput(
        label="🎭 Select Emoji",
        placeholder="Enter an emoji for this role\nExamples:\n😀 - Standard emoji\n:custom_emoji: - Server emoji",
        required=True,
        max_length=100
    )

    def __init__(self, parent_view):
        super().__init__()
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await self.parent_view.parent_view.finalize_add_role(interaction, self.parent_view.role, self.emoji.value)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class AddRoleView(discord.ui.View):
    def __init__(self, cog, message_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.message_id = message_id
        self.role = None

    @discord.ui.button(label="Select Role", style=discord.ButtonStyle.blurple, emoji="👥")
    async def select_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            view = discord.ui.View(timeout=300)
            view.add_item(RoleSelect())
            view.parent_view = self
            await interaction.response.edit_message(
                content="👥 **Select a role to add:**",
                view=view
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    async def finalize_add_role(self, interaction: discord.Interaction, role: discord.Role, emoji: str):
        try:
            await interaction.response.defer(ephemeral=True)

            # Get message data
            conn = sqlite3.connect(self.cog.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT channel_id, mode, type FROM rr_embeds WHERE message_id = ?", (self.message_id,))
            result = cursor.fetchone()

            if not result:
                await interaction.followup.send("❌ Message not found!", ephemeral=True)
                conn.close()
                return

            channel_id, mode, msg_type = result

            # Check if role already exists
            cursor.execute("""
                SELECT id FROM reaction_roles 
                WHERE message_id = ? AND role_id = ?
            """, (self.message_id, role.id))
            if cursor.fetchone():
                await interaction.followup.send("❌ This role is already added!", ephemeral=True)
                conn.close()
                return

            # Add role to database
            label = role.name[:80]  # Truncate if too long
            cursor.execute("""
                INSERT INTO reaction_roles (guild_id, channel_id, message_id, role_id, emoji, label, mode, type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (interaction.guild.id, channel_id, self.message_id, role.id, emoji, label, mode, msg_type))
            conn.commit()
            conn.close()

            # Update the message
            try:
                channel = interaction.guild.get_channel(channel_id)
                message = await channel.fetch_message(self.message_id)

                if msg_type == "reaction":
                    await message.add_reaction(emoji)
                    await interaction.followup.send(f"✅ Added {role.mention} with {emoji}!", ephemeral=True)
                else:  # button
                    # Rebuild view
                    conn = sqlite3.connect(self.cog.db_path)
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT role_id, emoji, label 
                        FROM reaction_roles 
                        WHERE message_id = ?
                    """, (self.message_id,))
                    roles_data = [{"role_id": r[0], "emoji": r[1], "label": r[2]} for r in cursor.fetchall()]
                    conn.close()

                    view = ReactionRoleView(roles_data)
                    await message.edit(view=view)
                    await interaction.followup.send(f"✅ Added {role.mention} with {emoji}!", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"❌ Error updating message: {str(e)}", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Unexpected error: {str(e)}", ephemeral=True)

class RemoveRoleView(discord.ui.View):
    def __init__(self, cog, message_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.message_id = message_id

    @discord.ui.button(label="Select Role to Remove", style=discord.ButtonStyle.red, emoji="🗑️")
    async def select_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Get current roles for this message
            conn = sqlite3.connect(self.cog.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT role_id, emoji, label 
                FROM reaction_roles 
                WHERE message_id = ?
            """, (self.message_id,))
            roles = cursor.fetchall()
            conn.close()

            if not roles:
                await interaction.response.send_message("❌ No roles to remove!", ephemeral=True)
                return

            # Create role select dropdown
            options = []
            for role_id, emoji, label in roles:
                role = interaction.guild.get_role(role_id)
                if role:
                    options.append(discord.SelectOption(
                        label=label,
                        value=str(role_id),
                        description=f"ID: {role_id}",
                        emoji=emoji
                    ))

            if not options:
                await interaction.response.send_message("❌ No valid roles found!", ephemeral=True)
                return

            select = RoleRemoveSelect(options)
            view = discord.ui.View(timeout=300)
            view.add_item(select)
            view.parent_view = self

            await interaction.response.edit_message(
                content="🗑️ **Select a role to remove:**",
                view=view
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    async def remove_role(self, interaction: discord.Interaction, role_id: int):
        try:
            await interaction.response.defer(ephemeral=True)

            role = interaction.guild.get_role(role_id)
            if not role:
                await interaction.followup.send("❌ Role not found!", ephemeral=True)
                return

            conn = sqlite3.connect(self.cog.db_path)
            cursor = conn.cursor()

            # Get emoji and type
            cursor.execute("""
                SELECT emoji, type, channel_id 
                FROM reaction_roles 
                WHERE message_id = ? AND role_id = ?
            """, (self.message_id, role_id))
            result = cursor.fetchone()

            if not result:
                await interaction.followup.send("❌ Role not found for this message!", ephemeral=True)
                conn.close()
                return

            emoji, msg_type, channel_id = result

            # Delete from database
            cursor.execute("""
                DELETE FROM reaction_roles 
                WHERE message_id = ? AND role_id = ?
            """, (self.message_id, role_id))
            conn.commit()
            conn.close()

            # Update message
            try:
                channel = interaction.guild.get_channel(channel_id)
                message = await channel.fetch_message(self.message_id)

                if msg_type == "reaction":
                    await message.clear_reaction(emoji)
                else:
                    conn = sqlite3.connect(self.cog.db_path)
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT role_id, emoji, label 
                        FROM reaction_roles 
                        WHERE message_id = ?
                    """, (self.message_id,))
                    roles_data = [{"role_id": r[0], "emoji": r[1], "label": r[2]} for r in cursor.fetchall()]
                    conn.close()

                    if roles_data:
                        view = ReactionRoleView(roles_data)
                        await message.edit(view=view)
                    else:
                        await message.edit(view=None)

                await interaction.followup.send(f"✅ Removed {role.mention}!", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"❌ Error updating message: {str(e)}", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Unexpected error: {str(e)}", ephemeral=True)

class RoleRemoveSelect(discord.ui.Select):
    def __init__(self, options):
        super().__init__(placeholder="Select a role to remove...", options=options)

    async def callback(self, interaction: discord.Interaction):
        try:
            role_id = int(self.values[0])
            await self.view.parent_view.remove_role(interaction, role_id)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class EditEmbedView(discord.ui.View):
    def __init__(self, cog, message_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.message_id = message_id

    @discord.ui.button(label="Edit Title", style=discord.ButtonStyle.blurple, emoji="📝")
    async def edit_title(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.send_modal(EditTitleModal(self.cog, self.message_id))
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    @discord.ui.button(label="Edit Description", style=discord.ButtonStyle.blurple, emoji="📄")
    async def edit_description(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.send_modal(EditDescriptionModal(self.cog, self.message_id))
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    @discord.ui.button(label="Edit Footer", style=discord.ButtonStyle.blurple, emoji="👣")
    async def edit_footer(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.send_modal(EditFooterModal(self.cog, self.message_id))
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.gray, emoji="⬅️")
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            view = ReactionRoleManagementView(self.cog, self.message_id)
            await interaction.response.edit_message(
                content=f"🛠️ **Managing message ID:** `{self.message_id}`\nUse the buttons below to manage this message:",
                view=view
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class EditTitleModal(discord.ui.Modal, title="✏️ Edit Embed Title"):
    new_title = discord.ui.TextInput(
        label="📝 New Title",
        placeholder="Enter the new title for your embed...",
        required=True,
        max_length=256
    )

    def __init__(self, cog, message_id: int):
        super().__init__()
        self.cog = cog
        self.message_id = message_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await self.cog.edit_embed_field(interaction, self.message_id, "title", self.new_title.value)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class EditDescriptionModal(discord.ui.Modal, title="✏️ Edit Embed Description"):
    new_description = discord.ui.TextInput(
        label="📄 New Description",
        placeholder="Enter the new description\nPress ENTER for line breaks\nNot \\n characters",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=2000
    )

    def __init__(self, cog, message_id: int):
        super().__init__()
        self.cog = cog
        self.message_id = message_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await self.cog.edit_embed_field(interaction, self.message_id, "description", self.new_description.value)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class EditFooterModal(discord.ui.Modal, title="✏️ Edit Embed Footer"):
    new_footer = discord.ui.TextInput(
        label="👣 New Footer",
        placeholder="Enter new footer text\nLeave empty to remove the footer",
        required=False,
        max_length=2048
    )

    def __init__(self, cog, message_id: int):
        super().__init__()
        self.cog = cog
        self.message_id = message_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await self.cog.edit_embed_field(interaction, self.message_id, "footer", self.new_footer.value)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class ReactionRoleManagementView(discord.ui.View):
    def __init__(self, cog, message_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.message_id = message_id

    @discord.ui.button(label="Add Role", style=discord.ButtonStyle.green, emoji="➕")
    async def add_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            view = AddRoleView(self.cog, self.message_id)
            await interaction.response.edit_message(
                content="🛠️ **Add a new role to the message:**",
                view=view
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    @discord.ui.button(label="Remove Role", style=discord.ButtonStyle.red, emoji="➖")
    async def remove_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            view = RemoveRoleView(self.cog, self.message_id)
            await interaction.response.edit_message(
                content="🛠️ **Remove a role from the message:**",
                view=view
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    @discord.ui.button(label="Edit Embed", style=discord.ButtonStyle.blurple, emoji="✏️")
    async def edit_embed(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            view = EditEmbedView(self.cog, self.message_id)
            await interaction.response.edit_message(
                content="🛠️ **Edit the embed content:**",
                view=view
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    @discord.ui.button(label="View Info", style=discord.ButtonStyle.blurple, emoji="ℹ️")
    async def view_info(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.cog.show_message_info(interaction, self.message_id)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    @discord.ui.button(label="Delete Message", style=discord.ButtonStyle.danger, emoji="🗑️")
    async def delete_message(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.cog.delete_message(interaction, self.message_id)
            self.stop()
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class ManageExistingView(discord.ui.View):
    def __init__(self, cog, messages_data):
        super().__init__(timeout=300)
        self.cog = cog
        
        # Create select menu with existing messages
        options = []
        for msg_id, ch_id, mode, msg_type in messages_data[:25]:
            channel = cog.bot.get_channel(ch_id)
            channel_name = channel.name if channel else f"Unknown-{ch_id}"
            options.append(
                discord.SelectOption(
                    label=f"Message {msg_id}",
                    description=f"#{channel_name} | {mode} | {msg_type}",
                    value=str(msg_id)
                )
            )
        
        if options:
            select = discord.ui.Select(
                placeholder="Select a message to manage",
                options=options
            )
            select.callback = self.select_callback
            self.add_item(select)

    async def select_callback(self, interaction: discord.Interaction):
        try:
            message_id = int(interaction.data["values"][0])
            view = ReactionRoleManagementView(self.cog, message_id)
            await interaction.response.edit_message(
                content=f"🛠️ **Managing message ID:** `{message_id}`\nUse the buttons below to manage this message:",
                view=view
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

class ReactionRoles(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        os.makedirs("data", exist_ok=True)
        self.db_path = "data/reactionroles.db"
        self.init_db()
        bot.loop.create_task(self.setup_persistent_views())

    def init_db(self):
        """Initialize the SQLite database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reaction_roles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    role_id INTEGER NOT NULL,
                    emoji TEXT NOT NULL,
                    label TEXT,
                    mode TEXT NOT NULL,
                    type TEXT NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rr_embeds (
                    message_id INTEGER PRIMARY KEY,
                    guild_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    embed_data TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    type TEXT NOT NULL
                )
            """)
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Database initialization error: {e}")

    async def setup_persistent_views(self):
        """Setup persistent views for buttons after bot restart"""
        await self.bot.wait_until_ready()
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT DISTINCT message_id FROM rr_embeds WHERE type = 'button'")
            messages = cursor.fetchall()
            
            for (message_id,) in messages:
                cursor.execute("""
                    SELECT role_id, emoji, label 
                    FROM reaction_roles 
                    WHERE message_id = ? AND type = 'button'
                """, (message_id,))
                roles_data = [{"role_id": r[0], "emoji": r[1], "label": r[2]} for r in cursor.fetchall()]
                
                if roles_data:
                    view = ReactionRoleView(roles_data)
                    self.bot.add_view(view)
            
            conn.close()
        except Exception as e:
            print(f"Persistent views setup error: {e}")

    async def check_permissions(self, interaction: discord.Interaction) -> bool:
        """Check if user has permissions to use reaction role commands"""
        # Bot developers always have access
        if interaction.user.id in BOT_DEVELOPER_IDS:
            return True
            
        # Regular users need specific permissions
        if not interaction.user.guild_permissions.manage_roles:
            await interaction.response.send_message(
                "❌ You need 'Manage Roles' permission to use this command!",
                ephemeral=True
            )
            return False
            
        return True

    @app_commands.command(name="reactionrole", description="Create and manage reaction roles")
    @app_commands.describe(
        action="What do you want to do?",
        message_id="Message ID (for managing existing messages)"
    )
    async def reactionrole(
        self,
        interaction: discord.Interaction,
        action: Literal["create", "manage", "list"],
        message_id: Optional[str] = None
    ):
        """Single command to handle all reaction role operations"""
        try:
            # Check permissions for non-developers
            if not await self.check_permissions(interaction):
                return

            if action == "create":
                # Start the creation workflow
                view = CreateMessageView(self)
                await interaction.response.send_message(
                    "🎯 **Reaction Role Setup**\n\nLet's create a new reaction role message! Click below to start:",
                    view=view,
                    ephemeral=True
                )
            
            elif action == "manage":
                if message_id:
                    # Manage specific message
                    try:
                        msg_id = int(message_id)
                        # Verify message exists
                        conn = sqlite3.connect(self.db_path)
                        cursor = conn.cursor()
                        cursor.execute("SELECT message_id FROM rr_embeds WHERE message_id = ?", (msg_id,))
                        if not cursor.fetchone():
                            await interaction.response.send_message("❌ Message not found!", ephemeral=True)
                            conn.close()
                            return
                        conn.close()
                        
                        view = ReactionRoleManagementView(self, msg_id)
                        await interaction.response.send_message(
                            f"🛠️ **Managing message ID:** `{msg_id}`\nUse the buttons below to manage this message:",
                            view=view,
                            ephemeral=True
                        )
                    except:
                        await interaction.response.send_message("❌ Invalid message ID!", ephemeral=True)
                else:
                    # Show list to select from
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT message_id, channel_id, mode, type 
                        FROM rr_embeds 
                        WHERE guild_id = ?
                    """, (interaction.guild.id,))
                    messages = cursor.fetchall()
                    conn.close()
                    
                    if not messages:
                        await interaction.response.send_message("📭 No reaction role messages found!", ephemeral=True)
                        return
                    
                    view = ManageExistingView(self, messages)
                    await interaction.response.send_message(
                        "📋 **Select a message to manage:**",
                        view=view,
                        ephemeral=True
                    )
            
            elif action == "list":
                # List all messages
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT message_id, channel_id, mode, type 
                    FROM rr_embeds 
                    WHERE guild_id = ?
                """, (interaction.guild.id,))
                messages = cursor.fetchall()

                if not messages:
                    await interaction.response.send_message("📭 No reaction role messages found!", ephemeral=True)
                    conn.close()
                    return

                embed = discord.Embed(
                    title="📋 Reaction Role Messages",
                    description=f"Found {len(messages)} message(s)",
                    color=discord.Color.blue()
                )
                
                for msg_id, ch_id, mode, msg_type in messages:
                    cursor.execute("""
                        SELECT COUNT(*) FROM reaction_roles WHERE message_id = ?
                    """, (msg_id,))
                    role_count = cursor.fetchone()[0]
                    
                    channel = self.bot.get_channel(ch_id)
                    channel_mention = channel.mention if channel else f"Unknown ({ch_id})"
                    
                    embed.add_field(
                        name=f"Message ID: {msg_id}",
                        value=f"📍 {channel_mention} | ⚙️ {mode} | 🎨 {msg_type} | 👥 {role_count} roles",
                        inline=False
                    )

                conn.close()
                await interaction.response.send_message(embed=embed, ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Command error: {str(e)}", ephemeral=True)

    async def edit_embed_field(self, interaction: discord.Interaction, message_id: int, field: str, value: str):
        """Edit a specific field in the embed"""
        try:
            await interaction.response.defer(ephemeral=True)

            # Check permissions for non-developers
            if interaction.user.id not in BOT_DEVELOPER_IDS:
                # Get message channel to check permissions
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT channel_id FROM rr_embeds WHERE message_id = ?", (message_id,))
                result = cursor.fetchone()
                conn.close()

                if result:
                    channel_id = result[0]
                    channel = self.bot.get_channel(channel_id)
                    if channel and not channel.permissions_for(interaction.user).manage_messages:
                        await interaction.followup.send("❌ You need 'Manage Messages' permission in that channel!", ephemeral=True)
                        return

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Get current embed data
            cursor.execute("SELECT channel_id, embed_data FROM rr_embeds WHERE message_id = ?", (message_id,))
            result = cursor.fetchone()

            if not result:
                await interaction.followup.send("❌ Message not found!", ephemeral=True)
                conn.close()
                return

            channel_id, embed_data_json = result
            embed_data = json.loads(embed_data_json)

            # Update the specific field
            if field == "footer" and value == "":
                # Remove footer if empty
                embed_data[field] = None
            else:
                embed_data[field] = value

            # Update database
            cursor.execute("""
                UPDATE rr_embeds SET embed_data = ? WHERE message_id = ?
            """, (json.dumps(embed_data), message_id))
            conn.commit()
            conn.close()

            # Update the actual message
            try:
                channel = self.bot.get_channel(channel_id)
                message = await channel.fetch_message(message_id)

                # Rebuild embed
                embed = message.embeds[0] if message.embeds else discord.Embed()
                
                if embed_data['title']:
                    embed.title = embed_data['title']
                if embed_data['description']:
                    embed.description = embed_data['description']
                if embed_data['footer']:
                    embed.set_footer(text=embed_data['footer'])
                else:
                    embed.remove_footer()

                await message.edit(embed=embed)
                await interaction.followup.send(f"✅ Updated {field} successfully!", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"❌ Error updating message: {str(e)}", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Unexpected error: {str(e)}", ephemeral=True)

    async def show_message_info(self, interaction: discord.Interaction, message_id: int):
        """Show detailed info about a message"""
        try:
            await interaction.response.defer(ephemeral=True)

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT channel_id, mode, type, embed_data 
                FROM rr_embeds 
                WHERE message_id = ?
            """, (message_id,))
            result = cursor.fetchone()

            if not result:
                await interaction.followup.send("❌ Message not found!", ephemeral=True)
                conn.close()
                return

            channel_id, mode, msg_type, embed_data_json = result
            embed_data = json.loads(embed_data_json)
            channel = self.bot.get_channel(channel_id)

            cursor.execute("""
                SELECT role_id, emoji, label 
                FROM reaction_roles 
                WHERE message_id = ?
            """, (message_id,))
            roles = cursor.fetchall()
            conn.close()

            embed = discord.Embed(
                title="📊 Message Info",
                description=f"Message ID: `{message_id}`",
                color=discord.Color.green()
            )

            embed.add_field(name="📍 Channel", value=channel.mention if channel else "Unknown", inline=True)
            embed.add_field(name="⚙️ Mode", value=mode.title(), inline=True)
            embed.add_field(name="🎨 Type", value=msg_type.title(), inline=True)
            
            # Show embed content
            embed_content = []
            if embed_data.get('title'):
                embed_content.append(f"**Title:** {embed_data['title']}")
            if embed_data.get('description'):
                desc = embed_data['description'][:100] + "..." if len(embed_data['description']) > 100 else embed_data['description']
                embed_content.append(f"**Description:** {desc}")
            if embed_data.get('footer'):
                embed_content.append(f"**Footer:** {embed_data['footer']}")
            
            if embed_content:
                embed.add_field(name="📝 Embed Content", value="\n".join(embed_content), inline=False)

            if roles:
                role_list = []
                for role_id, emoji, label in roles:
                    role = interaction.guild.get_role(role_id)
                    role_mention = role.mention if role else f"Deleted ({role_id})"
                    role_list.append(f"{emoji} **{label}** → {role_mention}")
                
                embed.add_field(
                    name=f"👥 Roles ({len(roles)})",
                    value="\n".join(role_list),
                    inline=False
                )
            else:
                embed.add_field(name="👥 Roles", value="No roles added yet", inline=False)

            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)

    async def delete_message(self, interaction: discord.Interaction, message_id: int):
        """Delete a reaction role message"""
        try:
            await interaction.response.defer(ephemeral=True)

            # Check permissions for non-developers
            if interaction.user.id not in BOT_DEVELOPER_IDS:
                # Get message channel to check permissions
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT channel_id FROM rr_embeds WHERE message_id = ?", (message_id,))
                result = cursor.fetchone()
                conn.close()

                if result:
                    channel_id = result[0]
                    channel = self.bot.get_channel(channel_id)
                    if channel and not channel.permissions_for(interaction.user).manage_messages:
                        await interaction.followup.send("❌ You need 'Manage Messages' permission in that channel!", ephemeral=True)
                        return

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("SELECT channel_id FROM rr_embeds WHERE message_id = ?", (message_id,))
            result = cursor.fetchone()

            if not result:
                await interaction.followup.send("❌ Message not found!", ephemeral=True)
                conn.close()
                return

            channel_id = result[0]

            cursor.execute("DELETE FROM reaction_roles WHERE message_id = ?", (message_id,))
            cursor.execute("DELETE FROM rr_embeds WHERE message_id = ?", (message_id,))
            conn.commit()
            conn.close()

            try:
                channel = self.bot.get_channel(channel_id)
                message = await channel.fetch_message(message_id)
                await message.delete()
                await interaction.followup.send("✅ Message and all data deleted!", ephemeral=True)
            except:
                await interaction.followup.send("✅ Data deleted! (Message already removed)", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)

    # Keep the existing reaction handlers
    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        """Handle reaction role additions"""
        if payload.user_id == self.bot.user.id:
            return

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT role_id, mode FROM reaction_roles 
                WHERE message_id = ? AND emoji = ? AND type = 'reaction'
            """, (payload.message_id, str(payload.emoji)))
            result = cursor.fetchone()

            if not result:
                conn.close()
                return

            role_id, mode = result
            guild = self.bot.get_guild(payload.guild_id)
            member = guild.get_member(payload.user_id)
            role = guild.get_role(role_id)

            if not role or not member:
                conn.close()
                return

            if mode == "non-stackable":
                cursor.execute("""
                    SELECT role_id FROM reaction_roles 
                    WHERE message_id = ? AND type = 'reaction'
                """, (payload.message_id,))
                all_roles = [guild.get_role(r[0]) for r in cursor.fetchall() if guild.get_role(r[0])]
                
                for other_role in all_roles:
                    if other_role != role and other_role in member.roles:
                        await member.remove_roles(other_role)

            await member.add_roles(role)
            conn.close()
        except Exception as e:
            print(f"Reaction add error: {e}")

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        """Handle reaction role removals"""
        if payload.user_id == self.bot.user.id:
            return

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT role_id FROM reaction_roles 
                WHERE message_id = ? AND emoji = ? AND type = 'reaction'
            """, (payload.message_id, str(payload.emoji)))
            result = cursor.fetchone()

            if not result:
                conn.close()
                return

            role_id = result[0]
            guild = self.bot.get_guild(payload.guild_id)
            member = guild.get_member(payload.user_id)
            role = guild.get_role(role_id)

            if role and member and role in member.roles:
                await member.remove_roles(role)

            conn.close()
        except Exception as e:
            print(f"Reaction remove error: {e}")

async def setup(bot):
    await bot.add_cog(ReactionRoles(bot))
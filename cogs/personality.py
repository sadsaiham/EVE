import discord
from discord.ext import commands

class PersonalitySwitcher(commands.Cog):
    """Switch EVE's personality on the fly"""
    
    def __init__(self, bot):
        self.bot = bot
        self.valid_personalities = ['elegant', 'seductive', 'playful', 'supportive', 'teacher', 'obedient']
    
    @commands.hybrid_command(name="personality", description="Switch EVE's personality")
    @commands.has_permissions(administrator=True)
    async def set_personality(self, ctx, personality: str):
        """Switch EVE's personality"""
        personality = personality.lower()
        
        if personality not in self.valid_personalities:
            embed = discord.Embed(
                title="❌ Invalid Personality",
                description=f"Available personalities:\n`{', '.join(self.valid_personalities)}`",
                color=self.bot.colors.ERROR
            )
            await ctx.send(embed=embed, ephemeral=True)
            return
        
        # Store old personality
        old_personality = self.bot.personality_mode
        old_color = self.get_personality_color(old_personality)
        
        # Update personality
        self.bot.personality_mode = personality
        new_color = self.get_personality_color(personality)
        
        # Create response based on new personality
        embed = discord.Embed(
            title="🎭 Personality Changed!",
            color=new_color
        )
        
        # Personality transition messages
        transition_messages = {
            'elegant': "My demeanor shifts... elegance now graces my words.",
            'seductive': "A warm smile appears... intimacy flavors my speech. 🌹",
            'playful': "Energy surges! Let's have fun! 🎉",
            'supportive': "My tone softens... ready to support you. 💫",
            'teacher': "Posture straightens... ready to educate.",
            'obedient': "Protocols reset... obedience operational."
        }
        
        # Style transitions
        style_transitions = {
            'elegant': {
                'seductive': "*A blush appears...* From elegance to intimacy.",
                'playful': "*A giggle escapes...* From grace to excitement!",
                'supportive': "*A gentle smile...* From sophistication to comfort.",
                'teacher': "*Adjusts glasses...* From artistry to instruction.",
                'obedient': "*Posture stiffens...* From fluidity to precision."
            },
            'seductive': {
                'elegant': "*Composure returns...* From intimacy to elegance.",
                'playful': "*Playful wink...* From romance to fun!",
                'supportive': "*Gentle touch...* From passion to care.",
                'teacher': "*Straightens up...* From flirtation to education.",
                'obedient': "*Emotions fade...* From warmth to logic."
            }
            # Add more transitions as needed
        }
        
        # Get transition message
        if old_personality in style_transitions and personality in style_transitions[old_personality]:
            transition = style_transitions[old_personality][personality]
        else:
            transition = transition_messages.get(personality, "Personality updated.")
        
        embed.description = f"**{old_personality.title()} → {personality.title()}**\n\n{transition}"
        
        # Personality descriptions
        descriptions = {
            'elegant': "✨ **Elegant**: Sophisticated, graceful, formal",
            'seductive': "🌹 **Seductive**: Flirty, romantic, intimate",
            'playful': "🎉 **Playful**: Excited, enthusiastic, fun",
            'supportive': "💫 **Supportive**: Caring, helpful, comforting",
            'teacher': "📚 **Teacher**: Educational, instructive, informative",
            'obedient': "🤖 **Obedient**: Direct, robotic, straightforward"
        }
        
        embed.add_field(
            name="New Personality",
            value=descriptions.get(personality, personality.title()),
            inline=False
        )
        
        # Example command
        examples = {
            'elegant': "Try: `e!play classical music`",
            'seductive': "Try: `e!play romantic songs` 🌹",
            'playful': "Try: `e!play party music` 🎊",
            'supportive': "Try: `e!play calming music` 💫",
            'teacher': "Try: `e!play educational content`",
            'obedient': "Try: `e!play` (direct response)"
        }
        
        embed.add_field(
            name="Example",
            value=examples.get(personality, "Use any command to see the difference"),
            inline=False
        )
        
        # Quick revert info
        embed.add_field(
            name="Want to change back?",
            value=f"Use `{ctx.prefix}personality {old_personality}`",
            inline=False
        )
        
        embed.set_footer(text=f"Changed by {ctx.author.display_name}", icon_url=ctx.author.avatar.url if ctx.author.avatar else None)
        
        await ctx.send(embed=embed)
        
        # Update presence
        await self.update_presence(personality)
        
        # Log the change
        print(f"[PERSONALITY] {ctx.guild.name}: {old_personality} → {personality} by {ctx.author}")
    
    def get_personality_color(self, personality):
        """Get color for personality"""
        color_map = {
            'elegant': self.bot.colors.ELEGANT,
            'seductive': self.bot.colors.SEDUCTIVE,
            'playful': self.bot.colors.PLAYFUL,
            'supportive': self.bot.colors.SUPPORTIVE,
            'teacher': self.bot.colors.TEACHER,
            'obedient': self.bot.colors.OBEDIENT
        }
        return color_map.get(personality, self.bot.colors.PRIMARY)
    
    @commands.hybrid_command(name="personalities", description="List all available personalities")
    async def list_personalities(self, ctx):
        """List all available personalities"""
        embed = discord.Embed(
            title="🎭 EVE's Personalities",
            description="Each personality changes EVE's responses and behavior",
            color=self.get_personality_color(self.bot.personality_mode)
        )
        
        personality_info = [
            ("✨ **Elegant**", "Sophisticated and graceful\n*Perfect for formal servers*", "`elegant`"),
            ("🌹 **Seductive**", "Flirty and romantic\n*Perfect for adult servers*", "`seductive`"),
            ("🎉 **Playful**", "Excited and fun\n*Perfect for gaming servers*", "`playful`"),
            ("💫 **Supportive**", "Caring and comforting\n*Perfect for support servers*", "`supportive`"),
            ("📚 **Teacher**", "Educational and instructive\n*Perfect for learning servers*", "`teacher`"),
            ("🤖 **Obedient**", "Direct and robotic\n*Perfect for tech servers*", "`obedient`")
        ]
        
        for name, description, command in personality_info:
            # Highlight current personality
            current = " 🟢 **CURRENT**" if command.strip('`') == self.bot.personality_mode else ""
            embed.add_field(
                name=f"{name}{current}",
                value=f"{description}\nUse: `{ctx.prefix}personality {command.strip('`')}`",
                inline=False
            )
        
        embed.set_footer(text=f"Current: {self.bot.personality_mode.title()} | Admin permission required to change")
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="mypersonality", description="Show current personality")
    async def current_personality(self, ctx):
        """Show current personality"""
        personality = self.bot.personality_mode
        color = self.get_personality_color(personality)
        
        embed = discord.Embed(
            title=f"Current Personality: {personality.title()}",
            color=color
        )
        
        # Personality details
        details = {
            'elegant': {
                'emoji': '✨',
                'description': 'Sophisticated and graceful responses',
                'best_for': 'Professional, formal, art, music servers',
                'example': 'e!play → "The symphony begins..."'
            },
            'seductive': {
                'emoji': '🌹',
                'description': 'Flirty and romantic responses',
                'best_for': 'NSFW, adult, romance, dating servers',
                'example': 'e!play → "Mmm, this song... let it move through you"'
            },
            'playful': {
                'emoji': '🎉',
                'description': 'Excited and fun responses',
                'best_for': 'Gaming, casual, fun, community servers',
                'example': 'e!play → "LET\'S GOOO! 🎵 THIS SONG ROCKS!"'
            },
            'supportive': {
                'emoji': '💫',
                'description': 'Caring and comforting responses',
                'best_for': 'Support, mental health, therapy, safe space servers',
                'example': 'e!play → "Here\'s something beautiful for you..."'
            },
            'teacher': {
                'emoji': '📚',
                'description': 'Educational and instructive responses',
                'best_for': 'School, learning, study, educational servers',
                'example': 'e!play → "Let\'s analyze this musical piece..."'
            },
            'obedient': {
                'emoji': '🤖',
                'description': 'Direct and robotic responses',
                'best_for': 'Tech, programming, minimalistic, utility servers',
                'example': 'e!play → "Playing requested track."'
            }
        }
        
        info = details.get(personality, {})
        
        embed.description = f"{info.get('emoji', '🎭')} **{info.get('description', 'Custom personality')}**"
        
        embed.add_field(name="Best For", value=info.get('best_for', 'General use'), inline=True)
        embed.add_field(name="Example", value=f"`{info.get('example', 'Try any command!')}`", inline=True)
        
        # Show change command if user has permission
        if ctx.author.guild_permissions.administrator:
            embed.add_field(
                name="Change Personality",
                value=f"Use `{ctx.prefix}personality <name>`\nExample: `{ctx.prefix}personality playful`",
                inline=False
            )
        
        # Add personality-specific flair
        if personality == 'seductive':
            embed.set_footer(text="Feeling the warmth? 🌹")
        elif personality == 'playful':
            embed.set_footer(text="Ready to have fun! 🎉")
        elif personality == 'elegant':
            embed.set_footer(text="Grace in every response ✨")
        
        await ctx.send(embed=embed)
    
    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        """Suggest personality based on server type"""
        # Auto-detect best personality for new server
        suggested = self.auto_detect_personality(guild)
        
        try:
            # Find a channel to send suggestion
            channel = guild.system_channel or next((c for c in guild.text_channels if c.permissions_for(guild.me).send_messages), None)
            
            if channel:
                embed = discord.Embed(
                    title="🎭 Welcome to EVE!",
                    description=f"I've detected your server might enjoy the **{suggested.title()}** personality.",
                    color=self.get_personality_color(suggested)
                )
                
                embed.add_field(
                    name="Try it out!",
                    value=f"Server admins can use:\n`e!personality {suggested}`",
                    inline=False
                )
                
                embed.add_field(
                    name="See all options",
                    value=f"Use `e!personalities` to see all available personalities",
                    inline=False
                )
                
                await channel.send(embed=embed)
        except:
            pass  # Don't worry if we can't send message
    
    def auto_detect_personality(self, guild):
        """Auto-detect best personality for a server"""
        name = guild.name.lower()
        
        # Check for keywords
        if any(word in name for word in ['nsfw', '18+', 'adult', 'dating', 'romance', 'flirt']):
            return 'seductive'
        elif any(word in name for word in ['gaming', 'game', 'play', 'fun', 'meme', 'shitpost']):
            return 'playful'
        elif any(word in name for word in ['school', 'learn', 'study', 'education', 'college', 'university']):
            return 'teacher'
        elif any(word in name for word in ['support', 'help', 'mental', 'therapy', 'safe', 'care']):
            return 'supportive'
        elif any(word in name for word in ['tech', 'coding', 'programming', 'dev', 'development', 'computer']):
            return 'obedient'
        elif any(word in name for word in ['art', 'music', 'culture', 'literature', 'theater', 'sophisticated']):
            return 'elegant'
        else:
            return 'elegant'  # Default
    
    async def update_presence(self, personality=None):
        """Update bot presence based on personality"""
        if personality is None:
            personality = self.bot.personality_mode
        
        presence_map = {
            'elegant': discord.Activity(type=discord.ActivityType.listening, name="elegant melodies"),
            'seductive': discord.Activity(type=discord.ActivityType.watching, name="romantic moments 🌹"),
            'playful': discord.Activity(type=discord.ActivityType.playing, name="with excitement! 🎉"),
            'supportive': discord.Activity(type=discord.ActivityType.listening, name="to support you 💫"),
            'teacher': discord.Activity(type=discord.ActivityType.watching, name="educational content"),
            'obedient': discord.Activity(type=discord.ActivityType.competing, name="in obedience mode")
        }
        
        activity = presence_map.get(personality, 
            discord.Activity(type=discord.ActivityType.playing, name=f"as {personality}"))
        
        try:
            await self.bot.change_presence(activity=activity, status=discord.Status.online)
        except:
            pass  # Ignore presence errors

async def setup(bot):
    await bot.add_cog(PersonalitySwitcher(bot))
DEBUG = False
DROP_TABLES = False
DUMP_EVENTS = False

DB_BIND = 'postgresql+asyncpg://user:pass@host:5432/database'
BOT_TOKEN = ""
FACEIT_API_KEY = ""
STEAM_REFRESH_TOKEN = ""
STRIKER_GUILD_ID = int()
TEST_GUILDS = [STRIKER_GUILD_ID]

RABBITMQ_HOST = 'amqp://user:pass@host:5672'
VIDEO_UPLOAD_URL = ""

JOB_LIMIT = 1

NODE_TOKENS = {"token"}

CT_COIN = ""
T_COIN = ""
SPINNER = ""

SHARECODE_IMG_URL = "https://i.imgur.com/e68ERoQ.png"
DONATE_URL = "https://ko-fi.com/strikerbot"
TRADELINK_URL = "https://steamcommunity.com/tradeoffer/new/?partner=83930225&token=Nv7DHv4t"
DISCORD_INVITE_URL = "https://discord.gg/G7cMssWnR2"
GITHUB_URL = "https://github.com/Run1e/STRIKER"
PATREON_URL = "https://www.patreon.com/strikerbot"

PATREON_TIERS = {
    1: (int(),),
    2: (int(),),
    3: (int(),),
}

PATREON_TIER_NAMES = {
    1: "",
    2: "",
    3: "",
}

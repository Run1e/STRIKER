DEBUG = True
DROP_TABLES = False

DB_BIND = "postgresql+asyncpg://striker_user:striker_pass@localhost:5432/striker_db"
RABBITMQ_HOST = 'amqp://striker_user:striker_pass@localhost:5672'
VIDEO_UPLOAD_URL = "http://localhost:9090/uploader"
SENTRY_DSN = ""

BOT_TOKEN = ""
STEAM_REFRESH_TOKEN = ""
FACEIT_API_KEY = ""
STRIKER_GUILD_ID = int()

JOB_LIMIT = 3
TEST_GUILDS = [STRIKER_GUILD_ID]

TOKENS = {
    "not_a_secure_token",
}

CT_COIN = "<:ct_coin:817814167368368158>"
T_COIN = "<:t_coin:817814139116060724>"
SPINNER = "<a:spinner:817813763834249227>"
PATREON_EMOJI = "<:patreon:1107204176875880549>"
KOFI_EMOJI = "☕"
STEAM_EMOJI = "<:steam:1110569575055110275>"

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
    1: "Tier 1",
    2: "Tier 2",
    3: "Tier 3",
}


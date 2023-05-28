from pathlib import Path

DEBUG = True

API_ENDPOINT = "ws://localhost:9191/gateway"
API_TOKEN = "not_a_secure_token"

SENTRY_DSN = ""

KEEP_DEMO_COUNT = 10

DEMO_DIR = Path("")  # path where demos are stored (up to KEEP_DEMO_COUNT demos)
TEMP_DIR = Path("")  # temp dir, emptied on recorder startup
MMCFG_DIR = Path("")  # path for HLAE mmcfg directory (set to an empty dir)
HLAE_DIR = Path("")  # path to your HLAE install
STEAM_DIR = Path("C:/Program Files (x86)/Steam")

STEAM_BIN = STEAM_DIR / "steam.exe"
CSGO_DIR = STEAM_DIR / "steamapps/common/Counter-Strike Global Offensive/csgo"
CSGO_BIN = str(CSGO_DIR) + ".exe"

HLAE_BIN = HLAE_DIR / "HLAE.exe"
FFMPEG_BIN = HLAE_DIR / "ffmpeg/bin/ffmpeg.exe"

VIDEO_FILTERS = 'vibrance=intensity=0.5,colorbalance=rm=0.04,curves=all=0/0 0.5/0.65 1/1'

# ignore everything under here unless you know what you're doing
SANDBOXED = False
BOXES = ["csgo1", "csgo2"]
SANDBOXIE_USER = ""
SANDBOXIE_DIR = Path("C:/Program Files/Sandboxie-Plus")
SANDBOXIE_START = SANDBOXIE_DIR / "Start.exe"
SANDBOXIE_INI = SANDBOXIE_DIR / "Sandboxie.ini"

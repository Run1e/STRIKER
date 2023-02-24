from csgo.client import CSGOClient
from steam.client import SteamClient

import config

client = SteamClient()
cs = CSGOClient(client)

client.set_credential_location("cred")

print("Logging in...")
client.cli_login(username=config.STEAM_USER, password=config.STEAM_PASS)

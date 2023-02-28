from csgo.client import CSGOClient
from steam.client import SteamClient

client = SteamClient()
cs = CSGOClient(client)

client.set_credential_location("sentries")

username = input("Username: ")
password = input("Password: ")
client.cli_login(username=username, password=password)

import steam

client = steam.Client()


@client.event
async def on_login():
    print(f"Logged in as {client.user!r}")
    print("Refresh token:", client.refresh_token)
    await client.close()


user = input("user: ")
pw = input("password: ")
client.run(user, pw)

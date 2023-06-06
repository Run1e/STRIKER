# Contributing to STRIKER

Thanks in your interest in contributing to (or self-hosting) STRIKER!
I've tried to make the instructions as simple as possible.

The example configs assume you're running the compose file I've provided.
For other setups, the configs will obviously need to be changed accordingly.
These instructions are meant for development environments only, and is not production ready!

Please raise a GitHub issue if anything needs clarification or updating.

## Requirements and services

First, the basic requirements:
- [Python 3.11.3](https://www.python.org/)
- [Docker](https://docs.docker.com/get-docker/) (and `docker compose`, which comes with Docker Desktop)
- A secondary Steam account (for Game Coordinator through steam.py)

STRIKER requires a PostgreSQL server, RabbitMQ instance, and S3 storage -
all of which can be quickly spun up using the compose file:
```bash
docker compose -f dev-compose.yaml up -d
```

That gives you access to the following web interfaces:

| Service        | Endpoint                | Username     | Password     |
|----------------|-------------------------|--------------|--------------|
| RabbitMQ WebUI | http://localhost:15672/ | striker_user | striker_pass |
| MinIO WebUI    | http://localhost:9001/  | striker_user | striker_pass |

And you can access `psql` as follows:
```bash
docker exec -it postgres psql -U striker_user -d striker_db
```

### Extra steps for Windows

If you're on Windows,
you will also need Windows binaries of [gzip](https://gnuwin32.sourceforge.net/packages/gzip.htm)
and [bzip2](https://gnuwin32.sourceforge.net/packages/bzip2.htm) in your PATH.
Note: bzip2.exe requires bzip2.dll to be in the same directory to work.

If you're on an older version of Windows 10 than update 1803,
you will also need a [curl binary](https://curl.se/windows/).

## Building the configuration files

Copy over the example configs as a starting point:

```bash
cp bot/example_config.py bot/config.py
cp microservices/demoparse/example_config.py microservices/demoparse/config.py
cp microservices/gateway/example_config.py microservices/gateway/config.py
cp microservices/uploader/example_config.py microservices/uploader/config.py
cp microservices/recorder/example_config.py microservices/recorder/config.py
```

### Bot config

Located at `bot/config.py`

- `BOT_TOKEN`: [Create a Discord application+bot](https://discord.com/developers/applications) and get the bot token
- `STRIKER_GUILD_ID`: Create a Discord server for testing (or use an existing one) and use the servers Discord id
- `STEAM_REFRESH_TOKEN`: Run `bot/steam_login.py` and log into a Steam account. It will print a refresh key at the end that you can use
- `FACEIT_API_KEY`: (Optional) Create a [FACEIT App](https://developers.faceit.com/)

### Demoparse config

Located at `microservices/demoparse/config.py`

Go to your [MinIO WebUI](http://localhost:9001/), click on `Access Keys` -> `Create access key` -> `Create`
- `KEY_ID`: Use the value of `Access Key`
- `APPLICATION_KEY`: Use the value of `Secret Key`

### Uploader config

Located at `microservices/uploader/config.py`

- `BOT_TOKEN`: Same as the one in `bot/config.py`

### Recorder config

Located at `recorders/csgo/config.py`

Download the latest release of [HLAE](https://github.com/advancedfx/advancedfx/releases),
and extract it somewhere.

Also download [ffmpeg-git-full.7z](https://www.gyan.dev/ffmpeg/builds/),
and extract it into `{your hlae folder}/ffmpeg`,
such that you have `{your hlae folder}/ffmpeg/bin/ffmpeg.exe`.

- `HLAE_DIR`: The folder of your HLAE install/extract

## Virtual environment and requirements

Make a virtual environment (with your 3.11.3 install!):
```bash
python -m venv venv
```

Then activate the venv and install the requirements for development and all services:
```bash
pip install -r dev_requirements.txt
pip install -r bot/requirements.txt
pip install -r microservices/demoparse/requirements.txt
pip install -r microservices/gateway/requirements.txt
pip install -r microservices/uploader/requirements.txt
pip install -r recorders/csgo/requirements.txt
```

## Final steps

Invite your new bot to a server with an invite link.
You can quickly craft an invite link by inserting your bot client id into this url:
```
https://discord.com/api/oauth2/authorize?client_id={YOUR_CLIENT_ID_HERE}&permissions=274878286912&scope=bot%20applications.commands
```

Now you should be good to go! Obviously you should have Steam running and CS:GO installed as well.

If you use VS Code, here is my `launch.json` configurations.
If you're using something else, make sure to pay attention to the working directories used.

```json
       {
            "name": "bootstrap",
            "type": "python",
            "request": "launch",
            "program": "bootstrap.py",
            "cwd": "${workspaceFolder}",
            "console": "integratedTerminal",
            "justMyCode": true
        },
        {
            "name": "demoparse",
            "type": "python",
            "request": "launch",
            "program": "demoparse.py",
            "cwd": "${workspaceFolder}/microservices/demoparse",
            "console": "integratedTerminal",
            "justMyCode": true
        },
        {
            "name": "gateway",
            "type": "python",
            "request": "launch",
            "program": "gateway.py",
            "cwd": "${workspaceFolder}/microservices/gateway",
            "console": "integratedTerminal",
            "justMyCode": true
        },
        {
            "name": "recorder",
            "type": "python",
            "request": "launch",
            "program": "recorder.py",
            "cwd": "${workspaceFolder}/recorders/csgo",
            "console": "integratedTerminal",
            "justMyCode": true
        },
        {
            "name": "uploader",
            "type": "python",
            "request": "launch",
            "program": "uploader.py",
            "cwd": "${workspaceFolder}/microservices/uploader",
            "console": "integratedTerminal",
            "justMyCode": true
        },
```
import aiohttp


class HTTPException(Exception):
    pass

class Unauthorized(Exception):
    pass

class Forbidden(Exception):
    pass


class NotFound(Exception):
    pass


class FACEITApi:
    def __init__(self, api_key, timeout=6.0) -> None:
        self.api_key = api_key
        self._client = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {api_key}"},
        )
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def request(self, method, path):
        async with self._client.request(method, f"https://open.faceit.com/data/v4/{path}") as resp:
            status = resp.status
            if status == 200:
                return await resp.json()
            else:
                content_type = resp.content_type
                if content_type == "text/plain":
                    error_text = await resp.text()
                elif content_type == "application/json":
                    data = await resp.json()
                    error_text = data["errors"][0]["message"]

                if status == 401:
                    raise Unauthorized(error_text)
                elif status == 403:
                    raise Forbidden(error_text)
                elif status == 404:
                    raise NotFound(error_text)
                else:
                    raise HTTPException(error_text)

    async def match(self, match_id):
        return await self.request("GET", f"matches/{match_id}")

import asyncio

from micro.matchinfo.matchinfo import main

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()

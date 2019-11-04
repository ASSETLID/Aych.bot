#!/usr/bin/env python

import asyncio
import websockets
from websockets.exceptions import ConnectionClosed
from hummingbot.core.utils.async_utils import (safe_ensure_future)
MESSAGE_TIMEOUT = 30
PING_TIMEOUT = 10
WS_URL = "ws://localhost:8787"


async def ws_msgs_stream(ws: websockets.WebSocketClientProtocol):
    try:
        while True:
            print('recieving...')
            try:
                msg: str = await asyncio.wait_for(ws.recv(), timeout=MESSAGE_TIMEOUT)
                print(f"Recieving -> {msg}")
                yield msg
            except asyncio.TimeoutError:
                print("WebSocket ping timed out. Going to reconnect...")
                raise
            except ConnectionClosed:
                return
    except asyncio.TimeoutError:
        print("WebSocket ping timed out. Going to reconnect...")
        return
    except ConnectionClosed:
        return
    finally:
        await ws.close()


async def listener(stream: asyncio.Queue):
    while True:
        print('listening...')
        try:
            async with websockets.connect(WS_URL) as ws:
                ws: websockets.WebSocketClientProtocol = ws
                async for msg in ws_msgs_stream(ws):
                    print(f"Appending -> {msg}")
                    stream.put_nowait(msg)
        except asyncio.CancelledError:
            print('cancelled exception raised')
            raise
        except Exception:
            print('exception raised')
            raise


async def consumer(stream: asyncio.Queue):
    while True:
        try:
            msg = await stream.get()
            print(f"Consuming -> {msg}")

        except asyncio.CancelledError:
            raise
        except Exception:
            print(
                f"Unexpected error routing order book messages.",
                exc_info=True,
                app_warning_msg=f"Unexpected error routing order book messages. Retrying after 5 seconds."
            )
            await asyncio.sleep(5.0)


async def start_client():
    stream = asyncio.Queue()
    f1 = safe_ensure_future(listener(stream))
    f2 = safe_ensure_future(consumer(stream))
    await asyncio.gather(f1, f2)

asyncio.get_event_loop().run_until_complete(start_client())

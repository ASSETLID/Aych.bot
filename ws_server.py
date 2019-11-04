#!/usr/bin/env python

import asyncio
import websockets
import random

MSG_TYPE = ['confirmation', 'matching', 'new_eon']


async def producer(websocket, path):
    while True:
        print('starting server on 8787')
        rand = random.randint(0, 2)
        msg = MSG_TYPE[rand]
        print(f"Producing -> {msg}")
        await websocket.send(msg)
        await asyncio.sleep(2)

start_server = websockets.serve(producer, "localhost", 8787)
asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()

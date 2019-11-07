#!/usr/bin/env python

import asyncio
import websockets
from websockets.exceptions import ConnectionClosed
from typing import (Dict)
import ujson
from enum import Enum
MESSAGE_TIMEOUT = 30
PING_TIMEOUT = 10
WS_URL = "ws://localhost:8787"


class WSNotificationType(Enum):
    INCOMING_TRANSFER = 'INCOMING_TRANSFER'
    INCOMING_RECEIPT = 'INCOMING_RECEIPT'
    INCOMING_CONFIRMATION = 'INCOMING_CONFIRMATION'
    TIMEOUT_TRANSFER = 'TIMEOUT_TRANSFER'
    MATCHED_SWAP = 'MATCHED_SWAP'
    FINALIZED_SWAP = 'FINALIZED_SWAP'
    CANCELLED_SWAP = 'CANCELLED_SWAP'
    REGISTERED_WALLET = 'REGISTERED_WALLET'
    CONFIRMED_DEPOSIT = 'CONFIRMED_DEPOSIT'
    REQUESTED_WITHDRAWAL = 'REQUESTED_WITHDRAWAL'
    CONFIRMED_WITHDRAWAL = 'CONFIRMED_WITHDRAWAL'
    CHECKPOINT_CREATED = 'CHECKPOINT_CREATED'


class WSOperationType(Enum):
    SUBSCRIBE = 'subscribe'
    UNSUBSCRIBE = 'unsubscribe'
    PING = 'ping'
    ACK = 'ack'


class LQDWalletSync():
    __instance = None
    _ws: websockets.WebSocketClientProtocol = None

    def __init__(self):
        if LQDWalletSync.__instance is not None:
            raise Exception("New instance of a singleton class can not be created")
        else:
            self._state_streams: Dict[str, asyncio.Queue] = {}
            LQDWalletSync.__instance = self

    @staticmethod
    def getInstance():
        if LQDWalletSync.__instance is None:
            LQDWalletSync()
        return LQDWalletSync.__instance

    @property
    def ws(self) -> websockets.WebSocketClientProtocol:
        return self._ws

    async def start(self, stream: asyncio.Queue):
        if self._ws is not None:
            print('Websocket connection is already open')
            return
        while True:
            print('start listening...')
            try:
                async with websockets.connect(WS_URL) as ws:
                    self._ws = ws
                    async for msg in self.state_notifications_stream():
                        self.ws_stream_router(msg)
            except asyncio.CancelledError:
                print('Cancelled exception raised')
                raise
            except Exception:
                await asyncio.sleep(20.0)

    async def state_notifications_stream(self):
        try:
            while True:
                print('recieving...')
                try:
                    msg: str = await asyncio.wait_for(self.ws.recv(), timeout=MESSAGE_TIMEOUT)
                    decoded_msg = ujson.loads(msg)
                    # Reply with ACK message to the server
                    await self.send_ws_message('ack', {'uuid': decoded_msg['uuid']})
                    print(f"Recieving -> {msg}")
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await self.ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            print("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await self.ws.close()

    def ws_stream_router(self, msg):
        decoded_msg = ujson.loads(msg)
        msg_data = decoded_msg['data']
        msg_object_data = msg_data['data']
        stream_type = msg_data['type']
        event_type = msg_object_data['type']
        wallet_address = stream_type[6:]
        transfer_model_notifications = [WSNotificationType.INCOMING_TRANSFER,
                                        WSNotificationType.INCOMING_RECEIPT,
                                        WSNotificationType.INCOMING_CONFIRMATION,
                                        WSNotificationType.INCOMING_TIMEOUT,
                                        WSNotificationType.MATCHED_SWAP,
                                        WSNotificationType.FINALIZED_SWAP,
                                        WSNotificationType.CANCELLED_SWAP]
        others_model_notifications = [WSNotificationType.REGISTERED_WALLET,
                                      WSNotificationType.CONFIRMED_DEPOSIT,
                                      WSNotificationType.REQUESTED_WITHDRAWAL,
                                      WSNotificationType.CONFIRMED_WITHDRAWAL,
                                      WSNotificationType.CHECKPOINT_CREATED]

        if event_type in transfer_model_notifications:
            sender_token = msg_object_data['wallet']['token']
            recipient_token = msg_object_data['recipient']['token']
            if sender_token != recipient_token:
                self._state_streams[f"{recipient_token}/{wallet_address}"].put_nowait(msg_data)
            self._state_streams[f"{sender_token}/{wallet_address}"].put_nowait(msg_data)

        elif event_type in others_model_notifications:
            token = msg_object_data['token']
            self._state_streams[f"{token}/{wallet_address}"].put_nowait(msg_data)

    async def subscribe_wallet(self, wallet_address: str, token_address: str) -> asyncio.Queue:
        # TODO: Send subscription ws message
        wallet_stream_queue = asyncio.Queue()
        self._state_streams[f"{token_address}/{wallet_address}"] = wallet_stream_queue
        return wallet_stream_queue

    async def send_ws_message(self, op, args):
        data = {'op': op, 'args': args}
        await self.ws.send(ujson.dumps(data))

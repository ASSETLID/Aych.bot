import aiohttp
import ujson
from typing import (
    Dict,
)
from .tex_utils import (remove_0x_prefix)
OPERATOR_URL = 'https://rinkeby.liquidity.network'


async def get_current_eon():
    async with aiohttp.ClientSession() as client:
        async with client.get(f"{OPERATOR_URL}/audit/") as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error while fetching the latest eon number, status: {response.status}")
            data: Dict[str, any] = await response.json()
            return data["latest"]["eon_number"]


async def get_wallet_data(wallet_address: str, token_address: str, eon_number: int):
    async with aiohttp.ClientSession() as client:
        async with client.get(f"{OPERATOR_URL}/audit/{remove_0x_prefix(token_address)}/{remove_0x_prefix(wallet_address)}?eon_number={eon_number}") as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error while fetching wallet data, status: {response.status}")
            data: Dict[str, any] = await response.json()
            return data


async def get_registration_data(wallet_address: str, token_address: str):
    async with aiohttp.ClientSession() as client:
        async with client.get(f"{OPERATOR_URL}/audit/{remove_0x_prefix(token_address)}/{remove_0x_prefix(wallet_address)}/whois") as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error while fetching wallet data, status: {response.status}")
            data: Dict[str, any] = await response.json()
            return data


async def post_transfer(wallet_address: str,
                        token_address: str,
                        active_state_signature: str,
                        balance_marker_signature: str,
                        wallet_balance: str,
                        transfer):
    data = {
        "wallet": {
            "address": remove_0x_prefix(wallet_address),
            "token": remove_0x_prefix(token_address),
        },
        "amount": transfer["amount"],
        "nonce": transfer["nonce"],
        "eon_number": transfer["eon_number"],
        "recipient": remove_0x_prefix(transfer["recipient"]["address"]),
        "wallet_balance": wallet_balance,
        "wallet_signature": {"value": remove_0x_prefix(active_state_signature)},
        "wallet_balance_signature": {"value": remove_0x_prefix(balance_marker_signature)},
        "passive": True
    }
    async with aiohttp.ClientSession() as client:
        headers = {'content-type': 'application/json'}
        async with client.post(f"{OPERATOR_URL}/transfer/", data = ujson.dumps(data), headers = headers) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 201:
                raise IOError(f"Error sending transfer: {response.status}")
            data: Dict[str, any] = await response.json()
            return data

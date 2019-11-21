import aiohttp
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

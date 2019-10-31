import aiohttp
from typing import (
    Dict,
)

OPERATOR_URL = 'https://rinkeby.liquidity.network'


async def get_current_eon():
    async with aiohttp.ClientSession() as client:
        async with client.get(f"{OPERATOR_URL}/audit/") as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error while fetching the latest eon number" f"HTTP status is {response.status}.")
            data: Dict[str, any] = await response.json()
            return data.latest.eon_number


async def post_admission_request(token_address: str, wallet_address: str, signature: str):
    async with aiohttp.ClientSession() as client:
        async with client.post(f"{OPERATOR_URL}/admission/") as response:
            response: aiohttp.ClientResponse = response
            if response.status != 201:
                raise IOError(f"Error while sending admission request" f"HTTP status is {response.status}.")
            data: Dict[str, any] = await response.json()
            return data

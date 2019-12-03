import aiohttp
import ujson
from typing import (
    Dict,
)
from .tex_utils import (remove_0x_prefix)
from hummingbot.market.tex.lqd_wallet import LQDWallet
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


async def post_swap(credit_wallet: LQDWallet,
                    debit_wallet: LQDWallet,
                    credit_amount: str,
                    debit_amount: str,
                    credit_signatures: [str],
                    debit_signatures: [str],
                    credit_balance_signatures: [str],
                    debit_balance_signatures: [str],
                    fulfillment_signatures: [str],
                    eon_number: int,
                    nonce: str,
                    logger):
    data = {
        "wallet": {
            "address": remove_0x_prefix(debit_wallet.wallet_address),
            "token": remove_0x_prefix(debit_wallet.token_address),
        },
        "recipient": {
            "address": remove_0x_prefix(credit_wallet.wallet_address),
            "token": remove_0x_prefix(credit_wallet.token_address),
        },
        "amount": debit_amount,
        "amount_swapped": credit_amount,
        "nonce": nonce,
        "eon_number": eon_number,
        "credit_signature": map(lambda signature: {"value": signature}, credit_signatures),
        "debit_signature": map(lambda signature: {"value": signature}, debit_signatures),
        "credit_balance_signature": map(lambda signature: {"value": signature}, credit_balance_signatures),
        "debit_balance_signature": map(lambda signature: {"value": signature}, debit_balance_signatures),
        "fulfillment_signature": map(lambda signature: {"value": signature}, fulfillment_signatures),
    }
    async with aiohttp.ClientSession() as client:
        headers = {'content-type': 'application/json'}
        async with client.post(f"{OPERATOR_URL}/swap/", data = ujson.dumps(data), headers = headers) as response:
            response: aiohttp.ClientResponse = response
            # if response.status != 201:
            #     raise IOError(f"Error sending swap: {response.status}")
            data: Dict[str, any] = await response.json()
            logger().info(f"RESPONSE ==> {data}")
            return data

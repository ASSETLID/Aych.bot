import threading
from web3.auto import w3
from web3 import Web3
from hummingbot.wallet.ethereum.web3_wallet import Web3Wallet

EMPTY_HASH = '0x0000000000000000000000000000000000000000000000000000000000000000'


def set_interval(func, sec):

    def func_wrapper():
        set_interval(func, sec)
        func()

    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t


def generate_state_checksum(contract_address: str, token_address: str, wallet_address: str, eon_number: str):
    contract_address_hash = Web3.soliditySha3(['address'], [contract_address])
    token_address_hash = Web3.soliditySha3(['address'], [token_address])
    wallet_address_hash = Web3.soliditySha3(['address'], [wallet_address])

    return Web3.soliditySha3(['bytes32', 'bytes32', 'bytes32', 'uint64',
                              'uint256', 'bytes32', 'uint256', 'uint256'],
                             [contract_address_hash, token_address_hash,
                              wallet_address_hash, 0, eon_number, EMPTY_HASH, 0, 0]).hex()


def sign_data(data, wallet: Web3Wallet):
    signature = w3.eth.account.sign_message(data, wallet.private_key)
    return f"{signature['r']}{signature['s']}{signature['v']}"


def is_same_hex(hex1: str, hex2: str) -> bool:
    return hex1.lower() == hex2.lower()


def next_power_of_2(x):
    return 1 if x == 0 else 2**(x - 1).bit_length()

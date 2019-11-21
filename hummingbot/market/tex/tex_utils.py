import threading
from eth_account import Account
from eth_account.messages import defunct_hash_message
from web3 import Web3
from eth_utils import keccak
from hummingbot.wallet.ethereum.web3_wallet import Web3Wallet

EMPTY_HASH = '0x0000000000000000000000000000000000000000000000000000000000000000'
SEED_MESSAGE = 'I want to use Liquidity Network Swaps'


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


def sign_data(data, wallet: Web3Wallet, logger):
    msg_hash = defunct_hash_message(lqd_hash_wrapper(data))
    signature = Account.signHash(msg_hash, wallet.private_key)
    return signature['signature']


def is_same_hex(hex1: str, hex2: str) -> bool:
    return hex1.lower() == hex2.lower()


def next_power_of_2(x):
    return 1 if x == 0 else 2**(x - 1).bit_length()


def lqd_hash_wrapper(msg):
    return Web3.soliditySha3(['string', 'string'], ['\x19Liquidity.Network Authorization:\n32', msg])


def generate_seed(wallet: Web3Wallet, logger):
    signature = sign_data(SEED_MESSAGE, wallet, logger)
    signature_str = signature.hex()[2:]
    seed = keccak(text=signature_str)
    return seed.hex()


def remove_0x_prefix(hex_str: str):
    return hex_str[2:] if hex_str.startswith('0x') else hex_str

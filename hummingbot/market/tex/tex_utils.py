import threading
from eth_account import Account
from eth_account.messages import defunct_hash_message
from web3 import Web3
from eth_utils import keccak

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


def sign_data(data, priv_key: str, data_type = 'bytes32'):
    msg_hash = defunct_hash_message(lqd_hash_wrapper(data, data_type))
    signature = Account.signHash(msg_hash, priv_key)
    return signature['signature']


def is_same_hex(hex1: str, hex2: str) -> bool:
    return hex1.lower() == hex2.lower()


def next_power_of_2(x):
    return 1 if x == 0 else 2**(x - 1).bit_length()


def lqd_hash_wrapper(data, data_type: str):
    return Web3.soliditySha3(['string', data_type], ['\x19Liquidity.Network Authorization:\n32', data])


def generate_seed(priv_key: str):
    signature = sign_data(data = SEED_MESSAGE, priv_key = priv_key, data_type = 'string')
    signature_str = signature.hex()[2:]
    seed = keccak(text=signature_str)
    return seed.hex()


def remove_0x_prefix(hex_str: str):
    return hex_str[2:] if hex_str.startswith('0x') else hex_str


def hash_balance_marker(contract_address: str, token_address: str, wallet_address: str, eon_number: int, balance: int):
    contract_address_hash = Web3.soliditySha3(['address'], [contract_address])
    token_address_hash = Web3.soliditySha3(['address'], [token_address])
    wallet_address_hash = Web3.soliditySha3(['address'], [wallet_address])
    return Web3.soliditySha3(['bytes32', 'bytes32', 'bytes32', 'uint256', 'uint256'],
                             [contract_address_hash, token_address_hash, wallet_address_hash,
                              eon_number, balance])


def swap_freeze_hash(debit_token: str, credit_token: str, nonce: int):
    debit_token_hash = Web3.soliditySha3(['address'], [debit_token])
    credit_token_hash = Web3.soliditySha3(['address'], [credit_token])

    return Web3.soliditySha3(['bytes32', 'bytes32', 'uint256'], [debit_token_hash, credit_token_hash, nonce])


def extract_exchange_id(exchange_order_id: str, id_type: str):
    if len(exchange_order_id.split('/')) < 2:
        raise Exception(f"Invalid exchange order id format: {exchange_order_id}")
    if id_type not in ['id', 'tx_id']:
        raise Exception(f"Invalid id type: {id_type}")

    return exchange_order_id.split('/')[0] if id_type == 'id' else exchange_order_id.split('/')[1]

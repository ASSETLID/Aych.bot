from typing import (Dict)
from enum import Enum
from hummingbot.market.tex.tex_utils import (
    is_same_hex
)
from web3 import Web3
from hummingbot.market.tex.lqd_eon import LQDEon
import functools

EMPTY_HASH = '0x0000000000000000000000000000000000000000000000000000000000000000'


class ActiveStateType(Enum):
    SENDER = 'sender_active_state'
    RECIPIENT = 'recipient_active_state'
    SENDER_CANCELLATION = 'sender_cancellation_active_state'
    RECIPIENT_CANCELLATION = 'recipient_cancellation_active_state'
    RECIPIENT_FINALIZATION = 'recipient_finalization_active_state'
    FULFILLMENT = 'recipient_fulfillment_active_state'


class LQDWallet():

    def __init__(self,
                 token_address: str,
                 wallet_address: str,
                 contract_address: str,
                 eon_number: int,
                 trail_identifier: int,
                 eons: Dict[int, LQDEon]):
        self._eons = eons
        self._latest_eon_number = eon_number
        self._token_address = token_address
        self._wallet_address = wallet_address
        self._contract_address = contract_address
        self._trail_identifier = trail_identifier

    @property
    def latest_eon_number(self) -> int:
        eon_numbers = self.eons.keys()
        if len(eon_numbers) == 0:
            return eon_numbers[len(eon_numbers) - 1]

    @property
    def token_address(self) -> str:
        return self._token_address

    @property
    def wallet_address(self) -> str:
        return self._wallet_address

    @property
    def contract_address(self) -> str:
        return self._contract_address

    @property
    def trail_identifier(self) -> int:
        return self._trail_identifier

    @property
    def eons(self) -> Dict[int, LQDEon]:
        return self._eons

    def starting_balance(self, eon_number: int) -> float:
        merkle_proof = self.eons[eon_number].merkle_proof
        if not merkle_proof:
            return 0
        return merkle_proof['right'] - merkle_proof['left']

    def spent_and_gained(self, eon_number: int) -> Dict[str, float]:
        spent = 0
        gained = 0
        transfers = self.eons[eon_number].transfers
        for transfer in transfers:
            is_sender = is_same_hex(transfer['wallet']['address'], self.wallet_address) and \
                is_same_hex(transfer['wallet']['token'], self.token_address)
            is_swap = transfer['amount_swapped']
            if is_sender:
                if is_swap:
                    # TODO: Should only add the current eon matched out
                    spent += transfer['matched_amounts']['matched_out']
                else:
                    spent += transfer['amount']
            else:
                if is_swap:
                    # TODO: Should only add the current eon matched in
                    gained += transfer['matched_amounts']['matched_in']
                else:
                    gained += transfer['amount']

        return {'spent': spent, 'gained': gained}

    def balance(self, eon_number: int) -> float:
        deposits = self.eons[eon_number].deposits
        withdrawals = self.eons[eon_number].withdrawals
        deposits_amount = functools.reduce(lambda total, deposit: total + deposit['amount'], deposits)
        withdrawals_amount = functools.reduce(lambda total, withdrawal: total + withdrawal['amount'], withdrawals)
        state_amount = self.spent_and_gained(eon_number)
        return self.starting_balance(eon_number) + state_amount['gained'] - \
            state_amount['spent'] + deposits_amount - withdrawals_amount

    def latest_wallet_state(self, state_type: str):
        transfers = self.eons[self.latest_eon_number].transfers
        latest_transfer = transfers(len(transfers) - 1)

        if state_type:
            return latest_transfer[state_type]

        is_sender = is_same_hex(latest_transfer['wallet']['address'], self.wallet_address) and \
            is_same_hex(latest_transfer['wallet']['token'], self.token_address)

        if is_sender:
            if latest_transfer['cancelled']:
                return latest_transfer[ActiveStateType.SENDER_CANCELLATION]
            else:
                return latest_transfer[ActiveStateType.SENDER]
        else:
            if latest_transfer['cancelled']:
                return latest_transfer[ActiveStateType.RECIPIENT_CANCELLATION]
            elif latest_transfer['complete']:
                return latest_transfer[ActiveStateType.RECIPIENT_FINALIZATION] or latest_transfer[ActiveStateType.RECIPIENT]
            else:
                latest_transfer[ActiveStateType.RECIPIENT]

    def normal_transfer_hash(self, transfer) -> str:
        nonce = transfer['nonce']
        if transfer['passive']:
            position = transfer['position'] or 2 ** 256 - 1
            nonce = int(Web3.soliditySha3(['uint256', 'uint256'], [nonce, position]).toHex(), 16)

        is_sender = is_same_hex(transfer['wallet']['address'], self.wallet_address) and \
            is_same_hex(transfer['wallet']['token'], self.token_address)

        target = self.wallet_address if is_sender else transfer['recipient']['wallet']
        target_hash = Web3.soliditySha3(['address'], [target])

        return Web3.soliditySha3(['bytes32', 'uint256', 'uint64', 'uint256'],
                                 [target_hash, transfer['amount'], transfer['recipient_trail_identifier'], nonce]).hex()

    def swap_transfer_hash(self, transfer) -> str:
        sender_token_hash = Web3.soliditySha3(['address'], [transfer['wallet']['token']])
        recipient_token_hash = Web3.soliditySha3(['address'], [transfer['recipient']['token']])

        return Web3.soliditySha3(['bytes32', 'bytes32', 'uint64', 'uint256', 'uint256', 'uint256', 'uint256'],
                                 [sender_token_hash, recipient_token_hash, transfer['recipient_trail_identifier'],
                                  transfer['amount'], transfer['amount_swapped'],
                                  self.starting_balance(transfer['eon_number']), transfer['nonce']])

    def transfer_hash(self, transfer) -> str:
        if transfer['is_padding']:
            return EMPTY_HASH

        is_swap = transfer['amount_swapped']
        if is_swap:
            return self.swap_transfer_hash(transfer)
        else:
            return self.normal_transfer_hash(transfer)

    def active_state_hash(self) -> str:
        contract_address_hash = Web3.soliditySha3(['address'], [self.contract_address])
        token_address_hash = Web3.soliditySha3(['address'], [self.token_address])
        wallet_address_hash = Web3.soliditySha3(['address'], [self.wallet_address])
        transaction_set_hash = self.construct_merkle_tree(self.eons[self.latest_eon_number].transfers)['hash']
        state_amount = self.spent_and_gained(self.latest_eon_number)
        return Web3.soliditySha3(['bytes32', 'bytes32', 'bytes32', 'uint64', 'uint256',
                                  'bytes32', 'uint256', 'uint256'],
                                 [contract_address_hash, token_address_hash, wallet_address_hash,
                                  self.trail_identifier, self.latest_eon_number, transaction_set_hash,
                                  state_amount['spent'], state_amount['gained']])

    def construct_merkle_tree(self, transfers) -> str:
        transfers_count = len(transfers)

        if transfers_count == 0:
            return {'height': 0, 'hash': EMPTY_HASH}
        elif transfers_count == 1:
            return {'height': 0, 'hash': self.transfer_hash(transfers[0])}

        mid = transfers_count // 2
        left = transfers[0:mid]
        right = transfers[mid:transfers_count]

        left_child = self.calculate_merkle_tree_hash(self, left)
        right_child = self.calculate_merkle_tree_hash(self, right)

        height = left_child['height'] + 1
        node_hash = Web3.soliditySha3(['uint32', 'bytes32', 'bytes32'],
                                      [left_child['height'], left_child['hash'], right_child('hash')])

        result = {'height': height, 'hash': node_hash, 'left_child': left_child, 'right_child': right_child}

        left_child['parent'] = result
        right_child['parent'] = result

        return result

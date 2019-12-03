from typing import (Dict)
import asyncio
from hummingbot.market.tex.tex_utils import (
    is_same_hex,
    next_power_of_2,
    remove_0x_prefix
)
from web3 import Web3
from hummingbot.market.tex.lqd_eon import LQDEon
import hummingbot.market.tex.constants.ws_notifications as WSNotificationType
import hummingbot.market.tex.constants.active_states as ActiveStateType
from hummingbot.logger import HummingbotLogger
import logging
import functools
from copy import deepcopy
EMPTY_HASH = '0x0000000000000000000000000000000000000000000000000000000000000000'
FULFILLED_SWAP_MARKER = 2 ** 256 - 1
lw_logger = None


class LQDWallet():

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global lw_logger
        if lw_logger is None:
            lw_logger = logging.getLogger(__name__)
        return lw_logger

    def __init__(self,
                 token_address: str,
                 wallet_address: str,
                 contract_address: str,
                 trail_identifier: int,
                 current_eon: LQDEon,
                 previous_eon: LQDEon,
                 ws_stream: asyncio.Queue):
        self._current_eon = current_eon
        self._previous_eon = previous_eon
        self._token_address = token_address
        self._wallet_address = wallet_address
        self._contract_address = contract_address
        self._trail_identifier = trail_identifier
        self._ws_stream = ws_stream
        self._ws_confirmations_stream = asyncio.Queue()

    @property
    def current_eon(self) -> LQDEon:
        return self._current_eon

    @property
    def previous_eon(self) -> LQDEon:
        return self._previous_eon

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
    def ws_confirmations_stream(self):
        return self._ws_confirmations_stream

    def starting_balance(self, eon: LQDEon = None) -> float:
        if eon is None:
            eon = self.current_eon
        merkle_proof = eon.merkle_proof
        if not merkle_proof:
            return 0
        return int(merkle_proof['right']) - int(merkle_proof['left'])

    def spent_and_gained(self, eon: LQDEon = None) -> Dict[str, float]:
        if eon is None:
            eon = self.current_eon
        self.logger().info(f"**TRANSFERS => {eon.transfers}")
        transfers = [transfer for transfer in eon.transfers if not transfer['passive'] or not is_same_hex(transfer['recipient']['address'], self.wallet_address)]
        if len(transfers) == 0:
            return {'spent': 0, 'gained': 0}

        last_transfer = transfers[len(transfers) - 1]
        is_outgoing = is_same_hex(last_transfer['wallet']['address'], self.wallet_address) and \
            is_same_hex(last_transfer['wallet']['token'], self.token_address)
        is_swap = last_transfer['amount_swapped']
        self.logger().info(f"SPENT_GAINED => {last_transfer}")
        if is_swap and last_transfer[ActiveStateType.RECIPIENT_FINALIZATION] is None and \
                not last_transfer['cancelled'] and not last_transfer['voided'] and last_transfer['appended']:
            current_matched_out, current_matched_in = self.get_current_eon_matched_amounts(last_transfer)
            if is_outgoing:
                spent = int(last_transfer[ActiveStateType.SENDER]['updated_spendings'])
                gained = int(last_transfer[ActiveStateType.SENDER]['updated_gains']) + int(last_transfer['amount']) - current_matched_out
                return {'spent': spent, 'gained': gained}
            else:
                spent = int(last_transfer[ActiveStateType.SENDER]['updated_spendings'])
                gained = int(last_transfer[ActiveStateType.SENDER]['updated_gains'] + current_matched_in)
        else:
            last_transfer_active_state = self.transfer_active_state(last_transfer)
            spent = int(last_transfer_active_state['updated_spendings'])
            gained = int(last_transfer_active_state['updated_gains'])

            if is_swap:
                current_matched_out, current_matched_in = self.get_current_eon_matched_amounts(last_transfer)

                if last_transfer['complete'] and is_outgoing:
                    total_matched_out = int(last_transfer['matched_amounts']['matched_out'])
                    gained += total_matched_out - current_matched_out

                if last_transfer['cancelled'] and last_transfer[ActiveStateType.RECIPIENT_CANCELLATION] is None and not is_outgoing:
                    gained += current_matched_in
            return {'spent': spent, 'gained': gained}

    def passively_gained(self, eon: LQDEon = None) -> int:
        if eon is None:
            eon = self.current_eon
        passive_incoming_transfers = [transfer for transfer in eon.transfers if transfer['passive'] and is_same_hex(transfer['recipient']['address'], self.wallet_address)]
        gained = 0
        for transfer in passive_incoming_transfers:
            gained += int(transfer['amount'])
        return gained

    def get_current_eon_matched_amounts(self, transfer):
        matched_out = int(transfer['matched_amounts']['matched_out'])
        matched_in = int(transfer['matched_amounts']['matched_in'])
        if self.previous_eon is None:
            return matched_out, matched_in
        tx_id = transfer['tx_id']
        # Look for a swap transfer with the same tx_id in the previous eon
        previous_eon_filtered = [swap for swap in self.previous_eon.transfers if swap['tx_id'] == tx_id]

        if len(previous_eon_filtered) != 0:
            previous_transfer = previous_eon_filtered[0]
            previous_matched_out = int(previous_transfer['matched_amounts']['matched_out'])
            previous_matched_in = int(previous_transfer['matched_amounts']['matched_in'])
            return matched_out - previous_matched_out, matched_in - previous_matched_in

        # Swap is in it's first eon
        return matched_out, matched_in

    def balance(self, eon: LQDEon = None) -> int:
        if eon is None:
            eon = self.current_eon
        deposits = eon.deposits
        withdrawals = eon.withdrawals
        deposits_amount = functools.reduce(lambda total, deposit: total + int(deposit['amount']), deposits, 0)
        withdrawals_amount = functools.reduce(lambda total, withdrawal: total + int(withdrawal['amount']), withdrawals, 0)
        state_amount = self.spent_and_gained(eon)
        return self.starting_balance(eon) + self.passively_gained(eon) + state_amount['gained'] - \
            state_amount['spent'] + deposits_amount - withdrawals_amount

    def transfer_active_state(self, transfer):
        is_outgoing = is_same_hex(transfer['wallet']['address'], self.wallet_address) and is_same_hex(transfer['wallet']['token'], self.token_address)

        is_swap = transfer['amount_swapped']
        if is_swap and transfer['processed']:
            if is_outgoing:
                return transfer[ActiveStateType.SENDER_CANCELLATION] if transfer['cancelled'] else transfer[ActiveStateType.SENDER]
            else:
                if transfer[ActiveStateType.RECIPIENT_FINALIZATION] is not None:
                    return transfer[ActiveStateType.RECIPIENT_FINALIZATION]
                elif transfer['cancelled']:
                    return transfer[ActiveStateType.RECIPIENT_CANCELLATION]
                elif transfer['complete']:
                    return transfer[ActiveStateType.FULFILLMENT]
                return transfer[ActiveStateType.RECIPIENT]

        else:
            if is_outgoing:
                return transfer[ActiveStateType.SENDER]
            else:
                return transfer[ActiveStateType.RECIPIENT]

    def normal_transfer_hash(self, transfer) -> str:
        nonce = int(transfer['nonce'])
        if transfer['passive']:
            position = int(transfer['position']) if transfer['position'] else 2 ** 256 - 1
            nonce = int(remove_0x_prefix(Web3.soliditySha3(['uint256', 'uint256'], [position, nonce]).hex()), 16)
        is_sender = is_same_hex(transfer['wallet']['address'], self.wallet_address) and \
            is_same_hex(transfer['wallet']['token'], self.token_address)
        target = self.wallet_address if not is_sender else transfer['recipient']['address']
        target_hash = Web3.soliditySha3(['address'], [target])
        return Web3.soliditySha3(['bytes32', 'uint256', 'uint64', 'uint256'],
                                 [target_hash, int(transfer['amount']), transfer['recipient_trail_identifier'], nonce]).hex()

    def swap_transfer_hash(self, transfer) -> str:
        sender_token_hash = Web3.soliditySha3(['address'], [transfer['wallet']['token']])
        recipient_token_hash = Web3.soliditySha3(['address'], [transfer['recipient']['token']])
        starting_balance = self.starting_balance() if 'starting_balance' not in transfer else transfer['starting_balance']
        is_recipient = transfer['recipient']['token'] == self.token_address
        if is_recipient and (transfer['complete'] or transfer['cancelled']):
            starting_balance = FULFILLED_SWAP_MARKER
        swap_hash = Web3.soliditySha3(['bytes32', 'bytes32', 'uint64', 'uint256', 'uint256', 'uint256', 'uint256'],
                                      [sender_token_hash, recipient_token_hash, transfer['recipient_trail_identifier'],
                                       int(transfer['amount']), int(transfer['amount_swapped']),
                                       starting_balance, int(transfer['nonce'])])
        return swap_hash

    def transfer_hash(self, transfer) -> str:
        if 'is_padding' in transfer and transfer['is_padding']:
            return EMPTY_HASH

        is_swap = transfer['amount_swapped']
        if is_swap:
            return self.swap_transfer_hash(transfer)
        else:
            return self.normal_transfer_hash(transfer)

    def active_state_hash(self, spent: int = None, gained: int = None, eon_number: int = None):
        contract_address_hash = Web3.soliditySha3(['address'], [self.contract_address])
        token_address_hash = Web3.soliditySha3(['address'], [self.token_address])
        wallet_address_hash = Web3.soliditySha3(['address'], [self.wallet_address])
        transaction_set_hash = self.calculate_tx_set_hash(self.current_eon.transfers)
        state_amount = {'spent': spent, 'gained': gained} if spent is not None and gained is not None else self.spent_and_gained(self.current_eon)
        self.logger().info(f"Active State => {[self.contract_address,self.token_address, self.wallet_address, self.trail_identifier, self.current_eon.eon_number, transaction_set_hash, state_amount['spent'], state_amount['gained']]}")
        return Web3.soliditySha3(['bytes32', 'bytes32', 'bytes32', 'uint64', 'uint256',
                                  'bytes32', 'uint256', 'uint256'],
                                 [contract_address_hash, token_address_hash, wallet_address_hash,
                                  self.trail_identifier, self.current_eon.eon_number if eon_number is None else eon_number, transaction_set_hash,
                                  state_amount['spent'], state_amount['gained']])

    def calculate_tx_set_hash(self, transfers) -> str:
        # filter out incoming passive transfers
        transfers = [transfer for transfer in transfers if not transfer['passive'] or not is_same_hex(transfer['recipient']['address'], self.wallet_address)]
        self.logger().info(f"Transfers => {transfers}")
        if len(transfers) > 0:
            padding_length = next_power_of_2(len(transfers)) - len(transfers)
            transfers += [{'is_padding': True}] * padding_length

        return self.construct_merkle_tree(transfers)['hash']

    def construct_merkle_tree(self, transfers):
        transfers_count = len(transfers)

        if transfers_count == 0:
            return {'height': 0, 'hash': EMPTY_HASH}
        elif transfers_count == 1:
            return {'height': 0, 'hash': self.transfer_hash(transfers[0])}

        mid = transfers_count // 2
        left = transfers[0:mid]
        right = transfers[mid:transfers_count]

        left_child = self.construct_merkle_tree(left)
        right_child = self.construct_merkle_tree(right)

        height = left_child['height'] + 1
        node_hash = Web3.soliditySha3(['uint32', 'bytes32', 'bytes32'],
                                      [left_child['height'], left_child['hash'], right_child['hash']])

        result = {'height': height, 'hash': node_hash, 'left_child': left_child, 'right_child': right_child}

        left_child['parent'] = result
        right_child['parent'] = result

        return result

    async def start_notification_consumer(self):
        self.logger().info(f"Starting wallet notification listener...")
        while True:
            try:
                msg = await self._ws_stream.get()
                self.logger().info(f"Wallet stream message -> {msg}")
                self.state_updater(msg)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error occurred while consuming notifications: {e}")
                await asyncio.sleep(5.0)

    # Updating is done in place by overriding (Check concurrency issues)
    def state_updater(self, msg):
        msg_type = msg['type']
        transfer_model_notifications = [WSNotificationType.INCOMING_TRANSFER,
                                        WSNotificationType.INCOMING_RECEIPT,
                                        WSNotificationType.INCOMING_CONFIRMATION,
                                        WSNotificationType.MATCHED_SWAP,
                                        WSNotificationType.FINALIZED_SWAP,
                                        WSNotificationType.CANCELLED_SWAP]

        if msg_type in transfer_model_notifications:
            transfer_update = msg['data']
            transfer_found = False
            for i, transfer in enumerate(self.current_eon.transfers):
                if transfer['id'] == transfer_update['id']:
                    self.current_eon.transfers[i] = transfer_update
                    transfer_found = True
                    break
            if not transfer_found:
                self.current_eon.transfers.append(transfer_update)
        elif msg_type == WSNotificationType.REGISTERED_WALLET:
            registration_data = msg['data']['registration']
            self._trail_identifier = registration_data['trail_identifier']
            self._ws_confirmations_stream.put_nowait(registration_data)
        elif msg_type == WSNotificationType.CONFIRMED_DEPOSIT:
            deposit = msg['data']
            self.current_eon.deposits.append(deposit)
        elif msg_type == WSNotificationType.CONFIRMED_WITHDRAWAL:
            withdrawal = msg['data']
            self.current_eon.withdrawals.append(withdrawal)
        elif msg_type == WSNotificationType.CHECKPOINT_CREATED:
            self.previous_eon = self.current_eon
            wallet_data = msg['data']
            current_merkle_proof = {}
            current_eon_number = -1
            for merkle_proof in enumerate(wallet_data['merkle_proofs']):
                if merkle_proof['eon_number'] > current_eon_number:
                    current_merkle_proof = merkle_proof
                    current_eon_number = merkle_proof['eon_number']

            merkle_proof = current_merkle_proof
            eon_number = current_eon_number
            transfers = [transfer for transfer in wallet_data['transfers']
                         if transfer['eon_number'] == current_eon_number]
            deposits = [deposit for deposit in wallet_data['deposits']
                        if deposit['eon_number'] == current_eon_number]
            withdrawals = [withdrawal for withdrawal in wallet_data['withdrawals']
                           if withdrawal['eon_number'] == current_eon_number]
            self.current_eon = LQDEon(transfers, deposits, withdrawals, merkle_proof, eon_number)

    def clone(self):
        return LQDWallet(self.token_address,
                         self.wallet_address,
                         self.contract_address,
                         self.trail_identifier,
                         deepcopy(self.current_eon),
                         deepcopy(self.previous_eon),
                         self._ws_stream)

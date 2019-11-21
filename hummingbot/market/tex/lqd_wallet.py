from typing import (Dict)
from enum import Enum
import asyncio
from hummingbot.market.tex.tex_utils import (
    is_same_hex,
    next_power_of_2
)
from web3 import Web3
from hummingbot.market.tex.lqd_eon import LQDEon
from hummingbot.market.tex.lqd_wallet_sync import WSNotificationType
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

    def starting_balance(self, eon: LQDEon) -> float:
        merkle_proof = eon.merkle_proof
        if not merkle_proof:
            return 0
        return merkle_proof['right'] - merkle_proof['left']

    def spent_and_gained(self, eon: LQDEon) -> Dict[str, float]:
        spent = 0
        gained = 0
        transfers = eon.transfers
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

    def balance(self, eon: LQDEon) -> float:
        deposits = eon.deposits
        withdrawals = eon.withdrawals
        deposits_amount = functools.reduce(lambda total, deposit: total + deposit['amount'], deposits)
        withdrawals_amount = functools.reduce(lambda total, withdrawal: total + withdrawal['amount'], withdrawals)
        state_amount = self.spent_and_gained(eon)
        return self.starting_balance(eon) + state_amount['gained'] - \
            state_amount['spent'] + deposits_amount - withdrawals_amount

    def latest_wallet_state(self, state_type: str):
        transfers = self.current_eon.transfers
        # TODO: If no transfers -> Empty active state
        latest_transfer = transfers[len(transfers) - 1]

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
                                  self.starting_balance(self.current_eon), transfer['nonce']])

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

    def calculate_tx_set_hash(self, transfers) -> str:
        # filter out incoming passive transfers
        transfers = filter(lambda transfer: not (transfer['passive'] or
                                                 is_same_hex(transfer['recipient']['address'],
                                                             self.wallet_address)), transfers)
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

        left_child = self.calculate_merkle_tree_hash(self, left)
        right_child = self.calculate_merkle_tree_hash(self, right)

        height = left_child['height'] + 1
        node_hash = Web3.soliditySha3(['uint32', 'bytes32', 'bytes32'],
                                      [left_child['height'], left_child['hash'], right_child('hash')])

        result = {'height': height, 'hash': node_hash, 'left_child': left_child, 'right_child': right_child}

        left_child['parent'] = result
        right_child['parent'] = result

        return result

    async def ws_notification_consumer(self):
        while True:
            try:
                msg = await self._ws_stream.get()
                self.state_updater(msg)
                print(f"Consuming -> {msg}")

            except asyncio.CancelledError:
                raise
            except Exception:
                print(
                    f"Unexpected error routing order book messages.",
                    exc_info=True,
                    app_warning_msg=f"Unexpected error routing order book messages. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)

    # Updating is done in place by overriding (Check concurrency issues)
    def state_updater(self, msg):
        msg_type = msg['type']
        transfer_model_notifications = [WSNotificationType.INCOMING_TRANSFER,
                                        WSNotificationType.INCOMING_RECEIPT,
                                        WSNotificationType.INCOMING_CONFIRMATION,
                                        WSNotificationType.INCOMING_TIMEOUT,
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

            self.current_eon.merkle_proof = current_merkle_proof
            self.current_eon.eon_number = current_eon_number
            self.current_eon.transfers = [transfer for transfer in wallet_data['transfers']
                                          if transfer['eon_number'] == current_eon_number]
            self.current_eon.deposits = [deposit for deposit in wallet_data['deposits']
                                         if deposit['eon_number'] == current_eon_number]
            self.current_eon.withdrawals = [withdrawal for withdrawal in wallet_data['withdrawals']
                                            if withdrawal['eon_number'] == current_eon_number]

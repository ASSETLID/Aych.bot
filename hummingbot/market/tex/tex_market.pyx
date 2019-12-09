from collections import (defaultdict, OrderedDict)
import aiohttp
import asyncio
from async_timeout import timeout
from decimal import Decimal
import logging
import pandas as pd
import re
import time
from typing import (
    Any,
    Dict,
    List,
    AsyncIterable,
    Optional,
    Coroutine,
    Tuple,
)
import conf
from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler
from hummingbot.core.clock cimport Clock
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.market.tex.tex_api_order_book_data_source import TEXAPIOrderBookDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.core.event.events import (
    MarketEvent,
    MarketWithdrawAssetEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    OrderCancelledEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    MarketTransactionFailureEvent,
    MarketOrderFailureEvent,
    OrderType,
    TradeType,
    TradeFee
)
from hummingbot.market.market_base import (
    MarketBase,
    NaN,
    s_decimal_NaN)
from hummingbot.market.tex.lqd_wallet import LQDWallet
from hummingbot.market.tex.lqd_eon import LQDEon
from hummingbot.market.tex.lqd_wallet_sync import LQDWalletSync
from hummingbot.market.tex.tex_operator_api import (get_current_eon,
                                                    get_wallet_data,
                                                    get_registration_data,
                                                    post_transfer, post_swap,
                                                    post_swap_freezing,
                                                    post_swap_cancellation,
                                                    post_swap_finalization)
from hummingbot.market.tex.tex_order_book_tracker import TEXOrderBookTracker
from hummingbot.market.tex.tex_in_flight_order import TEXInFlightOrder
from hummingbot.market.tex.tex_utils import (sign_data,
                                             generate_seed,
                                             hash_balance_marker,
                                             remove_0x_prefix,
                                             swap_freeze_hash)
from hummingbot.market.tex.tex_crypto import HDPrivateKey, HDKey
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.transaction_tracker import TransactionTracker
from hummingbot.market.trading_rule cimport TradingRule
from hummingbot.wallet.ethereum.web3_wallet import Web3Wallet
from eth_utils import (to_checksum_address)
from copy import (copy, deepcopy)
import random
from web3 import Web3
s_logger = None

s_decimal_0 = Decimal(0)
NETWORK = 'RINKEBY'
ETH_RPC_URL = 'https://rinkeby.infura.io/v3/9aed27c49d81418687a11e11aa00be0a'
UPDATE_BALANCES_INTERVAL = 5
SUB_WALLET_COUNT = 5
# TODO: Should be auto fetched.
CONTRACT_ADDRESS = '0x66b26B6CeA8557D6d209B33A30D69C11B0993a3a'
cdef class TEXMarketTransactionTracker(TransactionTracker):
    cdef:
        TEXMarket _owner

    def __init__(self, owner: TEXMarket):
        super().__init__()
        self._owner = owner

    cdef c_did_timeout_tx(self, str tx_id):
        TransactionTracker.c_did_timeout_tx(self, tx_id)
        self._owner.c_did_timeout_tx(tx_id)

cdef class TEXMarket(MarketBase):
    MARKET_RECEIVED_ASSET_EVENT_TAG = MarketEvent.ReceivedAsset.value
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted.value
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted.value
    MARKET_WITHDRAW_ASSET_EVENT_TAG = MarketEvent.WithdrawAsset.value
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled.value
    MARKET_TRANSACTION_FAILURE_EVENT_TAG = MarketEvent.TransactionFailure.value
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure.value
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled.value
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated.value
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated.value

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    def __init__(self,
                 wallet: Web3Wallet,
                 ethereum_rpc_url: str,
                 poll_interval: float = 5.0,
                 order_book_tracker_data_source_type: OrderBookTrackerDataSourceType =
                 OrderBookTrackerDataSourceType.EXCHANGE_API,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):

        super().__init__()
        self._trading_required = trading_required
        self._order_book_tracker = TEXOrderBookTracker(data_source_type=order_book_tracker_data_source_type, trading_pairs=trading_pairs)
        self._lqd_wallet_sync = LQDWalletSync()
        self._ev_loop = asyncio.get_event_loop()
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._poll_interval = poll_interval
        self._in_flight_orders = {}
        self._in_flight_cancels = OrderedDict()
        self._tx_tracker = TEXMarketTransactionTracker(self)
        self._data_source_type = order_book_tracker_data_source_type
        self._status_polling_task = None
        self._order_tracker_task = None
        self._lqd_wallet_sync_task = None
        self._shared_client = None
        self._async_scheduler = AsyncCallScheduler(call_interval=0.5)
        self._last_pull_timestamp = 0
        self._trading_pairs = trading_pairs
        self._w3 = Web3(Web3.HTTPProvider(ETH_RPC_URL))
        self._wallet = wallet  # Main account web3 wallet
        self._eth_sub_wallets = []  # Defines an array of sub wallets where each item in the list is a dictionary having `address` and `private_key`
        self._network_id = int(self._w3.net.version)  # Defines the network id of the current w3 instance.
        self._wallet_map = {}  # {key: wallet} -> Where key is of the form `token/address` and wallet is of type LQDWallet.
        self._sub_wallets_status = {'available': [], 'blocked': []}  # {available: [], blocked: []} -> Defines the availability of sub wallets.

    @property
    def name(self) -> str:
        return "tex"

    @property
    def status_dict(self):
        return {
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "order_books_initialized": self._order_book_tracker.ready,
        }

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def wallet(self) -> Web3Wallet:
        return self._wallet

    @property
    def in_flight_orders(self) -> Dict[str, TEXInFlightOrder]:
        return self._in_flight_orders

    @property
    def tracking_states(self) -> Dict[str, any]:
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
        }

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        self._in_flight_orders.update({
            key: TEXInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    async def get_active_exchange_markets(self) -> pd.DataFrame:
        return await TEXAPIOrderBookDataSource.get_active_exchange_markets()

    cdef OrderBook c_get_order_book(self, str trading_pair):
        cdef:
            dict order_books = self._order_book_tracker.order_books

        if trading_pair not in order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return order_books[trading_pair]

    async def _status_polling_loop(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()
                await self._update_balances()
                await self._sync_sub_wallets()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"TEX Status Polling Loop Error: {e}")
                self.logger().network(
                    "Unexpected error while fetching account and status updates.",
                    exc_info=True,
                    app_warning_msg=f"Failed to fetch account updates on TEX. Check network connection.")

    async def _update_balances(self):
        cdef:
            double current_timestamp = self._current_timestamp
        if current_timestamp - self._last_update_balances_timestamp > UPDATE_BALANCES_INTERVAL or len(self._account_balances) > 0:
            available_balances, total_balances = await self._get_lqd_balances()
            self._account_available_balances = available_balances
            self._account_balances = total_balances
            self._last_update_balances_timestamp = current_timestamp

    async def _create_wallet(self, wallet_address: str, private_key: str, token_address: str, eon_number: int) -> LQDWallet:
        wallet_data = await get_wallet_data(wallet_address = wallet_address, token_address = token_address, eon_number = eon_number)
        trail_identifier = wallet_data['registration']['trail_identifier']
        previous_eon = None if wallet_data['registration']['eon_number'] == eon_number else self._init_eon(wallet_data, eon_number - 1)
        current_eon = self._init_eon(wallet_data, eon_number)
        ws_stream = await self._lqd_wallet_sync.subscribe_wallet(wallet_address = wallet_address, token_address = token_address)
        lqd_wallet = LQDWallet(token_address = token_address, wallet_address = wallet_address, private_key = private_key,
                               contract_address = CONTRACT_ADDRESS, trail_identifier = trail_identifier,
                               current_eon = current_eon, previous_eon = previous_eon, ws_stream = ws_stream)

        safe_ensure_future(lqd_wallet.start_notification_consumer())
        self._wallet_map[f"{token_address}/{wallet_address}"] = lqd_wallet
        return lqd_wallet

    def _generate_sub_wallets(self):
        if len(self._eth_sub_wallets) == 0:
            seed = generate_seed(self.wallet.private_key)
            master_key = HDPrivateKey.master_key_from_mnemonic(seed)
            root_keys = HDKey.from_path(master_key, "m/44'/60'/0'")
            acct_priv_key = root_keys[-1]
            for i in range(SUB_WALLET_COUNT):
                keys = HDKey.from_path(acct_priv_key, '{change}/{index}'.format(change=0, index=i))
                private_key = keys[-1]
                public_key = private_key.public_key
                address = to_checksum_address(private_key.public_key.address())
                eth_sub_wallet = {'address': address, 'private_key': private_key._key.to_hex()}
                self._eth_sub_wallets.append(eth_sub_wallet)

    def _init_eon(self, wallet_data: Dict, eon_number: int) -> LQDEon:
        merkle_proofs_filtered = [proof for proof in wallet_data['merkle_proofs'] if proof['eon_number'] == eon_number]
        merkle_proof = None if len(merkle_proofs_filtered) == 0 else merkle_proofs_filtered[0]
        transfers = [transfer for transfer in wallet_data['transfers'] if transfer['eon_number'] == eon_number]
        deposits = [deposit for deposit in wallet_data['deposits'] if deposit['eon_number'] == eon_number]
        withdrawals = [withdrawal for withdrawal in wallet_data['withdrawals'] if withdrawal['eon_number'] == eon_number]
        return LQDEon(transfers = transfers, deposits = deposits, withdrawals = withdrawals,
                      merkle_proof = merkle_proof, eon_number = eon_number)

    async def _init_LQD_wallets(self):
        current_eon = await get_current_eon()
        markets = await self.get_active_exchange_markets()
        all_wallets_addresses = [self.wallet.address, *[account['address'] for account in self._eth_sub_wallets]]

        # Creating main wallet
        for trading_pair in self._trading_pairs:
            market = markets.loc[trading_pair]
            base_token = market.baseAssetAddress
            quote_token = market.quoteAssetAddress
            await safe_gather(*[self._create_wallet(self.wallet.address,
                                                    self.wallet.private_key,
                                                    token_address,
                                                    current_eon) for token_address in [base_token, quote_token]])
        # Creating sub wallets
        for sub_wallet in self._eth_sub_wallets:
            for trading_pair in self._trading_pairs:
                market = markets.loc[trading_pair]
                base_token = market.baseAssetAddress
                quote_token = market.quoteAssetAddress
                await safe_gather(*[self._create_wallet(sub_wallet['address'],
                                                        sub_wallet['private_key'],
                                                        token_address,
                                                        current_eon) for token_address in [base_token, quote_token]])

    async def _get_lqd_balances(self):
        main_wallet_address = self.wallet.address
        available_balances = {}
        total_balances = {}
        markets = await self.get_active_exchange_markets()

        for trading_pair in self._trading_pairs:
            market = markets.loc[trading_pair]
            base_token_address = market.baseAssetAddress
            quote_token_address = market.quoteAssetAddress
            base_token = market.baseAsset
            quote_token = market.quoteAsset
            base_token_wallet = self._wallet_map[f"{base_token_address}/{main_wallet_address}"]
            quote_token_wallet = self._wallet_map[f"{quote_token_address}/{main_wallet_address}"]
            base_token_balance = base_token_wallet.balance()
            quote_token_balance = quote_token_wallet.balance()
            available_balances[f"f{base_token}"] = Decimal(base_token_balance * 10.0 ** (-18.0))
            available_balances[f"f{quote_token}"] = Decimal(quote_token_balance * 10.0 ** (-18.0))
            base_token_locked_balance = 0
            quote_token_locked_balance = 0
            for sub_wallet in self._eth_sub_wallets:
                base_sub_wallet = self._wallet_map[f"{base_token_address}/{sub_wallet['address']}"]
                quote_sub_wallet = self._wallet_map[f"{quote_token_address}/{sub_wallet['address']}"]
                base_token_locked_balance += base_sub_wallet.balance()
                quote_token_locked_balance += quote_sub_wallet.balance()
            total_balances[f"f{base_token}"] = available_balances[f"f{base_token}"] + Decimal(base_token_locked_balance * 10.0 ** (-18.0))
            total_balances[f"f{quote_token}"] = available_balances[f"f{quote_token}"] + Decimal(quote_token_locked_balance * 10.0 ** (-18.0))

        self.logger().info(f"BALANCES => {available_balances}, TOTAL => {total_balances}")
        return available_balances, total_balances

    async def _process_sub_wallet(self, sub_wallet: LQDWallet):
        is_used = False
        non_finalized_swaps = []
        swap_transfers = [transfer for transfer in sub_wallet.current_eon.transfers if transfer["amount_swapped"]]
        for transfer in swap_transfers:
            is_tx_pending = not transfer["complete"] and not transfer["cancelled"] and not transfer["voided"]
            no_cancellation_state = transfer["cancelled"] and not transfer["sender_cancellation_active_state"]
            if is_tx_pending or no_cancellation_state:
                is_used = True
            if transfer["recipient"]["token"] == sub_wallet.token_address and transfer["complete"] and not transfer["recipient_finalization_active_state"]:
                non_finalized_swaps.append(transfer)

        # TODO: Finalize swaps here
        # TODO: Harvest free sub wallets
        for swap in non_finalized_swaps:
            self.logger().info(f"Finalizing => Swap: {swap}")
            await self._finalize_swap(swap)

        if not is_used:
            await self._harvest_sub_wallet(sub_wallet)

        return is_used

    async def _sync_sub_wallets(self, first_sync: bool = False):
        markets = await self.get_active_exchange_markets()
        # TODO: Handle multple pairs (Currently handling only one token pair)
        trading_pair = self._trading_pairs[0]
        market = markets.loc[trading_pair]
        tokens = [market.baseAssetAddress, market.quoteAssetAddress]

        if first_sync:
            self._sub_wallets_status["blocked"] = [index for index in range(SUB_WALLET_COUNT)]
        for index in self._sub_wallets_status["blocked"]:
            eth_sub_wallet = self._eth_sub_wallets[index]
            is_account_free = True
            for token in tokens:
                sub_wallet_address = eth_sub_wallet["address"]
                sub_wallet = self._wallet_map[f"{token}/{sub_wallet_address}"]
                is_used = await self._process_sub_wallet(sub_wallet)
                if is_used:
                    is_account_free = False
            if is_account_free:
                self._sub_wallets_status["blocked"].remove(index)
                self._sub_wallets_status["available"].append(index)

        # Sort swap wallets
        self._sub_wallets_status["available"].sort()
        self._sub_wallets_status["blocked"].sort()
        self.logger().info(f"Sub Wallets Status: {self._sub_wallets_status}")

    async def _send_transfer(self,
                             sender_address: str,
                             recipient_address: str,
                             amount: int,
                             token_address: str):
        sender_wallet = self._wallet_map[f"{token_address}/{sender_address}"]
        sender_balance = sender_wallet.balance()

        if sender_balance < amount:
            raise Exception('Insufficient Balance to send transfer')

        recipient_registration = await get_registration_data(recipient_address, token_address)
        nonce = random.randint(1, 999999)
        new_transfer = {'id': 0,
                        'amount': str(int(amount)),
                        'amount_swapped': None,
                        'wallet': {'address': sender_address,
                                   'token': token_address},
                        'recipient': {'address': recipient_address,
                                      'token': token_address},
                        'recipient_trail_identifier': recipient_registration["trail_identifier"],
                        'nonce': str(nonce),
                        'passive': True,
                        'position': None,
                        'eon_number': sender_wallet.current_eon.eon_number,
                        'processed': False, 'complete': False, 'voided': False, 'cancelled': False, 'appended': False,
                        'matched_amounts': {'in': '0', 'out': '0', 'matched_in': '0', 'matched_out': '0'}
                        }
        sender_wallet_clone = sender_wallet.clone()
        spent_gained = sender_wallet_clone.spent_and_gained()
        sender_wallet_clone.current_eon.transfers.append(new_transfer)
        active_state_hash = sender_wallet_clone.active_state_hash(spent = spent_gained['spent'] + amount, gained = spent_gained['gained'])
        balance_marker = hash_balance_marker(contract_address = CONTRACT_ADDRESS, token_address = token_address,
                                             wallet_address = sender_address, eon_number = sender_wallet.current_eon.eon_number,
                                             balance = sender_balance - int(amount))

        active_state_signature = sign_data(active_state_hash.hex(), sender_wallet.private_key)
        balance_marker_signature = sign_data(balance_marker.hex(), sender_wallet.private_key)
        try:
            transfer = await post_transfer(wallet_address = sender_address,
                                           token_address = token_address,
                                           active_state_signature = active_state_signature.hex(),
                                           balance_marker_signature = balance_marker_signature.hex(),
                                           wallet_balance = str(sender_balance - int(amount)),
                                           transfer = new_transfer)
            # Append transfer to wallet
            transfer['recipient'] = {'address': transfer['recipient'], 'token': transfer['wallet']['token']}
            transfer['amount_swapped'] = None
            transfer['cancelled'] = False
            transfer['voided'] = False
            sender_wallet.current_eon.transfers.append(transfer)
            return transfer
        except Exception as e:
            self.logging().error(f'Error occurred while trying to transfer, {e.__class__}: {e}')
            raise e

    async def _send_swap(self, credit_token: str, debit_token: str, credit_amount: int, debit_amount: int):
        if len(self._sub_wallets_status["available"]) == 0:
            raise Exception('Swap limit reached!')

        swap_wallet_index = self._sub_wallets_status["available"].pop(0)

        # Get Swap Sub Wallets
        swap_eth_wallet = self._eth_sub_wallets[swap_wallet_index]
        self.logger().info(f"**Swap eth wallet => {swap_eth_wallet}")
        credit_wallet = self._wallet_map[f"{credit_token}/{swap_eth_wallet['address']}"]
        debit_wallet = self._wallet_map[f"{debit_token}/{swap_eth_wallet['address']}"]
        # Calculate Fund
        fund_amount = debit_amount - debit_wallet.balance()
        self.logger().info(f"**Fund Amount => {fund_amount}")
        # Make sure debit wallet has balance exactly equal to debit amount
        if fund_amount > 0:
            await self._send_transfer(self.wallet.address, swap_eth_wallet['address'], fund_amount, debit_token)
        elif fund_amount < 0:
            await self._send_transfer(swap_eth_wallet['address'], self.wallet.address, abs(fund_amount), debit_token)

        nonce = random.randint(1, 999999)
        hashes = self._swap_creation_hashes(credit_amount, debit_amount, credit_wallet, debit_wallet, nonce)
        credit_signatures = list(map(lambda c_hash: remove_0x_prefix(sign_data(c_hash.hex(), debit_wallet.private_key).hex()), hashes['credit_active_states']))
        debit_signatures = list(map(lambda d_hash: remove_0x_prefix(sign_data(d_hash.hex(), debit_wallet.private_key).hex()), hashes['debit_active_states']))
        fulfillment_signatures = list(map(lambda f_hash: remove_0x_prefix(sign_data(f_hash.hex(), debit_wallet.private_key).hex()), hashes['fulfillment_active_states']))
        credit_balance_signatures = list(map(lambda c_b_hash: remove_0x_prefix(sign_data(c_b_hash.hex(), debit_wallet.private_key).hex()), hashes['credit_balance_markers']))
        debit_balance_signatures = list(map(lambda d_b_hash: remove_0x_prefix(sign_data(d_b_hash.hex(), debit_wallet.private_key).hex()), hashes['debit_balance_markers']))

        try:
            swap = await post_swap(credit_wallet = credit_wallet,
                                   debit_wallet = debit_wallet,
                                   credit_amount = str(credit_amount),
                                   debit_amount = str(debit_amount),
                                   credit_signatures = credit_signatures,
                                   debit_signatures = debit_signatures,
                                   credit_balance_signatures = credit_balance_signatures,
                                   debit_balance_signatures = debit_balance_signatures,
                                   fulfillment_signatures = fulfillment_signatures,
                                   eon_number = credit_wallet.current_eon.eon_number,
                                   nonce = str(nonce),
                                   logger = self.logger)

            self._sub_wallets_status["blocked"].append(swap_wallet_index)
            # Append swap to credit/debit transfers
            swap['matched_amounts'] = {'in': 0, 'out': 0}
            swap['cancelled'] = False
            swap['voided'] = False
            credit_wallet.current_eon.transfers.append(swap)
            debit_wallet.current_eon.transfers.append(swap)
            return swap
        except Exception as e:
            self.logging().error(f'Error occurred while trying to swap, {e.__class__}: {e}')
            self._sub_wallets_status["available"].append(swap_wallet_index)
            raise e

    def _swap_creation_hashes(self,
                              credit_amount: Decimal,
                              debit_amount: Decimal,
                              credit_wallet: LQDWallet,
                              debit_wallet: LQDWallet,
                              nonce: int):

        credit_starting_balance = credit_wallet.starting_balance(credit_wallet.current_eon)
        debit_starting_balance = debit_wallet.starting_balance(debit_wallet.current_eon)
        current_eon_number = debit_wallet.current_eon.eon_number
        current_credit_wallet = credit_wallet.clone()
        current_debit_wallet = debit_wallet.clone()
        credit_active_state_hashes = []
        debit_active_state_hashes = []
        credit_balance_marker_hashes = []
        debit_balance_marker_hashes = []
        fulfillment_active_state_hashes = []
        for i in range(SUB_WALLET_COUNT):
            if i != 0:
                credit_new_eon = LQDEon([], [], [], {}, current_eon_number + i)
                debit_new_eon = LQDEon([], [], [], {}, current_eon_number + i)
                current_credit_wallet = LQDWallet(token_address = credit_wallet.token_address,
                                                  wallet_address = credit_wallet.wallet_address,
                                                  private_key = credit_wallet.private_key,
                                                  contract_address = CONTRACT_ADDRESS,
                                                  trail_identifier = credit_wallet.trail_identifier,
                                                  current_eon = credit_new_eon,
                                                  previous_eon = None,
                                                  ws_stream = None)
                current_debit_wallet = LQDWallet(token_address = debit_wallet.token_address,
                                                 wallet_address = debit_wallet.wallet_address,
                                                 private_key = debit_wallet.private_key,
                                                 contract_address = CONTRACT_ADDRESS,
                                                 trail_identifier = debit_wallet.trail_identifier,
                                                 current_eon = debit_new_eon,
                                                 previous_eon = None,
                                                 ws_stream = None)
                credit_starting_balance = 0
                debit_starting_balance = debit_amount

            credit_swap_transfer = {'id': 0,
                                    'amount': str(debit_amount),
                                    'amount_swapped': str(credit_amount),
                                    'wallet': {'address': debit_wallet.wallet_address,
                                               'token': debit_wallet.token_address},
                                    'recipient': {'address': credit_wallet.wallet_address,
                                                  'token': credit_wallet.token_address},
                                    'recipient_trail_identifier': credit_wallet.trail_identifier,
                                    'nonce': str(nonce),
                                    'passive': False,
                                    'position': None,
                                    'eon_number': current_eon_number + i,
                                    'processed': False, 'complete': False, 'voided': False, 'cancelled': False, 'appended': False,
                                    'matched_amounts': {'in': '0', 'out': '0', 'matched_in': '0', 'matched_out': '0'},
                                    'starting_balance': credit_starting_balance}

            debit_swap_transfer = {'id': 0,
                                   'amount': str(debit_amount),
                                   'amount_swapped': str(credit_amount),
                                   'wallet': {'address': debit_wallet.wallet_address,
                                              'token': debit_wallet.token_address},
                                   'recipient': {'address': credit_wallet.wallet_address,
                                                 'token': credit_wallet.token_address},
                                   'recipient_trail_identifier': credit_wallet.trail_identifier,
                                   'nonce': str(nonce),
                                   'passive': False,
                                   'position': None,
                                   'eon_number': current_eon_number + i,
                                   'processed': False, 'complete': False, 'voided': False, 'cancelled': False, 'appended': False,
                                   'matched_amounts': {'in': '0', 'out': '0', 'matched_in': '0', 'matched_out': '0'},
                                   'starting_balance': debit_starting_balance}

            fulfillment_swap_transfer = {'id': 0,
                                         'amount': str(debit_amount),
                                         'amount_swapped': str(credit_amount),
                                         'wallet': {'address': debit_wallet.wallet_address,
                                                    'token': debit_wallet.token_address},
                                         'recipient': {'address': credit_wallet.wallet_address,
                                                       'token': credit_wallet.token_address},
                                         'recipient_trail_identifier': credit_wallet.trail_identifier,
                                         'nonce': str(nonce),
                                         'passive': False,
                                         'position': None,
                                         'eon_number': current_eon_number + i,
                                         'processed': False, 'complete': True, 'voided': False, 'cancelled': False, 'appended': False,
                                         'matched_amounts': {'in': '0', 'out': '0', 'matched_in': '0', 'matched_out': '0'},
                                         'starting_balance': credit_starting_balance}

            current_fulfillment_wallet = current_credit_wallet.clone()

            debit_spent_gained = current_debit_wallet.spent_and_gained()
            current_debit_wallet.current_eon.transfers.append(debit_swap_transfer)
            debit_active_state_hash = current_debit_wallet.active_state_hash(spent = debit_spent_gained['spent'] + debit_amount,
                                                                             gained = debit_spent_gained['gained'],
                                                                             eon_number = current_eon_number + i)

            credit_spent_gained = current_credit_wallet.spent_and_gained()
            current_credit_wallet.current_eon.transfers.append(credit_swap_transfer)
            credit_active_state_hash = current_credit_wallet.active_state_hash(spent = credit_spent_gained['spent'],
                                                                               gained = credit_spent_gained['gained'],
                                                                               eon_number = current_eon_number + i)

            fulfillment_spent_gained = current_fulfillment_wallet.spent_and_gained()
            current_fulfillment_wallet.current_eon.transfers.append(fulfillment_swap_transfer)
            fulfillment_hash = current_fulfillment_wallet.active_state_hash(spent = credit_spent_gained['spent'],
                                                                            gained = credit_spent_gained['gained'] + credit_amount,
                                                                            eon_number = current_eon_number + i)

            credit_balance_marker = hash_balance_marker(contract_address = CONTRACT_ADDRESS, token_address = credit_wallet.token_address,
                                                        wallet_address = credit_wallet.wallet_address, eon_number = current_eon_number + i,
                                                        balance = 0)
            debit_balance_marker = hash_balance_marker(contract_address = CONTRACT_ADDRESS, token_address = debit_wallet.token_address,
                                                       wallet_address = debit_wallet.wallet_address, eon_number = current_eon_number + i,
                                                       balance = 0)

            credit_active_state_hashes.append(credit_active_state_hash)
            debit_active_state_hashes.append(debit_active_state_hash)
            fulfillment_active_state_hashes.append(fulfillment_hash)
            credit_balance_marker_hashes.append(credit_balance_marker)
            debit_balance_marker_hashes.append(debit_balance_marker)

        return {'credit_active_states': credit_active_state_hashes,
                'debit_active_states': debit_active_state_hashes,
                'fulfillment_active_states': fulfillment_active_state_hashes,
                'credit_balance_markers': credit_balance_marker_hashes,
                'debit_balance_markers': debit_balance_marker_hashes}

    async def _cancel_swap(self, swap):
        debit_address = swap['wallet']['address']
        debit_token = swap['wallet']['token']
        credit_token = swap['recipient']['token']
        debit_wallet = self._wallet_map[f"{debit_token}/{debit_address}"]
        eth_sub_wallet = [wallet for wallet in self._eth_sub_wallets if wallet['address'] == debit_address][0]

        freeze_hash = swap_freeze_hash(debit_token, credit_token, int(swap['nonce']))
        freeze_signature = sign_data(freeze_hash.hex(), debit_wallet.private_key).hex()
        self.logger().info(f"Posting freeze signatures")
        # await post_swap_freezing(freeze_signature, swap['id'], self.logger)
        self.logger().info(f"Swap Frozen")
        hashes = self._swap_cancellation_hashes(swap)
        debit_cancellation_signatures = [sign_data(state_hash.hex(), debit_wallet.private_key).hex() for state_hash in hashes['debit_cancellation_hashes']]
        credit_cancellation_signatures = [sign_data(state_hash.hex(), debit_wallet.private_key).hex() for state_hash in hashes['credit_cancellation_hashes']]
        try:
            await post_swap_cancellation(debit_cancellation_signatures,
                                         credit_cancellation_signatures,
                                         swap['id'],
                                         self.logger)
        except Exception as e:
            self.logging().error(f'Error occurred while trying to cancel swap, {e.__class__}: {e}')
            raise e

    def _swap_cancellation_hashes(self, swap):
        debit_cancellation_hashes = []
        credit_cancellation_hashes = []

        debit_address = swap['wallet']['address']
        debit_token = swap['wallet']['token']
        credit_address = swap['recipient']['address']
        credit_token = swap['recipient']['token']

        debit_wallet = self._wallet_map[f"{debit_token}/{debit_address}"]
        credit_wallet = self._wallet_map[f"{credit_token}/{credit_address}"]

        current_eon_number = swap['eon_number']
        swap_matched_out = int(swap['matched_amounts']['out'])
        swap_debit_amount = int(swap['amount'])
        swap_credit_amount = int(swap['amount_swapped'])

        debit_spent_gained = debit_wallet.spent_and_gained()
        debit_spent = debit_spent_gained['spent']
        debit_gained = debit_spent_gained['gained'] + swap_debit_amount - swap_matched_out
        debit_cancellation_hash = debit_wallet.active_state_hash(spent = debit_spent, gained = debit_gained, eon_number = current_eon_number)

        credit_spent_gained = credit_wallet.spent_and_gained()
        credit_spent = credit_spent_gained['spent']
        credit_gained = credit_spent_gained['gained']
        credit_cancellation_hash = credit_wallet.active_state_hash(spent = credit_spent + swap_credit_amount, gained = credit_gained + swap_credit_amount, eon_number = current_eon_number)

        debit_cancellation_hashes.append(debit_cancellation_hash)
        credit_cancellation_hashes.append(credit_cancellation_hash)

        # TODO: Calculate the eons left for this swap
        for i in range(1, 5):
            credit_new_eon = LQDEon([], [], [], {}, current_eon_number + i)
            debit_new_eon = LQDEon([], [], [], {}, current_eon_number + i)
            future_credit_wallet = LQDWallet(token_address = credit_wallet.token_address,
                                             wallet_address = credit_wallet.wallet_address,
                                             private_key = credit_wallet.private_key,
                                             contract_address = CONTRACT_ADDRESS,
                                             trail_identifier = credit_wallet.trail_identifier,
                                             current_eon = credit_new_eon,
                                             previous_eon = None,
                                             ws_stream = None)
            future_debit_wallet = LQDWallet(token_address = debit_wallet.token_address,
                                            wallet_address = debit_wallet.wallet_address,
                                            private_key = debit_wallet.private_key,
                                            contract_address = CONTRACT_ADDRESS,
                                            trail_identifier = debit_wallet.trail_identifier,
                                            current_eon = debit_new_eon,
                                            previous_eon = None,
                                            ws_stream = None)

            future_debit_spent_gained = max(debit_spent, debit_gained) + 1
            future_credit_spent_gained = max(credit_spent, credit_gained) + swap_credit_amount + 1

            debit_cancellation_hash = future_debit_wallet.active_state_hash(spent = future_debit_spent_gained, gained = future_debit_spent_gained, eon_number = current_eon_number + i)
            credit_cancellation_hash = future_credit_wallet.active_state_hash(spent = future_credit_spent_gained, gained = future_credit_spent_gained, eon_number = current_eon_number + i)

            debit_cancellation_hashes.append(debit_cancellation_hash)
            credit_cancellation_hashes.append(credit_cancellation_hash)

        return {'debit_cancellation_hashes': debit_cancellation_hashes,
                'credit_cancellation_hashes': credit_cancellation_hashes}

    async def _finalize_swap(self, swap):
        sub_wallet_address = swap['recipient']['address']
        credit_token = swap['recipient']['token']
        credit_wallet = self._wallet_map[f"{credit_token}/{sub_wallet_address}"]
        finalization_hashes = self._swap_finalization_hashes(swap)
        finalization_signatures = [sign_data(state_hash.hex(), credit_wallet.private_key).hex() for state_hash in finalization_hashes]

        try:
            await post_swap_finalization(finalization_signatures,
                                         swap['id'],
                                         self.logger)
        except Exception as e:
            self.logging().error(f'Error occurred while trying to finalize swap, {e.__class__}: {e}')
            raise e

    def _swap_finalization_hashes(self, swap):
        finalization_hashes = []
        credit_address = swap['recipient']['address']
        credit_token = swap['recipient']['token']
        swap_debit_amount = int(swap['amount'])
        swap_credit_amount = int(swap['amount_swapped'])
        current_eon_number = swap['eon_number']

        credit_wallet = self._wallet_map[f"{credit_token}/{credit_address}"]
        credit_spent_gained = credit_wallet.spent_and_gained()
        finalization_hash = credit_wallet.active_state_hash(spent = credit_spent_gained['spent'] + swap_credit_amount,
                                                            gained = credit_spent_gained['gained'] + swap_credit_amount,
                                                            eon_number = current_eon_number)
        finalization_hashes.append(finalization_hash)

        for i in range(1, 5):
            credit_new_eon = LQDEon([], [], [], {}, current_eon_number + i)
            future_credit_wallet = LQDWallet(token_address = credit_wallet.token_address,
                                             wallet_address = credit_wallet.wallet_address,
                                             private_key = credit_wallet.private_key,
                                             contract_address = CONTRACT_ADDRESS,
                                             trail_identifier = credit_wallet.trail_identifier,
                                             current_eon = credit_new_eon,
                                             previous_eon = None,
                                             ws_stream = None)

            future_spent_gained = max(credit_spent_gained['spent'], credit_spent_gained['gained']) + swap_credit_amount + 1
            future_finalization_hash = future_credit_wallet.active_state_hash(spent = future_spent_gained,
                                                                              gained = future_spent_gained,
                                                                              eon_number = current_eon_number + i)

            finalization_hashes.append(future_finalization_hash)

        return finalization_hashes

    async def _harvest_sub_wallet(self, sub_wallet: LQDWallet):
        balance = sub_wallet.balance()
        if balance > 0:
            self.logger().info("Harvesting...")
            await self._send_transfer(sender_address = sub_wallet.wallet_address,
                                      recipient_address = self.wallet.address,
                                      amount = balance,
                                      token_address = sub_wallet.token_address)

    async def start_network(self):
        print('starting network')
        if self._order_tracker_task is not None:
            self._stop_network()
        self._generate_sub_wallets()
        self._lqd_wallet_sync_task = safe_ensure_future(self._lqd_wallet_sync.start())
        await self._init_LQD_wallets()
        await self._update_balances()
        await self._sync_sub_wallets(first_sync = True)
        # await self._send_transfer(self.wallet.address, '0xDE9Aa519E2Ee3135D008920e6a85dda5EB91C9A1', 0.001, CONTRACT_ADDRESS)
        # await self._send_swap("0x66b26B6CeA8557D6d209B33A30D69C11B0993a3a", "0xA9F86DD014C001Acd72d5b25831f94FaCfb48717", 20000000000000, 1000000000000000)
        # await self._cancel_swap(SWAP)
        self._order_tracker_task = safe_ensure_future(self._order_book_tracker.start())
        self._status_polling_task = safe_ensure_future(self._status_polling_loop())

    def _stop_network(self):
        if self._order_tracker_task is not None:
            self._order_tracker_task.cancel()
            self._status_polling_task.cancel()
            self._lqd_wallet_sync_task.cancel()
        self._order_tracker_task = self._status_polling_task = None

    async def stop_network(self):
        self._stop_network()
        if self._shared_client is not None:
            await self._shared_client.close()
            self._shared_client = None

    async def check_network(self) -> NetworkStatus:
        return NetworkStatus.CONNECTED

    cdef c_tick(self, double timestamp):
        cdef:
            int64_t last_tick = <int64_t > (self._last_timestamp / self._poll_interval)
            int64_t current_tick = <int64_t > (timestamp / self._poll_interval)

        self._tx_tracker.c_tick(timestamp)
        MarketBase.c_tick(self, timestamp)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    async def _http_client(self) -> aiohttp.ClientSession:
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    cdef object c_get_fee(self,
                          str base_currency,
                          str quote_currency,
                          object order_type,
                          object order_side,
                          object amount,
                          object price):

        return TradeFee(percent=Decimal("0.00"))

    cdef object c_get_order_price_quantum(self, str trading_pair, object price):
        cdef:
            quote_asset_decimals = 18
        decimals_quantum = Decimal(f"1e-{quote_asset_decimals}")
        return decimals_quantum

    cdef object c_get_order_size_quantum(self, str trading_pair, object amount):
        cdef:
            base_asset_decimals = 18
        decimals_quantum = Decimal(f"1e-{base_asset_decimals}")
        return decimals_quantum

    def quantize_order_amount(self, trading_pair: str, amount: Decimal, price: Decimal = s_decimal_NaN) -> Decimal:
        return self.c_quantize_order_amount(trading_pair, amount, price)

    cdef object c_quantize_order_amount(self, str trading_pair, object amount, object price=s_decimal_0):
        return MarketBase.c_quantize_order_amount(self, trading_pair, amount)

    async def place_order(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          price: Decimal,
                          is_buy: bool):

        total_price = amount * price
        credit_amount = amount if is_buy else total_price
        debit_amount = total_price if is_buy else amount
        markets = await self.get_active_exchange_markets()
        market = markets.loc[trading_pair]
        tokens = [market.baseAssetAddress, market.quoteAssetAddress]
        credit_token = tokens[0] if is_buy else tokens[1]
        debit_token = tokens[1] if is_buy else tokens[0]

        order = await self._send_swap(credit_token = credit_token,
                                      debit_token = debit_token,
                                      credit_amount = int(credit_amount * 10 ** 18),
                                      debit_amount = int(debit_amount * 10 ** 18))
        return order

    async def cancel_order(self, client_order_id: str):
        order = self.in_flight_orders.get(client_order_id)
        if not order:
            self.logger().info(f"Failed to cancel order {client_order_id}. Order not found in tracked orders.")
            if client_order_id in self._in_flight_cancels:
                del self._in_flight_cancels[client_order_id]
            return {}

        swap = await self._cancel_swap(order.swap)
        self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                             OrderCancelledEvent(self._current_timestamp, client_order_id))
        return swap

    cdef str c_buy(self,
                   str trading_pair,
                   object amount,
                   object order_type=OrderType.MARKET,
                   object price=s_decimal_0,
                   dict kwargs={}):
        cdef:
            int64_t tracking_nonce = <int64_t > (time.time() * 1e6)
            str order_id = str(f"buy-{trading_pair}-{tracking_nonce}")

        safe_ensure_future(self.execute_buy(order_id, trading_pair, amount, order_type, price))
        return order_id

    cdef str c_sell(self,
                    str trading_pair,
                    object amount,
                    object order_type=OrderType.MARKET,
                    object price=s_decimal_0,
                    dict kwargs={}):
        cdef:
            int64_t tracking_nonce = <int64_t > (time.time() * 1e6)
            str order_id = str(f"sell-{trading_pair}-{tracking_nonce}")

        safe_ensure_future(self.execute_sell(order_id, trading_pair, amount, order_type, price))
        return order_id

    async def execute_buy(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Decimal) -> str:
        try:
            order_result = None
            self.c_start_tracking_order(client_order_id = order_id,
                                        exchange_order_id = None,
                                        trading_pair = trading_pair,
                                        order_type = order_type,
                                        trade_type = TradeType.BUY,
                                        price = price,
                                        amount = amount)

            if order_type is OrderType.LIMIT:
                self.logger().info(f"Placing Limit Order: amount={amount} price={price}")
                order_result = await self.place_order(order_id = order_id,
                                                      trading_pair = trading_pair,
                                                      amount = amount,
                                                      price = price,
                                                      is_buy = True)
            elif order_type is OrderType.MARKET:
                price = self.c_get_price(trading_pair, True)
                shifted_price = price * Decimal(10.0 ** 18)
                self.logger().info(f"Placing Market Order: amount={amount} price={price}")
                order_result = await self.place_order(order_id = order_id,
                                                      trading_pair = trading_pair,
                                                      amount = amount,
                                                      price = price,
                                                      is_buy = True)
            else:
                raise ValueError(f"Invalid OrderType {order_type}. Aborting.")

            exchange_order_id = order_result["id"]
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None and exchange_order_id:
                tracked_order.update_exchange_order_id(exchange_order_id)
                tracked_order.swap = order_result
                self.c_trigger_event(self.MARKET_BUY_ORDER_CREATED_EVENT_TAG,
                                     BuyOrderCreatedEvent(
                                         self._current_timestamp,
                                         order_type,
                                         trading_pair,
                                         amount,
                                         price,
                                         order_id
                                     ))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.c_stop_tracking_order(order_id)
            self.logger().network(
                f"Error submitting buy order to TEX for {amount} {trading_pair}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit buy order to TEX: {e}"
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp,
                                                         order_id,
                                                         order_type)
                                 )

    async def execute_sell(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType,
                           price: Decimal) -> str:

        try:
            order_result = None
            self.c_start_tracking_order(client_order_id = order_id,
                                        exchange_order_id = None,
                                        trading_pair = trading_pair,
                                        order_type = order_type,
                                        trade_type = TradeType.BUY,
                                        price = price,
                                        amount = amount)

            if order_type is OrderType.LIMIT:
                self.logger().info(f"Placing Limit Order: amount={amount} price={price}")
                order_result = await self.place_order(order_id = order_id,
                                                      trading_pair = trading_pair,
                                                      amount = amount,
                                                      price = price,
                                                      is_buy = False)
            elif order_type is OrderType.MARKET:
                price = self.c_get_price(trading_pair, True)
                shifted_price = price * Decimal(10.0 ** 18)
                self.logger().info(f"Placing Market Order: amount={amount} price={price}")
                order_result = await self.place_order(order_id = order_id,
                                                      trading_pair = trading_pair,
                                                      amount = amount,
                                                      price = price,
                                                      is_buy = False)
            else:
                raise ValueError(f"Invalid OrderType {order_type}. Aborting.")

            exchange_order_id = order_result["id"]
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None and exchange_order_id:
                tracked_order.update_exchange_order_id(exchange_order_id)
                tracked_order.swap = order_result
                self.c_trigger_event(self.MARKET_SELL_ORDER_CREATED_EVENT_TAG,
                                     SellOrderCreatedEvent(
                                         self._current_timestamp,
                                         order_type,
                                         trading_pair,
                                         amount,
                                         price,
                                         order_id
                                     ))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.c_stop_tracking_order(order_id)
            self.logger().network(
                f"Error submitting sell order to TEX for {amount} {trading_pair}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit buy order to TEX: {e}"
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp,
                                                         order_id,
                                                         order_type)
                                 )

    cdef c_cancel(self, str trading_pair, str client_order_id):
        # If there's an ongoing cancel on this order within the expiry time, don't do it again.
        if self._in_flight_cancels.get(client_order_id, 0) > self._current_timestamp - self.CANCEL_EXPIRY_TIME:
            return

        # Maintain the in flight orders list vs. expiry invariant.
        cdef:
            list keys_to_delete = []

        for k, cancel_timestamp in self._in_flight_cancels.items():
            if cancel_timestamp < self._current_timestamp - self.CANCEL_EXPIRY_TIME:
                keys_to_delete.append(k)
            else:
                break
        for k in keys_to_delete:
            del self._in_flight_cancels[k]

        # Record the in-flight cancellation.
        self._in_flight_cancels[client_order_id] = self._current_timestamp

        # Execute the cancel asynchronously.
        safe_ensure_future(self.cancel_order(client_order_id))

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        incomplete_orders = [o for o in self.in_flight_orders.values() if not o.is_done]
        client_order_ids = [o.client_order_id for o in incomplete_orders]
        order_id_set = set(client_order_ids)
        tasks = [self.cancel_order(i) for i in client_order_ids]
        successful_cancellations = []

        try:
            async with timeout(timeout_seconds):
                cancellation_results = await safe_gather(*tasks, return_exceptions=True)
                for cid, cr in zip(client_order_ids, cancellation_results):
                    if isinstance(cr, Exception):
                        continue
                    if isinstance(cr, dict) and cr.get("success") == 1:
                        order_id_set.remove(cid)
                        successful_cancellations.append(CancellationResult(cid, True))
        except Exception as e:
            self.logger().network(
                f"Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg=f"Failed to cancel orders on IDEX. Check Ethereum wallet and network connection."
            )

        failed_cancellations = [CancellationResult(oid, False) for oid in order_id_set]
        return successful_cancellations + failed_cancellations

    cdef c_start_tracking_order(self,
                                str client_order_id,
                                str exchange_order_id,
                                str trading_pair,
                                object trade_type,
                                object order_type,
                                object amount,
                                object price):
        self._in_flight_orders[client_order_id] = TEXInFlightOrder(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount
        )

    cdef c_stop_tracking_order(self, str order_id):
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]

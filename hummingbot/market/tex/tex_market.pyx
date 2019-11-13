from collections import defaultdict
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
from hummingbot.market.tex.tex_operator_api import (get_current_eon)
from hummingbot.market.tex.tex_order_book_tracker import TEXOrderBookTracker
from hummingbot.market.tex.tex_in_flight_order import TEXInFlightOrder
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.transaction_tracker import TransactionTracker
from hummingbot.market.trading_rule cimport TradingRule
from hummingbot.wallet.ethereum.web3_wallet import Web3Wallet
from web3 import Web3
s_logger = None

s_decimal_0 = Decimal(0)
NETWORK= 'RINKEBY'
ETH_RPC_URL = 'https://rinkeby.infura.io/v3/9aed27c49d81418687a11e11aa00be0a'
UPDATE_BALANCES_INTERVAL = 5
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
                 symbols: Optional[List[str]] = None,
                 trading_required: bool = True):

        super().__init__()
        self._trading_required = trading_required
        self._order_book_tracker = TEXOrderBookTracker(data_source_type=order_book_tracker_data_source_type, symbols=symbols)
        self._ev_loop = asyncio.get_event_loop()
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._poll_interval = poll_interval
        self._in_flight_orders = {}
        self._tx_tracker = TEXMarketTransactionTracker(self)
        self._data_source_type = order_book_tracker_data_source_type
        self._status_polling_task = None
        self._order_tracker_task = None
        self._shared_client = None
        self._async_scheduler = AsyncCallScheduler(call_interval=0.5)
        self._last_pull_timestamp = 0
        self.logger().info(f"RPC ->> {ETH_RPC_URL}")
        self._w3 = Web3(Web3.HTTPProvider(ETH_RPC_URL))
        self._wallet = wallet
        self._network_id = int(self._w3.net.version)

    @property
    def name(self) -> str:
        return "tex"

    @property
    def status_dict(self):
        return {
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "order_books_initialized": self._order_book_tracker.ready,
            # TODO: Implement the following
            "account_registered": False,
            "sub_accounts_registered": False,
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

    cdef OrderBook c_get_order_book(self, str symbol):
        cdef:
            dict order_books = self._order_book_tracker.order_books

        if symbol not in order_books:
            raise ValueError(f"No order book exists for '{symbol}'.")
        return order_books[symbol]

    async def _status_polling_loop(self):
        print('starting status polling')
        while True:
            try:
                print('creating poll notifier')
                self._poll_notifier = asyncio.Event()
                print('waiting poll notifier')
                await self._poll_notifier.wait()
                print('gathering')
                await self._update_balances()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"TEX Status Polling Loop Error: {e}")
                self.logger().network(
                    "Unexpected error while fetching account and status updates.",
                    exc_info=True,
                    app_warning_msg=f"Failed to fetch account updates on TEX. Check network connection.")

    async def _register_wallet(self):
        current_eon_number = await get_current_eon()
        print(current_eon_number)
        return ''

    async def _update_balances(self):
        cdef:
            double current_timestamp = self._current_timestamp
        if current_timestamp - self._last_update_balances_timestamp > UPDATE_BALANCES_INTERVAL or len(self._account_balances) > 0:
            available_balances, total_balances = await self._get_lqd_balances()
            self._account_available_balances = available_balances
            self._account_balances = total_balances
            self._last_update_balances_timestamp = current_timestamp

    async def _get_lqd_balances(self):
        return 0, 0

    async def start_network(self):
        print('starting network')
        if self._order_tracker_task is not None:
            self._stop_network()
        await self._register_wallet()
        await self._update_balances()
        self._order_tracker_task = safe_ensure_future(self._order_book_tracker.start())
        self._status_polling_task = safe_ensure_future(self._status_polling_loop())

    def _stop_network(self):
        if self._order_tracker_task is not None:
            self._order_tracker_task.cancel()
            self._status_polling_task.cancel()
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
            int64_t last_tick = <int64_t>(self._last_timestamp / self._poll_interval)
            int64_t current_tick = <int64_t>(timestamp / self._poll_interval)

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

        if order_type is OrderType.LIMIT:
            return TradeFee(percent=Decimal("0.00"))

    cdef object c_get_order_price_quantum(self, str symbol, object price):
        cdef:
            quote_asset_decimals = 18
        decimals_quantum = Decimal(f"1e-{quote_asset_decimals}")
        return decimals_quantum

    cdef object c_get_order_size_quantum(self, str symbol, object amount):
        cdef:
            base_asset_decimals = 18
        decimals_quantum = Decimal(f"1e-{base_asset_decimals}")
        return decimals_quantum

    def quantize_order_amount(self, symbol: str, amount: Decimal, price: Decimal = s_decimal_NaN) -> Decimal:
        return self.c_quantize_order_amount(symbol, amount, price)

    cdef object c_quantize_order_amount(self, str symbol, object amount, object price=s_decimal_0):
        quantized_amount = MarketBase.c_quantize_order_amount(self, symbol, amount)
        actual_price = Decimal(price or self.get_price(symbol, True))
        amount_quote = quantized_amount * actual_price
        return quantized_amount
    # TODO: To be implemented <<<<<<<<<<>>>>>>>>>>
    cdef str c_buy(self, str symbol, object amount, object order_type=OrderType.MARKET, object price=s_decimal_NaN,
                   dict kwargs={}):
        return ''

    cdef str c_sell(self, str symbol, object amount, object order_type=OrderType.MARKET, object price=s_decimal_NaN,
                    dict kwargs={}):
        return ''

    cdef c_cancel(self, str symbol, str order_id):
        return ''

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        return ''

    async def execute_buy(self, symbol: str, amount: Decimal, price: Decimal, order_type: OrderType) -> str:
        return ''

    async def execute_sell(self, symbol: str, amount: Decimal, price: Decimal, order_type: OrderType) -> str:
        return ''

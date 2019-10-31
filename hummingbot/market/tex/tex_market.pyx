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
from hummingbot.market.tex.tedx_api_order_book_data_source import TEXAPIOrderBookDataSource
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
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.market.tex.tex_order_book_tracker import TEXOrderBookTracker
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.transaction_tracker import TransactionTracker
from hummingbot.market.trading_rule cimport TradingRule
from hummingbot.market.tex.tex_in_flight_order import TEXInFlightOrder
from hummingbot.wallet.ethereum.web3_wallet import Web3Wallet
from web3 import Web3

s_logger = None

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
        self._async_scheduler = AsyncCallScheduler(call_interval=0.5)
        self._last_pull_timestamp = 0
        self._w3 = Web3(Web3.HTTPProvider(ethereum_rpc_url))
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
            "account_registered": True,
            "sub_accounts_registered": True,
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
    def in_flight_orders(self) -> Dict[str, BinanceInFlightOrder]:
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

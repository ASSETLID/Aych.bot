#!/usr/bin/env python

import aiohttp
import asyncio
import logging
import pandas as pd
from typing import (
    Dict,
    List,
    Optional
)
import time
from hummingbot.logger import HummingbotLogger
from hummingbot.market.tex.tex_order_book import TEXOrderBook
from hummingbot.core.data_type.order_book_message import TEXOrderBookMessage
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
TEX_REST_URL = "https://rinkeby.liquidity.network"


class TEXAPIOrderBookDataSource(OrderBookTrackerDataSource):

    def __init__(self, symbols: Optional[List[str]] = None):
        super().__init__()
        self._symbols: Optional[List[str]] = symbols
        self._get_tracking_pair_done_event: asyncio.Event = asyncio.Event()

    _baobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._baobds_logger is None:
            cls._baobds_logger = logging.getLogger(__name__)
        return cls._baobds_logger

    @classmethod
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Return all hub's available pairs
        """

        pairs = ["fLQD-fETH", "fLQD-fCO2", "fCO2-fETH", "fFCO-fETH", "fLQD-fFCO", "fFCO-fCO2"]
        Tokens = [{
            "tokenAddress": "0x66b26B6CeA8557D6d209B33A30D69C11B0993a3a",
            "name": "Ethereum",
            "shortName": "ETH"
        }, {
            "tokenAddress": "0xA9F86DD014C001Acd72d5b25831f94FaCfb48717",
            "name": "LQD",
            "shortName": "LQD"
        }, {
            "tokenAddress": "0x773104aA7fF27Abc94e251392a45661fcb4CB302",
            "name": "CarbonCredits",
            "shortName": "CO2"
        }, {
            "tokenAddress": "0xa5022f14E82C18b78B137460333F48a9841Be44e",
            "name": "FedirCoin",
            "shortName": "FCO"
        }]

        data = []
        # TODO: Change implementation + calculate volume/USDVolume
        for trading_pair in pairs:
            if "-" in trading_pair:
                quote_asset = trading_pair.split("-")[1][1:4]
                base_asset = trading_pair.split("-")[0][1:4]
                quote_address = ''
                base_address = ''
                for token in Tokens:
                    if quote_asset == token["shortName"]:
                        quote_address = token["tokenAddress"]
                    if base_asset == token["shortName"]:
                        base_address = token["tokenAddress"]
                    data.append({
                        "market": trading_pair,
                        "baseAsset": base_asset,
                        "quoteAsset": quote_asset,
                        "baseAddress": base_address,
                        "quoteAddress": quote_address,
                        "volume": 1000,
                        "USDVolume": 1000
                    })
        return data

    async def get_trading_pairs(self) -> List[str]:
        if self._symbols is None:
            pairList = []
            active_markets = await self.get_active_exchange_markets()
            for pairs in active_markets:
                pairList.append(pairs["market"])
            trading_pairs: List[str] = pairList
            self._symbols = trading_pairs
        else:
            trading_pairs: List[str] = self._symbols
        return trading_pairs

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            retval: Dict[str, OrderBookTrackerEntry] = {}
            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair)
                    snapshot_timestamp: float = time.time()
                    snapshot_msg: TEXOrderBookMessage = TEXOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        {"symbol": trading_pair}
                    )
                    tex_order_book: OrderBook = self.order_book_create_function()
                    print(snapshot_msg)
                    tex_order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)

                    retval[trading_pair] = OrderBookTrackerEntry(
                        trading_pair,
                        snapshot_timestamp,
                        tex_order_book,
                    )

                    self.logger().info(f"Initialized order book for {trading_pair}. "
                                       f"{index+1}/{number_of_pairs} completed.")
                    await asyncio.sleep(1.0)
                except Exception:
                    self.logger().error(f"Error initializing order book for {trading_pair}.", exc_info=True)
                    await asyncio.sleep(5.0)
            return retval

    async def get_snapshot(self, client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, any]:
        active_markets = await self.get_active_exchange_markets()
        base_address = ''
        quote_address = ''
        for pair in active_markets:
            if trading_pair == pair["market"]:
                base_address = pair["baseAddress"]
                quote_address = pair["quoteAddress"]

        async with client.get(f"{TEX_REST_URL}/audit/swaps/{base_address}/{quote_address}") as response:
            print(base_address, quote_address)
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching TEX market snapshot for {trading_pair}. " f"HTTP status is {response.status}.")
            print(response)
            data: Dict[str, any] = await response.json()
            return data

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        # Not implemented yet
        pass

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        # Tex's API do not provide order book diffs yet. (i.e only snapshots)
        pass

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with aiohttp.ClientSession() as client:
                    for trading_pair in trading_pairs:
                        try:
                            snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: TEXOrderBookMessage = TEXOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                snapshot_timestamp,
                                {"symbol": trading_pair}
                            )
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair} at {snapshot_timestamp}")
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except IOError:
                            self.logger().network(
                                f"Error getting snapshot for {trading_pair}.",
                                exc_info=True,
                                app_warning_msg=f"Error getting snapshot for {trading_pair}. Check network connection."
                            )
                            await asyncio.sleep(5.0)
                        except Exception:
                            self.logger().error(f"Error processing snapshot for {trading_pair}.", exc_info=True)
                            await asyncio.sleep(5.0)
                    await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error listening for order book snapshot.",
                    exc_info=True,
                    app_warning_msg=f"Unexpected error listening for order book snapshot. Check network connection."
                )
                await asyncio.sleep(5.0)

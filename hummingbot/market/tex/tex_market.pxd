from libc.stdint cimport int64_t

from hummingbot.market.market_base cimport MarketBase
from hummingbot.core.data_type.transaction_tracker cimport TransactionTracker


cdef class TEXMarket(MarketBase):
    cdef:
        object _tex_client
        object _ev_loop
        object _poll_notifier
        double _last_timestamp
        double _poll_interval
        double _last_pull_timestamp
        dict _in_flight_deposits
        dict _in_flight_orders
        dict _order_not_found_records
        TransactionTracker _tx_tracker
        dict _withdraw_rules
        dict _trading_rules
        dict _trade_fees
        double _last_update_trade_fees_timestamp
        object _data_source_type
        public object _status_polling_task
        public object _order_tracker_task
        public object _trading_rules_polling_task
        object _async_scheduler
        object _set_server_time_offset_task
        object _w3
        object _wallet
        object _network_id
        double _last_update_balances_timestamp
        object _shared_client

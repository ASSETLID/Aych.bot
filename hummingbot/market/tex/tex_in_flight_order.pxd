from hummingbot.market.in_flight_order_base cimport InFlightOrderBase

cdef class TEXInFlightOrder(InFlightOrderBase):
    cdef:
        public object trade_id_set

from typing import (List)


class LQDEon():
    def __init__(self, transfers: List, deposits: List, withdrawals: List, merkle_proof):
        self._transfers = transfers
        self._deposits = deposits
        self._withdrawals = withdrawals
        self._merkle_proof = merkle_proof

    @property
    def transfers(self) -> List:
        return self._transfers

    @property
    def deposits(self):
        return self._deposits

    @property
    def withdrawals(self):
        return self._withdrawals

    @property
    def merkle_proof(self):
        return self._merkle_proof

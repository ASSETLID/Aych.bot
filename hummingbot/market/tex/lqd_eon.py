from typing import (List)


class LQDEon():
    def __init__(self, transfers: List, deposits: List, withdrawals: List, merkle_proof, eon_number: int):
        self._transfers = transfers
        self._deposits = deposits
        self._withdrawals = withdrawals
        self._merkle_proof = merkle_proof
        self._eon_number = eon_number

    @property
    def transfers(self) -> List:
        return self._transfers

    @property
    def deposits(self) -> List:
        return self._deposits

    @property
    def withdrawals(self) -> List:
        return self._withdrawals

    @property
    def merkle_proof(self):
        return self._merkle_proof

    @property
    def eon_number(self) -> int:
        return self._eon_number

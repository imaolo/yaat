from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from yaat.maester import Maester

class Trader:
    def __init__(self, maester: Maester):
        self.maester = maester
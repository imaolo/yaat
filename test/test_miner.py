from yaat.miner import Miner
import unittest

class TestMiner(unittest.TestCase):
    def setUp(self): self.miner = Miner(None)

    def test_mine_simple(self):
        self.miner.mine()
from yaat.util import myprint
from yaat.maester import Maester
from yaat.miner import Miner
from datetime import datetime

if __name__ == '__main__':
    miner = Miner(Maester(connstr=None, dbdir='yaatdb_local'))

    syms = ['SPY', 'XLK', 'XLV', 'XLY', 'IBB', 'XLF', 'XLP', 'XLE', 'XLU', 'XLI','XLB']
    inserted = miner.mine_alpha(60, datetime(2010, 1, 1), datetime(2020, 1, 1), syms)
    myprint("inserted", inserted)
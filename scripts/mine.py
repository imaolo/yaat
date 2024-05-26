from yaat.util import myprint
from yaat.maester import Maester
from yaat.miner import Miner
from datetime import datetime

if __name__ == '__main__':
    miner = Miner(Maester())

    syms = ['SPY', 'XLK', 'XLV', 'XLY', 'IBB', 'XLF', 'XLP', 'XLE', 'XLU', 'XLI','XLB']
    inserted = miner.mine_alpha(1, datetime(2005, 1, 1), datetime(2024, 1, 1), syms)
    for ins in inserted: print(ins)
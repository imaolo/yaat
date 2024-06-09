from yaat.util import DEBUG
from yaat.maester import Maester, TimeRange, DATE_FORMAT
import argparse

### common usage ### 

# nohup python3.11 main.py mine --start 2005-1-1 --end 2020-1-1 --symbols SPY XLK XLV XLY IBB XLF XLP XLE XLU XLI XLB --freq_min 1 --db_connstr localhost:27017 > mine.log 2>&1 &

### usage ### 

# main parser
main_parser = argparse.ArgumentParser(description='[YAAT] Yet Another Automated Trader')
main_subparser = main_parser.add_subparsers(dest='cmd', required=True, help='yaat command help')

# commands
mine_parser = main_subparser.add_parser(n:='mine', help=f"{n} command help")

# mine command arguments
mine_parser.add_argument('--symbols', nargs='+', required=True, help='tickers to mine in form "sym1 sym2 sym3')
mine_parser.add_argument('--start', type=TimeRange.clean_date, required=True, help=f"start date in the format {DATE_FORMAT}")
mine_parser.add_argument('--end', type=TimeRange.clean_date, required=True, help=f"end date in the format {DATE_FORMAT}")
mine_parser.add_argument('--freq_min', type=int, default=60, help=f"frequency in minutes between tickers")
mine_parser.add_argument('--db_connstr', default=None, help=f"connection string for the database")
mine_parser.add_argument('--db_dir', default=None, help=f"directory for the database")

if __name__ == '__main__':

    args = main_parser.parse_args()

    if args.cmd == 'mine':
        if DEBUG: print(f"mining {args.symbols} from {args.start} to {args.end}")
        m = Maester(args.db_connstr, args.db_dir)
        for sym in args.symbols:
            if DEBUG: print(f"mining {sym}")
            m.alpha_mine(args.start, args.end, sym, args.freq_min)
    else:
        raise RuntimeError(f"improper arguemnts {args}")
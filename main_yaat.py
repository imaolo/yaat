from yaat.maester import Maester
from yaat.util import getenv, myprint, filesz
import argparse
from tabulate import tabulate

ROOT = getenv('ROOT', "data")

main_parser = argparse.ArgumentParser(description='[YAAT] Yet Another Automated Trader')
main_subparser = main_parser.add_subparsers(dest='cmd', required=True, help='Sub-command help')

maester_parser = main_subparser.add_parser(n:='maester', help=f'{n} command help. list datasets and models')
maester_parser.add_argument('--mode', type=str, default='datasets', help='either datasets or models')

args = main_parser.parse_args()

if args.cmd == "maester":
    assert args.mode in {'models', 'datasets'}
    myprint(args.mode)

    maester = Maester(ROOT)
    cols = []
    if args.mode == 'datasets':
        for k, v in getattr(maester, args.mode).items():
            cols.append([k, filesz(v.dataset.fp), v.status.data])
        print(tabulate(cols, headers=['name', 'size', 'status']))
    else:
        for k, v in getattr(maester, args.mode).items():
            cols.append([k, filesz(v.dataset.fp)])
        print(tabulate(cols, headers=['name', 'size']))
import yaat, signal, sys

signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(0))
if __name__ == '__main__':
    yaat.run()
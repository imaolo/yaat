from typing import Any, Callable
import os, time

def getenv(key:str, default:Any=0): return type(default)(int(os.getenv(key, default)) if isinstance(default, bool) else os.getenv(key, default))
def wait_until_true(fn: Callable[[], bool], timeout:float=30.0, pause:float=5.0, msg:str=''):
    end = time.time() + timeout
    while not fn():
        if time.time() >= end:
            raise TimeoutError(f"Timeout exceeded {timeout=}, {pause=}, {msg=}")
        time.sleep(pause)
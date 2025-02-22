from typing import Any, Callable
import os, time

def getenv(key:str, default:Any=0): return type(default)(int(os.getenv(key, default)) if isinstance(default, bool) else os.getenv(key, default))
def wait_until_true(fn: Callable[[], bool], timeout:int=30, pause:int=5):
    end = time.time() + timeout
    while not fn():
        if time.time() >= end:
            raise TimeoutError("Timeout exceeded")
        time.sleep(pause)
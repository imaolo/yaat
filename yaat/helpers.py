from typing import Any, Callable
from dotenv import load_dotenv
from requests import get
import os, time, httpx

load_dotenv()

def urlQueryString(**kwargs) -> str: return '?'+''.join(map(lambda kv: kv[0] + '=' + str(kv[1]) + '&', kwargs.items()))[:-1]
async def fetchjson(url: str, headers: dict | None = None):
    async with httpx.AsyncClient() as client:
        return (await client.get(url, headers=headers)).json()
def getenv(key:str, default:Any=0): return type(default)(int(os.getenv(key, default)) if isinstance(default, bool) else os.getenv(key, default))
def wait_until_true(fn: Callable[[], bool], timeout:float=30.0, pause:float=5.0, msg:str=''):
    end = time.time() + timeout
    while not fn():
        if time.time() >= end:
            raise TimeoutError(f"Timeout exceeded {timeout=}, {pause=}, {msg=}")
        time.sleep(pause)
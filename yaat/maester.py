from dataclasses import dataclass, asdict
import dropbox, json

@dataclass(frozen=True)
class Navigator:
    def __new__(cls, *args, **kwargs): raise TypeError(f"{cls.__name__} is static")
    models:str = './models'
    data:str = './data'

@dataclass(frozen=True)
class ModelEntry:
    dir_name: str
    status_fn:str
    weights_fn: str
    args_fn: str
    args: dict

    @classmethod
    def from_json(cls, obj:str) -> 'ModelEntry': return cls(**json.loads(obj))
    def to_json(self) -> str: return json.dumps(asdict(self))

@dataclass(frozen=True)
class DataEntry:
    fn: str

    @classmethod
    def from_csv(cls, fn) -> 'DataEntry': pass
    def to_csv(self) -> str: pass

class Maester:
    def __init__(self, key):
        self.dbx = dropbox.Dropbox(key)
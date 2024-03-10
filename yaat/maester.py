from yaat.util import path
from typing import List, Any, Dict, Optional
from enum import Enum, auto
from dropbox import Dropbox, files
from pandas import DataFrame
import json, os

class Entry:
    root:str = 'entries'
    class Status(Enum): created = auto(); running = auto(); finished = auto(); error = auto()

    def __init__(self):
        Maester.create_folder(self.root)
        self._attrs, self._readonly_attrs, self._dir_attrs, self._append_attrs, self._data_attrs = (set() for _ in range(5))
        self.exists_ok = True # hack

    def __setattr__(self, key:str, val:Any):
        if hasattr(self, '_attrs') and key in self._attrs:
            if key in self._readonly_attrs: assert not hasattr(self, key), key
            elif key in self._dir_attrs: Maester.create_folder(path(type(self).root, val), exists_ok=self.exists_ok)
            elif key in self._append_attrs: Maester.append_file(path(type(self).root, key), val)
            elif key in self._data_attrs: pass # TODO set
        super().__setattr__(key, val)

    def __getattr__(self, key) -> Any:
        if key not in self.__dict__: raise AttributeError
        # TODO retrieve
        return self.__dict__[key]

    def regattr(self, key:str, val:Any, readonly:bool=False, append:bool=False, is_dir:bool=False, \
                is_data:bool=False, type:Optional[str]=None, exists_ok:bool=True) -> Any:
        self._attrs.add(key)
        if append: self._append_attrs.add(key)
        if is_dir: self._dir_attrs.add(key)
        if is_data: self._data_attrs.add(key)
        if readonly: self._readonly_attrs.add(key)
        exists_ok_prev, self.exists_ok = self.exists_ok, exists_ok
        setattr(self, key, val)
        self.exists_ok = exists_ok_prev
        return getattr(self, key)

    def set_error(self, errm:Optional[str]): pass # TODO
    def to_json(self) -> str: pass # TODO
    def from_csv(self, paths:str) -> List[DataFrame]: pass # TODO return pandas.read_csv(path)

    @staticmethod
    def to_pddf(paths: List[str]): pass # TODO

    @classmethod
    def from_json(cls, jsond:str) -> 'Entry': return cls(**json.loads(jsond))

class ModelEntry(Entry):
    root:str = 'models'
    def __init__(self, name:str, args:Dict[str, str | int], status:Entry.Status = Entry.Status.created, weights: Optional[Any]=None):
        super().__init__()
        self.name = self.regattr('name', name, readonly=True, is_dir=True)
        self.args = self.regattr('args', args, readonly=True, type='json')
        self.status = self.regattr('status', status, append=True, type='text')
        self.weights = self.regattr('weights', weights, is_data=True)

class DataEntry(Entry):
    root:str = 'data_entries'
    def __init__(self, name:str, data:Any):
        super().__init__()
        self.name = self.regattr('name', name, readonly=True, is_dir=True)
        self.data = self.regattr('data', data, readonly=True, is_data=True, type='csv')

class _Maester:
    def __init__(self, dbx:Optional[Dropbox]=None, local:str='data'):
        assert dbx or local
        self.dbx, self.local = dbx, local
        self.create_folder(ModelEntry.root)
        self.create_folder(DataEntry.root)

    def create_folder(self, fp: str, exists_ok:bool=True):
        def _create_folder(call, arg, err):
            try: call(arg)
            except err as e:
                if exists_ok: return
                raise e
        if self.dbx: _create_folder(self.dbx.files_create_folder_v2, fp, files.CreateFolderError)
        if self.local: _create_folder(os.mkdir, path(self.local, fp), FileExistsError)

    def append_file(self, fp: str, data:str):
        if self.dbx: pass # TODO
        if self.local:
            with open(path(self.local, fp), 'a') as f: f.write(data)

    # TODO - rest of maester...
Maester = _Maester() # TODO env vars
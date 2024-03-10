from yaat.util import path, getenv
from typing import List, Any, Dict, Optional
from enum import Enum, auto
from dropbox import Dropbox, files
import os, sys

class Entry:
    def_mem_threshold:int = 10e6
    root:str = 'entries'
    class Status(Enum): created = auto(); running = auto(); finished = auto(); error = auto()

    def __init__(self, name, mem_th:int=def_mem_threshold):
        Maester.create_folder(self.root)
        self._attrs, self._readonly_attrs, self._dir_attrs, self._append_attrs = (set() for _ in range(4))
        self.name, self.status, self.mem_th = name, self.Status.created, mem_th
        self.exists_ok = True # hack

    def __setattr__(self, key:str, val:Any):
        if hasattr(self, '_attrs') and key in self._attrs:
            val = str(val)
            if key in self._readonly_attrs: assert not hasattr(self, key), f"{self.__dict__}. \n\n {key}"
            elif key in self._dir_attrs: Maester.create_folder(path(type(self).root, val), exists_ok=self.exists_ok)
            elif key in self._append_attrs: Maester.append_file(path(type(self).root, key), val)
            else: Maester.write_file(path(type(self).root, key), val)
            if sys.getsizeof(val) < self.mem_th: super().__setattr__(key, val)
            return
        super().__setattr__(key, val)

    def __getattr__(self, key) -> Any:
        def _super():
            if key not in self.__dict__: raise AttributeError
            return self.__dict__[key]
        if '_attrs' not in self.__dict__ or key not in self.__dict__['_attrs']: return _super()
        if key not in self.__dict__:
            data = Maester.get_file(path(type(self).root, key))
            if data: return data
        return _super()

    def regattr(self, key:str, val:Any, readonly:bool=False, append:bool=False, \
                is_dir:bool=False, type:Optional[str]=None, exists_ok:bool=True) -> Any:
        self._attrs.add(key)
        if append: self._append_attrs.add(key)
        if is_dir: self._dir_attrs.add(key)
        if readonly: self._readonly_attrs.add(key)
        exists_ok_prev, self.exists_ok = self.exists_ok, exists_ok
        setattr(self, key, val)
        self.exists_ok = exists_ok_prev
        return getattr(self, key)

    def set_error(self, errm:Optional[str]):
        self.status = self.Status.error
        Maester.write_file(path(self.root, self.name+"_error"), errm)

class ModelEntry(Entry):
    root:str = 'models'
    def __init__(self, name:str, args:Dict[str, str | int], status:Entry.Status = Entry.Status.created, weights: Optional[Any]=None):
        super().__init__(name)
        self.name = self.regattr('name', name, readonly=True, is_dir=True)
        self.args = self.regattr('args', args, readonly=True, type='json')
        self.status = self.regattr('status', status, append=True, type='text')
        self.weights = self.regattr('weights', weights, write=True)

class DataEntry(Entry):
    root:str = 'data_entries'
    def __init__(self, name:str, data:Any):
        super().__init__(name)
        self.name = self.regattr('name', name, readonly=True, is_dir=True)
        self.data = self.regattr('data', data, readonly=True, is_data=True, type='csv')

class _Maester:
    def __init__(self, local:str='data'):
        self.local = local
        self.create_folder(local)
        self.create_folder(Entry.root)
        self.create_folder(ModelEntry.root)
        self.create_folder(DataEntry.root)

    def create_folder(self, fp: str, exists_ok:bool=True):
        if os.path.isdir(fp:=path(self.local, fp)) and exists_ok: return
        os.mkdir(fp)

    def append_file(self, fp: str, data:str):
        with open(path(self.local, fp), 'a') as f: f.write(data)

    def write_file(self, fp:str, data:str):
        with open(path(self.local, fp), 'w') as f: f.write(data)

    def get_file(self, fp:str) -> str:
        if not os.path.exists(path(self.local, fp)): return None
        with open(path(self.local, fp), 'r') as f: return f.read()
    # TODO - rest of maester...
Maester = _Maester(getenv('LOCDIR', "data", req=False)) # TODO env vars
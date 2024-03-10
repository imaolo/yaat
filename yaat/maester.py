from yaat.util import path, getenv
from typing import Any, Dict, Optional
from enum import Enum, auto
import os, sys

class Entry:
    def_mem_threshold:int = 10e6
    root:str = 'entries'
    class Status(Enum): created = auto(); running = auto(); finished = auto(); error = auto()

    def __init__(self, name:str, status:Status=Status.created, mem_th:int=def_mem_threshold):
        self._attrs, self._readonly_attrs, self._dir_attrs, self._append_attrs = (set() for _ in range(4))
        self.mem_th = mem_th
        self.exists_ok = True # hack
        self.regattr('name', name, is_dir=True)
        self.regattr('status', status.name, append=True, type='text')

    def __setattr__(self, key:str, val:Any):
        if hasattr(self, '_attrs') and key in self._attrs:
            val = str(val)
            p = path(Maester.root, type(self).root)
            if key in self._readonly_attrs: assert not hasattr(self, key), f"{self.__dict__}. \n\n {key}"
            elif key in self._dir_attrs: Maester.create_folder(path(p, val), exists_ok=self.exists_ok)
            elif key in self._append_attrs: Maester.append_file(path(p, key), val)
            else: Maester.write_file(path(p, key), val)
            if sys.getsizeof(val) < self.mem_th: super().__setattr__(key, val)
            return
        super().__setattr__(key, val)

    def __getattr__(self, key) -> Any:
        def _super_getattr():
            if key not in self.__dict__: raise AttributeError
            return self.__dict__[key]
        if '_attrs' not in self.__dict__ or key not in self.__dict__['_attrs']: return _super_getattr()
        if key not in self.__dict__:
            if data:=Maester.get_file(path(Maester.root, type(self).root, key)): return data
        return _super_getattr()

    def regattr(self, key:str, val:Any, readonly:bool=False, append:bool=False, \
                is_dir:bool=False, type:Optional[str]=None, exists_ok:bool=True):
        self._attrs.add(key)
        if append: self._append_attrs.add(key)
        if is_dir: self._dir_attrs.add(key)
        if readonly: self._readonly_attrs.add(key)
        exists_ok_prev, self.exists_ok = self.exists_ok, exists_ok
        setattr(self, key, val)
        self.exists_ok = exists_ok_prev

    def set_error(self, errm:Optional[str]):
        self.status = self.Status.error.name
        Maester.write_file(path(Maester.root, self.root, self.name+"_error"), errm)

class ModelEntry(Entry):
    root:str = 'models'
    def __init__(self, name:str, args:Dict[str, str | int], status:Entry.Status=Entry.Status.created, weights: Optional[Any]=None):
        super().__init__(name, status)
        self.regattr('args', args, readonly=True, type='json')
        self.regattr('weights', weights)

class DataEntry(Entry):
    root:str = 'data'
    def __init__(self, name:str, data:Any, status:Entry.Status=Entry.Status.created):
        super().__init__(name, status)
        self.regattr('data', data, readonly=True, type='csv')

class _Maester:
    def __init__(self, root:str='data'):
        self.root = root
        self.create_folder(p:=self.root)
        self.create_folder(path(p, Entry.root))
        self.create_folder(path(p, ModelEntry.root))
        self.create_folder(path(p, DataEntry.root))

    def create_folder(self, fp: str, exists_ok:bool=True):
        if os.path.isdir(fp) and exists_ok: return
        os.mkdir(fp)

    def append_file(self, fp: str, data:str):
        with open(fp, 'a') as f: f.write(data)

    def write_file(self, fp:str, data:str):
        with open(fp, 'w') as f: f.write(data)

    def get_file(self, fp:str, req=False) -> Optional[str]:
        if not req and not os.path.isfile(fp): return None
        with open(fp, 'r') as f: return f.read()
    # TODO - rest of maester...
Maester = _Maester(getenv('ROOT', "data", req=False))
from yaat.util import path, getenv, exists, rm
from typing import Any, Dict, Optional
from enum import Enum, auto
import os, sys

class Attribute:
    def __init__(self, fp:str, data:Optional[Any]=None, mem_th:int=getenv("MEMTH", 10e6), \
                 readonly:bool=False, append:bool=False, is_dir:bool=False, exist_ok:bool=True) -> None:
        self.fp, self.mem_th = fp, mem_th
        self.readonly, self.append, self.is_dir, self.exist_ok = readonly, append, is_dir, exist_ok
        self._data = str(data) if data and sys.getsizeof(data) < mem_th else None

        if readonly: assert not exists(fp), f"{os.listdir(path(*self.fp.split('/')[:-1]))}, {self.fp}"
        if is_dir: return os.makedirs(self.fp, exist_ok=exist_ok)

        os.makedirs(path(*self.fp.split('/')[:-1]), exist_ok=exist_ok)
        with open(self.fp, 'a' if append else 'w') as f: f.write(str(data))

    @property
    def data(self) -> Any:
        if self._data: return str(self._data)
        with open(self.fp, 'r') as f:
            if sys.getsizeof(data:=f.read()) < self.mem_th: self._data = data
            return str(data)

    @data.setter
    def data(self, data: Any):
        assert not (self.readonly or self.is_dir)
        with open(self.fp, 'a' if self.append else 'w') as f: f.write(str(data))
        self._data = str(data) if data and sys.getsizeof(data) < self.mem_th else None

    @data.deleter
    def data(self): rm(self.fp); self._data = None

    def __getstate__(self): state = self.__dict__.copy(); state['_data'] = None; return state
    
    def __setstate__(self, state): self.__dict__.update(state); return state
# TODO
# class Entry:
#     def_mem_threshold:int = 10e6
#     root:str = 'entries'
#     class Status(Enum): created = auto(); running = auto(); finished = auto(); error = auto()

#     def __init__(self, fp:str, mem_th:int=def_mem_threshold):
#         self.fp, self.mem_th = path(fp, type(self).root), mem_th
#         Maester.create_folder(self.fp, exists_ok=False)
#         self.status = Attribute(path(self.fp, 'status'), self.Status.created.name, append=True)

# class ModelEntry(Entry):
#     root:str = 'models'
#     # def __init__(self, name:str, args:Dict[str, str | int], status:Entry.Status=Entry.Status.created, weights: Optional[Any]=None):
#     #     super().__init__(name, status)
#     #     self.regattr('args', args, readonly=True)
#     #     self.regattr('weights', weights)

# class DataEntry(Entry):
#     root:str = 'data'
#     # def __init__(self, name:str, data:Any, status:Entry.Status=Entry.Status.created):
#     #     super().__init__(name, status)
#     #     self.regattr('data', data, readonly=True)

# class _Maester:
#     def __init__(self, root:str='data', mem_th:int=10e6):
#         self.root, self.mem_th = root, mem_th
#         if not os.path.isdir(root): os.mkdir(root)
#         self.create_folder(Entry.root)
#         self.create_folder(ModelEntry.root)
#         self.create_folder(DataEntry.root)
    
#     def create_entry(self, name:str): pass
    
#     def exists(self, fp:str) -> bool: return os.path.isdir(fp) or os.path.isfile(fp)

#     def create_folder(self, fp: str, exists_ok:bool=True):
#         if os.path.isdir(fp:=path(self.root, fp)) and exists_ok: return
#         os.mkdir(fp)

#     def append_file(self, fp: str, data:str):
#         with open(path(self.root, fp), 'a') as f: f.write(data)

#     def write_file(self, fp:str, data:str):
#         with open(path(self.root, fp), 'w') as f: f.write(data)

#     def get_file(self, fp:str, req=False) -> Optional[str]:
#         if not req and not os.path.isfile(fp): return None
#         with open(fp, 'r') as f: return f.read()
#     # TODO - rest of maester...
# Maester = _Maester(root=getenv('ROOT', "data"), mem_th=getenv("MEMTH", 10e6))

    # def __setattr__(self, key:str, val:Any):
    #     if hasattr(self, '_attrs') and key in self._attrs:
    #         val = str(val)
    #         p = path(Maester.root, type(self).root)
    #         if key in self._readonly_attrs: assert not hasattr(self, key), f"{self.__dict__}. \n\n {key}"
    #         elif key in self._dir_attrs: Maester.create_folder(path(type(self).root, val), exists_ok=self.exists_ok)
    #         elif key in self._append_attrs: Maester.append_file(path(p, key), val)
    #         else: Maester.write_file(path(p, key), val)
    #         if sys.getsizeof(val) < self.mem_th: super().__setattr__(key, val if sys.getsizeof(val) < self.mem_th else None)
    #     super().__setattr__(key, val)

    # def __getattr__(self, key) -> Any:
    #     def _super_getattr():
    #         if key not in self.__dict__: raise AttributeError
    #         return self.__dict__[key]
    #     if '_attrs' not in self.__dict__ or key not in self.__dict__['_attrs']: return _super_getattr()
    #     if key not in self.__dict__:
    #         if data:=Maester.get_file(path(Maester.root, type(self).root, key)): return data
    #     return _super_getattr()

    # def regattr(self, key:str, val:Any, readonly:bool=False, append:bool=False, \
    #             is_dir:bool=False, exists_ok:bool=True):
    #     self._attrs.add(key)
    #     if append: self._append_attrs.add(key)
    #     if is_dir: self._dir_attrs.add(key)
    #     if readonly: self._readonly_attrs.add(key)
    #     exists_ok_prev, self.exists_ok = self.exists_ok, exists_ok
    #     setattr(self, key, val)
    #     self.exists_ok = exists_ok_prev



    # def set_error(self, errm:Optional[str]):
    #     self.status = self.Status.error.name
    #     Maester.write_file(path(Maester.root, self.root, self.name+"_error"), errm)
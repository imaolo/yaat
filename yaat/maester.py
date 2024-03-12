from yaat.util import getenv, rm, write, read, siblings, leaf, append, path, parent, objsz, mkdirs
from typing import Any, Optional, Type, Callable
from enum import Enum, auto

ROOT = getenv('ROOT', "data")
MEMTH_ENTRY = getenv('MEMTH_ENTRY', 500e6)
MEMTH_ATTR = MEMTH_ENTRY

class AttributeBuffer:

    def __set_name__(self, owner:Type['Attribute'], name:str):
        self.obj, self.pname = None, '_'+name
        setattr(owner, self.pname, None)

    def __get__(self, obj:Optional['Attribute']=None, objtype=None) -> Any:
        if not self.obj or obj: self.obj = obj
        assert self.obj

        if buf:=getattr(self.obj, self.pname): return buf
        if not obj: return self.obj.reader(self.obj.fp)

        setattr(self, self.pname, data if objsz(data:=self.obj.reader(self.obj.fp)) < self.obj.mem_th else None)
        return data

    def __set__(self, obj:'Attribute', val:Any):
        assert not obj.readonly and not obj.appendonly, f"invalid write, try append +=, {obj.readonly=}, {obj.appendonly=}"
        setattr(obj, self.pname, val if objsz(val) < obj.mem_th else None)
        obj.writer(obj.fp, val)

    def __iadd__(self, val:Any):
        assert self.obj.appender and not self.obj.readonly, f"invalid append, {self.obj.appender=}, {self.obj.readonly=}"
        self.obj.appender(self.obj.fp, val)
        setattr(self.obj, self.pname, self.obj.reader(self.obj.fp))
        return getattr(self.obj, self.pname)

    def __delete__(self, obj:'Attribute'): rm(obj.fp); del obj._buf

class Attribute:
    buf: AttributeBuffer = AttributeBuffer()
    def __init__(self, fp:str, data:Any, mem_th:int=MEMTH_ATTR, readonly:bool=False, appendonly:bool=False, \
                 writer:Callable=write, reader:Callable=read, appender:Optional[Callable]=append) -> None:
        assert not (l:=leaf(fp)) in (s:=siblings(fp)), f"{l} cannot exist, directory {parent(fp)} contents: {s}"
        self.fp, self.mem_th, self.readonly, self.appendonly = fp, mem_th, False, False
        self.writer, self.reader, self.appender = writer, reader, appender

        # write, then configure
        self.buf, self.readonly, self.appendonly = data, readonly, appendonly

    def __getstate__(self): state = self.__dict__.copy(); state['buf'] = None; return state
    def __setstate__(self, state): self.__dict__.update(state); return state

# TODO
# class Entry:
#     root:str = 'entries'
#     class Status(Enum): created = auto(); running = auto(); finished = auto(); error = auto()
#     def __init__(self, fp:str, mem_th:int=MEMTH_ENTRY):
#         self.fp = fp; mkdirs(fp, False)
#         self.status = Attribute(path(fp, 'status'), data=self.Status.created.name)
#         self.num_error = 0

#     def set_error(self, errm:str):
#         self.status.data = self.Status.error.name
#         self.error = Attribute(path(self.fp, 'error_'+self.num_error), data=errm)

# TODO
# class Entry:
#     def_mem_threshold:int = 10e6
#     root:str = 'entries's
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
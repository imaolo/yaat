from yaat.util import getenv, rm, write, read, siblings, leaf, append, path, parent, objsz, mkdirs, filesz, dict2str
from typing import Any, Optional, Type, Callable, Dict
from enum import Enum, auto
import torch

ROOT = getenv('ROOT', "data")
ENTRY_MEM = getenv('ENTRY_MEM', 500e6)
ATTR_MEM = ENTRY_MEM

class AttributeBuffer:
    def __set_name__(self, owner:Type['Attribute'], name:str):
        self.obj, self.pname = owner, '_'+name
        self.set_cache(None)

    def set_cache(self, val:Any): setattr(self.obj, self.pname, val)

    def __get__(self, obj:'Attribute', objtype:Type['Attribute']) -> Any:
        assert obj; self.obj = obj
        self.set_cache(obj.reader(obj.fp) if filesz(obj.fp) < obj.mem else None)
        return self

    def __set__(self, obj:'Attribute', val:Any):
        if isinstance(val, AttributeBuffer): return self # append already happened
        assert obj; self.obj = obj
        assert not obj.readonly and not obj.appendonly, f"invalid write, try append +=, {obj.readonly=}, {obj.appendonly=}"
        self.set_cache(val if objsz(val) < obj.mem else None)
        obj.writer(obj.fp, val)

    def __iadd__(self, val:Any):
        assert self.obj.appender and not self.obj.readonly, f"invalid append, {self.obj.appender=}, {self.obj.readonly=}"
        self.obj.appender(self.obj.fp, '\n'+val)
        self.set_cache(self.obj.reader(self.obj.fp) if filesz(self.obj.fp) < self.obj.mem else None)
        return self

    def __delete__(self, obj:'Attribute'): delattr(obj, self.pname)

class Attribute:
    # read & delete data, write & append buf

    buf: AttributeBuffer = AttributeBuffer()
    def __init__(self, fp:str, data:Any, mem:int=ATTR_MEM, readonly:bool=False, appendonly:bool=False, \
                 writer:Callable=write, reader:Callable=read, appender:Optional[Callable]=append) -> None:
        assert not (l:=leaf(fp)) in (s:=siblings(fp)), f"{l} cannot exist, directory {parent(fp)} contents: {s}"
        self.fp, self.mem, self.readonly, self.appendonly = fp, mem, False, False
        self.writer, self.reader, self.appender = writer, reader, appender

        # write, then configure
        self.buf, self.readonly, self.appendonly = data, readonly, appendonly

    @property
    def data(self) -> Any: return self._buf if self._buf else self.reader(self.fp)

    @data.deleter
    def data(self): rm(self.fp); del self.buf

    def __getstate__(self): state = self.__dict__.copy(); state['buf'] = None; return state
    def __setstate__(self, state): self.__dict__.update(state); return state

class Entry:
    class Status(Enum): created = auto(); running = auto(); finished = auto(); error = auto()
    def __init__(self, fp:str, mem:int=ENTRY_MEM):
        self.fp, self.mem = fp, mem
        mkdirs(fp, exist_ok=False)
        self.status = Attribute(path(fp, 'status'), data=self.Status.created.name, appendonly=True, mem=mem)
        self.num_err = 0

    def set_error(self, errm:str):
        self.status.buf += self.Status.error.name
        self.error = Attribute(path(self.fp, 'error_'+str(self.num_err)), data=errm, mem=self.mem)
        self.num_err += 1

class ModelEntry(Entry):
    def __init__(self, fp:str, args:Dict[str, str | int], weights: Optional[Any], mem:int=ENTRY_MEM):
        super().__init__(fp, mem)
        self.mem = self.mem
        self.args = Attribute(path(fp, 'args'), dict2str(args), readonly=True, mem=mem)
        self.weights = Attribute(path(fp, 'weights'), weights, reader=torch.load, writer=lambda fp, val: torch.save(val, fp), mem=mem)

class DataEntry(Entry):
    def __init__(self, fp:str, data:Any):
        super().__init__(fp)
        self.data = Attribute(path(fp, 'data'), data, readonly=True)


# class Maester:
#     def __init__(self, fp:str, mem:int=10e6):
#         self.root, self.mem = root, mem
#         if not os.path.isdir(root): os.mkdir(root)
#         self.create_folder(Entry.root)
#         self.create_folder(ModelEntry.root)
#         self.create_folder(DataEntry.root)
#     # TODO - rest of maester...
# Maester = _Maester(root=getenv('ROOT', "data"), mem=getenv("MEMTH", 10e6))
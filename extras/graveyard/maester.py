from yaat.util import getenv, rm, write, read, siblings, leaf, path, parent, objsz, mkdirs, \
                        filesz, dict2str, serialize, construct, children, filename, TypeDict, exists, \
                        readlines, writelines, runcmd
from typing import Any, Optional, Type, Dict, List
from functools import partial
from enum import Enum, auto
import torch, gdown, os, numpy as np, pandas as pd
ENTRY_MEM = getenv('ENTRY_MEM', 100)
ATTR_MEM = ENTRY_MEM

class Loader: 
    readers = TypeDict({bytes: partial(read, mode='rb'), \
                        str: read, \
                        list: readlines, \
                        pd.DataFrame: pd.read_csv, \
                        np.ndarray: np.load, \
                        torch.nn.Module: torch.load})

    writers = TypeDict({bytes: partial(write, mode='wb'), \
                        str: write, \
                        list: writelines, \
                        pd.DataFrame: lambda fp, df: df.to_csv(fp, index=False), \
                        np.ndarray: np.save, \
                        torch.nn.Module: lambda fp, mdl: torch.save(mdl.state_dict(), fp)})

    appenders = TypeDict({bytes: partial(write, mode='ab'), \
                          str: partial(write, mode='a'), \
                          list: partial(writelines, mode='a'), \
                          pd.DataFrame: lambda fp, df: df.to_csv(fp, header=False, mode='a', index=False)})

class AttributeBuffer:
    def __set_name__(self, owner:Type['Attribute'], name:str):
        self.obj, self.pname = owner, '_'+name
        self.set_cache(None)

    def set_cache(self, val:Any): setattr(self.obj, self.pname, val)

    def __get__(self, obj:'Attribute', objtype:Type['Attribute']) -> Any:
        assert obj; self.obj = obj
        self.set_cache(Loader.readers[obj.type](obj.fp) if filesz(obj.fp) < obj.mem else None)
        return self

    def __set__(self, obj:'Attribute', val:Any):
        if isinstance(val, AttributeBuffer): return self # append already happened
        assert obj; self.obj = obj
        assert not obj.readonly and not obj.appendonly, f"invalid write, try append +=, {obj.readonly=}, {obj.appendonly=}"
        self.set_cache(val if objsz(val) < obj.mem else None)
        Loader.writers[obj.type](obj.fp, val)

    def __iadd__(self, val:Any):
        assert self.obj.type in Loader.appenders and not self.obj.readonly, f"invalid append, {self.obj.type=}, {self.obj.readonly=}"
        if self.obj.type is pd.DataFrame and not isinstance(val, pd.DataFrame): val = pd.DataFrame({col:[val] for col, val in zip(self.obj.data.columns.tolist(), val)})
        Loader.appenders[self.obj.type](self.obj.fp, val)
        self.set_cache(None)
        return self

class Attribute:
    # read & delete data, write & append buf

    buf: AttributeBuffer = AttributeBuffer()
    def __init__(self, fp:str, data:Optional[Any], mem:int=ATTR_MEM, readonly:bool=False, appendonly:bool=False) -> None:
        assert not (l:=leaf(fp)) in (s:=siblings(fp)), f"{l} cannot exist, directory {parent(fp)} contents: {s}"
        self.fp, self.mem, self.readonly, self.appendonly, self.type = fp, mem, False, False, type(data)

        # write, then configure
        self.buf, self.readonly, self.appendonly = data, readonly, appendonly

    @property
    def data(self) -> Any: return self._buf if self._buf is not None else Loader.readers[self.type](self.fp)

    @data.deleter
    def data(self): rm(self.fp); del self

    def __getstate__(self) -> Dict[str, Any]: return {k:v for k, v in self.__dict__.items() if k != self.buf.pname}

    @property
    def name(self) -> str: return filename(self.fp)

class Entry:
    class Status(Enum): created = auto(); running = auto(); finished = auto(); error = auto()
    def __init__(self, fp:str, mem:int=ENTRY_MEM):
        self.fp, self.mem = fp, mem
        mkdirs(fp, exist_ok=False)
        self.status = Attribute(path(fp, 'status'), data=[self.Status.created.name], appendonly=True, mem=mem)
        self.obj = Attribute(path(fp, 'obj'), serialize(self))
        self.error = None
        self.num_err = 0

    def set_error(self, errm:str):
        self.status.buf += [self.Status.error.name]
        self.error = Attribute(path(self.fp, 'error_'+str(self.num_err)), data=errm, mem=self.mem)
        self.num_err += 1

    def save(self): self.obj.buf = serialize(self)

    @staticmethod
    def load(fp:str) -> 'Entry': return construct(read(fp, 'rb'))

class ModelEntry(Entry):
    def __init__(self, fp:str, args:Dict[str, str | int], model: torch.nn.Module, mem:int=0):
        super().__init__(fp, mem)
        self.mem = self.mem
        self.args = Attribute(path(fp, 'args'), dict2str(args), readonly=True, mem=mem)
        self.weights = Attribute(path(fp, 'weights'), model, mem=mem)
        self.save()

class DatasetEntry(Entry):
    def __init__(self, fp:str, data=Dict[str, List[Any] | pd.Series], mem:int=ENTRY_MEM):
        super().__init__(fp, mem)
        self.cols = list(data.keys())
        self.dataset = Attribute(path(fp, 'dataset'), pd.DataFrame(data), appendonly=True)
        self.save()

class PredEntry(Entry):
    def __init__(self, fp:str, pred:np.ndarray, model:str, dataset:str, mem:int=ENTRY_MEM):
        super().__init__(fp, mem)
        self.pred = Attribute(path(fp, 'pred.npy'), pred, appendonly=True)
        self.model, self.dataset = model, dataset
        self.save()

import zipfile


class Maester3:
    db_name: str = 'yaatdb'




    def __init__(self, connstr:Optional[str]='mongodb://54.205.245.140:27017/'):
        if connstr is None: pass # start the database
        else: pass # connect to the database
            


class Maester2:
    seed_url = 'https://drive.google.com/uc?id=1BNyA3aqYnKNDkJcOovGKoKBAHOBgws0_'
    def __init__(self, fp:str='./maester_data'):
        self.fp = fp
        # download, unzip, rename
        if not exists(self.fp):
            pref = set(os.listdir())
            zfn = gdown.download(self.seed_url, quiet=False)
            with zipfile.ZipFile(zfn, 'r') as zf: zf.extractall()
            rm(zfn)
            postf = set(os.listdir()) - pref
            assert len(postf) == 1
            os.rename(postf.pop(), self.fp)


    def create_model(name:str,)






class Maester:
    def __init__(self, fp:str = './.yaat', mem:int=10e6):
        self.fp, self.mem = fp, mem
        if not exists(fp):
            fn = gdown.download('https://drive.google.com/uc?id=1BNyA3aqYnKNDkJcOovGKoKBAHOBgws0_', quiet=False)
            runcmd(f'unzip {fn}'); rm(fn)
        mkdirs(fp, exist_ok=True)
        mkdirs(path(fp, 'models'), exist_ok=True)
        mkdirs(path(fp, 'datasets'), exist_ok=True)
        mkdirs(path(fp, 'preds'), exist_ok=True)
        self.models: Dict[str, ModelEntry] = {}
        self.datasets: Dict[str, DatasetEntry] = {}
        self.preds: Dict[str, PredEntry] = {}
        self.sync()

    def create_model(self, name:str, *args, **kwargs): self.models[name] = ModelEntry(path(self.fp, 'models', name), *args, **kwargs)
    def create_dataset(self, name:str, *args, **kwargs): self.datasets[name] = DatasetEntry(path(self.fp, 'datasets', name), *args, **kwargs)
    def create_pred(self, name:str, *args, **kwargs): self.preds[name] = PredEntry(path(self.fp, 'preds', name), *args, **kwargs)

    def sync(self):
        self.models = {filename(mfp): Entry.load(path(self.fp, 'models', mfp, 'obj')) for mfp in children(path(self.fp, 'models'))}
        self.datasets = {filename(dsfp): Entry.load(path(self.fp, 'datasets', dsfp, 'obj')) for dsfp in children(path(self.fp, 'datasets'))} 
        self.preds = {filename(dsfp): Entry.load(path(self.fp, 'preds', dsfp, 'obj')) for dsfp in children(path(self.fp, 'preds'))} 
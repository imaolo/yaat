from __future__ import annotations
from yaat.mongo import MongoDoc, CollDoc, IndexDoc

class Doc1(MongoDoc):
    f1: str
    f2: int

class Doc2(MongoDoc):
    d1: Doc1
    f1: int

class Doc1Array(MongoDoc):
    l1: list[str]
    l2: list[list[int]]
    f3: int

class Doc2Array(MongoDoc):
    l1: list[Doc1Array]
    l2: list[list[Doc1Array]]
    l3: list[float]
    f1: bool


class Doc1Dict(MongoDoc):
    dict1: dict
    f1: int

class Doc1TypeDoc(MongoDoc):   
    f1: int
    t1: type[MongoDoc]

class Doc1EmptyIndexDoc(MongoDoc, colldoc=CollDoc(index=IndexDoc(args=[], kwargs={}))):
    f1: int
    t1: type[Doc1]
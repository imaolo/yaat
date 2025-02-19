from yaat.mongo import MongoDoc
import unittest

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

class TestMongoDoc(unittest.TestCase):

    def test_doc(self):
        self.assertEqual(Doc1.get_schema(),{
            'bsonType': ['object'],
            'properties': {
                'f1': {'bsonType': ['string']},
                'f2': {'bsonType': ['int']}
            },
            'required': ['f1', 'f2']
        })

    def test_doc_nest(self):
        self.assertEqual(Doc2.get_schema(),{
            'bsonType': ['object'],
            'properties': {
                'f1': {'bsonType': ['int']},
                'd1': {
                    'bsonType': ['object'],
                    'properties': {
                        'f1': {'bsonType': ['string']},
                        'f2': {'bsonType': ['int']}},
                    'required': ['f1', 'f2']}},
            'required': ['d1', 'f1']})

    def test_array(self):
        self.assertEqual(Doc1Array.get_schema(),{
            'bsonType': ['object'],
            'properties': {
                'f3': {'bsonType': ['int']},
                'l1': {
                    'bsonType': ['array'],
                    'items': {'bsonType': ['string']}},
                'l2': {
                    'bsonType': ['array'],
                    'items': {
                        'bsonType': ['array'],
                        'items': {'bsonType': ['int']}}}},
            'required': ['l1', 'l2', 'f3']})

    def test_array_nest(self):
        from pprint import pprint
        self.assertEqual(Doc2Array.get_schema(), {
            'bsonType': ['object'],
            'properties': {
                'f1': {'bsonType': ['bool']},
                'l1': {
                    'bsonType': ['array'],
                    'items': {
                        'bsonType': ['object'],
                        'properties': {
                            'f3': {'bsonType': ['int']},
                            'l1': {
                                'bsonType': ['array'],
                                'items': {'bsonType': ['string']}},
                            'l2': {
                                'bsonType': ['array'],
                                'items': {
                                    'bsonType': ['array'],
                                    'items': {'bsonType': ['int']}}}},
                        'required': ['l1', 'l2', 'f3']}},
                'l2': {
                    'bsonType': ['array'],
                    'items': {
                        'bsonType': ['array'],
                        'items': {
                            'bsonType': ['object'],
                            'properties': {
                                'f3': {'bsonType': ['int']},
                                'l1': {
                                    'bsonType': ['array'],
                                    'items': {'bsonType': ['string']}},
                                'l2': {
                                    'bsonType': ['array'],
                                    'items': {
                                        'bsonType': ['array'],
                                        'items': {'bsonType': ['int']}}}},
                            'required': ['l1', 'l2', 'f3']}}},
                'l3': {
                    'bsonType': ['array'],
                    'items': {'bsonType': ['double']}}},
            'required': ['l1', 'l2', 'l3', 'f1']
        })
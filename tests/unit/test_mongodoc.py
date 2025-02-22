from tests.common import Doc1, Doc2, Doc1Array, Doc2Array, Doc1Dict, Doc1TypeDoc
from yaat.mongo import MongoDoc
import unittest


class TestMongoDoc(unittest.TestCase):

    def test_simple(self):
        self.assertEqual(Doc1(f1='some string', f2=1).dict, {'f1': 'some string', 'f2':1})

    def test_nest(self):
        self.assertEqual(Doc2(d1=Doc1(f1='some string', f2=1), f1=1).dict, {'d1':{'f1': 'some string', 'f2':1}, 'f1':1})

    def test_nest(self):
        self.assertEqual(Doc2(d1=Doc1(f1='some string', f2=1), f1=1).dict, {'d1':{'f1': 'some string', 'f2':1}, 'f1':1})

    def test_array(self):
        l1=['lstr1', 'lstr2']
        l2=[[1, 2], [2, 5]]
        doc = Doc1Array(l1=l1, l2=l2, f3=1)
        self.assertEqual(doc.dict, {'l1': l1, 'l2':l2, 'f3':1})

class TestMongoDocSchema(unittest.TestCase):
    # TODO test union and optionals

    def test_simple(self):
        self.assertEqual(Doc1.schema.pop('$jsonSchema'),{
            'bsonType': 'object',
            'properties': {
                'f1': {'bsonType': 'string'},
                'f2': {'bsonType': 'int'},
                '_id': {'bsonType': 'objectId'}
            },
            'required': ['f1', 'f2'],
            'additionalProperties': False
        })

    def test_nest(self):
        self.assertEqual(Doc2.schema.pop('$jsonSchema'),{
            'bsonType': 'object',
            'properties': {
                '_id': {'bsonType': 'objectId'},
                'f1': {'bsonType': 'int'},
                'd1': {
                    'bsonType': 'object',
                    'properties': {
                        'f1': {'bsonType': 'string'},
                        'f2': {'bsonType': 'int'}},
                    'required': ['f1', 'f2'],
                    'additionalProperties': False}},
            'required': ['d1', 'f1'],
            'additionalProperties': False})

    def test_array(self):
        self.assertEqual(Doc1Array.schema.pop('$jsonSchema'),{
            'bsonType': 'object',
            'properties': {
                '_id': {'bsonType': 'objectId'},
                'f3': {'bsonType': 'int'},
                'l1': {
                    'bsonType': 'array',
                    'items': {'bsonType': 'string'}},
                'l2': {
                    'bsonType': 'array',
                    'items': {
                        'bsonType': 'array',
                        'items': {'bsonType': 'int'}}}},
            'required': ['l1', 'l2', 'f3'],
            'additionalProperties': False})

    def test_array_nest(self):
        self.assertEqual(Doc2Array.schema.pop('$jsonSchema'), {
            'bsonType': 'object',
            'properties': {
                '_id': {'bsonType': 'objectId'},
                'f1': {'bsonType': 'bool'},
                'l1': {
                    'bsonType': 'array',
                    'items': {
                        'bsonType': 'object',
                        'properties': {
                            'f3': {'bsonType': 'int'},
                            'l1': {
                                'bsonType': 'array',
                                'items': {'bsonType': 'string'}},
                            'l2': {
                                'bsonType': 'array',
                                'items': {
                                    'bsonType': 'array',
                                    'items': {'bsonType': 'int'}}}},
                        'required': ['l1', 'l2', 'f3'],
                        'additionalProperties': False}},
                'l2': {
                    'bsonType': 'array',
                    'items': {
                        'bsonType': 'array',
                        'items': {
                            'bsonType': 'object',
                            'properties': {
                                'f3': {'bsonType': 'int'},
                                'l1': {
                                    'bsonType': 'array',
                                    'items': {'bsonType': 'string'}},
                                'l2': {
                                    'bsonType': 'array',
                                    'items': {
                                        'bsonType': 'array',
                                        'items': {'bsonType': 'int'}}}},
                            'required': ['l1', 'l2', 'f3'],
                            'additionalProperties': False}}},
                'l3': {
                    'bsonType': 'array',
                    'items': {'bsonType': 'double'}}},
            'required': ['l1', 'l2', 'l3', 'f1'],
            'additionalProperties': False
        })

    def test_dict(self):
        self.assertEqual(Doc1Dict.schema.pop('$jsonSchema'), {
            'additionalProperties': False,
            'bsonType': 'object',
            'properties': {
                '_id': {'bsonType': 'objectId'},
                'dict1': {'bsonType': 'object'},
                'f1': {'bsonType': 'int'}},
            'required': ['dict1', 'f1']})

    @unittest.skip("TODO annotate dict raises runtime error. should these become docs?")
    def test_dict2(self):
        class Doc2Dict(MongoDoc):
            dict1: dict[str, str]

    # TODO - remove support?
    def test_type_doc(self):
        self.assertEqual(Doc1TypeDoc.schema.pop('$jsonSchema'), {
            'additionalProperties': False,
            'bsonType': 'object',
            'properties': {
                '_id': {'bsonType': 'objectId'},
                'f1': {'bsonType': 'int'},
                't1': {'bsonType': 'str'}},
            'required': ['f1', 't1']
        })
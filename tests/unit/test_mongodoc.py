from tests.common import Doc1, Doc2, Doc1Array, Doc2Array, Doc1Dict, Doc1TypeDoc, MongoDoc
import unittest
from yaat.yaat import MongoClient
class TestMongoDoc(unittest.TestCase):
    # TODO test union and optionals

    def test_doc(self):
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

    def test_doc_nest(self):
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

    # TODO failure
    @unittest.skip("annotate dict raise runtime error. should these become docs?")
    def test_dict2(self):
        class Doc2Dict(MongoDoc):
            dict1: dict[str, str]

    def test_doc_type_doc(self):
        self.assertEqual(Doc1TypeDoc(t1=Doc1, f1=1).schema.pop('$jsonSchema'), {
            'additionalProperties': False,
            'bsonType': 'object',
            'properties': {
                '_id': {'bsonType': 'objectId'},
                'f1': {'bsonType': 'int'},
                't1': {'bsonType': 'str'}},
            'required': ['f1', 't1']
        })
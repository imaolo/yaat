from common import Doc1, Doc2, Doc1Array, Doc2Array
import unittest

class TestMongoDoc(unittest.TestCase):
    # TODO test union and optionals

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
from typing import Dict, Optional
from dataclasses import fields
from yaat.informer import InformerArgs


pybson_tmap = {
    str: 'string',
    int: 'int',
    bool: 'bool',
    float: 'double',
    Optional[str]: ['string', 'null']
}

class Maester:
    
    informer_weights_schema: Dict = {
        'title': 'Weights for informer models',
        'required': [field.name for field in fields(InformerArgs)]
                        + ['weights', 'dataset'],
        'properties': {field.name: {'bsonType': pybson_tmap[field.type]} for field in fields(InformerArgs)}
                        | {'weights': {'bsonType': 'binData'}}
                        | {'dataset': {'bsonType': 'object'}}
    }
from yaat.state import State
from yaat.scraper import TopMoverResultDoc, TopMoverJobDoc

State.document_models = [
    TopMoverResultDoc,
    TopMoverJobDoc
]
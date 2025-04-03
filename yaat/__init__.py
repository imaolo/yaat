from yaat.state import State
from yaat.job import APSJobDoc
from yaat.scraper import TopMoverJobDoc, TopMoverResultDoc

State.document_models = [
    APSJobDoc,
    TopMoverJobDoc,
    TopMoverResultDoc
]
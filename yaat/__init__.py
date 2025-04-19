from yaat.state import State
from yaat.job import APSJobDoc
from yaat.coingecko import TopMoverJobDoc, TopMoverResultDoc

State.document_models = [
    APSJobDoc,
    TopMoverJobDoc,
    TopMoverResultDoc
]
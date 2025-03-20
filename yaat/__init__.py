from yaat.state import State, APSJobDoc
from yaat.scraper import TopMoverResultDoc, TopMoverQueryDoc, TopMoverJobDoc

State.document_models.update({
    APSJobDoc,
    TopMoverResultDoc,
    TopMoverQueryDoc,
    TopMoverJobDoc
})
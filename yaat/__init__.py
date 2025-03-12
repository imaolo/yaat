from yaat.state import State, APSJobDoc
from yaat.scraper import JobDoc, TopMoverResultDoc, TopMoverQueryDoc, TopMoverJobDoc

State.document_models.update({
    JobDoc,
    APSJobDoc,
    TopMoverResultDoc,
    TopMoverQueryDoc,
    TopMoverJobDoc
})
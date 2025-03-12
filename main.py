from yaat.state import State
from beanie import Document
from fastapi import FastAPI, APIRouter
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from yaat.doc import DocUIMetadata, Doc
import os

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'yaat/client/dist')

@asynccontextmanager
async def lifespan(app: FastAPI):
    await State.init()

    yield

    State.terminate()

app = FastAPI(title='Title', lifespan=lifespan)
app.mount('/dist', StaticFiles(directory=STATIC_DIR), name='dist')
app.add_route('/', lambda _: RedirectResponse(url="/dist/index.html"))
app.add_route('/hb', lambda _: {'status': 'ok', 'message': "Heartbeat acknowledged"})

@app.get('/metadatas', response_model=list[DocUIMetadata])
def get_metadatas() -> list[dict]:
    return [model.get_metadata() for model in State.document_models if issubclass(model, Doc) if model.readable]

def register_crud(router: APIRouter, schema: type[Doc]):
    @router.post('/'+schema.__name__)
    async def create(doc: dict):
        doc.pop('_id', None)
        await schema(**doc).create()

    print('/'+schema.__name__)
    @router.get('/'+schema.__name__, response_model=list[schema])
    async def read() -> list[Document]:
        return await schema.find().to_list()

    @router.put('/'+schema.__name__, response_model=list[schema])
    async def update(doc: Document) -> Document:
        raise RuntimeError("TODO not implemented")

    @router.delete('/'+schema.__name__, response_model=list[schema])
    async def delete(doc: Document) -> Document:
        raise RuntimeError("TODO not implemented")


router = APIRouter()
for schema in State.document_models:
    register_crud(router, schema)
app.include_router(router)


# schema_instance = schema()
# @router.put(prefix + "/{item_id}")
# async def update_o(item_id: str, payload: dict = Body(...)):
#     item = document.objects(id=item_id).first()
#     if not item:
#         raise HTTPException(status_code=404, detail=f"{document.__name__} not found")
#     for key, value in payload.items():
#         setattr(item, key, value)
#     item.save()
#     return schema_instance.dump(item)

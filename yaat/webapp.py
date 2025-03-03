
from fastapi import FastAPI, APIRouter, Body
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from beanie import init_beanie, Document
from yaat.scraper import TopMoverDoc, TopMoverQueryDoc
from yaat.helpers import getenv
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, 'client/dist')

document_models = [TopMoverQueryDoc, TopMoverDoc]

@asynccontextmanager
async def lifespan(_: FastAPI):
    from apscheduler.jobstores.mongodb import MongoDBJobStore
    from apscheduler.schedulers.background import BackgroundScheduler
    from motor.motor_asyncio import AsyncIOMotorClient

    host = 'mongo' if getenv('DOCKER', False) else 'localhost'
    client = AsyncIOMotorClient(f"mongodb://{host}:27017")

    await init_beanie(database=client['yaatdb'], document_models=document_models)

    jobstore = MongoDBJobStore(client=client.delegate)
    scheduler = BackgroundScheduler(jobstores={'default':jobstore})
    scheduler.start()

    for duration in TopMoverQueryDoc.Duration:
        for top_coin in TopMoverQueryDoc.TopCoins:
            query = TopMoverQueryDoc(duration=duration, top_coin=top_coin)
            docs = await TopMoverQueryDoc.find(query.model_dump(), limit=2).to_list()
            match len(docs):
                case 0: await query.create()
                case 1: pass
                case _: raise RuntimeError("error")

    yield

    scheduler.shutdown(wait=True)

app = FastAPI(title='Title', lifespan=lifespan)

app.mount('/dist', StaticFiles(directory=STATIC_DIR), name='dist')

app.add_route('/', lambda _: RedirectResponse(url="/dist/index.html"))

app.add_route('/hb', lambda _: {'status': 'ok', 'message': "Heartbeat acknowledged"})

def register_endpoint(router: APIRouter, doct: type[Document]):

    @router.get('/'+doct.__name__, response_model=list[doct])
    async def get_documents():
        return await doct.find({}, projection_model=doct).to_list()

    @router.post('/'+doct.__name__, response_model=doct)
    async def post_document(data: dict = Body(...)):
        await (doc:=doct(**data)).create()
        return doc

    @router.get('/schema/'+doct.__name__, response_class=JSONResponse)
    async def get_schema():
        (schema:=doct.model_json_schema())['properties'].pop('_id', None)
        return schema
    
router = APIRouter()
for model in document_models:
    register_endpoint(router, model)
app.include_router(router)

def run():
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=80)

# from fastapi import FastAPI
# from fastapi_crudrouter import MongoCRUDRouter
# from yaat.scraper import TopMoverQueryDoc

# app = FastAPI()

# # Use your Beanie model's underlying Motor collection.
# collection = TopMoverQueryDoc.get_motor_collection()

# app.include_router(
#     MongoCRUDRouter(
#         schema=TopMoverQueryDoc,
#         collection=collection,
#         prefix="topmover"  # This will create endpoints like /topmover/
#     )
# )

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

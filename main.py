from yaat.state import State
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
import os, fastapi_paginate, contextlib

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'yaat/client/dist')

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    await State.init()
    yield
    State.terminate()

app = FastAPI(title='Title', lifespan=lifespan)
app.mount('/dist', StaticFiles(directory=STATIC_DIR), name='dist')
app.add_route('/', lambda _: RedirectResponse(url="/dist/index.html"))
app.add_route('/hb', lambda _: {'status': 'ok', 'message': "Heartbeat acknowledged"})
app.include_router(State.get_router())
fastapi_paginate.add_pagination(app)

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

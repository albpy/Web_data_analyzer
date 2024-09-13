import os
#---------------------------
# from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
# from core.database import get_db
# from sqlalchemy.orm import Session
# import json
#-----------------------------------------
# import redis.asyncio as redis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer
# from fastapi_limiter import FastAPILimiter

from core.middlewares.security import SecurityMiddleware
# from routes.sp import sp
from routes import otb
# from routes.budgetPlan import budget
# from routes.admin import router as admin_aouter
# from routes.ASR import router as asr_router
# from routes.authentication import router as auth_router
# from routes.data_master import router as data_master_router
# from routes.wssi import wssi

api = FastAPI(
    debug=True,
    title="BMAPS API",
    summary="API for backend services for BMAPS",
    contact={
        "name": "Albpy",
        "url": "https://github.com/albpy",
        "email": "albinjos64@gmail.com",
    },
    version="0.1.0",
)

# Add the api routes
api.include_router(otb)

# Add the CORS middleware
api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# @api.websocket("/get_data_ws")
# async def get_data_ws(websocket: WebSocket, db: Session = Depends(get_db)):
#     await websocket.accept()
#     try:
#         while True:
#             data = await websocket.receive_text()
#             print(f"Received message: {data}")
#             response = {"message": "Data received", "data": data}
#             await websocket.send_text(json.dumps(response))
#     except Exception as e:
#         print(f"Error: {e}")
#     finally:
#         await websocket.close()
# Add the security middleware
# api.add_middleware(SecurityMiddleware)

# @api.on_event("startup")
# async def startup():
#     redis_obj = redis.StrictRedis(
#         host=os.getenv("REDIS_HOST"), 
#         password=os.getenv("REDIS_PASSWORD"), 
#         encoding="utf-8", 
#         decode_responses=True
#     )
#     await FastAPILimiter.init(redis_obj)
    
# security = HTTPBearer()

# Protected route that requires authentication
@api.get("/")
async def protected_base_route():
    return {"message": "Hi my friend, how are you?"}
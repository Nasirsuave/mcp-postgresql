from fastapi import FastAPI
from app.api.v1.routers import tools

app = FastAPI()


app.include_router(tools.router, tags=["tools"])


@app.get("/")
async def root():
    return {"message": "Hello Welcome Bigger Applications!"}
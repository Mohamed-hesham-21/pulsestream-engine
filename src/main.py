import os
from fastapi import FastAPI
from src.routers.comments import router as comments_router

app = FastAPI(
    title='PulseStream Engine',
    version='1.0.0',
)

app.include_router(comments_router)

@app.get('/health')
async def health_check():
    return {
        "status": "healthy",
        "node_id": os.getenv("NODE_ID", "node-1"),
        "region": os.getenv("REGION", "us-east-2a"),  
    }

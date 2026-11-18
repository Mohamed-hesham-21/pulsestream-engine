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

# - N8N_BLOCK_ENV_ACCESS_IN_NODE=false
#       - PAYMOB_SECRET_KEY=egy_sk_live_xxxxxxxxxxxxxxxxx
#       - PAYMOB_PUBLIC_KEY=egy_pk_live_xxxxxxxxxxxxxxxxx
#       - PAYMOB_HMAC_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxx
#       - PAYMOB_INTEGRATION_IDS=4431070,4431071
#       - PLAN_BASIC_EGP=300
#       - PLAN_PREMIUM_EGP=600
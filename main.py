from typing import Any, Dict, List

from fastapi import FastAPI
from fastapi import HTTPException
app = FastAPI()


@app.post("/v1/consumer")
async def root(request:dict):
    for item in request:
        print(request.get(item))
    return True

@app.post("/v1/consumerX")
async def root(request:str):
    raise HTTPException(status_code=400)

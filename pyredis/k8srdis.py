from fastapi import FastAPI, HTTPException, Request
from redis import Redis
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Initialize Redis client
redis_client = Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True
)

def query_run_subroutine():
    user_keys = redis_client.keys('*users*')
    location_keys = redis_client.keys('*location*')

    # Print the keys
    print("User Keys:")
    for key in user_keys:
        print(key)
    
    print("\nLocation Keys:")
    for key in location_keys:
        print(key)

    return {"user_keys": user_keys, "location_keys": location_keys}

@app.get("/data/{id}")
async def get_data(id: str):
    # Try to get data from Redis
    data = redis_client.get(id)
    
    if data is None:
        raise HTTPException(status_code=404, detail="Data not found")
    
    return {"id": id, "data": data}

@app.get("/queryrun")
async def query_run():
    return query_run_subroutine()

@app.get("/{path:path}")
async def catch_all(path: str, request: Request):
    if 'queryrun' in request.url.path:
        return query_run_subroutine()
    else:
        raise HTTPException(status_code=404, detail="Not Found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)
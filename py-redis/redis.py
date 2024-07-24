from fastapi import FastAPI, HTTPException
import redis
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Initialize Redis client
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=os.getenv('REDIS_PORT', 6379),
    db=0,
    decode_responses=True
)

@app.get("/data/{id}")
async def get_data(id: str):
    # Try to get data from Redis
    data = redis_client.get(id)
    
    if data is None:
        raise HTTPException(status_code=404, detail="Data not found")
    
    return {"id": id, "data": data}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
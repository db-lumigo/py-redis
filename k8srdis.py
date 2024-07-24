from fastapi import FastAPI, HTTPException, Request
from redis import Redis
import os
from dotenv import load_dotenv
import logging
from logging.config import dictConfig

# Logging configuration
log_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        },
    },
    "handlers": {
        "file": {
            "formatter": "default",
            "class": "logging.FileHandler",
            "filename": "app.log",
        },
        "console": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "root": {"level": "INFO", "handlers": ["file", "console"]},
}

# Configure logging
dictConfig(log_config)
logger = logging.getLogger(__name__)

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

    logger.info(f"User Keys: {user_keys}")
    logger.info(f"Location Keys: {location_keys}")
    
    return {"user_keys": user_keys, "location_keys": location_keys}
    
def query_bad_run_subrutine():
    user_keys = redis_client.keys('*users*')
    location_keys = redis_client.count('*location*')

    # Print the keys
    print("User Keys:")
    for key in user_keys:
        print(key)
    
    print("\nLocation Keys:")
    for key in location_keys:
        print(key)

    logger.info(f"User Keys: {user_keys}")
    logger.info(f"Location Keys: {location_keys}")
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

@app.get("/badbubu")
async def query_run():
    return query_bad_run_subrutine()

@app.get("/{path:path}")
async def catch_all(path: str, request: Request):
    if 'queryrun' in request.url.path:
        return query_run_subroutine()
    else:
        raise HTTPException(status_code=404, detail="Not Found")

if __name__ == "__main__":
    import uvicorn
    logger.info("starting to listen")
    logger.info(os.getpid())
    logger.info(os.getcwd())
    uvicorn.run(app, host="0.0.0.0", port=8005)
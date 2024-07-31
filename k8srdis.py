from fastapi import FastAPI, HTTPException, Request, Body
from redis import Redis
import os
from dotenv import load_dotenv
import logging
from logging.config import dictConfig
import boto3
from datetime import datetime

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

# Initialize S3 client
s3 = boto3.client('s3')
BUCKET_NAME = os.environ.get('BUCKET_NAME')

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

@app.post("/write_to_s3")
async def write_to_s3(data: dict = Body(default=None)):
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="BUCKET_NAME environment variable not set")

    if data is None:
        # Default case when no body is sent
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"default_{current_time}.txt"
        file_content = f"This is a default file created at {current_time}"
    else:
        file_name = data.get('file_name')
        file_content = data.get('file_content')

        if not file_name:
            current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"unnamed_{current_time}.txt"

        if not file_content:
            file_content = "This file was created with no content."

    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=file_content)
        logger.info(f"File {file_name} written to S3 bucket {BUCKET_NAME}")
        return {
            "message": f"File {file_name} written to S3 bucket {BUCKET_NAME}",
            "file_name": file_name,
            "file_content": file_content
        }
    except Exception as e:
        logger.error(f"Error writing to S3: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error writing to S3: {str(e)}")

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
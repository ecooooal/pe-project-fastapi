import logging

# Configure root logger
logging.basicConfig(
    level=logging.DEBUG,  # Use DEBUG for detailed info, INFO for less verbosity
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler("app/fastapi_worker.log", encoding="utf-8")  # Logs to file    
    ]
)   

logger = logging.getLogger("app")   

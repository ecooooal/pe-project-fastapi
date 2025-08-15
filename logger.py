import logging
import os

log_dir = "app"
log_file = "fastapi.log"
os.makedirs(log_dir, exist_ok=True) 

log_path = os.path.join(log_dir, log_file)

# Configure root logger
logging.basicConfig(
    level=logging.DEBUG,  # Use DEBUG for detailed info, INFO for less verbosity
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8")    ]
    )   

logger = logging.getLogger("app")   

import logging
import os

log_dir = "app"
log_file = "fastapi.log"
os.makedirs(log_dir, exist_ok=True)

log_path = os.path.join(log_dir, log_file)

logger = logging.getLogger("app")
logger.setLevel(logging.DEBUG)

# Avoid adding duplicate handlers on repeated imports
if not logger.hasHandlers():
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

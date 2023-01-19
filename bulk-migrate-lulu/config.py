import os

MIN_DATE = "2022-01-01T00:00:00.000Z"
MAX_DATE = "2022-11-25T14:36:42.000Z"

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

LOG_LEVEL = os.getenv("UPP_LOG_LEVEL", "INFO")
BATCH_SIZE = 50

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Base directory
BASE_DIR = Path(__file__).resolve().parent

# MQTT Configuration
MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_PORT = int(os.getenv('MQTT_PORT'))
MQTT_TOPIC = os.getenv('MQTT_TOPIC')

# Full Database URL from .env
DB_URL = os.getenv('POSTGRES_URI')
if not DB_URL:
    raise ValueError("POSTGRES_URI not set in environment variables.")

# Excel Configuration
EXCEL_FILE_NAME = os.getenv('EXCEL_FILE_NAME')
if not EXCEL_FILE_NAME:
    raise ValueError("EXCEL_FILE_NAME not set in environment variables.")

EXCEL_FILE_PATH = BASE_DIR / EXCEL_FILE_NAME

SHEET_DATA_GUIDE = 'data_guide'
SHEET_TAGS = 'tags'



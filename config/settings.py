import os
from pathlib import Path

# Define project root
project_root = Path(__file__).parent.parent.absolute()

# Data directories
RAW_DATA_DIR = os.path.join(project_root, "data/raw")
PROCESSED_DATA_DIR = os.path.join(project_root, "data/processed")

# SQLite configuration
DATABASE_PATH = os.path.join(PROCESSED_DATA_DIR, "nba_data.db") 
DB_URL = f"sqlite:///{DATABASE_PATH}"

# API settings
API_REQUEST_TIMEOUT = 30  # seconds

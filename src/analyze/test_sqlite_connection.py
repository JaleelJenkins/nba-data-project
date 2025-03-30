# src/test_sqlite_connection.py
from sqlalchemy import create_engine
import os
from pathlib import Path

from config.settings import DB_URL, DATABASE_PATH

# Make sure directory exists
os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)

# Create engine
print(f"Connecting to SQLite database at {DATABASE_PATH}")
engine = create_engine(DB_URL)

# Test connection
connection = engine.connect()
print("Connection successful!")
connection.close()
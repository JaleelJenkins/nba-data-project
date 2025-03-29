"""
Database loader for storing processed NBA data
"""
import os
import pandas as pd
from pathlib import Path
import sqlite3
from sqlalchemy import create_engine

from config.settings import DATABASE_PATH, PROCESSED_DATA_DIR


class NBADataLoader:
    """Class to load processed NBA data into a database"""
    
    def __init__(self):
        """Initialize the loader"""
        # Ensure database directory exists
        os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
        
        # Create database connection
        self.engine = create_engine(f"sqlite:///{DATABASE_PATH}")
        
    def load_live_games(self, df=None, file_path=None):
        """Load live games data into the database"""
        if df is None and file_path is None:
            # Find the latest processed live games file
            processed_dir = Path(PROCESSED_DATA_DIR)
            live_game_files = list(processed_dir.glob("live_games_processed_*.csv"))
            
            if not live_game_files:
                print("No processed live games files found")
                return False
                
            file_path = max(live_game_files, key=lambda x: x.stat().st_mtime)
            print(f"Loading data from {file_path}")
        
        try:
            # Load data
            if df is None:
                df = pd.read_csv(file_path)
            
            # Load to database
            df.to_sql("live_games", self.engine, if_exists="replace", index=False)
            print(f"Loaded {len(df)} live games to database")
            return True
        except Exception as e:
            print(f"Error loading live games data: {e}")
            return False
    
    def load_recent_games(self, df=None, file_path=None):
        """Load recent games data into the database"""
        if df is None and file_path is None:
            # Find the latest processed recent games file
            processed_dir = Path(PROCESSED_DATA_DIR)
            recent_game_files = list(processed_dir.glob("recent_games_processed_*.csv"))
            
            if not recent_game_files:
                print("No processed recent games files found")
                return False
                
            file_path = max(recent_game_files, key=lambda x: x.stat().st_mtime)
            print(f"Loading data from {file_path}")
        
        try:
            # Load data
            if df is None:
                df = pd.read_csv(file_path)
            
            # Load to database
            df.to_sql("recent_games", self.engine, if_exists="replace", index=False)
            print(f"Loaded {len(df)} recent games to database")
            return True
        except Exception as e:
            print(f"Error loading recent games data: {e}")
            return False
    
    def query_data(self, query):
        """Run a query against the database"""
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            print(f"Error running query: {e}")
            return None


if __name__ == "__main__":
    # Test the loader
    loader = NBADataLoader()
    loader.load_live_games()
    loader.load_recent_games()
    
    # Test query
    result = loader.query_data("SELECT * FROM live_games LIMIT 5")
    if result is not None:
        print(result.head())
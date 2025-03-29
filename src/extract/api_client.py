"""
NBA API Client for extracting NBA data
"""
import json
import os
from datetime import datetime
from pathlib import Path
import sys

# Add the project root to the Python path (similar approach as in main.py)
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

# Now import from config
from config.settings import RAW_DATA_DIR

# Import NBA API modules
try:
    from nba_api.live.nba.endpoints import scoreboard
    print("Successfully imported nba_api modules")
except ImportError as e:
    print(f"Error importing nba_api modules: {e}")

class NBADataExtractor:
    """Class to extract data from the NBA API"""
    
    def __init__(self):
        """Initialize the extractor"""
        # Ensure raw data directory exists
        raw_dir = os.path.join(project_root, RAW_DATA_DIR)
        os.makedirs(raw_dir, exist_ok=True)
        print(f"Raw data directory: {raw_dir}")
    
    def get_live_games(self):
        """Get live games from the NBA API"""
        print("Fetching live games...")
        try:
            # Get the current scoreboard
            games = scoreboard.ScoreBoard()
            games_dict = games.get_dict()
            
            # Save raw data
            # self._save_raw_data(games_dict, "live_games")
            print("Successfully fetched live games")
            
            return games_dict
        except Exception as e:
            print(f"Error fetching live games: {e}")
            return None

if __name__ == "__main__":
    # Test the extractor
    extractor = NBADataExtractor()
    print("Created extractor")
    live_games = extractor.get_live_games()
    print("Test complete")

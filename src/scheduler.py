# src/scheduler.py
import time
import schedule
import datetime
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.absolute()
sys.path.insert(0, str(project_root))

from extract.warehouse_extractor import NBAWarehouseExtractor

def update_teams():
    """Update the teams dimension table"""
    print(f"[{datetime.datetime.now()}] Updating teams...")
    extractor = NBAWarehouseExtractor()
    extractor.extract_teams()
    extractor.close()
    print(f"[{datetime.datetime.now()}] Teams update complete")

def update_recent_games():
    """Update recent games"""
    print(f"[{datetime.datetime.now()}] Updating recent games...")
    extractor = NBAWarehouseExtractor()
    games = extractor.extract_games(days_back=3)  # Last 3 days
    
    # Update player stats for each game
    for game in games:
        extractor.extract_player_game_stats(game['game_id'])
    
    extractor.close()
    print(f"[{datetime.datetime.now()}] Recent games update complete")

def initial_load():
    """Perform initial load of historical data"""
    print(f"[{datetime.datetime.now()}] Performing initial load...")
    
    extractor = NBAWarehouseExtractor()
    
    # Load teams
    extractor.extract_teams()
    
    # Load games from the last 90 days
    games = extractor.extract_games(days_back=90)
    
    # Load player stats for each game
    for game in games:
        extractor.extract_player_game_stats(game['game_id'])
    
    extractor.close()
    print(f"[{datetime.datetime.now()}] Initial load complete")

if __name__ == "__main__":
    print("Starting NBA data warehouse scheduler...")
    
    # Perform initial load
    initial_load()
    
    # Schedule regular updates
    schedule.every().day.at("04:00").do(update_teams)  # Update teams daily at 4 AM
    schedule.every(3).hours.do(update_recent_games)    # Update games every 3 hours
    
    # Run the scheduler
    while True:
        schedule.run_pending()
        time.sleep(60)  # Sleep for 1 minute
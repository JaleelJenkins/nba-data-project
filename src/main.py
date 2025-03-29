"""
Main script to run the NBA data pipeline
"""
import sys
import os
from pathlib import Path
import json

# Get the absolute path of the project root directory
project_root = Path(__file__).parent.parent.absolute()

# Add the project root to the Python path
sys.path.insert(0, str(project_root))

# Now import the modules (after adjusting the path)
print("Starting NBA data pipeline")
print(f"Project root: {project_root}")

from config.settings import RAW_DATA_DIR, PROCESSED_DATA_DIR

def fetch_live_games():
    """Fetch live NBA games"""
    print("\nFetching live NBA games...")
    
    try:
        from nba_api.live.nba.endpoints import scoreboard
        
        # Get the current scoreboard
        games = scoreboard.ScoreBoard()
        games_dict = games.get_dict()
        
        # Save the raw data
        raw_dir = os.path.join(project_root, RAW_DATA_DIR)
        os.makedirs(raw_dir, exist_ok=True)
        
        # Create a filename with timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"live_games_{timestamp}.json"
        filepath = os.path.join(raw_dir, filename)
        
        # Save the data to a file
        with open(filepath, 'w') as f:
            json.dump(games_dict, f, indent=2)
        
        print(f"Data successfully saved to: {filepath}")
        
        # Display a summary of the data
        if 'scoreboard' in games_dict and 'games' in games_dict['scoreboard']:
            games_list = games_dict['scoreboard']['games']
            print(f"\nFound {len(games_list)} game(s):")
            
            for game in games_list:
                home_team = f"{game.get('homeTeam', {}).get('teamCity', '')} {game.get('homeTeam', {}).get('teamName', '')}"
                away_team = f"{game.get('awayTeam', {}).get('teamCity', '')} {game.get('awayTeam', {}).get('teamName', '')}"
                home_score = game.get('homeTeam', {}).get('score', 0)
                away_score = game.get('awayTeam', {}).get('score', 0)
                status = game.get('gameStatusText', '')
                
                print(f"  • {away_team} ({away_score}) @ {home_team} ({home_score}) - {status}")
        else:
            print("No games found in the response. This could be because there are no games today or the API structure has changed.")
        
        return True
        
    except Exception as e:
        print(f"Error fetching live games: {e}")
        return False

def main():
    print("NBA data pipeline initialized successfully!")
    print(f"Raw data will be stored in: {os.path.join(project_root, RAW_DATA_DIR)}")
    print(f"Processed data will be stored in: {os.path.join(project_root, PROCESSED_DATA_DIR)}")
    
    # Fetch live NBA games
    fetch_live_games()

if __name__ == "__main__":
    main()

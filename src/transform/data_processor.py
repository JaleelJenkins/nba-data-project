"""
Data processor for transforming NBA data
"""
import pandas as pd
import os
from pathlib import Path
import json

from config.settings import RAW_DATA_DIR, PROCESSED_DATA_DIR


class NBADataProcessor:
    """Class to process and transform NBA data"""
    
    def __init__(self):
        """Initialize the processor"""
        # Ensure processed data directory exists
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    
    def process_live_games(self, data):
        """Process live games data"""
        if not data or 'scoreboard' not in data:
            print("No valid live games data to process")
            return None
        
        # Extract the games list
        games = data['scoreboard'].get('games', [])
        
        # Create a DataFrame
        games_list = []
        for game in games:
            game_info = {
                'game_id': game.get('gameId'),
                'game_status': game.get('gameStatus'),
                'game_status_text': game.get('gameStatusText'),
                'period': game.get('period'),
                'regulation_periods': game.get('regulationPeriods'),
                'game_clock': game.get('gameClock'),
                'home_team_id': game.get('homeTeam', {}).get('teamId'),
                'home_team_name': game.get('homeTeam', {}).get('teamName'),
                'home_team_city': game.get('homeTeam', {}).get('teamCity'),
                'home_team_score': game.get('homeTeam', {}).get('score'),
                'away_team_id': game.get('awayTeam', {}).get('teamId'),
                'away_team_name': game.get('awayTeam', {}).get('teamName'),
                'away_team_city': game.get('awayTeam', {}).get('teamCity'),
                'away_team_score': game.get('awayTeam', {}).get('score'),
            }
            games_list.append(game_info)
        
        # Create DataFrame
        df = pd.DataFrame(games_list)
        
        # Save processed data
        self._save_processed_data(df, "live_games")
        
        return df
    
    def process_recent_games(self, data):
        """Process recent games data"""
        if not data or 'resultSets' not in data:
            print("No valid recent games data to process")
            return None
        
        # Extract game data
        try:
            # The data structure can be complex, so we need to handle it carefully
            result_sets = data['resultSets'][0]
            headers = result_sets['headers']
            rows = result_sets['rowSet']
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=headers)
            
            # Save processed data
            self._save_processed_data(df, "recent_games")
            
            return df
        except Exception as e:
            print(f"Error processing recent games: {e}")
            return None
    
    def _save_processed_data(self, df, data_type):
        """Save processed data to CSV"""
        if df is None or df.empty:
            print(f"No data to save for {data_type}")
            return
            
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{data_type}_processed_{timestamp}.csv"
        filepath = Path(PROCESSED_DATA_DIR) / filename
        
        df.to_csv(filepath, index=False)
        print(f"Processed data saved to {filepath}")


if __name__ == "__main__":
    # Test the processor with a sample file
    processor = NBADataProcessor()
    
    # Find the latest raw live games file
    raw_dir = Path(RAW_DATA_DIR)
    live_game_files = list(raw_dir.glob("live_games_*.json"))
    
    if live_game_files:
        latest_file = max(live_game_files, key=lambda x: x.stat().st_mtime)
        print(f"Processing {latest_file}")
        
        with open(latest_file, 'r') as f:
            data = json.load(f)
        
        processed = processor.process_live_games(data)
        print("Processing complete")
    else:
        print("No raw live games files found")
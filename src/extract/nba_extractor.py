import json
import os
from datetime import datetime
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from nba_api.stats.static import teams, players
from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats.endpoints import leaguegamefinder, commonplayerinfo, playergamelog

from config.settings import RAW_DATA_DIR
from load.sqlite_warehouse import SQLiteWarehouse

class NBADataExtractor:
    """Class to extract data from NBA API"""
    
    def __init__(self):
        """Initialize the extractor"""
        self.warehouse = SQLiteWarehouse()
        self.raw_dir = os.path.join(project_root, RAW_DATA_DIR)
        os.makedirs(self.raw_dir, exist_ok=True)
    
    def extract_teams(self):
        """Extract team data"""
        print("Extracting team data...")
        
        # Get teams from the API
        nba_teams = teams.get_teams()
        
        # Save raw data
        self._save_raw_data(nba_teams, "teams")
        
        # Transform and load to warehouse
        teams_data = []
        for team in nba_teams:
            teams_data.append({
                'team_id': str(team['id']),
                'team_name': team['nickname'],
                'team_city': team['city'],
                'team_abbreviation': team['abbreviation'],
                'conference': team.get('conference'),
                'division': team.get('division')
            })
        
        # Load to warehouse
        self.warehouse.load_teams(teams_data)
        
        return teams_data
    
    def extract_players(self, limit=50):
        """Extract player data (limited to avoid rate limiting)"""
        print(f"Extracting data for {limit} players...")
        
        # Get active players from the API
        all_players = players.get_active_players()
        
        # Limit the number of players to extract to avoid rate limiting
        players_subset = all_players[:limit]
        
        # Save raw data
        self._save_raw_data(players_subset, "players_subset")
        
        # Transform and load to warehouse
        players_data = []
        for player in players_subset:
            players_data.append({
                'player_id': str(player['id']),
                'first_name': player['first_name'],
                'last_name': player['last_name'],
                'jersey_num': None,  # Would need additional API call
                'position': None,    # Would need additional API call
                'height_inches': None,
                'weight_lbs': None,
                'birth_date': None,
                'draft_year': None
            })
        
        # Load to warehouse
        self.warehouse.load_players(players_data)
        
        return players_data
    
    def _save_raw_data(self, data, data_type):
        """Save raw data to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{data_type}_{timestamp}.json"
        filepath = os.path.join(self.raw_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Raw data saved to {filepath}")
    
    def close(self):
        """Close connections"""
        self.warehouse.close()

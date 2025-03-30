# src/extract/warehouse_extractor.py
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from nba_api.stats.endpoints import leaguegamefinder, commonteamroster, boxscoretraditionalv2
from nba_api.stats.static import teams, players

from config.settings import RAW_DATA_DIR
from load.data_warehouse import NBADataWarehouse

class NBAWarehouseExtractor:
    """Class to extract data for the NBA data warehouse"""
    
    def __init__(self):
        """Initialize the extractor"""
        self.warehouse = NBADataWarehouse()
        self.raw_dir = os.path.join(project_root, RAW_DATA_DIR)
        os.makedirs(self.raw_dir, exist_ok=True)
    
    def extract_teams(self):
        """Extract and load team data"""
        print("Extracting team data...")
        nba_teams = teams.get_teams()
        
        # Save raw data
        self._save_raw_data(nba_teams, "teams")
        
        # Transform and load to warehouse
        teams_data = []
        for team in nba_teams:
            teams_data.append({
                'team_id': team['id'],
                'team_name': team['nickname'],
                'team_city': team['city'],
                'team_abbreviation': team['abbreviation'],
                'conference': team.get('conference'),
                'division': team.get('division')
            })
        
        self.warehouse.load_teams(teams_data)
        return teams_data
    
    def extract_games(self, days_back=7):
        """Extract and load game data for recent games"""
        print(f"Extracting game data for the last {days_back} days...")
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        date_from = start_date.strftime("%m/%d/%Y")
        
        # Get games
        game_finder = leaguegamefinder.LeagueGameFinder(
            date_from_nullable=date_from,
            league_id_nullable='00'  # NBA
        )
        games_dict = game_finder.get_dict()
        
        # Save raw data
        self._save_raw_data(games_dict, "recent_games")
        
        # Transform and load to warehouse
        games_data = []
        if 'resultSets' in games_dict and len(games_dict['resultSets']) > 0:
            result_set = games_dict['resultSets'][0]
            headers = result_set['headers']
            rows = result_set['rowSet']
            
            unique_game_ids = set()
            for row in rows:
                game_dict = dict(zip(headers, row))
                game_id = game_dict.get('GAME_ID')
                
                # Skip duplicates (each game appears twice, once for each team)
                if game_id in unique_game_ids:
                    continue
                
                unique_game_ids.add(game_id)
                
                # Determine home and away teams
                is_home = game_dict.get('MATCHUP', '').find(' vs. ') != -1
                team_id = game_dict.get('TEAM_ID')
                opponent_id = None  # We'll need to find this from another api call
                
                game_data = {
                    'game_id': game_id,
                    'game_date': game_dict.get('GAME_DATE'),
                    'season': game_dict.get('SEASON_ID'),
                    'season_type': 'Regular Season' if game_dict.get('SEASON_ID', '').startswith('2') else 'Playoffs',
                    'home_team_id': team_id if is_home else opponent_id,
                    'away_team_id': opponent_id if is_home else team_id,
                    'venue_id': None,  # Would need additional API call
                    'home_team_score': game_dict.get('PTS') if is_home else None,
                    'away_team_score': None if is_home else game_dict.get('PTS')
                }
                
                games_data.append(game_data)
                
                # In a real implementation, you'd need additional API calls to get:
                # - Complete game details (period scores, etc)
                # - Opponent team ID
                # - Venue information
        
        self.warehouse.load_games(games_data)
        return games_data
    
    def extract_player_game_stats(self, game_id):
        """Extract and load player game stats for a specific game"""
        print(f"Extracting player stats for game {game_id}...")
        
        # Get box score
        box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        box_score_dict = box_score.get_dict()
        
        # Save raw data
        self._save_raw_data(box_score_dict, f"boxscore_{game_id}")
        
        # Transform and load to warehouse
        player_stats = []
        if 'resultSets' in box_score_dict:
            for result_set in box_score_dict['resultSets']:
                if result_set['name'] == 'PlayerStats':
                    headers = result_set['headers']
                    rows = result_set['rowSet']
                    
                    for row in rows:
                        stat_dict = dict(zip(headers, row))
                        
                        player_stat = {
                            'game_id': game_id,
                            'player_id': stat_dict.get('PLAYER_ID'),
                            'team_id': stat_dict.get('TEAM_ID'),
                            'minutes_played': self._convert_minutes(stat_dict.get('MIN')),
                            'points': stat_dict.get('PTS'),
                            'assists': stat_dict.get('AST'),
                            'rebounds': stat_dict.get('REB'),
                            'steals': stat_dict.get('STL'),
                            'blocks': stat_dict.get('BLK'),
                            'turnovers': stat_dict.get('TO'),
                            'personal_fouls': stat_dict.get('PF'),
                            'fg_made': stat_dict.get('FGM'),
                            'fg_attempted': stat_dict.get('FGA'),
                            'fg_pct': stat_dict.get('FG_PCT'),
                            'fg3_made': stat_dict.get('FG3M'),
                            'fg3_attempted': stat_dict.get('FG3A'),
                            'fg3_pct': stat_dict.get('FG3_PCT'),
                            'ft_made': stat_dict.get('FTM'),
                            'ft_attempted': stat_dict.get('FTA'),
                            'ft_pct': stat_dict.get('FT_PCT'),
                            'plus_minus': stat_dict.get('PLUS_MINUS')
                        }
                        
                        player_stats.append(player_stat)
        
        self.warehouse.load_player_game_stats(player_stats)
        return player_stats
    
    def _convert_minutes(self, minutes_str):
        """Convert minutes string (e.g. '12:34') to decimal minutes"""
        if not minutes_str:
            return 0
            
        try:
            if ':' in minutes_str:
                mins, secs = minutes_str.split(':')
                return int(mins) + int(secs) / 60
            return float(minutes_str)
        except (ValueError, TypeError):
            return 0
    
    def _save_raw_data(self, data, data_type):
        """Save raw data to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{data_type}_{timestamp}.json"
        filepath = os.path.join(self.raw_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Raw data saved to {filepath}")
    
    def close(self):
        """Close the warehouse connection"""
        self.warehouse.close()

# Example usage
if __name__ == "__main__":
    extractor = NBAWarehouseExtractor()
    
    # Extract teams
    teams_data = extractor.extract_teams()
    
    # Extract recent games
    games_data = extractor.extract_games(days_back=30)
    
    # Extract player stats for each game
    for game in games_data:
        extractor.extract_player_game_stats(game['game_id'])
    
    extractor.close()
    print("Data warehouse extraction complete!")
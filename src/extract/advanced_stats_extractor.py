# src/extract/advanced_stats_extractor.py

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import sys
import numpy as np
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from nba_api.stats.endpoints import (
    shotchartdetail, hustlestatsboxscore, 
    boxscoreadvancedv2, boxscoreplayertrackv2,
    playerestimatedmetrics
)
from load.postgres_warehouse import PostgresWarehouse
from config.settings import RAW_DATA_DIR

class NBAAdvancedStatsExtractor:
    """Class to extract advanced player statistics"""
    
    def __init__(self):
        """Initialize the extractor"""
        self.warehouse = PostgresWarehouse()
        self.raw_dir = os.path.join(project_root, RAW_DATA_DIR)
        os.makedirs(self.raw_dir, exist_ok=True)
    
    def extract_player_advanced_stats(self, game_id, player_id, team_id):
        """Extract advanced stats for a player in a specific game"""
        print(f"Extracting advanced stats for player {player_id} in game {game_id}")
        
        # Composite identifier for player-game
        player_game_id = f"{game_id}_{player_id}"
        
        # Initialize advanced stats dictionary
        advanced_stats = {
            'shot_tracking': {},
            'defensive': {},
            'playmaking': {},
            'efficiency': {},
            'hustle': {}
        }
        
        # Extract shot chart data
        try:
            shot_chart = shotchartdetail.ShotChartDetail(
                team_id=team_id,
                player_id=player_id,
                game_id_nullable=game_id,
                context_measure_simple='FGA'
            )
            shot_data = shot_chart.get_dict()
            
            # Save raw data
            self._save_raw_data(shot_data, f"shot_chart_{player_game_id}")
            
            # Process shot data
            self._process_shot_data(shot_data, advanced_stats)
        except Exception as e:
            print(f"Error extracting shot chart data: {e}")
        
        # Extract advanced box score
        try:
            adv_box = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id)
            adv_data = adv_box.get_dict()
            
            # Save raw data
            self._save_raw_data(adv_data, f"boxscore_advanced_{game_id}")
            
            # Process advanced data
            self._process_advanced_data(adv_data, player_id, advanced_stats)
        except Exception as e:
            print(f"Error extracting advanced box score: {e}")
        
        # Extract player tracking
        try:
            tracking = boxscoreplayertrackv2.BoxScorePlayerTrackV2(game_id=game_id)
            tracking_data = tracking.get_dict()
            
            # Save raw data
            self._save_raw_data(tracking_data, f"player_tracking_{game_id}")
            
            # Process tracking data
            self._process_tracking_data(tracking_data, player_id, advanced_stats)
        except Exception as e:
            print(f"Error extracting player tracking: {e}")
        
        # Extract hustle stats
        try:
            hustle = hustlestatsboxscore.HustleStatsBoxScore(game_id=game_id)
            hustle_data = hustle.get_dict()
            
            # Save raw data
            self._save_raw_data(hustle_data, f"hustle_stats_{game_id}")
            
            # Process hustle data
            self._process_hustle_data(hustle_data, player_id, advanced_stats)
        except Exception as e:
            print(f"Error extracting hustle stats: {e}")
        
        # Load data to warehouse
        self.warehouse.load_player_advanced_stats(player_game_id, advanced_stats)
        
        return advanced_stats
    
    def _process_shot_data(self, shot_data, advanced_stats):
        """Process shot chart data into shot zone statistics"""
        if 'resultSets' not in shot_data or len(shot_data['resultSets']) == 0:
            return
        
        try:
            # Extract shot data
            result_set = shot_data['resultSets'][0]
            headers = result_set['headers']
            rows = result_set['rowSet']
            
            if len(rows) == 0:
                return
            
            # Convert to DataFrame for easier processing
            shots_df = pd.DataFrame(rows, columns=headers)
            
            # Process shots by distance
            # 0-3 ft (At the rim)
            shots_0_3ft = shots_df[shots_df['SHOT_ZONE_BASIC'] == 'Restricted Area']
            advanced_stats['shot_tracking']['shots_made_0_3ft'] = len(shots_0_3ft[shots_0_3ft['SHOT_MADE_FLAG'] == 1])
            advanced_stats['shot_tracking']['shots_attempted_0_3ft'] = len(shots_0_3ft)
            
            if len(shots_0_3ft) > 0:
                advanced_stats['shot_tracking']['shots_pct_0_3ft'] = advanced_stats['shot_tracking']['shots_made_0_3ft'] / advanced_stats['shot_tracking']['shots_attempted_0_3ft']
            
            # 3-10 ft (Paint)
            shots_3_10ft = shots_df[shots_df['SHOT_ZONE_BASIC'] == 'In The Paint (Non-RA)']
            advanced_stats['shot_tracking']['shots_made_3_10ft'] = len(shots_3_10ft[shots_3_10ft['SHOT_MADE_FLAG'] == 1])
            advanced_stats['shot_tracking']['shots_attempted_3_10ft'] = len(shots_3_10ft)
            
            if len(shots_3_10ft) > 0:
                advanced_stats['shot_tracking']['shots_pct_3_10ft'] = advanced_stats['shot_tracking']['shots_made_3_10ft'] / advanced_stats['shot_tracking']['shots_attempted_3_10ft']
            
            # 10-16 ft (Mid-range)
            shots_10_16ft = shots_df[(shots_df['SHOT_ZONE_BASIC'] == 'Mid-Range') & (shots_df['SHOT_ZONE_AREA'].isin(['Left Side(L)', 'Right Side(R)', 'Center(C)']))]
            advanced_stats['shot_tracking']['shots_made_10_16ft'] = len(shots_10_16ft[shots_10_16ft['SHOT_MADE_FLAG'] == 1])
            advanced_stats['shot_tracking']['shots_attempted_10_16ft'] = len(shots_10_16ft)
            
            if len(shots_10_16ft) > 0:
                advanced_stats['shot_tracking']['shots_pct_10_16ft'] = advanced_stats['shot_tracking']['shots_made_10_16ft'] / advanced_stats['shot_tracking']['shots_attempted_10_16ft']
            
            # 16ft - 3pt (Long mid-range)
            shots_16ft_3pt = shots_df[(shots_df['SHOT_ZONE_BASIC'] == 'Mid-Range') & ~(shots_df['SHOT_ZONE_AREA'].isin(['Left Side(L)', 'Right Side(R)', 'Center(C)']))]
            advanced_stats['shot_tracking']['shots_made_16ft_3pt'] = len(shots_16ft_3pt[shots_16ft_3pt['SHOT_MADE_FLAG'] == 1])
            advanced_stats['shot_tracking']['shots_attempted_16ft_3pt'] = len(shots_16ft_3pt)
            
            if len(shots_16ft_3pt) > 0:
                advanced_stats['shot_tracking']['shots_pct_16ft_3pt'] = advanced_stats['shot_tracking']['shots_made_16ft_3pt'] / advanced_stats['shot_tracking']['shots_attempted_16ft_3pt']
            
            # Corner 3s
            corner_3s = shots_df[(shots_df['SHOT_ZONE_BASIC'] == 'Above the Break 3') & (shots_df['SHOT_ZONE_AREA'].isin(['Left Side Center(LC)', 'Right Side Center(RC)']))]
            advanced_stats['shot_tracking']['corner_3_made'] = len(corner_3s[corner_3s['SHOT_MADE_FLAG'] == 1])
            advanced_stats['shot_tracking']['corner_3_attempted'] = len(corner_3s)
            
            if len(corner_3s) > 0:
                advanced_stats['shot_tracking']['corner_3_pct'] = advanced_stats['shot_tracking']['corner_3_made'] / advanced_stats['shot_tracking']['corner_3_attempted']
            
            # Above break 3s
            above_break_3s = shots_df[(shots_df['SHOT_ZONE_BASIC'] == 'Above the Break 3') & ~(shots_df['SHOT_ZONE_AREA'].isin(['Left Side Center(LC)', 'Right Side Center(RC)']))]
            advanced_stats['shot_tracking']['above_break_3_made'] = len(above_break_3s[above_break_3s['SHOT_MADE_FLAG'] == 1])
            advanced_stats['shot_tracking']['above_break_3_attempted'] = len(above_break_3s)
            
            if len(above_break_3s) > 0:
                advanced_stats['shot_tracking']['above_break_3_pct'] = advanced_stats['shot_tracking']['above_break_3_made'] / advanced_stats['shot_tracking']['above_break_3_attempted']
            
            # Dunks
            dunks = shots_df[shots_df['ACTION_TYPE'].str.contains('Dunk', na=False)]
            advanced_stats['shot_tracking']['dunk_made'] = len(dunks[dunks['SHOT_MADE_FLAG'] == 1])
            advanced_stats['shot_tracking']['dunk_attempted'] = len(dunks)
        
        except Exception as e:
            print(f"Error processing shot data: {e}")
    
    def _process_advanced_data(self, adv_data, player_id, advanced_stats):
        """Process advanced box score data"""
        if 'resultSets' not in adv_data:
            return
        
        try:
            # Find player stats
            for result_set in adv_data['resultSets']:
                if result_set['name'] == 'PlayerStats':
                    headers = result_set['headers']
                    rows = result_set['rowSet']
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(rows, columns=headers)
                    
                    # Find player row
                    player_row = df[df['PLAYER_ID'] == int(player_id)]
                    if player_row.empty:
                        continue
                    
                    # Extract efficiency stats
                    advanced_stats['efficiency']['offensive_rating'] = player_row['OFF_RATING'].values[0]
                    advanced_stats['efficiency']['defensive_rating'] = player_row['DEF_RATING'].values[0]
                    advanced_stats['efficiency']['net_rating'] = player_row['NET_RATING'].values[0]
                    advanced_stats['efficiency']['effective_fg_pct'] = player_row['EFG_PCT'].values[0]
                    advanced_stats['efficiency']['true_shooting_pct'] = player_row['TS_PCT'].values[0]
                    advanced_stats['efficiency']['offensive_rebound_pct'] = player_row['OREB_PCT'].values[0]
                    advanced_stats['efficiency']['defensive_rebound_pct'] = player_row['DREB_PCT'].values[0]
                    advanced_stats['efficiency']['total_rebound_pct'] = player_row['REB_PCT'].values[0]
                    
                    # Extract playmaking stats
                    advanced_stats['playmaking']['assist_pct'] = player_row['AST_PCT'].values[0]
                    advanced_stats['playmaking']['assist_to_turnover_ratio'] = player_row['AST_TO'].values[0]
                    advanced_stats['playmaking']['assist_ratio'] = player_row['AST_RATIO'].values[0]
                    advanced_stats['playmaking']['usage_pct'] = player_row['USG_PCT'].values[0]
                    
                    # Extract defensive stats
                    advanced_stats['defensive']['steal_pct'] = player_row['STL_PCT'].values[0]
                    advanced_stats['defensive']['block_pct'] = player_row['BLK_PCT'].values[0]
                    break
        
        except Exception as e:
            print(f"Error processing advanced data: {e}")
    
    def _process_tracking_data(self, tracking_data, player_id, advanced_stats):
        """Process player tracking data"""
        if 'resultSets' not in tracking_data:
            return
        
        try:
            # Find player stats
            for result_set in tracking_data['resultSets']:
                if result_set['name'] == 'PlayerTrackingStats':
                    headers = result_set['headers']
                    rows = result_set['rowSet']
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(rows, columns=headers)
                    
                    # Find player row
                    player_row = df[df['PLAYER_ID'] == int(player_id)]
                    if player_row.empty:
                        continue
                    
                    # Extract playmaking stats
                    advanced_stats['playmaking']['potential_assists'] = player_row['POTENTIAL_AST'].values[0]
                    advanced_stats['playmaking']['assist_points_created'] = player_row['AST_PTS_CREATED'].values[0]
                    advanced_stats['playmaking']['passes_made'] = player_row['PASSES_MADE'].values[0]
                    advanced_stats['playmaking']['passes_received'] = player_row['PASSES_RECEIVED'].values[0]
                    advanced_stats['playmaking']['secondary_assists'] = player_row['SECONDARY_AST'].values[0]
                    advanced_stats['playmaking']['free_throw_assists'] = player_row['FT_AST'].values[0]
                    
                    # Extract touches
                    advanced_stats['playmaking']['time_of_possession'] = player_row['TIME_OF_POSS'].values[0]
                    advanced_stats['playmaking']['avg_dribbles_per_touch'] = player_row['AVG_DRIB_PER_TOUCH'].values[0]
                    advanced_stats['playmaking']['avg_touch_time'] = player_row['AVG_TOUCH_TIME'].values[0]
                    advanced_stats['playmaking']['elbow_touches'] = player_row['ELBOW_TOUCHES'].values[0]
                    advanced_stats['playmaking']['post_touches'] = player_row['POST_TOUCHES'].values[0]
                    advanced_stats['playmaking']['paint_touches'] = player_row['PAINT_TOUCHES'].values[0]
                    advanced_stats['playmaking']['front_court_touches'] = player_row['FRONT_CT_TOUCHES'].values[0]
                    
                    # Extract efficiency stats
                    advanced_stats['efficiency']['points_per_touch'] = player_row['PTS_PER_TOUCH'].values[0]
                    
                    # Extract speed/distance
                    advanced_stats['hustle']['distance_miles'] = player_row['DIST_MILES'].values[0]
                    advanced_stats['hustle']['distance_miles_offense'] = player_row['DIST_MILES_OFF'].values[0]
                    advanced_stats['hustle']['distance_miles_defense'] = player_row['DIST_MILES_DEF'].values[0]
                    advanced_stats['hustle']['avg_speed_mph'] = player_row['AVG_SPEED'].values[0]
                    advanced_stats['hustle']['avg_speed_mph_offense'] = player_row['AVG_SPEED_OFF'].values[0]
                    advanced_stats['hustle']['avg_speed_mph_defense'] = player_row['AVG_SPEED_DEF'].values[0]
                    break
        
        except Exception as e:
            print(f"Error processing tracking data: {e}")
    
    def _process_hustle_data(self, hustle_data, player_id, advanced_stats):
        """Process hustle stats data"""
        if 'resultSets' not in hustle_data:
            return
        
        try:
            # Find player hustle stats
            for result_set in hustle_data['resultSets']:
                if result_set['name'] == 'PlayerHustleStats':
                    headers = result_set['headers']
                    rows = result_set['rowSet']
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(rows, columns=headers)
                    
                    # Find player row
                    player_row = df[df['PLAYER_ID'] == int(player_id)]
                    if player_row.empty:
                        continue
                    
                    # Extract hustle stats
                    advanced_stats['hustle']['contested_shots_2pt'] = player_row['CONTESTED_SHOTS_2PT'].values[0]
                    advanced_stats['hustle']['contested_shots_3pt'] = player_row['CONTESTED_SHOTS_3PT'].values[0]
                    advanced_stats['hustle']['deflections'] = player_row['DEFLECTIONS'].values[0]
                    advanced_stats['hustle']['loose_balls_recovered'] = player_row['LOOSE_BALLS_RECOVERED'].values[0]
                    advanced_stats['hustle']['charges_drawn'] = player_row['CHARGES_DRAWN'].values[0]
                    advanced_stats['hustle']['screen_assists'] = player_row['SCREEN_ASSISTS'].values[0]
                    advanced_stats['hustle']['screen_assist_points'] = player_row['SCREEN_AST_PTS'].values[0]
                    advanced_stats['hustle']['box_outs'] = player_row['BOX_OUTS'].values[0]
                    advanced_stats['hustle']['box_outs_offensive'] = player_row['BOX_OUTS_OFF'].values[0]
                    advanced_stats['hustle']['box_outs_defensive'] = player_row['BOX_OUTS_DEF'].values[0]
                    
                    # Update defensive stats
                    advanced_stats['defensive']['contested_shots'] = (
                        player_row['CONTESTED_SHOTS_2PT'].values[0] +
                        player_row['CONTESTED_SHOTS_3PT'].values[0]
                    )
                    advanced_stats['defensive']['deflections'] = player_row['DEFLECTIONS'].values[0]
                    advanced_stats['defensive']['charges_drawn'] = player_row['CHARGES_DRAWN'].values[0]
                    advanced_stats['defensive']['box_outs'] = player_row['BOX_OUTS'].values[0]
                    advanced_stats['defensive']['loose_balls_recovered'] = player_row['LOOSE_BALLS_RECOVERED'].values[0]
                    break
        
        except Exception as e:
            print(f"Error processing hustle data: {e}")
    
    def _save_raw_data(self, data, data_type):
        """Save raw data to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{data_type}_{timestamp}.json"
        filepath = os.path.join(self.raw_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    
    def close(self):
        """Close connections"""
        self.warehouse.close()

# Example usage
if __name__ == "__main__":
    extractor = NBAAdvancedStatsExtractor()
    
    # Example: Extract advanced stats for LeBron James in a specific game
    game_id = "0022200063"  # Replace with a real game ID
    player_id = "2544"      # LeBron James
    team_id = "1610612747"  # Lakers
    
    stats = extractor.extract_player_advanced_stats(game_id, player_id, team_id)
    extractor.close()
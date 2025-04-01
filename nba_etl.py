"""
NBA Data Warehouse - Scheduled ETL Job
--------------------------------------
This script runs a scheduled ETL process to keep the NBA data warehouse up-to-date.
It handles extraction, transformation, and loading for all tables in the database.
"""
import json
import os
import sqlite3
import pandas as pd
import numpy as np
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
import schedule
import sys
from http.client import RemoteDisconnected

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("NBA-ETL")

try:
    from nba_api.stats.static import teams, players
    from nba_api.stats.endpoints import (
        leaguegamefinder, boxscoretraditionalv2, 
        boxscoreadvancedv2, boxscoreplayertrackv2,
        hustlestatsboxscore, shotchartdetail
    )
except ImportError:
    logger.error("Error: nba_api package not installed. Run: pip install nba_api")
    sys.exit(1)

# Create directories
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

# Database settings
DATABASE_PATH = "data/processed/nba_data.db"
RAW_DATA_DIR = "data/raw"

class NBADataWarehouse:
    """Class to manage the NBA data warehouse ETL process"""
    
    def __init__(self):
        """Initialize the warehouse manager"""
        self.conn = None
        self.cursor = None
        self.team_id_map = {}
        self.player_id_map = {}
    
    def connect_to_db(self):
        """Connect to the SQLite database"""
        try:
            self.conn = sqlite3.connect(DATABASE_PATH)
            self.cursor = self.conn.cursor()
            logger.info("Connected to database")
            return True
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            return False
    
    def close_db(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def ensure_tables_exist(self):
        """Ensure all required tables exist in the database"""
        if not self.conn:
            if not self.connect_to_db():
                return False
        
        tables_to_check = [
            "dim_teams", "dim_players", "dim_games", "dim_dates", "dim_venues",
            "fact_game_stats", "fact_player_game_stats", "fact_player_shot_tracking",
            "fact_player_defensive", "fact_player_efficiency", "fact_player_hustle",
            "fact_player_playmaking"
        ]
        
        tables_to_create = {
            "dim_teams": """
                CREATE TABLE IF NOT EXISTS dim_teams (
                    team_id TEXT PRIMARY KEY,
                    team_name TEXT,
                    team_city TEXT,
                    team_abbreviation TEXT,
                    conference TEXT,
                    division TEXT,
                    inserted_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """,
            "dim_players": """
                CREATE TABLE IF NOT EXISTS dim_players (
                    player_id TEXT PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    jersey_num INTEGER,
                    position TEXT,
                    height_inches INTEGER,
                    weight_lbs INTEGER,
                    birth_date DATE,
                    draft_year INTEGER,
                    inserted_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """,
            "dim_games": """
                CREATE TABLE IF NOT EXISTS dim_games (
                    game_id TEXT PRIMARY KEY,
                    game_date DATE,
                    season TEXT,
                    season_type TEXT,
                    inserted_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """,
            "dim_dates": """
                CREATE TABLE IF NOT EXISTS dim_dates (
                    date_id TEXT PRIMARY KEY,
                    full_date DATE,
                    day_of_week INTEGER,
                    day_name TEXT,
                    day_of_month INTEGER,
                    day_of_year INTEGER,
                    week_of_year INTEGER,
                    month_num INTEGER,
                    month_name TEXT,
                    quarter INTEGER,
                    year INTEGER,
                    is_weekend INTEGER,
                    inserted_at TIMESTAMP
                )
            """,
            "dim_venues": """
                CREATE TABLE IF NOT EXISTS dim_venues (
                    venue_id TEXT PRIMARY KEY,
                    venue_name TEXT,
                    city TEXT,
                    state TEXT,
                    country TEXT,
                    capacity INTEGER,
                    inserted_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """,
            "fact_game_stats": """
                CREATE TABLE IF NOT EXISTS fact_game_stats (
                    game_id TEXT PRIMARY KEY,
                    date_id TEXT,
                    home_team_id TEXT,
                    away_team_id TEXT,
                    home_team_score INTEGER,
                    away_team_score INTEGER,
                    home_q1_score INTEGER,
                    home_q2_score INTEGER,
                    home_q3_score INTEGER,
                    home_q4_score INTEGER,
                    away_q1_score INTEGER,
                    away_q2_score INTEGER,
                    away_q3_score INTEGER,
                    away_q4_score INTEGER,
                    home_ot_score INTEGER,
                    away_ot_score INTEGER,
                    attendance INTEGER,
                    lead_changes INTEGER,
                    times_tied INTEGER,
                    duration_minutes INTEGER,
                    inserted_at TIMESTAMP
                )
            """,
            "fact_player_game_stats": """
                CREATE TABLE IF NOT EXISTS fact_player_game_stats (
                    player_game_id TEXT PRIMARY KEY,
                    game_id TEXT,
                    player_id TEXT,
                    team_id TEXT,
                    date_id TEXT,
                    minutes_played REAL,
                    points INTEGER,
                    assists INTEGER,
                    rebounds INTEGER,
                    steals INTEGER,
                    blocks INTEGER,
                    turnovers INTEGER,
                    personal_fouls INTEGER,
                    fg_made INTEGER,
                    fg_attempted INTEGER,
                    fg_pct REAL,
                    fg3_made INTEGER,
                    fg3_attempted INTEGER,
                    fg3_pct REAL,
                    ft_made INTEGER,
                    ft_attempted INTEGER,
                    ft_pct REAL,
                    plus_minus INTEGER,
                    inserted_at TIMESTAMP
                )
            """,
            "fact_player_shot_tracking": """
                CREATE TABLE IF NOT EXISTS fact_player_shot_tracking (
                    player_game_id TEXT PRIMARY KEY,
                    shots_made_0_3ft INTEGER,
                    shots_attempted_0_3ft INTEGER,
                    shots_pct_0_3ft REAL,
                    shots_made_3_10ft INTEGER,
                    shots_attempted_3_10ft INTEGER,
                    shots_pct_3_10ft REAL,
                    shots_made_10_16ft INTEGER,
                    shots_attempted_10_16ft INTEGER,
                    shots_pct_10_16ft REAL,
                    shots_made_16ft_3pt INTEGER,
                    shots_attempted_16ft_3pt INTEGER,
                    shots_pct_16ft_3pt REAL,
                    corner_3_made INTEGER,
                    corner_3_attempted INTEGER,
                    corner_3_pct REAL,
                    above_break_3_made INTEGER,
                    above_break_3_attempted INTEGER,
                    above_break_3_pct REAL,
                    dunk_made INTEGER,
                    dunk_attempted INTEGER,
                    inserted_at TIMESTAMP
                )
            """,
            "fact_player_defensive": """
                CREATE TABLE IF NOT EXISTS fact_player_defensive (
                    player_game_id TEXT PRIMARY KEY,
                    defensive_rebounds INTEGER,
                    contested_shots INTEGER,
                    contested_shots_made INTEGER,
                    contested_shots_pct REAL,
                    deflections INTEGER,
                    charges_drawn INTEGER,
                    box_outs INTEGER,
                    loose_balls_recovered INTEGER,
                    screen_assists INTEGER,
                    defensive_win_shares REAL,
                    defensive_rating REAL,
                    steals_per_foul REAL,
                    blocks_per_foul REAL,
                    block_pct REAL,
                    steal_pct REAL,
                    defensive_box_plus_minus REAL,
                    inserted_at TIMESTAMP
                )
            """,
            "fact_player_efficiency": """
                CREATE TABLE IF NOT EXISTS fact_player_efficiency (
                    player_game_id TEXT PRIMARY KEY,
                    true_shooting_pct REAL,
                    effective_fg_pct REAL,
                    offensive_rating REAL,
                    offensive_win_shares REAL,
                    offensive_box_plus_minus REAL,
                    player_impact_estimate REAL,
                    game_score REAL,
                    points_per_shot REAL,
                    points_per_touch REAL,
                    points_per_possession REAL,
                    offensive_rebound_pct REAL,
                    defensive_rebound_pct REAL,
                    total_rebound_pct REAL,
                    net_rating REAL,
                    value_added REAL,
                    estimated_wins_added REAL,
                    inserted_at TIMESTAMP
                )
            """,
            "fact_player_hustle": """
                CREATE TABLE IF NOT EXISTS fact_player_hustle (
                    player_game_id TEXT PRIMARY KEY,
                    contested_shots_2pt INTEGER,
                    contested_shots_3pt INTEGER,
                    deflections INTEGER,
                    loose_balls_recovered INTEGER,
                    charges_drawn INTEGER,
                    screen_assists INTEGER,
                    screen_assist_points INTEGER,
                    box_outs INTEGER,
                    box_outs_offensive INTEGER,
                    box_outs_defensive INTEGER,
                    distance_miles REAL,
                    distance_miles_offense REAL,
                    distance_miles_defense REAL,
                    avg_speed_mph REAL,
                    avg_speed_mph_offense REAL,
                    avg_speed_mph_defense REAL,
                    inserted_at TIMESTAMP
                )
            """,
            "fact_player_playmaking": """
                CREATE TABLE IF NOT EXISTS fact_player_playmaking (
                    player_game_id TEXT PRIMARY KEY,
                    potential_assists INTEGER,
                    assist_points_created INTEGER,
                    passes_made INTEGER,
                    passes_received INTEGER,
                    secondary_assists INTEGER,
                    free_throw_assists INTEGER,
                    assist_to_turnover_ratio REAL,
                    assist_ratio REAL,
                    assist_pct REAL,
                    time_of_possession REAL,
                    avg_dribbles_per_touch REAL,
                    avg_touch_time REAL,
                    usage_pct REAL,
                    front_court_touches INTEGER,
                    elbow_touches INTEGER,
                    post_touches INTEGER,
                    paint_touches INTEGER,
                    inserted_at TIMESTAMP
                )
            """
        }
        
        tables_missing = []
        
        # Check which tables exist
        for table in tables_to_check:
            self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
            if not self.cursor.fetchone():
                tables_missing.append(table)
        
        # Create missing tables
        for table in tables_missing:
            if table in tables_to_create:
                try:
                    self.cursor.execute(tables_to_create[table])
                    logger.info(f"Created table: {table}")
                except Exception as e:
                    logger.error(f"Error creating table {table}: {e}")
                    return False
        
        self.conn.commit()
        
        if tables_missing:
            logger.info(f"Created {len(tables_missing)} missing tables: {', '.join(tables_missing)}")
        else:
            logger.info("All required tables already exist")
        
        return True
    
    def _save_raw_data(self, data, data_type):
        """Save raw API data to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{data_type}_{timestamp}.json"
        filepath = os.path.join(RAW_DATA_DIR, filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Raw data saved to {filepath}")
        return filepath
    
    def _ensure_date_exists(self, date_str):
        """Ensure date exists in dim_dates table"""
        if not date_str:
            return None
            
        try:
            # Convert string to date object
            if isinstance(date_str, str):
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            else:
                date_obj = date_str
                date_str = date_obj.strftime("%Y-%m-%d")
            
            # Create date_id (YYYYMMDD format)
            date_id = date_str.replace('-', '')
            
            # Check if date already exists
            self.cursor.execute("SELECT 1 FROM dim_dates WHERE date_id = ?", (date_id,))
            if not self.cursor.fetchone():
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Insert date
                self.cursor.execute("""
                INSERT INTO dim_dates (
                    date_id, full_date, day_of_week, day_name,
                    day_of_month, day_of_year, week_of_year,
                    month_num, month_name, quarter, year,
                    is_weekend, inserted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    date_id,
                    date_str,
                    date_obj.weekday(),
                    date_obj.strftime("%A"),
                    date_obj.day,
                    date_obj.timetuple().tm_yday,
                    date_obj.isocalendar()[1],
                    date_obj.month,
                    date_obj.strftime("%B"),
                    (date_obj.month - 1) // 3 + 1,
                    date_obj.year,
                    1 if date_obj.weekday() >= 5 else 0,  # 5 = Saturday, 6 = Sunday
                    now
                ))
                
                self.conn.commit()
                logger.info(f"Added date {date_str} to dim_dates")
            
            return date_id
            
        except Exception as e:
            logger.error(f"Error ensuring date exists for {date_str}: {e}")
            return None
    
    def load_teams(self):
        """Load teams into dim_teams table"""
        logger.info("Loading teams data...")
        
        try:
            # Get teams from API
            nba_teams = teams.get_teams()
            
            # Save raw data
            self._save_raw_data(nba_teams, "teams")
            
            # Load teams into database
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            teams_added = 0
            teams_updated = 0
            
            for team in nba_teams:
                team_id = str(team['id'])
                
                # Store in map for later use
                self.team_id_map[team['abbreviation']] = team_id
                
                # Check if team already exists
                self.cursor.execute("SELECT 1 FROM dim_teams WHERE team_id = ?", (team_id,))
                if self.cursor.fetchone():
                    # Update existing team
                    self.cursor.execute("""
                    UPDATE dim_teams SET
                        team_name = ?,
                        team_city = ?,
                        team_abbreviation = ?,
                        conference = ?,
                        division = ?,
                        updated_at = ?
                    WHERE team_id = ?
                    """, (
                        team['nickname'],
                        team['city'],
                        team['abbreviation'],
                        team.get('conference'),
                        team.get('division'),
                        now,
                        team_id
                    ))
                    teams_updated += 1
                else:
                    # Insert new team
                    self.cursor.execute("""
                    INSERT INTO dim_teams (
                        team_id, team_name, team_city, team_abbreviation,
                        conference, division, inserted_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        team_id,
                        team['nickname'],
                        team['city'],
                        team['abbreviation'],
                        team.get('conference'),
                        team.get('division'),
                        now,
                        now
                    ))
                    teams_added += 1
            
            self.conn.commit()
            logger.info(f"Teams loaded: {teams_added} added, {teams_updated} updated")
            return True
            
        except Exception as e:
            logger.error(f"Error loading teams: {e}")
            return False
    
    def load_players(self):
        """Load players into dim_players table"""
        logger.info("Loading players data...")
        
        try:
            # Get active players from API
            active_players = players.get_active_players()
            
            # Save raw data
            self._save_raw_data(active_players, "active_players")
            
            # Load players into database
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            players_added = 0
            players_updated = 0
            
            for player in active_players:
                player_id = str(player['id'])
                
                # Store in map for later use
                self.player_id_map[player_id] = player_id
                
                # Check if player already exists
                self.cursor.execute("SELECT 1 FROM dim_players WHERE player_id = ?", (player_id,))
                if self.cursor.fetchone():
                    # Update existing player
                    self.cursor.execute("""
                    UPDATE dim_players SET
                        first_name = ?,
                        last_name = ?,
                        updated_at = ?
                    WHERE player_id = ?
                    """, (
                        player['first_name'],
                        player['last_name'],
                        now,
                        player_id
                    ))
                    players_updated += 1
                else:
                    # Insert new player
                    self.cursor.execute("""
                    INSERT INTO dim_players (
                        player_id, first_name, last_name, inserted_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """, (
                        player_id,
                        player['first_name'],
                        player['last_name'],
                        now,
                        now
                    ))
                    players_added += 1
            
            self.conn.commit()
            logger.info(f"Players loaded: {players_added} added, {players_updated} updated")
            return True
            
        except Exception as e:
            logger.error(f"Error loading players: {e}")
            return False
    
    def load_games(self, days_back=3):
        """Load recent games into dim_games and fact_game_stats tables"""
        logger.info(f"Loading games data for the last {days_back} days...")
        
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            date_from = start_date.strftime("%m/%d/%Y")
            
            logger.info(f"Fetching games from {date_from} to today...")
            
            # Get recent games
            game_finder = leaguegamefinder.LeagueGameFinder(
                date_from_nullable=date_from,
                league_id_nullable='00'  # NBA
            )
            games_dict = game_finder.get_dict()
            
            # Save raw data
            self._save_raw_data(games_dict, "recent_games")
            
            # Process game data
            games_added = 0
            games_updated = 0
            game_stats_added = 0
            game_stats_updated = 0
            
            if 'resultSets' in games_dict and len(games_dict['resultSets']) > 0:
                result_set = games_dict['resultSets'][0]
                headers = result_set['headers']
                rows = result_set['rowSet']
                
                # Create DataFrame for easier processing
                df = pd.DataFrame(rows, columns=headers)
                
                # Group games by GAME_ID
                game_groups = df.groupby('GAME_ID')
                
                # Process each game
                for game_id, game_df in game_groups:
                    # Get game info from first row
                    game_info = game_df.iloc[0]
                    game_date = game_info['GAME_DATE']
                    season = game_info['SEASON_ID']
                    
                    # Ensure date exists in dim_dates
                    date_id = self._ensure_date_exists(game_date)
                    
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Check if game already exists
                    self.cursor.execute("SELECT 1 FROM dim_games WHERE game_id = ?", (game_id,))
                    if self.cursor.fetchone():
                        # Update existing game
                        self.cursor.execute("""
                        UPDATE dim_games SET
                            game_date = ?,
                            season = ?,
                            updated_at = ?
                        WHERE game_id = ?
                        """, (
                            game_date,
                            season,
                            now,
                            game_id
                        ))
                        games_updated += 1
                    else:
                        # Insert new game
                        self.cursor.execute("""
                        INSERT INTO dim_games (
                            game_id, game_date, season, season_type, inserted_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            game_id,
                            game_date,
                            season,
                            'Regular Season',  # Simplified - would need logic for playoffs
                            now,
                            now
                        ))
                        games_added += 1
                    
                    # Now process game stats if we have at least 2 teams
                    unique_teams = game_df['TEAM_ID'].unique()
                    if len(unique_teams) >= 2:
                        # For simplicity, assume first team is home, second is away
                        # In production, you'd want more robust logic
                        home_team_id = str(unique_teams[0])
                        away_team_id = str(unique_teams[1])
                        
                        # Get team data
                        home_team_df = game_df[game_df['TEAM_ID'] == int(home_team_id)]
                        away_team_df = game_df[game_df['TEAM_ID'] == int(away_team_id)]
                        
                        if not home_team_df.empty and not away_team_df.empty:
                            home_score = home_team_df['PTS'].iloc[0]
                            away_score = away_team_df['PTS'].iloc[0]
                            
                            # Check if game stats already exist
                            self.cursor.execute("SELECT 1 FROM fact_game_stats WHERE game_id = ?", (game_id,))
                            if self.cursor.fetchone():
                                # Update existing game stats
                                try:
                                    self.cursor.execute("""
                                    UPDATE fact_game_stats SET
                                        date_id = ?,
                                        home_team_id = ?,
                                        away_team_id = ?,
                                        home_team_score = ?,
                                        away_team_score = ?,
                                        inserted_at = ?
                                    WHERE game_id = ?
                                    """, (
                                        date_id,
                                        home_team_id,
                                        away_team_id,
                                        home_score,
                                        away_score,
                                        now,
                                        game_id
                                    ))
                                    game_stats_updated += 1
                                except sqlite3.OperationalError as e:
                                    # Handle case where columns might be different
                                    logger.warning(f"Column mismatch in fact_game_stats: {e}")
                                    
                                    # Try simplified update
                                    self.cursor.execute("""
                                    UPDATE fact_game_stats SET
                                        home_team_id = ?,
                                        away_team_id = ?,
                                        home_team_score = ?,
                                        away_team_score = ?,
                                        inserted_at = ?
                                    WHERE game_id = ?
                                    """, (
                                        home_team_id,
                                        away_team_id,
                                        home_score,
                                        away_score,
                                        now,
                                        game_id
                                    ))
                                    game_stats_updated += 1
                            else:
                                # Insert new game stats
                                try:
                                    self.cursor.execute("""
                                    INSERT INTO fact_game_stats (
                                        game_id, date_id, home_team_id, away_team_id,
                                        home_team_score, away_team_score, inserted_at
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                    """, (
                                        game_id,
                                        date_id,
                                        home_team_id,
                                        away_team_id,
                                        home_score,
                                        away_score,
                                        now
                                    ))
                                    game_stats_added += 1
                                except sqlite3.OperationalError as e:
                                    # Handle case where columns might be different
                                    logger.warning(f"Column mismatch in fact_game_stats: {e}")
                
                self.conn.commit()
                logger.info(f"Games loaded: {games_added} added, {games_updated} updated")
                logger.info(f"Game stats loaded: {game_stats_added} added, {game_stats_updated} updated")
                
                # Return list of game IDs for player stats processing
                unique_game_ids = list(game_groups.groups.keys())
                return unique_game_ids
            
            else:
                logger.info("No games found in the specified date range")
                return []
            
        except Exception as e:
            logger.error(f"Error loading games: {e}")
            return []
    
    def load_player_game_stats(self, game_ids, limit=5):
        """Load player game stats for the specified games"""
        if not game_ids:
            logger.info("No games to process player stats for")
            return False
        
        # Limit the number of games to process
        game_ids_to_process = game_ids[:limit]
        logger.info(f"Loading player stats for {len(game_ids_to_process)} games...")
        
        players_added = 0
        player_stats_added = 0
        shot_tracking_added = 0
        defensive_stats_added = 0
        efficiency_stats_added = 0
        hustle_stats_added = 0
        playmaking_stats_added = 0
        
        for game_id in game_ids_to_process:
            logger.info(f"Processing player stats for game {game_id}...")
            
            try:
                # Get box score
                box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
                box_score_dict = box_score.get_dict()
                
                # Save raw data
                self._save_raw_data(box_score_dict, f"boxscore_{game_id}")
                
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Get game date for date_id
                self.cursor.execute("SELECT game_date FROM dim_games WHERE game_id = ?", (game_id,))
                result = self.cursor.fetchone()
                game_date = result[0] if result else None
                date_id = self._ensure_date_exists(game_date)
                
                # Process player stats
                if 'resultSets' in box_score_dict:
                    for result_set in box_score_dict['resultSets']:
                        if result_set['name'] == 'PlayerStats':
                            headers = result_set['headers']
                            rows = result_set['rowSet']
                            
                            # Create DataFrame
                            player_df = pd.DataFrame(rows, columns=headers)
                            
                            # Process each player
                            for _, player_row in player_df.iterrows():
                                player_id = str(player_row['PLAYER_ID'])
                                player_name = player_row['PLAYER_NAME']
                                team_id = str(player_row['TEAM_ID'])
                                
                                # Check if player exists in dim_players
                                self.cursor.execute("SELECT 1 FROM dim_players WHERE player_id = ?", (player_id,))
                                if not self.cursor.fetchone():
                                    # Split name into first and last
                                    name_parts = player_name.split(" ", 1)
                                    first_name = name_parts[0]
                                    last_name = name_parts[1] if len(name_parts) > 1 else ""
                                    
                                    # Insert player
                                    self.cursor.execute("""
                                    INSERT INTO dim_players (
                                        player_id, first_name, last_name, inserted_at, updated_at
                                    ) VALUES (?, ?, ?, ?, ?)
                                    """, (
                                        player_id,
                                        first_name,
                                        last_name,
                                        now,
                                        now
                                    ))
                                    players_added += 1
                                
                                # Create player_game_id
                                player_game_id = f"{game_id}_{player_id}"
                                
                                # Insert basic player game stats
                                try:
                                    self.cursor.execute("""
                                    INSERT OR REPLACE INTO fact_player_game_stats (
                                        player_game_id, game_id, player_id, team_id, date_id,
                                        minutes_played, points, assists, rebounds, steals, blocks, turnovers,
                                        personal_fouls, fg_made, fg_attempted, fg_pct, fg3_made, fg3_attempted, fg3_pct,
                                        ft_made, ft_attempted, ft_pct, plus_minus, inserted_at
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """, (
                                        player_game_id, game_id, player_id, team_id, date_id,
                                        player_row['MIN'],
                                        player_row['PTS'],
                                        player_row['AST'],
                                        player_row['REB'],
                                        player_row['STL'],
                                        player_row['BLK'],
                                        player_row['TO'],
                                        player_row['PF'],
                                        player_row['FGM'],
                                        player_row['FGA'],
                                        player_row['FG_PCT'],
                                        player_row['FG3M'],
                                        player_row['FG3A'],
                                        player_row['FG3_PCT'],
                                        player_row['FTM'],
                                        player_row['FTA'],
                                        player_row['FT_PCT'],
                                        player_row['PLUS_MINUS'],
                                        now
                                    ))
                                    player_stats_added += 1
                                except sqlite3.OperationalError as e:
                                    # Handle case where columns might be different
                                    logger.warning(f"Schema mismatch in fact_player_game_stats: {e}")
                                    continue
                                
                                # Process advanced stats - we'll do this in separate methods
                                # to keep the code more organized
                                self._process_shot_tracking(player_game_id, game_id, player_id, team_id)
                                self._process_advanced_stats(player_game_id, game_id, player_id, team_id)
                                self._process_hustle_stats(player_game_id, game_id, player_id, team_id)
                
                # Add small delay to avoid API rate limiting
                logger.info(f"Completed processing for game {game_id}. Waiting before next game...")
                time.sleep(15)
                
            except Exception as e:
                logger.error(f"Error processing player stats for game {game_id}: {e}")
                continue
        
        self.conn.commit()
        logger.info(f"Player stats processed: {player_stats_added} player game stats records added/updated")
        logger.info(f"Additional players added: {players_added}")
        
        return True
    
    def _process_shot_tracking(self, player_game_id, game_id, player_id, team_id):
        """Process shot tracking data for a player-game with retry logic"""
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                # Get shot chart data
                shot_chart = shotchartdetail.ShotChartDetail(
                    team_id=team_id,
                    player_id=player_id,
                    game_id_nullable=game_id,
                    context_measure_simple='FGA'
                )
                shot_data = shot_chart.get_dict()
                
                # Save raw data (commented out to reduce disk usage)
                # self._save_raw_data(shot_data, f"shot_chart_{player_game_id}")
                
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Process shot data
                shots_0_3ft = 0
                shots_0_3ft_made = 0
                shots_3_10ft = 0
                shots_3_10ft_made = 0
                shots_10_16ft = 0
                shots_10_16ft_made = 0
                shots_16ft_3pt = 0
                shots_16ft_3pt_made = 0
                corner_3 = 0
                corner_3_made = 0
                above_break_3 = 0
                above_break_3_made = 0
                dunk_attempted = 0
                dunk_made = 0
                
                if 'resultSets' in shot_data and len(shot_data['resultSets']) > 0:
                    result_set = shot_data['resultSets'][0]
                    headers = result_set['headers']
                    rows = result_set['rowSet']
                    
                    if rows:
                        df = pd.DataFrame(rows, columns=headers)
                        
                        # Process by shot zone
                        for _, shot in df.iterrows():
                            shot_made = shot['SHOT_MADE_FLAG'] == 1
                            
                            # Process by shot zone
                            if shot['SHOT_ZONE_BASIC'] == 'Restricted Area':
                                shots_0_3ft += 1
                                if shot_made:
                                    shots_0_3ft_made += 1
                            elif shot['SHOT_ZONE_BASIC'] == 'In The Paint (Non-RA)':
                                shots_3_10ft += 1
                                if shot_made:
                                    shots_3_10ft_made += 1
                            elif shot['SHOT_ZONE_BASIC'] == 'Mid-Range':
                                if shot['SHOT_ZONE_RANGE'] == '8-16 ft.':
                                    shots_10_16ft += 1
                                    if shot_made:
                                        shots_10_16ft_made += 1
                                else:
                                    shots_16ft_3pt += 1
                                    if shot_made:
                                        shots_16ft_3pt_made += 1
                            elif shot['SHOT_ZONE_BASIC'] == 'Above the Break 3':
                                above_break_3 += 1
                                if shot_made:
                                    above_break_3_made += 1
                            elif shot['SHOT_ZONE_BASIC'] == 'Corner 3':
                                corner_3 += 1
                                if shot_made:
                                    corner_3_made += 1
                            
                            # Check for dunks
                            if 'DUNK' in shot.get('ACTION_TYPE', ''):
                                dunk_attempted += 1
                                if shot_made:
                                    dunk_made += 1
                
                # Calculate percentages
                shots_0_3ft_pct = shots_0_3ft_made / shots_0_3ft if shots_0_3ft > 0 else None
                shots_3_10ft_pct = shots_3_10ft_made / shots_3_10ft if shots_3_10ft > 0 else None
                shots_10_16ft_pct = shots_10_16ft_made / shots_10_16ft if shots_10_16ft > 0 else None
                shots_16ft_3pt_pct = shots_16ft_3pt_made / shots_16ft_3pt if shots_16ft_3pt > 0 else None
                corner_3_pct = corner_3_made / corner_3 if corner_3 > 0 else None
                above_break_3_pct = above_break_3_made / above_break_3 if above_break_3 > 0 else None
                
                # Insert into fact_player_shot_tracking
                try:
                    # Check if record exists first
                    self.cursor.execute("SELECT 1 FROM fact_player_shot_tracking WHERE player_game_id = ?", (player_game_id,))
                    if self.cursor.fetchone():
                        # Update existing record
                        self.cursor.execute("""
                        UPDATE fact_player_shot_tracking SET
                            shots_made_0_3ft = ?, shots_attempted_0_3ft = ?, shots_pct_0_3ft = ?,
                            shots_made_3_10ft = ?, shots_attempted_3_10ft = ?, shots_pct_3_10ft = ?,
                            shots_made_10_16ft = ?, shots_attempted_10_16ft = ?, shots_pct_10_16ft = ?,
                            shots_made_16ft_3pt = ?, shots_attempted_16ft_3pt = ?, shots_pct_16ft_3pt = ?,
                            corner_3_made = ?, corner_3_attempted = ?, corner_3_pct = ?,
                            above_break_3_made = ?, above_break_3_attempted = ?, above_break_3_pct = ?,
                            dunk_made = ?, dunk_attempted = ?
                        WHERE player_game_id = ?
                        """, (
                            shots_0_3ft_made, shots_0_3ft, shots_0_3ft_pct,
                            shots_3_10ft_made, shots_3_10ft, shots_3_10ft_pct,
                            shots_10_16ft_made, shots_10_16ft, shots_10_16ft_pct,
                            shots_16ft_3pt_made, shots_16ft_3pt, shots_16ft_3pt_pct,
                            corner_3_made, corner_3, corner_3_pct,
                            above_break_3_made, above_break_3, above_break_3_pct,
                            dunk_made, dunk_attempted,
                            player_game_id
                        ))
                    else:
                        # Insert new record
                        self.cursor.execute("""
                        INSERT INTO fact_player_shot_tracking (
                            player_game_id, shots_made_0_3ft, shots_attempted_0_3ft, shots_pct_0_3ft,
                            shots_made_3_10ft, shots_attempted_3_10ft, shots_pct_3_10ft,
                            shots_made_10_16ft, shots_attempted_10_16ft, shots_pct_10_16ft,
                            shots_made_16ft_3pt, shots_attempted_16ft_3pt, shots_pct_16ft_3pt,
                            corner_3_made, corner_3_attempted, corner_3_pct,
                            above_break_3_made, above_break_3_attempted, above_break_3_pct,
                            dunk_made, dunk_attempted, inserted_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            player_game_id,
                            shots_0_3ft_made, shots_0_3ft, shots_0_3ft_pct,
                            shots_3_10ft_made, shots_3_10ft, shots_3_10ft_pct,
                            shots_10_16ft_made, shots_10_16ft, shots_10_16ft_pct,
                            shots_16ft_3pt_made, shots_16ft_3pt, shots_16ft_3pt_pct,
                            corner_3_made, corner_3, corner_3_pct,
                            above_break_3_made, above_break_3, above_break_3_pct,
                            dunk_made, dunk_attempted, now
                        ))
                    
                    logger.debug(f"Added shot tracking data for {player_game_id}")
                    return True
                    
                except sqlite3.OperationalError as e:
                    logger.warning(f"Schema mismatch in fact_player_shot_tracking: {e}")
                    return False
                    
            except (ConnectionError, TimeoutError, ConnectionResetError, RemoteDisconnected) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Connection error for shot tracking {player_game_id} (attempt {attempt+1}/{max_retries}): {e}")
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    logger.error(f"Failed to process shot tracking for {player_game_id} after {max_retries} attempts: {e}")
                    return False
            except Exception as e:
                logger.error(f"Error processing shot data for {player_game_id}: {e}")
                return False
    
    def _process_advanced_stats(self, player_game_id, game_id, player_id, team_id):
        """Process advanced stats for a player-game with retry logic"""
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                # Get advanced box score
                adv_box = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id)
                adv_data = adv_box.get_dict()
                
                # Add delay before next API call to avoid rate limiting
                time.sleep(1)
                
                # Get player tracking
                tracking = boxscoreplayertrackv2.BoxScorePlayerTrackV2(game_id=game_id)
                tracking_data = tracking.get_dict()
                
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Process advanced data
                if 'resultSets' in adv_data:
                    for result_set in adv_data['resultSets']:
                        if result_set['name'] == 'PlayerStats':
                            headers = result_set['headers']
                            rows = result_set['rowSet']
                            
                            # Create DataFrame
                            df = pd.DataFrame(rows, columns=headers)
                            
                            # Find player row
                            player_row = df[df['PLAYER_ID'] == int(player_id)]
                            if not player_row.empty:
                                # Extract efficiency metrics - with error handling for missing columns
                                try:
                                    offensive_rating = player_row['OFF_RATING'].iloc[0] if 'OFF_RATING' in player_row.columns else None
                                    defensive_rating = player_row['DEF_RATING'].iloc[0] if 'DEF_RATING' in player_row.columns else None
                                    net_rating = player_row['NET_RATING'].iloc[0] if 'NET_RATING' in player_row.columns else None
                                    effective_fg_pct = player_row['EFG_PCT'].iloc[0] if 'EFG_PCT' in player_row.columns else None
                                    true_shooting_pct = player_row['TS_PCT'].iloc[0] if 'TS_PCT' in player_row.columns else None
                                    offensive_rebound_pct = player_row['OREB_PCT'].iloc[0] if 'OREB_PCT' in player_row.columns else None
                                    defensive_rebound_pct = player_row['DREB_PCT'].iloc[0] if 'DREB_PCT' in player_row.columns else None
                                    total_rebound_pct = player_row['REB_PCT'].iloc[0] if 'REB_PCT' in player_row.columns else None
                                    assist_pct = player_row['AST_PCT'].iloc[0] if 'AST_PCT' in player_row.columns else None
                                    
                                    # These fields may not exist - use safe accessor
                                    steal_pct = player_row['STL_PCT'].iloc[0] if 'STL_PCT' in player_row.columns else None
                                    block_pct = player_row['BLK_PCT'].iloc[0] if 'BLK_PCT' in player_row.columns else None
                                    usage_pct = player_row['USG_PCT'].iloc[0] if 'USG_PCT' in player_row.columns else None
                                except Exception as e:
                                    logger.warning(f"Error extracting advanced metrics: {e}")
                                    # Set defaults
                                    offensive_rating = defensive_rating = net_rating = None
                                    effective_fg_pct = true_shooting_pct = None
                                    offensive_rebound_pct = defensive_rebound_pct = total_rebound_pct = None
                                    assist_pct = steal_pct = block_pct = usage_pct = None
                                
                                # Insert into fact_player_efficiency
                                try:
                                    # Check if record exists
                                    self.cursor.execute("SELECT 1 FROM fact_player_efficiency WHERE player_game_id = ?", (player_game_id,))
                                    if self.cursor.fetchone():
                                        # Update existing record
                                        self.cursor.execute("""
                                        UPDATE fact_player_efficiency SET
                                            true_shooting_pct = ?, effective_fg_pct = ?,
                                            offensive_rating = ?, offensive_rebound_pct = ?,
                                            defensive_rebound_pct = ?, total_rebound_pct = ?,
                                            net_rating = ?
                                        WHERE player_game_id = ?
                                        """, (
                                            true_shooting_pct, effective_fg_pct,
                                            offensive_rating, offensive_rebound_pct,
                                            defensive_rebound_pct, total_rebound_pct,
                                            net_rating, player_game_id
                                        ))
                                    else:
                                        # Insert new record
                                        self.cursor.execute("""
                                        INSERT INTO fact_player_efficiency (
                                            player_game_id, true_shooting_pct, effective_fg_pct,
                                            offensive_rating, offensive_rebound_pct,
                                            defensive_rebound_pct, total_rebound_pct,
                                            net_rating, inserted_at
                                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        """, (
                                            player_game_id, true_shooting_pct, effective_fg_pct,
                                            offensive_rating, offensive_rebound_pct,
                                            defensive_rebound_pct, total_rebound_pct,
                                            net_rating, now
                                        ))
                                    
                                    logger.debug(f"Added efficiency data for {player_game_id}")
                                except sqlite3.OperationalError as e:
                                    logger.warning(f"Schema mismatch in fact_player_efficiency: {e}")
                                
                                # Insert into fact_player_defensive - only if we have defensive stats
                                if defensive_rating is not None or steal_pct is not None or block_pct is not None:
                                    try:
                                        # Check if record exists
                                        self.cursor.execute("SELECT 1 FROM fact_player_defensive WHERE player_game_id = ?", (player_game_id,))
                                        if self.cursor.fetchone():
                                            # Update existing record
                                            self.cursor.execute("""
                                            UPDATE fact_player_defensive SET
                                                defensive_rating = ?,
                                                steal_pct = ?, block_pct = ?
                                            WHERE player_game_id = ?
                                            """, (
                                                defensive_rating,
                                                steal_pct, block_pct,
                                                player_game_id
                                            ))
                                        else:
                                            # Insert new record
                                            self.cursor.execute("""
                                            INSERT INTO fact_player_defensive (
                                                player_game_id, defensive_rating,
                                                steal_pct, block_pct,
                                                inserted_at
                                            ) VALUES (?, ?, ?, ?, ?)
                                            """, (
                                                player_game_id, defensive_rating,
                                                steal_pct, block_pct,
                                                now
                                            ))
                                        
                                        logger.debug(f"Added defensive data for {player_game_id}")
                                    except sqlite3.OperationalError as e:
                                        logger.warning(f"Schema mismatch in fact_player_defensive: {e}")
                                
                                # Insert into fact_player_playmaking - only if we have playmaking stats
                                if assist_pct is not None or usage_pct is not None:
                                    try:
                                        # Check if record exists
                                        self.cursor.execute("SELECT 1 FROM fact_player_playmaking WHERE player_game_id = ?", (player_game_id,))
                                        if self.cursor.fetchone():
                                            # Update existing record
                                            self.cursor.execute("""
                                            UPDATE fact_player_playmaking SET
                                                assist_pct = ?, usage_pct = ?
                                            WHERE player_game_id = ?
                                            """, (
                                                assist_pct, usage_pct, player_game_id
                                            ))
                                        else:
                                            # Insert new record
                                            self.cursor.execute("""
                                            INSERT INTO fact_player_playmaking (
                                                player_game_id, assist_pct, usage_pct, inserted_at
                                            ) VALUES (?, ?, ?, ?)
                                            """, (
                                                player_game_id, assist_pct, usage_pct, now
                                            ))
                                        
                                        logger.debug(f"Added playmaking data for {player_game_id}")
                                    except sqlite3.OperationalError as e:
                                        logger.warning(f"Schema mismatch in fact_player_playmaking: {e}")
                
                # Process tracking data
                if 'resultSets' in tracking_data:
                    for result_set in tracking_data['resultSets']:
                        if result_set['name'] == 'PlayerTrackingStats':
                            headers = result_set['headers']
                            rows = result_set['rowSet']
                            
                            # Create DataFrame
                            df = pd.DataFrame(rows, columns=headers)
                            
                            # Find player row
                            player_row = df[df['PLAYER_ID'] == int(player_id)]
                            if not player_row.empty:
                                # Extract tracking metrics with safe accessors
                                try:
                                    dist_miles = player_row['DIST_MILES'].iloc[0] if 'DIST_MILES' in player_row.columns else None
                                    dist_miles_off = player_row['DIST_MILES_OFF'].iloc[0] if 'DIST_MILES_OFF' in player_row.columns else None
                                    dist_miles_def = player_row['DIST_MILES_DEF'].iloc[0] if 'DIST_MILES_DEF' in player_row.columns else None
                                    speed = player_row['AVG_SPEED'].iloc[0] if 'AVG_SPEED' in player_row.columns else None
                                    speed_off = player_row['AVG_SPEED_OFF'].iloc[0] if 'AVG_SPEED_OFF' in player_row.columns else None
                                    speed_def = player_row['AVG_SPEED_DEF'].iloc[0] if 'AVG_SPEED_DEF' in player_row.columns else None
                                    
                                    potential_assists = player_row['POTENTIAL_AST'].iloc[0] if 'POTENTIAL_AST' in player_row.columns else None
                                    assist_points_created = player_row['AST_PTS_CREATED'].iloc[0] if 'AST_PTS_CREATED' in player_row.columns else None
                                    passes_made = player_row['PASSES_MADE'].iloc[0] if 'PASSES_MADE' in player_row.columns else None
                                    passes_received = player_row['PASSES_RECEIVED'].iloc[0] if 'PASSES_RECEIVED' in player_row.columns else None
                                except Exception as e:
                                    logger.warning(f"Error extracting tracking metrics: {e}")
                                    dist_miles = dist_miles_off = dist_miles_def = None
                                    speed = speed_off = speed_def = None
                                    potential_assists = assist_points_created = None
                                    passes_made = passes_received = None
                                
                                # Insert into fact_player_hustle
                                if any(v is not None for v in [dist_miles, dist_miles_off, dist_miles_def, speed, speed_off, speed_def]):
                                    try:
                                        # Check if record exists
                                        self.cursor.execute("SELECT 1 FROM fact_player_hustle WHERE player_game_id = ?", (player_game_id,))
                                        if self.cursor.fetchone():
                                            # Update existing record
                                            self.cursor.execute("""
                                            UPDATE fact_player_hustle SET
                                                distance_miles = ?, distance_miles_offense = ?,
                                                distance_miles_defense = ?, avg_speed_mph = ?, 
                                                avg_speed_mph_offense = ?, avg_speed_mph_defense = ?
                                            WHERE player_game_id = ?
                                            """, (
                                                dist_miles, dist_miles_off,
                                                dist_miles_def, speed, speed_off,
                                                speed_def, player_game_id
                                            ))
                                        else:
                                            # Insert new record
                                            self.cursor.execute("""
                                            INSERT INTO fact_player_hustle (
                                                player_game_id, distance_miles, distance_miles_offense,
                                                distance_miles_defense, avg_speed_mph, avg_speed_mph_offense,
                                                avg_speed_mph_defense, inserted_at
                                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                            """, (
                                                player_game_id, dist_miles, dist_miles_off,
                                                dist_miles_def, speed, speed_off,
                                                speed_def, now
                                            ))
                                        
                                        logger.debug(f"Added hustle data for {player_game_id}")
                                    except sqlite3.OperationalError as e:
                                        logger.warning(f"Schema mismatch in fact_player_hustle: {e}")
                                
                                # Update fact_player_playmaking with tracking data
                                if any(v is not None for v in [potential_assists, assist_points_created, passes_made, passes_received]):
                                    try:
                                        # Check if record exists
                                        self.cursor.execute("SELECT 1 FROM fact_player_playmaking WHERE player_game_id = ?", (player_game_id,))
                                        if self.cursor.fetchone():
                                            # Update existing record
                                            self.cursor.execute("""
                                            UPDATE fact_player_playmaking SET
                                                potential_assists = ?,
                                                assist_points_created = ?,
                                                passes_made = ?,
                                                passes_received = ?
                                            WHERE player_game_id = ?
                                            """, (
                                                potential_assists,
                                                assist_points_created,
                                                passes_made,
                                                passes_received,
                                                player_game_id
                                            ))
                                        else:
                                            # Insert new record with just these fields
                                            self.cursor.execute("""
                                            INSERT INTO fact_player_playmaking (
                                                player_game_id, potential_assists,
                                                assist_points_created, passes_made,
                                                passes_received, inserted_at
                                            ) VALUES (?, ?, ?, ?, ?, ?)
                                            """, (
                                                player_game_id, potential_assists,
                                                assist_points_created, passes_made,
                                                passes_received, now
                                            ))
                                        
                                        logger.debug(f"Updated playmaking data for {player_game_id}")
                                    except sqlite3.OperationalError as e:
                                        logger.warning(f"Schema mismatch updating fact_player_playmaking: {e}")
                
                return True
                
            except (ConnectionError, TimeoutError, ConnectionResetError, RemoteDisconnected) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Connection error for advanced stats {player_game_id} (attempt {attempt+1}/{max_retries}): {e}")
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    logger.error(f"Failed to process advanced stats for {player_game_id} after {max_retries} attempts: {e}")
                    return False
            except Exception as e:
                logger.error(f"Error processing advanced stats for {player_game_id}: {e}")
                return False
        
    def _process_hustle_stats(self, player_game_id, game_id, player_id, team_id):
        """Process hustle stats for a player-game with retry logic"""
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                # Get hustle stats
                hustle = hustlestatsboxscore.HustleStatsBoxScore(game_id=game_id)
                hustle_data = hustle.get_dict()
                
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Process hustle data
                if 'resultSets' in hustle_data:
                    for result_set in hustle_data['resultSets']:
                        if result_set['name'] == 'PlayerHustleStats':
                            headers = result_set['headers']
                            rows = result_set['rowSet']
                            
                            # Create DataFrame
                            df = pd.DataFrame(rows, columns=headers)
                            
                            # Find player row
                            player_row = df[df['PLAYER_ID'] == int(player_id)]
                            if not player_row.empty:
                                # Extract hustle metrics with error handling
                                try:
                                    contested_shots_2pt = player_row['CONTESTED_SHOTS_2PT'].iloc[0] if 'CONTESTED_SHOTS_2PT' in player_row.columns else 0
                                    contested_shots_3pt = player_row['CONTESTED_SHOTS_3PT'].iloc[0] if 'CONTESTED_SHOTS_3PT' in player_row.columns else 0
                                    deflections = player_row['DEFLECTIONS'].iloc[0] if 'DEFLECTIONS' in player_row.columns else 0
                                    loose_balls_recovered = player_row['LOOSE_BALLS_RECOVERED'].iloc[0] if 'LOOSE_BALLS_RECOVERED' in player_row.columns else 0
                                    charges_drawn = player_row['CHARGES_DRAWN'].iloc[0] if 'CHARGES_DRAWN' in player_row.columns else 0
                                    screen_assists = player_row['SCREEN_ASSISTS'].iloc[0] if 'SCREEN_ASSISTS' in player_row.columns else 0
                                    screen_assist_points = player_row['SCREEN_AST_PTS'].iloc[0] if 'SCREEN_AST_PTS' in player_row.columns else 0
                                    box_outs = player_row['BOX_OUTS'].iloc[0] if 'BOX_OUTS' in player_row.columns else 0
                                    box_outs_off = player_row['BOX_OUTS_OFF'].iloc[0] if 'BOX_OUTS_OFF' in player_row.columns else 0
                                    box_outs_def = player_row['BOX_OUTS_DEF'].iloc[0] if 'BOX_OUTS_DEF' in player_row.columns else 0
                                except Exception as e:
                                    logger.warning(f"Error extracting hustle metrics: {e}")
                                    contested_shots_2pt = contested_shots_3pt = deflections = 0
                                    loose_balls_recovered = charges_drawn = screen_assists = 0
                                    screen_assist_points = box_outs = box_outs_off = box_outs_def = 0
                                
                                # Insert or update fact_player_hustle
                                try:
                                    # First check if the record exists
                                    self.cursor.execute("SELECT 1 FROM fact_player_hustle WHERE player_game_id = ?", (player_game_id,))
                                    if self.cursor.fetchone():
                                        # Update existing record
                                        self.cursor.execute("""
                                        UPDATE fact_player_hustle SET
                                            contested_shots_2pt = ?,
                                            contested_shots_3pt = ?,
                                            deflections = ?,
                                            loose_balls_recovered = ?,
                                            charges_drawn = ?,
                                            screen_assists = ?,
                                            screen_assist_points = ?,
                                            box_outs = ?,
                                            box_outs_offensive = ?,
                                            box_outs_defensive = ?
                                        WHERE player_game_id = ?
                                        """, (
                                            contested_shots_2pt,
                                            contested_shots_3pt,
                                            deflections,
                                            loose_balls_recovered,
                                            charges_drawn,
                                            screen_assists,
                                            screen_assist_points,
                                            box_outs,
                                            box_outs_off,
                                            box_outs_def,
                                            player_game_id
                                        ))
                                    else:
                                        # Insert new record
                                        self.cursor.execute("""
                                        INSERT INTO fact_player_hustle (
                                            player_game_id, 
                                            contested_shots_2pt,
                                            contested_shots_3pt,
                                            deflections,
                                            loose_balls_recovered,
                                            charges_drawn,
                                            screen_assists,
                                            screen_assist_points,
                                            box_outs,
                                            box_outs_offensive,
                                            box_outs_defensive,
                                            inserted_at
                                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        """, (
                                            player_game_id,
                                            contested_shots_2pt,
                                            contested_shots_3pt,
                                            deflections,
                                            loose_balls_recovered,
                                            charges_drawn,
                                            screen_assists,
                                            screen_assist_points,
                                            box_outs,
                                            box_outs_off,
                                            box_outs_def,
                                            now
                                        ))
                                    
                                    logger.debug(f"Added hustle data for {player_game_id}")
                                except sqlite3.OperationalError as e:
                                    logger.warning(f"Schema mismatch in fact_player_hustle: {e}")
                                
                                # Update fact_player_defensive if it exists
                                contested_shots = contested_shots_2pt + contested_shots_3pt
                                
                                try:
                                    self.cursor.execute("SELECT 1 FROM fact_player_defensive WHERE player_game_id = ?", (player_game_id,))
                                    if self.cursor.fetchone():
                                        self.cursor.execute("""
                                        UPDATE fact_player_defensive SET
                                            contested_shots = ?,
                                            deflections = ?,
                                            charges_drawn = ?,
                                            loose_balls_recovered = ?
                                        WHERE player_game_id = ?
                                        """, (
                                            contested_shots,
                                            deflections,
                                            charges_drawn,
                                            loose_balls_recovered,
                                            player_game_id
                                        ))
                                        
                                        logger.debug(f"Updated defensive data for {player_game_id}")
                                except sqlite3.OperationalError as e:
                                    logger.warning(f"Schema mismatch updating fact_player_defensive: {e}")
                
                # Successfully processed, return
                return True
                
            except (ConnectionError, TimeoutError, ConnectionResetError, RemoteDisconnected) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Connection error for {player_game_id} (attempt {attempt+1}/{max_retries}): {e}")
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    logger.error(f"Failed to process hustle stats for {player_game_id} after {max_retries} attempts: {e}")
                    return False
            except Exception as e:
                logger.error(f"Error processing hustle stats for {player_game_id}: {e}")
                return False

def run_etl():
    """Run the full ETL process"""
    logger.info("Starting NBA data warehouse ETL job")
    
    warehouse = NBADataWarehouse()
    
    # Step 1: Connect to database
    if not warehouse.connect_to_db():
        logger.error("Failed to connect to database. Exiting.")
        return False
    
    # Step 2: Ensure tables exist
    if not warehouse.ensure_tables_exist():
        logger.error("Failed to ensure all tables exist. Exiting.")
        warehouse.close_db()
        return False
    
    # Step 3: Load dimensions
    # Step 3.1: Load teams
    warehouse.load_teams()

        # Step 3.2: Load players
    warehouse.load_players()
    
    # Step 4: Load games and fact tables
    # First load game data for the last 3 days
    game_ids = warehouse.load_games(days_back=3)
    
    # Then load player stats for those games
    if game_ids:
        warehouse.load_player_game_stats(game_ids, limit=5)
    
    # Step 5: Close database connection
    warehouse.close_db()
    
    logger.info("NBA data warehouse ETL job completed")
    return True

def schedule_etl():
    """Set up the ETL job schedule"""
    # Run daily at 4:00 AM
    schedule.every().day.at("04:00").do(run_etl)
    
    # Also run immediately when the script starts
    run_etl()
    
    logger.info("ETL scheduler started. Press Ctrl+C to stop.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Sleep for 1 minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")

if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="NBA Data Warehouse ETL")
    parser.add_argument("--run-now", action="store_true", help="Run the ETL job immediately")
    parser.add_argument("--schedule", action="store_true", help="Run the ETL job on a schedule")
    args = parser.parse_args()
    
    if args.run_now:
        run_etl()
    elif args.schedule:
        schedule_etl()
    else:
        print("Please specify either --run-now or --schedule")
        print("Example: python nba_etl.py --run-now")
        print("Example: python nba_etl.py --schedule")
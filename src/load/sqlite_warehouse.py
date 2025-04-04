import os
import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from config.settings import DATABASE_PATH

class SQLiteWarehouse:
    """Class to manage the NBA SQLite data warehouse"""
    
    def __init__(self):
        """Initialize the data warehouse"""
        # Ensure database directory exists
        os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
        
        # Connect to database
        self.conn = sqlite3.connect(DATABASE_PATH)
        
        # Create tables
        self.create_schema()
    
    def create_schema(self):
        """Create the data warehouse schema"""
        cursor = self.conn.cursor()
        
        # Create dimension tables
        cursor.execute('''
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
        ''')
        
        cursor.execute('''
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
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS dim_games (
            game_id TEXT PRIMARY KEY,
            game_date DATE,
            season TEXT,
            season_type TEXT,
            home_team_id TEXT,
            away_team_id TEXT,
            venue_id TEXT,
            inserted_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        ''')
        
        cursor.execute('''
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
        ''')
        
        cursor.execute('''
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
        ''')
        
        # Create fact tables
        cursor.execute('''
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
        ''')
        
        cursor.execute('''
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
        ''')
        
        # Create advanced player stats tables
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fact_player_shot_tracking (
            player_game_id TEXT PRIMARY KEY,
            shots_made_0_3ft INTEGER,
            shots_attempted_0_3ft INTEGER,
            shots_pct_0_3ft REAL,
            inserted_at TIMESTAMP
        )
        ''')
        
        self.conn.commit()
        print("SQLite data warehouse schema created successfully")
    
    def load_teams(self, teams_data):
        """Load teams into the dimension table"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        cursor = self.conn.cursor()
        for team in teams_data:
            cursor.execute('''
            INSERT OR REPLACE INTO dim_teams (
                team_id, team_name, team_city, team_abbreviation,
                conference, division, inserted_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                team['team_id'],
                team['team_name'],
                team['team_city'],
                team['team_abbreviation'],
                team.get('conference'),
                team.get('division'),
                now,
                now
            ))
        
        self.conn.commit()
        print(f"Loaded {len(teams_data)} teams into data warehouse")
    
    def load_players(self, players_data):
        """Load players into the dimension table"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        cursor = self.conn.cursor()
        for player in players_data:
            cursor.execute('''
            INSERT OR REPLACE INTO dim_players (
                player_id, first_name, last_name, jersey_num,
                position, height_inches, weight_lbs, birth_date,
                draft_year, inserted_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                player['player_id'],
                player['first_name'],
                player['last_name'],
                player.get('jersey_num'),
                player.get('position'),
                player.get('height_inches'),
                player.get('weight_lbs'),
                player.get('birth_date'),
                player.get('draft_year'),
                now,
                now
            ))
        
        self.conn.commit()
        print(f"Loaded {len(players_data)} players into data warehouse")
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()

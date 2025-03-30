"""
NBA Data Pipeline Runner
------------------------
This script runs the complete pipeline:
1. Extract data from NBA API
2. Transform and load into SQLite warehouse
3. Generate analytics and visualizations
"""

import os
import sys
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
from datetime import datetime

# Create directories
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)
os.makedirs("config", exist_ok=True)
os.makedirs("src/extract", exist_ok=True)
os.makedirs("src/transform", exist_ok=True)
os.makedirs("src/load", exist_ok=True)
os.makedirs("src/analyze", exist_ok=True)

# Create settings file if it doesn't exist
if not os.path.exists("config/settings.py"):
    with open("config/settings.py", "w") as f:
        f.write("""import os
from pathlib import Path

# Define project root
project_root = Path(__file__).parent.parent.absolute()

# Data directories
RAW_DATA_DIR = os.path.join(project_root, "data/raw")
PROCESSED_DATA_DIR = os.path.join(project_root, "data/processed")

# SQLite configuration
DATABASE_PATH = os.path.join(PROCESSED_DATA_DIR, "nba_data.db")
DB_URL = f"sqlite:///{DATABASE_PATH}"

# API settings
API_REQUEST_TIMEOUT = 30  # seconds
""")

# Create the SQLite warehouse module
with open("src/load/sqlite_warehouse.py", "w") as f:
    f.write("""import os
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
    \"\"\"Class to manage the NBA SQLite data warehouse\"\"\"
    
    def __init__(self):
        \"\"\"Initialize the data warehouse\"\"\"
        # Ensure database directory exists
        os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
        
        # Connect to database
        self.conn = sqlite3.connect(DATABASE_PATH)
        
        # Create tables
        self.create_schema()
    
    def create_schema(self):
        \"\"\"Create the data warehouse schema\"\"\"
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
        \"\"\"Load teams into the dimension table\"\"\"
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
        \"\"\"Load players into the dimension table\"\"\"
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
        \"\"\"Close the database connection\"\"\"
        if self.conn:
            self.conn.close()
""")

# Create a basic extractor
with open("src/extract/nba_extractor.py", "w") as f:
    f.write("""import json
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
    \"\"\"Class to extract data from NBA API\"\"\"
    
    def __init__(self):
        \"\"\"Initialize the extractor\"\"\"
        self.warehouse = SQLiteWarehouse()
        self.raw_dir = os.path.join(project_root, RAW_DATA_DIR)
        os.makedirs(self.raw_dir, exist_ok=True)
    
    def extract_teams(self):
        \"\"\"Extract team data\"\"\"
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
        \"\"\"Extract player data (limited to avoid rate limiting)\"\"\"
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
        \"\"\"Save raw data to file\"\"\"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{data_type}_{timestamp}.json"
        filepath = os.path.join(self.raw_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Raw data saved to {filepath}")
    
    def close(self):
        \"\"\"Close connections\"\"\"
        self.warehouse.close()
""")

# Create a simple analytics module
with open("src/analyze/simple_analytics.py", "w") as f:
    f.write("""import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from config.settings import DATABASE_PATH

class NBASimpleAnalytics:
    \"\"\"Simple NBA analytics\"\"\"
    
    def __init__(self):
        \"\"\"Initialize the analytics\"\"\"
        self.conn = sqlite3.connect(DATABASE_PATH)
    
    def get_team_stats(self):
        \"\"\"Get teams from the database\"\"\"
        query = "SELECT * FROM dim_teams"
        teams = pd.read_sql(query, self.conn)
        return teams
    
    def get_player_stats(self):
        \"\"\"Get players from the database\"\"\"
        query = "SELECT * FROM dim_players"
        players = pd.read_sql(query, self.conn)
        return players
    
    def team_analysis(self):
        \"\"\"Analyze teams by conference and division\"\"\"
        teams = self.get_team_stats()
        
        if teams.empty:
            print("No team data found in the database.")
            return
        
        # Count teams by conference
        conference_counts = teams['conference'].value_counts()
        
        # Create a bar chart
        plt.figure(figsize=(10, 6))
        conference_counts.plot(kind='bar', color=['blue', 'red'])
        plt.title('NBA Teams by Conference')
        plt.xlabel('Conference')
        plt.ylabel('Number of Teams')
        plt.tight_layout()
        
        # Save and show
        plt.savefig('teams_by_conference.png')
        plt.show()
        
        return conference_counts
    
    def player_analysis(self):
        \"\"\"Analyze players\"\"\"
        players = self.get_player_stats()
        
        if players.empty:
            print("No player data found in the database.")
            return
        
        # Count first letter of last name
        players['first_letter'] = players['last_name'].str[0]
        letter_counts = players['first_letter'].value_counts().sort_index()
        
        # Create a bar chart
        plt.figure(figsize=(12, 6))
        letter_counts.plot(kind='bar', color='green')
        plt.title('NBA Players by First Letter of Last Name')
        plt.xlabel('First Letter')
        plt.ylabel('Number of Players')
        plt.tight_layout()
        
        # Save and show
        plt.savefig('players_by_letter.png')
        plt.show()
        
        return letter_counts
    
    def close(self):
        \"\"\"Close the database connection\"\"\"
        if self.conn:
            self.conn.close()
""")

# Main runner code
print("Starting NBA Data Pipeline")
print("-------------------------")

print("\n1. Setting up the environment...")
if not os.path.exists("src/__init__.py"):
    with open("src/__init__.py", "w") as f:
        pass

# Import the modules
import sys
sys.path.insert(0, ".")

print("\n2. Extracting data from NBA API...")
from src.extract.nba_extractor import NBADataExtractor

# Extract data
extractor = NBADataExtractor()
teams = extractor.extract_teams()
players = extractor.extract_players(limit=30)  # Limit to avoid rate limiting
extractor.close()

print("\n3. Running analytics...")
from src.analyze.simple_analytics import NBASimpleAnalytics

# Run analytics
analytics = NBASimpleAnalytics()
teams_analysis = analytics.team_analysis()
players_analysis = analytics.player_analysis()
analytics.close()

print("\n4. Data Pipeline Complete!")
print("-------------------------")
print(f"Teams extracted: {len(teams)}")
print(f"Players extracted: {len(players)}")
print("\nAnalysis charts have been saved to the current directory.")
print("You can view the SQLite database using DB Browser for SQLite or similar tools.")

"""
NBA Data Pipeline - Fixed Version
---------------------------------
This script extracts NBA data and stores it in SQLite
"""

import json
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path
import sys

try:
    from nba_api.stats.static import teams, players
except ImportError:
    print("Error: nba_api package not installed. Run: pip install nba_api")
    sys.exit(1)

# Create directories
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

# Database settings
DATABASE_PATH = "data/processed/nba_data.db"
RAW_DATA_DIR = "data/raw"

print("Starting NBA Data Pipeline")
print("-------------------------")

# Step 1: Create the SQLite database
print("\n1. Creating SQLite database...")
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

# Create tables
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
    inserted_at TIMESTAMP,
    updated_at TIMESTAMP
)
''')

conn.commit()
print("Database schema created successfully")

# Step 2: Extract and load teams
print("\n2. Extracting team data...")
nba_teams = teams.get_teams()

# Save raw data
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
raw_file = os.path.join(RAW_DATA_DIR, f"teams_{timestamp}.json")
with open(raw_file, 'w') as f:
    json.dump(nba_teams, f, indent=2)
print(f"Raw team data saved to {raw_file}")

# Load teams into database
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
for team in nba_teams:
    cursor.execute('''
    INSERT OR REPLACE INTO dim_teams (
        team_id, team_name, team_city, team_abbreviation,
        conference, division, inserted_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        str(team['id']),
        team['nickname'],
        team['city'],
        team['abbreviation'],
        team.get('conference'),
        team.get('division'),
        now,
        now
    ))

conn.commit()
print(f"Loaded {len(nba_teams)} teams into database")

# Step 3: Extract and load players (limited to avoid rate limiting)
print("\n3. Extracting player data...")
limit = 30  # Limit number of players to avoid rate limiting
all_players = players.get_active_players()
players_subset = all_players[:limit]

# Save raw data
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
raw_file = os.path.join(RAW_DATA_DIR, f"players_{timestamp}.json")
with open(raw_file, 'w') as f:
    json.dump(players_subset, f, indent=2)
print(f"Raw player data saved to {raw_file}")

# Load players into database
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
for player in players_subset:
    cursor.execute('''
    INSERT OR REPLACE INTO dim_players (
        player_id, first_name, last_name, inserted_at, updated_at
    ) VALUES (?, ?, ?, ?, ?)
    ''', (
        str(player['id']),
        player['first_name'],
        player['last_name'],
        now,
        now
    ))

conn.commit()
print(f"Loaded {len(players_subset)} players into database")

# Step 4: Run simple analytics
print("\n4. Running analytics...")

# Team analysis
try:
    query = "SELECT * FROM dim_teams"
    teams_df = pd.read_sql(query, conn)
    print(f"Retrieved {len(teams_df)} teams from database")
    
    # Just display the teams
    print("\nTeams by Conference:")
    for conf in teams_df['conference'].unique():
        if pd.notna(conf):
            conf_teams = teams_df[teams_df['conference'] == conf]
            print(f"{conf}: {len(conf_teams)} teams")
            for _, team in conf_teams.iterrows():
                print(f"  - {team['team_city']} {team['team_name']}")
    
    # Simple bar chart - division counts
    if 'division' in teams_df.columns and not teams_df['division'].isna().all():
        division_counts = teams_df['division'].value_counts()
        
        if len(division_counts) > 0:
            plt.figure(figsize=(12, 6))
            division_counts.plot(kind='bar', color='blue')
            plt.title('NBA Teams by Division')
            plt.xlabel('Division')
            plt.ylabel('Number of Teams')
            plt.tight_layout()
            plt.savefig('teams_by_division.png')
            print("\nCreated teams_by_division.png")
    
except Exception as e:
    print(f"Error in team analytics: {e}")

# Player analysis
try:
    query = "SELECT * FROM dim_players"
    players_df = pd.read_sql(query, conn)
    print(f"Retrieved {len(players_df)} players from database")
    
    # Display some player stats
    print("\nPlayer sample:")
    for _, player in players_df.head(5).iterrows():
        print(f"  - {player['first_name']} {player['last_name']}")
    
    # Create simple visualization - count by first letter
    if len(players_df) > 0:
        players_df['first_letter'] = players_df['last_name'].str[0]
        letter_counts = players_df['first_letter'].value_counts()
        
        plt.figure(figsize=(12, 6))
        letter_counts.plot(kind='bar', color='green')
        plt.title('NBA Players by First Letter of Last Name')
        plt.xlabel('First Letter')
        plt.ylabel('Number of Players')
        plt.tight_layout()
        plt.savefig('players_by_letter.png')
        print("\nCreated players_by_letter.png")
    
except Exception as e:
    print(f"Error in player analytics: {e}")

# Step 5: View database content
print("\n5. Database Content Summary:")
tables = ['dim_teams', 'dim_players']
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"  - {table}: {count} records")

# Close database connection
conn.close()

print("\nData Pipeline Complete!")
print("-------------------------")
print(f"Teams extracted: {len(nba_teams)}")
print(f"Players extracted: {len(players_subset)}")
print("\nYou can view the SQLite database using DB Browser for SQLite or similar tools.")

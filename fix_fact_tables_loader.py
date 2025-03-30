"""
NBA Data Warehouse Loader - Fixed for Existing Schema
----------------------------------------------------
This script loads data into fact tables with the existing schema
"""

import json
import os
import sqlite3
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path
import sys

try:
    from nba_api.stats.static import teams, players
    from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
except ImportError:
    print("Error: nba_api package not installed. Run: pip install nba_api")
    sys.exit(1)

# Create directories
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

# Database settings
DATABASE_PATH = "data/processed/nba_data.db"
RAW_DATA_DIR = "data/raw"

print("NBA Fact Table Data Loader")
print("-------------------------")

# Connect to the database
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

# Step 1: Check the actual schema of fact_game_stats
print("\n1. Checking database schema...")
cursor.execute("PRAGMA table_info(fact_game_stats)")
fact_game_columns = cursor.fetchall()
column_names = [col[1] for col in fact_game_columns]
print(f"fact_game_stats columns: {column_names}")

# Find if there's a game_status column
has_game_status = 'game_status' in column_names

# Step 2: Fetch and process games
print("\n2. Fetching games data...")

# Calculate date range - let's get games from the last 14 days
end_date = datetime.now()
start_date = end_date - timedelta(days=14)
date_from = start_date.strftime("%m/%d/%Y")

print(f"Fetching games from {date_from} to today...")

# Get recent games
try:
    game_finder = leaguegamefinder.LeagueGameFinder(
        date_from_nullable=date_from,
        league_id_nullable='00'  # NBA
    )
    games_dict = game_finder.get_dict()
    
    # Save raw data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_file = os.path.join(RAW_DATA_DIR, f"games_{timestamp}.json")
    with open(raw_file, 'w') as f:
        json.dump(games_dict, f, indent=2)
    print(f"Raw game data saved to {raw_file}")
    
    # Create a mapping from team abbreviations to IDs
    cursor.execute("SELECT team_id, team_abbreviation FROM dim_teams")
    team_abbr_to_id = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Process game data
    games_processed = 0
    unique_game_ids = {}  # Use a dict to store game_id -> [team_ids]
    
    if 'resultSets' in games_dict and len(games_dict['resultSets']) > 0:
        result_set = games_dict['resultSets'][0]
        headers = result_set['headers']
        rows = result_set['rowSet']
        
        # Create a DataFrame for easier processing
        df = pd.DataFrame(rows, columns=headers)
        
        # Group games by GAME_ID and process each game
        for game_id, game_df in df.groupby('GAME_ID'):
            # Get unique team IDs for this game
            team_ids = game_df['TEAM_ID'].unique().tolist()
            team_abbrs = game_df['TEAM_ABBREVIATION'].unique().tolist()
            
            # Store in our dictionary for later player stats processing
            unique_game_ids[game_id] = team_ids
            
            # Get game info from first row
            game_info = game_df.iloc[0]
            game_date = game_info['GAME_DATE']
            season = game_info['SEASON_ID']
            
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Insert into dim_games
            cursor.execute('''
            INSERT OR REPLACE INTO dim_games (
                game_id, game_date, season, season_type, inserted_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                game_id,
                game_date,
                season,
                'Regular Season',  # Simplified
                now,
                now
            ))
            
            # We need to identify home vs away teams
            # This is simplified - in production you would need more robust logic
            if len(team_ids) >= 2:
                # Assume first team is home, second is away (this is not always correct)
                home_team_id = str(team_ids[0])
                away_team_id = str(team_ids[1])
                
                # Get scores
                home_team_df = game_df[game_df['TEAM_ID'] == int(home_team_id)]
                away_team_df = game_df[game_df['TEAM_ID'] == int(away_team_id)]
                
                if not home_team_df.empty and not away_team_df.empty:
                    home_score = home_team_df['PTS'].iloc[0]
                    away_score = away_team_df['PTS'].iloc[0]
                    
                    # Prepare insert statement based on schema
                    if has_game_status:
                        # Use the original statement with game_status
                        cursor.execute('''
                        INSERT OR REPLACE INTO fact_game_stats (
                            game_id, home_team_id, away_team_id, home_team_score, away_team_score,
                            game_status, inserted_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            game_id,
                            home_team_id,
                            away_team_id,
                            home_score,
                            away_score,
                            'Final',  # Simplified
                            now
                        ))
                    else:
                        # Omit game_status column
                        cursor.execute('''
                        INSERT OR REPLACE INTO fact_game_stats (
                            game_id, home_team_id, away_team_id, home_team_score, away_team_score,
                            inserted_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            game_id,
                            home_team_id,
                            away_team_id,
                            home_score,
                            away_score,
                            now
                        ))
                    
                    games_processed += 1
                    
                    if games_processed % 5 == 0:
                        print(f"Processed {games_processed} games...")
        
        conn.commit()
        print(f"\nLoaded {games_processed} games into fact_game_stats")
    
    # Step 3: Get player stats for each game
    print("\n3. Fetching player stats for games...")
    
    # Limit the number of games we process for player stats
    games_to_process = list(unique_game_ids.keys())[:5]  # Process up to 5 games
    
    players_added = 0
    player_stats_added = 0
    
    for game_id in games_to_process:
        print(f"Fetching player stats for game {game_id}...")
        
        # Add a delay to avoid hitting API rate limits
        time.sleep(1)
        
        try:
            # Get box score
            box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
            box_score_dict = box_score.get_dict()
            
            # Save raw data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            box_raw_file = os.path.join(RAW_DATA_DIR, f"boxscore_{game_id}_{timestamp}.json")
            with open(box_raw_file, 'w') as f:
                json.dump(box_score_dict, f, indent=2)
            
            # Process player stats
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
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
                            
                            # Make sure the player exists in dim_players
                            cursor.execute("SELECT COUNT(*) FROM dim_players WHERE player_id = ?", (player_id,))
                            if cursor.fetchone()[0] == 0:
                                # Split name into first and last (simple approach)
                                name_parts = player_name.split(" ", 1)
                                first_name = name_parts[0]
                                last_name = name_parts[1] if len(name_parts) > 1 else ""
                                
                                # Insert player
                                cursor.execute('''
                                INSERT INTO dim_players (
                                    player_id, first_name, last_name, inserted_at, updated_at
                                ) VALUES (?, ?, ?, ?, ?)
                                ''', (
                                    player_id,
                                    first_name,
                                    last_name,
                                    now,
                                    now
                                ))
                                
                                players_added += 1
                            
                            # Create player_game_id
                            player_game_id = f"{game_id}_{player_id}"
                            
                            # Check the schema of fact_player_game_stats
                            cursor.execute("PRAGMA table_info(fact_player_game_stats)")
                            player_cols = [col[1] for col in cursor.fetchall()]
                            
                            # Insert player stats based on the schema
                            if all(col in player_cols for col in ['minutes', 'points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers']):
                                cursor.execute('''
                                INSERT OR REPLACE INTO fact_player_game_stats (
                                    player_game_id, game_id, player_id, team_id,
                                    minutes, points, rebounds, assists, steals, blocks, turnovers,
                                    inserted_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (
                                    player_game_id,
                                    game_id,
                                    player_id,
                                    team_id,
                                    player_row['MIN'],
                                    player_row['PTS'],
                                    player_row['REB'],
                                    player_row['AST'],
                                    player_row['STL'],
                                    player_row['BLK'],
                                    player_row['TO'],
                                    now
                                ))
                                player_stats_added += 1
                            else:
                                print(f"Warning: fact_player_game_stats schema doesn't match expected columns.")
                                print(f"Expected: minutes, points, rebounds, assists, steals, blocks, turnovers")
                                print(f"Found: {player_cols}")
                                break
            
        except Exception as e:
            print(f"Error processing box score for game {game_id}: {e}")
    
    conn.commit()
    print(f"Added {players_added} new players to dim_players")
    print(f"Added {player_stats_added} player game stats to fact_player_game_stats")
    
except Exception as e:
    print(f"Error fetching game data: {e}")

# Step 4: Show database summary
print("\n4. Database Content Summary:")
tables = ['dim_teams', 'dim_players', 'dim_games', 'fact_game_stats', 'fact_player_game_stats']
for table in tables:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  - {table}: {count} records")
    except sqlite3.OperationalError as e:
        print(f"  - {table}: Error - {e}")

# Close connection
conn.close()

print("\nData loading complete!")
print("-------------------------")
print("You can now view the data in your SQLite database using DB Browser for SQLite or similar tools.")
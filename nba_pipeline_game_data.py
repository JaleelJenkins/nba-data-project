"""
NBA Data Pipeline - Extended Version with Game Data
--------------------------------------------------
This script extracts NBA data including games and stores it in SQLite
"""

import json
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pathlib import Path
import sys
import time

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

print("Starting NBA Data Pipeline")
print("-------------------------")

# Step 1: Create the SQLite database
print("\n1. Creating SQLite database...")
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

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
    inserted_at TIMESTAMP,
    updated_at TIMESTAMP
)
''')

# Create fact tables
cursor.execute('''
CREATE TABLE IF NOT EXISTS fact_game_stats (
    game_id TEXT PRIMARY KEY,
    home_team_id TEXT,
    away_team_id TEXT,
    home_team_score INTEGER,
    away_team_score INTEGER,
    game_status TEXT,
    inserted_at TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES dim_games (game_id),
    FOREIGN KEY (home_team_id) REFERENCES dim_teams (team_id),
    FOREIGN KEY (away_team_id) REFERENCES dim_teams (team_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS fact_player_game_stats (
    player_game_id TEXT PRIMARY KEY,
    game_id TEXT,
    player_id TEXT,
    team_id TEXT,
    minutes INTEGER,
    points INTEGER,
    rebounds INTEGER,
    assists INTEGER,
    steals INTEGER,
    blocks INTEGER,
    turnovers INTEGER,
    inserted_at TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES dim_games (game_id),
    FOREIGN KEY (player_id) REFERENCES dim_players (player_id),
    FOREIGN KEY (team_id) REFERENCES dim_teams (team_id)
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

# Create team ID mapping for later use
team_id_map = {}
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Load teams into database
for team in nba_teams:
    team_id = str(team['id'])
    team_id_map[team['abbreviation']] = team_id
    
    cursor.execute('''
    INSERT OR REPLACE INTO dim_teams (
        team_id, team_name, team_city, team_abbreviation,
        conference, division, inserted_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        team_id,
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

# Create player ID mapping for later use
player_id_map = {}
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Load players into database
for player in players_subset:
    player_id = str(player['id'])
    player_id_map[player['id']] = player_id
    
    cursor.execute('''
    INSERT OR REPLACE INTO dim_players (
        player_id, first_name, last_name, inserted_at, updated_at
    ) VALUES (?, ?, ?, ?, ?)
    ''', (
        player_id,
        player['first_name'],
        player['last_name'],
        now,
        now
    ))

conn.commit()
print(f"Loaded {len(players_subset)} players into database")

# Step 4: Extract and load game data
print("\n4. Extracting game data...")
try:
    # Calculate date range (last 7 days to avoid too much data)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    date_from = start_date.strftime("%m/%d/%Y")
    
    print(f"Fetching games from {date_from} to today...")
    
    # Get recent games
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
    
    # Process game data
    games_processed = 0
    unique_game_ids = set()
    
    # Check if we have resultSets in the data
    if 'resultSets' in games_dict and len(games_dict['resultSets']) > 0:
        result_set = games_dict['resultSets'][0]
        headers = result_set['headers']
        rows = result_set['rowSet']
        
        # Create a DataFrame for easier processing
        games_df = pd.DataFrame(rows, columns=headers)
        
        # Get unique game IDs
        for game_id in games_df['GAME_ID'].unique():
            if game_id in unique_game_ids:
                continue
                
            unique_game_ids.add(game_id)
            
            # Get game info
            game_rows = games_df[games_df['GAME_ID'] == game_id]
            
            if len(game_rows) >= 2:  # We need at least home and away team data
                # Get the first row for basic game info
                game_date = game_rows['GAME_DATE'].iloc[0]
                season_id = game_rows['SEASON_ID'].iloc[0]
                
                # Insert into dim_games
                cursor.execute('''
                INSERT OR REPLACE INTO dim_games (
                    game_id, game_date, season, season_type, inserted_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    game_id,
                    game_date,
                    season_id,
                    'Regular Season',  # Simplified - would need additional logic for playoffs
                    now,
                    now
                ))
                
                # Find home and away teams
                # This is simplified - in a real pipeline, you'd need more robust logic
                team_ids = game_rows['TEAM_ID'].tolist()
                team_abbrs = game_rows['TEAM_ABBREVIATION'].tolist()
                
                # For this example, we'll just pick the first two teams
                if len(team_ids) >= 2:
                    home_team_id = str(team_ids[0])
                    away_team_id = str(team_ids[1])
                    
                    home_score = game_rows['PTS'].iloc[0]
                    away_score = game_rows['PTS'].iloc[1]
                    
                    # Insert into fact_game_stats
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
                    
                    games_processed += 1
                    
                    # Let's also get player stats for this game (limited to avoid API rate limits)
                    if games_processed <= 2:  # Only process player stats for up to 2 games
                        print(f"  Fetching player stats for game {game_id}...")
                        
                        # Add a delay to avoid hitting API rate limits
                        time.sleep(1)
                        
                        try:
                            # Get box score
                            box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
                            box_score_dict = box_score.get_dict()
                            
                            # Save raw data
                            box_raw_file = os.path.join(RAW_DATA_DIR, f"boxscore_{game_id}_{timestamp}.json")
                            with open(box_raw_file, 'w') as f:
                                json.dump(box_score_dict, f, indent=2)
                            
                            # Process player stats
                            player_stats_processed = 0
                            
                            if 'resultSets' in box_score_dict:
                                for result_set in box_score_dict['resultSets']:
                                    if result_set['name'] == 'PlayerStats':
                                        p_headers = result_set['headers']
                                        p_rows = result_set['rowSet']
                                        
                                        # Create DataFrame
                                        player_stats_df = pd.DataFrame(p_rows, columns=p_headers)
                                        
                                        # Process each player's stats
                                        for _, player_row in player_stats_df.iterrows():
                                            player_id = str(player_row['PLAYER_ID'])
                                            team_id = str(player_row['TEAM_ID'])
                                            
                                            # Create a unique player_game_id
                                            player_game_id = f"{game_id}_{player_id}"
                                            
                                            # Insert into fact_player_game_stats
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
                                            
                                            player_stats_processed += 1
                            
                            print(f"  Loaded {player_stats_processed} player stats for game {game_id}")
                            
                        except Exception as e:
                            print(f"  Error fetching box score for game {game_id}: {e}")
    
    conn.commit()
    print(f"Loaded {games_processed} games into database")
    print(f"Unique game IDs found: {len(unique_game_ids)}")
    
except Exception as e:
    print(f"Error extracting game data: {e}")

# Step 5: Run analytics
print("\n5. Running analytics...")

# Team analysis
try:
    query = "SELECT * FROM dim_teams"
    teams_df = pd.read_sql(query, conn)
    print(f"Retrieved {len(teams_df)} teams from database")
    
    # Display teams by conference
    print("\nTeams by Conference:")
    for conf in teams_df['conference'].unique():
        if pd.notna(conf):
            conf_teams = teams_df[teams_df['conference'] == conf]
            print(f"{conf}: {len(conf_teams)} teams")
            for _, team in conf_teams.head(3).iterrows():  # Show only first 3 teams per conference
                print(f"  - {team['team_city']} {team['team_name']}")
            if len(conf_teams) > 3:
                print(f"  - ...and {len(conf_teams) - 3} more")
    
except Exception as e:
    print(f"Error in team analytics: {e}")

# Game analysis
try:
    query = """
    SELECT 
        g.game_id,
        g.game_date,
        ht.team_name AS home_team,
        at.team_name AS away_team,
        gs.home_team_score,
        gs.away_team_score
    FROM dim_games g
    JOIN fact_game_stats gs ON g.game_id = gs.game_id
    JOIN dim_teams ht ON gs.home_team_id = ht.team_id
    JOIN dim_teams at ON gs.away_team_id = at.team_id
    ORDER BY g.game_date DESC
    """
    games_df = pd.read_sql(query, conn)
    
    if not games_df.empty:
        print(f"\nRetrieved {len(games_df)} games from database")
        print("\nRecent Games:")
        
        for _, game in games_df.head(5).iterrows():
            print(f"  {game['game_date']}: {game['home_team']} {game['home_team_score']} vs {game['away_team']} {game['away_team_score']}")
        
        # Create visualization - score distribution
        plt.figure(figsize=(10, 6))
        
        # Calculate total scores
        games_df['total_score'] = games_df['home_team_score'] + games_df['away_team_score']
        
        # Create histogram
        plt.hist(games_df['total_score'], bins=10, color='navy', alpha=0.7)
        plt.title('Distribution of Total Game Scores')
        plt.xlabel('Total Score')
        plt.ylabel('Number of Games')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('game_score_distribution.png')
        print("\nCreated game_score_distribution.png")
    else:
        print("No games data found in the database")
        
except Exception as e:
    print(f"Error in game analytics: {e}")

# Player analysis
try:
    query = """
    SELECT 
        p.player_id,
        p.first_name || ' ' || p.last_name AS player_name,
        COUNT(pgs.game_id) AS games_played,
        ROUND(AVG(pgs.points), 1) AS avg_points,
        ROUND(AVG(pgs.rebounds), 1) AS avg_rebounds,
        ROUND(AVG(pgs.assists), 1) AS avg_assists
    FROM dim_players p
    JOIN fact_player_game_stats pgs ON p.player_id = pgs.player_id
    GROUP BY p.player_id, player_name
    ORDER BY avg_points DESC
    """
    player_stats_df = pd.read_sql(query, conn)
    
    if not player_stats_df.empty:
        print(f"\nRetrieved stats for {len(player_stats_df)} players")
        print("\nTop Scorers:")
        
        for _, player in player_stats_df.head(5).iterrows():
            print(f"  {player['player_name']}: {player['avg_points']} PPG, {player['avg_rebounds']} RPG, {player['avg_assists']} APG ({player['games_played']} games)")
        
        # Create visualization - player stats
        if len(player_stats_df) >= 5:
            top_players = player_stats_df.head(5)
            
            plt.figure(figsize=(12, 6))
            
            # Create grouped bar chart for top 5 players
            x = range(len(top_players))
            width = 0.25
            
            plt.bar([i - width for i in x], top_players['avg_points'], width, label='Points', color='red')
            plt.bar(x, top_players['avg_rebounds'], width, label='Rebounds', color='blue')
            plt.bar([i + width for i in x], top_players['avg_assists'], width, label='Assists', color='green')
            
            plt.title('Top 5 Scorers - Key Statistics')
            plt.xlabel('Player')
            plt.ylabel('Average per Game')
            plt.xticks(x, top_players['player_name'], rotation=45, ha='right')
            plt.legend()
            plt.tight_layout()
            plt.savefig('top_scorers.png')
            print("\nCreated top_scorers.png")
    else:
        print("No player game stats found in the database")
        
except Exception as e:
    print(f"Error in player stats analytics: {e}")

# Step 6: Database Content Summary
print("\n6. Database Content Summary:")
tables = ['dim_teams', 'dim_players', 'dim_games', 'fact_game_stats', 'fact_player_game_stats']
for table in tables:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  - {table}: {count} records")
    except sqlite3.OperationalError:
        print(f"  - {table}: table not found")

# Close database connection
conn.close()

print("\nData Pipeline Complete!")
print("-------------------------")
print("You can view the SQLite database using DB Browser for SQLite or similar tools.")
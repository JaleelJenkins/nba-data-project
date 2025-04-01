# etl/games_etl.py
import json
import os
from datetime import datetime, timedelta

from nba_api.stats.endpoints import leaguegamefinder

from config.etl_settings import get_logger, get_db_connection, RAW_DIR

def extract_games(days_back=7):
    """Extract games data from NBA API"""
    logger = get_logger("games_etl")
    logger.info(f"Starting games extraction for the last {days_back} days")
    
    try:
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        date_from = start_date.strftime("%m/%d/%Y")
        
        logger.info(f"Fetching games from {date_from} to today")
        
        # Extract games data
        game_finder = leaguegamefinder.LeagueGameFinder(
            date_from_nullable=date_from,
            league_id_nullable='00'  # NBA
        )
        games_dict = game_finder.get_dict()
        
        # Save raw data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = os.path.join(RAW_DIR, f"games_{timestamp}.json")
        with open(raw_file, 'w') as f:
            json.dump(games_dict, f, indent=2)
        logger.info(f"Raw data saved to {raw_file}")
        
        return games_dict
    except Exception as e:
        logger.error(f"Error extracting games: {e}")
        raise

def transform_games(games_dict):
    """Transform games data"""
    logger = get_logger("games_etl")
    logger.info("Starting games transformation")
    
    games = []
    game_stats = []
    
    if 'resultSets' in games_dict and len(games_dict['resultSets']) > 0:
        result_set = games_dict['resultSets'][0]
        headers = result_set['headers']
        rows = result_set['rowSet']
        
        # Process games
        unique_games = {}
        
        for row in rows:
            game_dict = dict(zip(headers, row))
            game_id = game_dict.get('GAME_ID')
            
            # Process unique games only
            if game_id not in unique_games:
                unique_games[game_id] = {
                    'game_id': game_id,
                    'game_date': game_dict.get('GAME_DATE'),
                    'season': game_dict.get('SEASON_ID'),
                    'season_type': 'Regular Season' if game_dict.get('SEASON_ID', '').startswith('2') else 'Playoffs'
                }
                games.append(unique_games[game_id])
            
            # Add team to track home/away
            team_id = str(game_dict.get('TEAM_ID'))
            is_home = game_dict.get('MATCHUP', '').find(' vs. ') != -1
            
            if 'teams' not in unique_games[game_id]:
                unique_games[game_id]['teams'] = []
            
            unique_games[game_id]['teams'].append({
                'team_id': team_id,
                'is_home': is_home,
                'points': game_dict.get('PTS')
            })
    
    # Create game stats
    for game_id, game in unique_games.items():
        if 'teams' in game and len(game['teams']) >= 2:
            home_teams = [t for t in game['teams'] if t['is_home']]
            away_teams = [t for t in game['teams'] if not t['is_home']]
            
            if home_teams and away_teams:
                home_team = home_teams[0]
                away_team = away_teams[0]
                
                game_stats.append({
                    'game_id': game_id,
                    'home_team_id': home_team['team_id'],
                    'away_team_id': away_team['team_id'],
                    'home_team_score': home_team['points'],
                    'away_team_score': away_team['points']
                })
    
    logger.info(f"Transformed {len(games)} games and {len(game_stats)} game stats")
    return games, game_stats

def load_games(games, game_stats):
    """Load games into the database"""
    logger = get_logger("games_etl")
    logger.info("Starting games load")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    games_loaded = 0
    stats_loaded = 0
    
    try:
        # Load dim_games
        for game in games:
            cursor.execute('''
            INSERT OR REPLACE INTO dim_games (
                game_id, game_date, season, season_type, inserted_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                game['game_id'],
                game['game_date'],
                game['season'],
                game['season_type'],
                now,
                now
            ))
            games_loaded += 1
        
        # Load fact_game_stats
        for stat in game_stats:
            cursor.execute('''
            INSERT OR REPLACE INTO fact_game_stats (
                game_id, home_team_id, away_team_id, home_team_score, away_team_score, inserted_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                stat['game_id'],
                stat['home_team_id'],
                stat['away_team_id'],
                stat['home_team_score'],
                stat['away_team_score'],
                now
            ))
            stats_loaded += 1
        
        conn.commit()
        logger.info(f"Loaded {games_loaded} games and {stats_loaded} game stats into database")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading games: {e}")
        raise
    finally:
        conn.close()

def games_etl(days_back=7):
    """Run the full games ETL process"""
    logger = get_logger("games_etl")
    logger.info("Starting games ETL process")
    
    try:
        # Extract
        games_dict = extract_games(days_back)
        
        # Transform
        games, game_stats = transform_games(games_dict)
        
        # Load
        load_games(games, game_stats)
        
        logger.info("Games ETL process completed successfully")
        return True
    except Exception as e:
        logger.error(f"Games ETL process failed: {e}")
        return False

if __name__ == "__main__":
    # Default to 7 days back
    games_etl(days_back=7)
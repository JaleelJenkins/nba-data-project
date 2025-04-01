# etl/player_game_stats_etl.py
import json
import os
import time
from datetime import datetime, timedelta

from nba_api.stats.endpoints import boxscoretraditionalv2

from config.etl_settings import get_logger, get_db_connection, RAW_DIR

def extract_player_stats(game_id):
    """Extract player game stats for a specific game"""
    logger = get_logger("player_game_stats_etl")
    logger.info(f"Extracting player stats for game {game_id}")
    
    try:
        # Add a delay to avoid hitting API rate limits
        time.sleep(1)
        
        # Get box score
        box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        box_score_dict = box_score.get_dict()
        
        # Save raw data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = os.path.join(RAW_DIR, f"boxscore_{game_id}_{timestamp}.json")
        with open(raw_file, 'w') as f:
            json.dump(box_score_dict, f, indent=2)
        logger.info(f"Raw box score data saved to {raw_file}")
        
        return box_score_dict
    except Exception as e:
        logger.error(f"Error extracting player stats for game {game_id}: {e}")
        raise

def transform_player_stats(box_score_dict, game_id):
    """Transform player game stats"""
    logger = get_logger("player_game_stats_etl")
    logger.info(f"Transforming player stats for game {game_id}")
    
    player_stats = []
    player_ids = set()  # Track players for later fact tables
    
    if 'resultSets' in box_score_dict:
        for result_set in box_score_dict['resultSets']:
            if result_set['name'] == 'PlayerStats':
                headers = result_set['headers']
                rows = result_set['rowSet']
                
                for row in rows:
                    player_dict = dict(zip(headers, row))
                    player_id = str(player_dict.get('PLAYER_ID'))
                    player_ids.add(player_id)
                    
                    # Create player_game_id
                    player_game_id = f"{game_id}_{player_id}"
                    
                    # Basic player stats
                    player_stats.append({
                        'player_game_id': player_game_id,
                        'game_id': game_id,
                        'player_id': player_id,
                        'team_id': str(player_dict.get('TEAM_ID')),
                        'minutes': player_dict.get('MIN'),
                        'points': player_dict.get('PTS'),
                        'rebounds': player_dict.get('REB'),
                        'assists': player_dict.get('AST'),
                        'steals': player_dict.get('STL'),
                        'blocks': player_dict.get('BLK'),
                        'turnovers': player_dict.get('TO')
                    })
    
    logger.info(f"Transformed {len(player_stats)} player game stats")
    return player_stats, player_ids

def load_player_stats(player_stats):
    """Load player game stats into the database"""
    logger = get_logger("player_game_stats_etl")
    logger.info("Starting player game stats load")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    count = 0
    
    try:
        for stat in player_stats:
            cursor.execute('''
            INSERT OR REPLACE INTO fact_player_game_stats (
                player_game_id, game_id, player_id, team_id,
                minutes, points, rebounds, assists, steals, blocks, turnovers,
                inserted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                stat['player_game_id'],
                stat['game_id'],
                stat['player_id'],
                stat['team_id'],
                stat['minutes'],
                stat['points'],
                stat['rebounds'],
                stat['assists'],
                stat['steals'],
                stat['blocks'],
                stat['turnovers'],
                now
            ))
            count += 1
        
        conn.commit()
        logger.info(f"Loaded {count} player game stats into database")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading player game stats: {e}")
        raise
    finally:
        conn.close()

def get_recent_games(days_back=7):
    """Get recent games from the database that need player stats"""
    logger = get_logger("player_game_stats_etl")
    logger.info(f"Getting recent games from the last {days_back} days")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get date range
        today = datetime.now().date()
        past_date = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        # Find games without player stats
        cursor.execute('''
        SELECT g.game_id
        FROM dim_games g
        LEFT JOIN fact_player_game_stats ps ON g.game_id = ps.game_id
        WHERE g.game_date >= ?
        GROUP BY g.game_id
        HAVING COUNT(ps.player_game_id) = 0
        LIMIT 5
        ''', (past_date,))
        
        game_ids = [row['game_id'] for row in cursor.fetchall()]
        logger.info(f"Found {len(game_ids)} recent games without player stats")
        return game_ids
    except Exception as e:
        logger.error(f"Error getting recent games: {e}")
        return []
    finally:
        conn.close()

def player_game_stats_etl(game_ids=None, days_back=7):
    """Run the full player game stats ETL process"""
    logger = get_logger("player_game_stats_etl")
    logger.info("Starting player game stats ETL process")
    
    try:
        # Get games to process if not provided
        if game_ids is None:
            game_ids = get_recent_games(days_back)
        
        if not game_ids:
            logger.info("No games to process")
            return True
        
        stats_count = 0
        
        # Process each game
        for game_id in game_ids:
            # Extract
            box_score_dict = extract_player_stats(game_id)
            
            # Transform
            player_stats, player_ids = transform_player_stats(box_score_dict, game_id)
            
            # Load
            load_player_stats(player_stats)
            
            stats_count += len(player_stats)
            
            # Small delay between API calls
            time.sleep(1)
        
        logger.info(f"Player game stats ETL process completed successfully with {stats_count} stats loaded")
        return True
    except Exception as e:
        logger.error(f"Player game stats ETL process failed: {e}")
        return False

if __name__ == "__main__":
    # Process recent games
    player_game_stats_etl(days_back=7)
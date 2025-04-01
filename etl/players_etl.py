# etl/players_etl.py
import json
import os
from datetime import datetime

from nba_api.stats.static import players

from config.etl_settings import get_logger, get_db_connection, RAW_DIR

def extract_players(limit=None):
    """Extract player data from NBA API"""
    logger = get_logger("players_etl")
    logger.info("Starting players extraction")
    
    try:
        # Extract players data
        all_players = players.get_active_players()
        
        # Limit players if requested (to avoid rate limiting)
        if limit:
            player_subset = all_players[:limit]
            logger.info(f"Extracted {len(player_subset)} players (limited from {len(all_players)})")
        else:
            player_subset = all_players
            logger.info(f"Extracted {len(player_subset)} players")
        
        # Save raw data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = os.path.join(RAW_DIR, f"players_{timestamp}.json")
        with open(raw_file, 'w') as f:
            json.dump(player_subset, f, indent=2)
        logger.info(f"Raw data saved to {raw_file}")
        
        return player_subset
    except Exception as e:
        logger.error(f"Error extracting players: {e}")
        raise

def transform_players(player_data):
    """Transform players data"""
    logger = get_logger("players_etl")
    logger.info("Starting players transformation")
    
    transformed_players = []
    for player in player_data:
        transformed_players.append({
            'player_id': str(player['id']),
            'first_name': player['first_name'],
            'last_name': player['last_name']
        })
    
    logger.info(f"Transformed {len(transformed_players)} players")
    return transformed_players

def load_players(transformed_players):
    """Load players into the database"""
    logger = get_logger("players_etl")
    logger.info("Starting players load")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    count = 0
    
    try:
        for player in transformed_players:
            cursor.execute('''
            INSERT OR REPLACE INTO dim_players (
                player_id, first_name, last_name, inserted_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ''', (
                player['player_id'],
                player['first_name'],
                player['last_name'],
                now,
                now
            ))
            count += 1
        
        conn.commit()
        logger.info(f"Loaded {count} players into database")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading players: {e}")
        raise
    finally:
        conn.close()

def players_etl(limit=None):
    """Run the full players ETL process"""
    logger = get_logger("players_etl")
    logger.info("Starting players ETL process")
    
    try:
        # Extract
        players_data = extract_players(limit)
        
        # Transform
        transformed_players = transform_players(players_data)
        
        # Load
        load_players(transformed_players)
        
        logger.info("Players ETL process completed successfully")
        return True
    except Exception as e:
        logger.error(f"Players ETL process failed: {e}")
        return False

if __name__ == "__main__":
    # Limit players to avoid rate limiting during testing
    players_etl(limit=100)
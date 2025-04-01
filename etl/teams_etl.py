# etl/teams_etl.py
import json
import os
import time
from datetime import datetime

from nba_api.stats.static import teams

from config.etl_settings import get_logger, get_db_connection, RAW_DIR

def extract_teams():
    """Extract team data from NBA API"""
    logger = get_logger("teams_etl")
    logger.info("Starting teams extraction")
    
    try:
        # Extract teams data
        nba_teams = teams.get_teams()
        logger.info(f"Extracted {len(nba_teams)} teams")
        
        # Save raw data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = os.path.join(RAW_DIR, f"teams_{timestamp}.json")
        with open(raw_file, 'w') as f:
            json.dump(nba_teams, f, indent=2)
        logger.info(f"Raw data saved to {raw_file}")
        
        return nba_teams
    except Exception as e:
        logger.error(f"Error extracting teams: {e}")
        raise

def transform_teams(nba_teams):
    """Transform teams data"""
    logger = get_logger("teams_etl")
    logger.info("Starting teams transformation")
    
    transformed_teams = []
    for team in nba_teams:
        transformed_teams.append({
            'team_id': str(team['id']),
            'team_name': team['nickname'],
            'team_city': team['city'],
            'team_abbreviation': team['abbreviation'],
            'conference': team.get('conference'),
            'division': team.get('division')
        })
    
    logger.info(f"Transformed {len(transformed_teams)} teams")
    return transformed_teams

def load_teams(transformed_teams):
    """Load teams into the database"""
    logger = get_logger("teams_etl")
    logger.info("Starting teams load")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    count = 0
    
    try:
        for team in transformed_teams:
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
                team['conference'],
                team['division'],
                now,
                now
            ))
            count += 1
        
        conn.commit()
        logger.info(f"Loaded {count} teams into database")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading teams: {e}")
        raise
    finally:
        conn.close()

def teams_etl():
    """Run the full teams ETL process"""
    logger = get_logger("teams_etl")
    logger.info("Starting teams ETL process")
    
    try:
        # Extract
        teams_data = extract_teams()
        
        # Transform
        transformed_teams = transform_teams(teams_data)
        
        # Load
        load_teams(transformed_teams)
        
        logger.info("Teams ETL process completed successfully")
        return True
    except Exception as e:
        logger.error(f"Teams ETL process failed: {e}")
        return False

if __name__ == "__main__":
    teams_etl()
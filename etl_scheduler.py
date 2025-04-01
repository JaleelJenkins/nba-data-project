# etl_scheduler.py (updated with all ETL jobs)
import schedule
import time
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.absolute()))

from config.etl_settings import get_logger
from etl.teams_etl import teams_etl
from etl.players_etl import players_etl
from etl.games_etl import games_etl
from etl.player_game_stats_etl import player_game_stats_etl
from etl.player_shot_tracking_etl import player_shot_tracking_etl
# Import other ETL modules as needed

def run_teams_etl():
    """Run the teams ETL process"""
    logger = get_logger("scheduler")
    logger.info("Running scheduled teams ETL job")
    teams_etl()

def run_players_etl():
    """Run the players ETL process"""
    logger = get_logger("scheduler")
    logger.info("Running scheduled players ETL job")
    players_etl(limit=100)

def run_games_etl():
    """Run the games ETL process"""
    logger = get_logger("scheduler")
    logger.info("Running scheduled games ETL job")
    games_etl(days_back=3)

def run_player_game_stats_etl():
    """Run the player game stats ETL process"""
    logger = get_logger("scheduler")
    logger.info("Running scheduled player game stats ETL job")
    player_game_stats_etl(days_back=3)

def run_player_shot_tracking_etl():
    """Run the player shot tracking ETL process"""
    logger = get_logger("scheduler")
    logger.info("Running scheduled player shot tracking ETL job")
    player_shot_tracking_etl(limit=10)

# Add functions for other ETL processes

def run_all_etl():
    """Run all ETL processes"""
    logger = get_logger("scheduler")
    logger.info("Running all ETL jobs")
    
    # Dimension tables
    run_teams_etl()
    run_players_etl()
    run_games_etl()
    
    # Fact tables
    run_player_game_stats_etl()
    run_player_shot_tracking_etl()
    # Run other ETL jobs

if __name__ == "__main__":
    logger = get_logger("scheduler")
    logger.info("Starting ETL scheduler")
    
    # Setup schedule
    schedule.every().sunday.at("01:00").do(run_teams_etl)
    schedule.every().sunday.at("02:00").do(run_players_etl)
    schedule.every().day.at("06:00").do(run_games_etl)
    schedule.every().day.at("07:00").do(run_player_game_stats_etl)
    schedule.every().day.at("08:00").do(run_player_shot_tracking_etl)
    # Schedule other ETL jobs
    
    # Run once at startup (optional)
    run_all_etl()
    
    # Run the scheduler
    logger.info("Scheduler running, press Ctrl+C to exit")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
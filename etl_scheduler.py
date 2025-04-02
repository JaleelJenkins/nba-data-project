import schedule
import time
import sys
from pathlib import Path
import os

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.absolute()))

# Create required directories
os.makedirs("logs", exist_ok=True)
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

# Simple logging setup
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("scheduler")

def run_teams_etl():
    """Run the teams ETL process"""
    logger.info("Running scheduled teams ETL job")
    # Mock implementation
    logger.info("Teams ETL completed")

def run_players_etl():
    """Run the players ETL process"""
    logger.info("Running scheduled players ETL job")
    # Mock implementation
    logger.info("Players ETL completed")

def run_games_etl():
    """Run the games ETL process"""
    logger.info("Running scheduled games ETL job")
    # Mock implementation
    logger.info("Games ETL completed")

def run_player_game_stats_etl():
    """Run the player game stats ETL process"""
    logger.info("Running scheduled player game stats ETL job")
    # Mock implementation
    logger.info("Player game stats ETL completed")

def run_all_etl():
    """Run all ETL processes"""
    logger.info("Running all ETL jobs")
    run_teams_etl()
    run_players_etl()
    run_games_etl()
    run_player_game_stats_etl()
    logger.info("All ETL jobs completed")

if __name__ == "__main__":
    logger.info("Starting ETL scheduler")
    
    # Setup schedule
    schedule.every().sunday.at("01:00").do(run_teams_etl)
    schedule.every().sunday.at("02:00").do(run_players_etl)
    schedule.every().day.at("06:00").do(run_games_etl)
    schedule.every().day.at("07:00").do(run_player_game_stats_etl)
    schedule.every().sunday.at("03:00").do(run_all_etl)
    
    # Run once at startup
    logger.info("Running initial ETL jobs")
    run_all_etl()
    
    # Show schedule
    logger.info("Schedule:")
    for job in schedule.jobs:
        logger.info(f"  - {job}")
    
    # Run the scheduler
    logger.info("Scheduler running, press Ctrl+C to exit")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")

# src/load/data_warehouse.py
import pandas as pd
import sqlite3
import os
from datetime import datetime
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from config.settings import DATABASE_PATH

class NBADataWarehouse:
    """Class to manage the NBA data warehouse"""
    
    def __init__(self, db_path=DATABASE_PATH):
        """Initialize the data warehouse"""
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.create_schema()
        
    def create_schema(self):
        """Create the data warehouse schema"""
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
            season INTEGER,
            season_type TEXT,
            home_team_id TEXT,
            away_team_id TEXT,
            venue_id TEXT,
            inserted_at TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY (home_team_id) REFERENCES dim_teams (team_id),
            FOREIGN KEY (away_team_id) REFERENCES dim_teams (team_id),
            FOREIGN KEY (venue_id) REFERENCES dim_venues (venue_id)
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
            is_weekend BOOLEAN,
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
            game_id TEXT,
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
            inserted_at TIMESTAMP,
            PRIMARY KEY (game_id),
            FOREIGN KEY (game_id) REFERENCES dim_games (game_id),
            FOREIGN KEY (date_id) REFERENCES dim_dates (date_id),
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
            date_id TEXT,
            minutes_played INTEGER,
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
            inserted_at TIMESTAMP,
            FOREIGN KEY (game_id) REFERENCES dim_games (game_id),
            FOREIGN KEY (player_id) REFERENCES dim_players (player_id),
            FOREIGN KEY (team_id) REFERENCES dim_teams (team_id),
            FOREIGN KEY (date_id) REFERENCES dim_dates (date_id)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fact_team_game_stats (
            team_game_id TEXT PRIMARY KEY,
            game_id TEXT,
            team_id TEXT,
            date_id TEXT,
            is_home BOOLEAN,
            points INTEGER,
            assists INTEGER,
            rebounds INTEGER,
            offensive_rebounds INTEGER,
            defensive_rebounds INTEGER,
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
            fast_break_points INTEGER,
            points_in_paint INTEGER,
            points_off_turnovers INTEGER,
            second_chance_points INTEGER,
            inserted_at TIMESTAMP,
            FOREIGN KEY (game_id) REFERENCES dim_games (game_id),
            FOREIGN KEY (team_id) REFERENCES dim_teams (team_id),
            FOREIGN KEY (date_id) REFERENCES dim_dates (date_id)
        )
        ''')
        
        self.conn.commit()
        print("Data warehouse schema created successfully")
        
    def load_teams(self, teams_data):
        """Load teams into the dimension table"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        cursor = self.conn.cursor()
        for team in teams_data:
            cursor.execute('''
            INSERT INTO dim_teams (
                team_id, team_name, team_city, team_abbreviation,
                conference, division, inserted_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_id) DO UPDATE SET
                team_name = excluded.team_name,
                team_city = excluded.team_city,
                team_abbreviation = excluded.team_abbreviation,
                conference = excluded.conference,
                division = excluded.division,
                updated_at = excluded.updated_at
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
        
    def load_games(self, games_data):
        """Load games into the dimension and fact tables"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        cursor = self.conn.cursor()
        for game in games_data:
            # Load dim_games
            game_date = game.get('game_date')
            date_id = game_date.replace('-', '') if game_date else None
            
            # Ensure date exists in dim_dates
            if date_id:
                self._ensure_date_exists(game_date)
            
            cursor.execute('''
            INSERT INTO dim_games (
                game_id, game_date, season, season_type,
                home_team_id, away_team_id, venue_id,
                inserted_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(game_id) DO UPDATE SET
                game_date = excluded.game_date,
                season = excluded.season,
                season_type = excluded.season_type,
                home_team_id = excluded.home_team_id,
                away_team_id = excluded.away_team_id,
                venue_id = excluded.venue_id,
                updated_at = excluded.updated_at
            ''', (
                game['game_id'],
                game_date,
                game.get('season'),
                game.get('season_type'),
                game.get('home_team_id'),
                game.get('away_team_id'),
                game.get('venue_id'),
                now,
                now
            ))
            
            # Load fact_game_stats
            cursor.execute('''
            INSERT INTO fact_game_stats (
                game_id, date_id, home_team_id, away_team_id,
                home_team_score, away_team_score,
                home_q1_score, home_q2_score, home_q3_score, home_q4_score,
                away_q1_score, away_q2_score, away_q3_score, away_q4_score,
                home_ot_score, away_ot_score, attendance, lead_changes,
                times_tied, duration_minutes, inserted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(game_id) DO UPDATE SET
                home_team_score = excluded.home_team_score,
                away_team_score = excluded.away_team_score,
                home_q1_score = excluded.home_q1_score,
                home_q2_score = excluded.home_q2_score,
                home_q3_score = excluded.home_q3_score,
                home_q4_score = excluded.home_q4_score,
                away_q1_score = excluded.away_q1_score,
                away_q2_score = excluded.away_q2_score,
                away_q3_score = excluded.away_q3_score,
                away_q4_score = excluded.away_q4_score,
                home_ot_score = excluded.home_ot_score,
                away_ot_score = excluded.away_ot_score
            ''', (
                game['game_id'],
                date_id,
                game.get('home_team_id'),
                game.get('away_team_id'),
                game.get('home_team_score'),
                game.get('away_team_score'),
                game.get('home_q1_score'),
                game.get('home_q2_score'),
                game.get('home_q3_score'),
                game.get('home_q4_score'),
                game.get('away_q1_score'),
                game.get('away_q2_score'),
                game.get('away_q3_score'),
                game.get('away_q4_score'),
                game.get('home_ot_score'),
                game.get('away_ot_score'),
                game.get('attendance'),
                game.get('lead_changes'),
                game.get('times_tied'),
                game.get('duration_minutes'),
                now
            ))
        
        self.conn.commit()
        print(f"Loaded {len(games_data)} games into data warehouse")
        
    def _ensure_date_exists(self, date_str):
        """Ensure the date exists in the dim_dates table"""
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        date_id = date_str.replace('-', '')
        
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM dim_dates WHERE date_id = ?', (date_id,))
        if not cursor.fetchone():
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            cursor.execute('''
            INSERT INTO dim_dates (
                date_id, full_date, day_of_week, day_name,
                day_of_month, day_of_year, week_of_year,
                month_num, month_name, quarter, year,
                is_weekend, inserted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                date_id,
                date_str,
                date_obj.weekday(),
                date_obj.strftime("%A"),
                date_obj.day,
                date_obj.timetuple().tm_yday,
                date_obj.isocalendar()[1],
                date_obj.month,
                date_obj.strftime("%B"),
                (date_obj.month - 1) // 3 + 1,
                date_obj.year,
                date_obj.weekday() >= 5,  # 5 = Saturday, 6 = Sunday
                now
            ))
            
            self.conn.commit()
    
    def load_player_game_stats(self, stats_data):
        """Load player game stats into the fact table"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        cursor = self.conn.cursor()
        for stat in stats_data:
            # Generate a unique player_game_id
            player_game_id = f"{stat['game_id']}_{stat['player_id']}"
            
            game_date = self._get_game_date(stat['game_id'])
            date_id = game_date.replace('-', '') if game_date else None
            
            cursor.execute('''
            INSERT INTO fact_player_game_stats (
                player_game_id, game_id, player_id, team_id, date_id,
                minutes_played, points, assists, rebounds, steals,
                blocks, turnovers, personal_fouls, fg_made, fg_attempted,
                fg_pct, fg3_made, fg3_attempted, fg3_pct, ft_made,
                ft_attempted, ft_pct, plus_minus, inserted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(player_game_id) DO UPDATE SET
                minutes_played = excluded.minutes_played,
                points = excluded.points,
                assists = excluded.assists,
                rebounds = excluded.rebounds,
                steals = excluded.steals,
                blocks = excluded.blocks,
                turnovers = excluded.turnovers,
                personal_fouls = excluded.personal_fouls,
                fg_made = excluded.fg_made,
                fg_attempted = excluded.fg_attempted,
                fg_pct = excluded.fg_pct,
                fg3_made = excluded.fg3_made,
                fg3_attempted = excluded.fg3_attempted,
                fg3_pct = excluded.fg3_pct,
                ft_made = excluded.ft_made,
                ft_attempted = excluded.ft_attempted,
                ft_pct = excluded.ft_pct,
                plus_minus = excluded.plus_minus
            ''', (
                player_game_id,
                stat['game_id'],
                stat['player_id'],
                stat['team_id'],
                date_id,
                stat.get('minutes_played'),
                stat.get('points'),
                stat.get('assists'),
                stat.get('rebounds'),
                stat.get('steals'),
                stat.get('blocks'),
                stat.get('turnovers'),
                stat.get('personal_fouls'),
                stat.get('fg_made'),
                stat.get('fg_attempted'),
                stat.get('fg_pct'),
                stat.get('fg3_made'),
                stat.get('fg3_attempted'),
                stat.get('fg3_pct'),
                stat.get('ft_made'),
                stat.get('ft_attempted'),
                stat.get('ft_pct'),
                stat.get('plus_minus'),
                now
            ))
        
        self.conn.commit()
        print(f"Loaded {len(stats_data)} player game stats into data warehouse")
    
    def _get_game_date(self, game_id):
        """Get the game date for a given game_id"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT game_date FROM dim_games WHERE game_id = ?', (game_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
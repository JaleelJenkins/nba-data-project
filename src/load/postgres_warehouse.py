# src/load/postgres_warehouse.py

import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, Boolean, DateTime, Date, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from config.settings import DB_URL

Base = declarative_base()

# Define ORM models for dimensional tables
class DimTeam(Base):
    __tablename__ = 'dim_teams'
    
    team_id = Column(String, primary_key=True)
    team_name = Column(String)
    team_city = Column(String)
    team_abbreviation = Column(String)
    conference = Column(String)
    division = Column(String)
    inserted_at = Column(DateTime)
    updated_at = Column(DateTime)

class DimPlayer(Base):
    __tablename__ = 'dim_players'
    
    player_id = Column(String, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    jersey_num = Column(Integer)
    position = Column(String)
    height_inches = Column(Integer)
    weight_lbs = Column(Integer)
    birth_date = Column(Date)
    draft_year = Column(Integer)
    inserted_at = Column(DateTime)
    updated_at = Column(DateTime)

class DimGame(Base):
    __tablename__ = 'dim_games'
    
    game_id = Column(String, primary_key=True)
    game_date = Column(Date)
    season = Column(String)
    season_type = Column(String)
    home_team_id = Column(String, ForeignKey('dim_teams.team_id'))
    away_team_id = Column(String, ForeignKey('dim_teams.team_id'))
    venue_id = Column(String, ForeignKey('dim_venues.venue_id'))
    inserted_at = Column(DateTime)
    updated_at = Column(DateTime)

class DimDate(Base):
    __tablename__ = 'dim_dates'
    
    date_id = Column(String, primary_key=True)
    full_date = Column(Date)
    day_of_week = Column(Integer)
    day_name = Column(String)
    day_of_month = Column(Integer)
    day_of_year = Column(Integer)
    week_of_year = Column(Integer)
    month_num = Column(Integer)
    month_name = Column(String)
    quarter = Column(Integer)
    year = Column(Integer)
    is_weekend = Column(Boolean)
    inserted_at = Column(DateTime)

class DimVenue(Base):
    __tablename__ = 'dim_venues'
    
    venue_id = Column(String, primary_key=True)
    venue_name = Column(String)
    city = Column(String)
    state = Column(String)
    country = Column(String)
    capacity = Column(Integer)
    inserted_at = Column(DateTime)
    updated_at = Column(DateTime)

# Define ORM models for fact tables
class FactGameStats(Base):
    __tablename__ = 'fact_game_stats'
    
    game_id = Column(String, ForeignKey('dim_games.game_id'), primary_key=True)
    date_id = Column(String, ForeignKey('dim_dates.date_id'))
    home_team_id = Column(String, ForeignKey('dim_teams.team_id'))
    away_team_id = Column(String, ForeignKey('dim_teams.team_id'))
    home_team_score = Column(Integer)
    away_team_score = Column(Integer)
    home_q1_score = Column(Integer)
    home_q2_score = Column(Integer)
    home_q3_score = Column(Integer)
    home_q4_score = Column(Integer)
    away_q1_score = Column(Integer)
    away_q2_score = Column(Integer)
    away_q3_score = Column(Integer)
    away_q4_score = Column(Integer)
    home_ot_score = Column(Integer)
    away_ot_score = Column(Integer)
    attendance = Column(Integer)
    lead_changes = Column(Integer)
    times_tied = Column(Integer)
    duration_minutes = Column(Integer)
    inserted_at = Column(DateTime)

class FactPlayerGameStats(Base):
    __tablename__ = 'fact_player_game_stats'
    
    player_game_id = Column(String, primary_key=True)
    game_id = Column(String, ForeignKey('dim_games.game_id'))
    player_id = Column(String, ForeignKey('dim_players.player_id'))
    team_id = Column(String, ForeignKey('dim_teams.team_id'))
    date_id = Column(String, ForeignKey('dim_dates.date_id'))
    minutes_played = Column(Float)
    points = Column(Integer)
    assists = Column(Integer)
    rebounds = Column(Integer)
    steals = Column(Integer)
    blocks = Column(Integer)
    turnovers = Column(Integer)
    personal_fouls = Column(Integer)
    fg_made = Column(Integer)
    fg_attempted = Column(Integer)
    fg_pct = Column(Float)
    fg3_made = Column(Integer)
    fg3_attempted = Column(Integer)
    fg3_pct = Column(Float)
    ft_made = Column(Integer)
    ft_attempted = Column(Integer)
    ft_pct = Column(Float)
    plus_minus = Column(Integer)
    inserted_at = Column(DateTime)

class FactTeamGameStats(Base):
    __tablename__ = 'fact_team_game_stats'
    
    team_game_id = Column(String, primary_key=True)
    game_id = Column(String, ForeignKey('dim_games.game_id'))
    team_id = Column(String, ForeignKey('dim_teams.team_id'))
    date_id = Column(String, ForeignKey('dim_dates.date_id'))
    is_home = Column(Boolean)
    points = Column(Integer)
    assists = Column(Integer)
    rebounds = Column(Integer)
    offensive_rebounds = Column(Integer)
    defensive_rebounds = Column(Integer)
    steals = Column(Integer)
    blocks = Column(Integer)
    turnovers = Column(Integer)
    personal_fouls = Column(Integer)
    fg_made = Column(Integer)
    fg_attempted = Column(Integer)
    fg_pct = Column(Float)
    fg3_made = Column(Integer)
    fg3_attempted = Column(Integer)
    fg3_pct = Column(Float)
    ft_made = Column(Integer)
    ft_attempted = Column(Integer)
    ft_pct = Column(Float)
    fast_break_points = Column(Integer)
    points_in_paint = Column(Integer)
    points_off_turnovers = Column(Integer)
    second_chance_points = Column(Integer)
    inserted_at = Column(DateTime)

# NEW FACT TABLES FOR ADVANCED PLAYER STATISTICS

class FactPlayerShotTracking(Base):
    __tablename__ = 'fact_player_shot_tracking'
    
    player_game_id = Column(String, ForeignKey('fact_player_game_stats.player_game_id'), primary_key=True)
    game_id = Column(String, ForeignKey('dim_games.game_id'))
    player_id = Column(String, ForeignKey('dim_players.player_id'))
    team_id = Column(String, ForeignKey('dim_teams.team_id'))
    date_id = Column(String, ForeignKey('dim_dates.date_id'))
    shots_made_0_3ft = Column(Integer)
    shots_attempted_0_3ft = Column(Integer)
    shots_pct_0_3ft = Column(Float)
    shots_made_3_10ft = Column(Integer)
    shots_attempted_3_10ft = Column(Integer)
    shots_pct_3_10ft = Column(Float)
    shots_made_10_16ft = Column(Integer)
    shots_attempted_10_16ft = Column(Integer)
    shots_pct_10_16ft = Column(Float)
    shots_made_16ft_3pt = Column(Integer)
    shots_attempted_16ft_3pt = Column(Integer)
    shots_pct_16ft_3pt = Column(Float)
    corner_3_made = Column(Integer)
    corner_3_attempted = Column(Integer)
    corner_3_pct = Column(Float)
    above_break_3_made = Column(Integer)
    above_break_3_attempted = Column(Integer)
    above_break_3_pct = Column(Float)
    dunk_made = Column(Integer)
    dunk_attempted = Column(Integer)
    inserted_at = Column(DateTime)

class FactPlayerDefensive(Base):
    __tablename__ = 'fact_player_defensive'
    
    player_game_id = Column(String, ForeignKey('fact_player_game_stats.player_game_id'), primary_key=True)
    game_id = Column(String, ForeignKey('dim_games.game_id'))
    player_id = Column(String, ForeignKey('dim_players.player_id'))
    team_id = Column(String, ForeignKey('dim_teams.team_id'))
    date_id = Column(String, ForeignKey('dim_dates.date_id'))
    defensive_rebounds = Column(Integer)
    contested_shots = Column(Integer)
    contested_shots_made = Column(Integer)
    contested_shots_pct = Column(Float)
    deflections = Column(Integer)
    charges_drawn = Column(Integer)
    box_outs = Column(Integer)
    loose_balls_recovered = Column(Integer)
    screen_assists = Column(Integer)
    defensive_win_shares = Column(Float)
    defensive_rating = Column(Float)
    steals_per_foul = Column(Float)
    blocks_per_foul = Column(Float)
    block_pct = Column(Float)
    steal_pct = Column(Float)
    defensive_box_plus_minus = Column(Float)
    inserted_at = Column(DateTime)

class FactPlayerPlaymaking(Base):
    __tablename__ = 'fact_player_playmaking'
    
    player_game_id = Column(String, ForeignKey('fact_player_game_stats.player_game_id'), primary_key=True)
    game_id = Column(String, ForeignKey('dim_games.game_id'))
    player_id = Column(String, ForeignKey('dim_players.player_id'))
    team_id = Column(String, ForeignKey('dim_teams.team_id'))
    date_id = Column(String, ForeignKey('dim_dates.date_id'))
    potential_assists = Column(Integer)
    assist_points_created = Column(Integer)
    passes_made = Column(Integer)
    passes_received = Column(Integer)
    secondary_assists = Column(Integer)
    free_throw_assists = Column(Integer)
    assist_to_turnover_ratio = Column(Float)
    assist_ratio = Column(Float)
    assist_pct = Column(Float)
    time_of_possession = Column(Float)
    avg_dribbles_per_touch = Column(Float)
    avg_touch_time = Column(Float)
    usage_pct = Column(Float)
    front_court_touches = Column(Integer)
    elbow_touches = Column(Integer)
    post_touches = Column(Integer)
    paint_touches = Column(Integer)
    inserted_at = Column(DateTime)

class FactPlayerEfficiency(Base):
    __tablename__ = 'fact_player_efficiency'
    
    player_game_id = Column(String, ForeignKey('fact_player_game_stats.player_game_id'), primary_key=True)
    game_id = Column(String, ForeignKey('dim_games.game_id'))
    player_id = Column(String, ForeignKey('dim_players.player_id'))
    team_id = Column(String, ForeignKey('dim_teams.team_id'))
    date_id = Column(String, ForeignKey('dim_dates.date_id'))
    true_shooting_pct = Column(Float)
    effective_fg_pct = Column(Float)
    offensive_rating = Column(Float)
    offensive_win_shares = Column(Float)
    offensive_box_plus_minus = Column(Float)
    player_impact_estimate = Column(Float)
    game_score = Column(Float)
    points_per_shot = Column(Float)
    points_per_touch = Column(Float)
    points_per_possession = Column(Float)
    offensive_rebound_pct = Column(Float)
    defensive_rebound_pct = Column(Float)
    total_rebound_pct = Column(Float)
    net_rating = Column(Float)
    value_added = Column(Float)
    estimated_wins_added = Column(Float)
    inserted_at = Column(DateTime)

class FactPlayerHustle(Base):
    __tablename__ = 'fact_player_hustle'
    
    player_game_id = Column(String, ForeignKey('fact_player_game_stats.player_game_id'), primary_key=True)
    game_id = Column(String, ForeignKey('dim_games.game_id'))
    player_id = Column(String, ForeignKey('dim_players.player_id'))
    team_id = Column(String, ForeignKey('dim_teams.team_id'))
    date_id = Column(String, ForeignKey('dim_dates.date_id'))
    contested_shots_2pt = Column(Integer)
    contested_shots_3pt = Column(Integer)
    deflections = Column(Integer)
    loose_balls_recovered = Column(Integer)
    charges_drawn = Column(Integer)
    screen_assists = Column(Integer)
    screen_assist_points = Column(Integer)
    box_outs = Column(Integer)
    box_outs_offensive = Column(Integer)
    box_outs_defensive = Column(Integer)
    distance_miles = Column(Float)
    distance_miles_offense = Column(Float)
    distance_miles_defense = Column(Float)
    avg_speed_mph = Column(Float)
    avg_speed_mph_offense = Column(Float)
    avg_speed_mph_defense = Column(Float)
    inserted_at = Column(DateTime)

class PostgresWarehouse:
    """Class to manage the NBA PostgreSQL data warehouse"""
    
    def __init__(self, db_url=DB_URL):
        """Initialize the data warehouse"""
        self.db_url = db_url
        self.engine = create_engine(db_url)
        self.create_schema()
        
        # Create session
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
    def create_schema(self):
        """Create the data warehouse schema"""
        Base.metadata.create_all(self.engine)
        print("PostgreSQL data warehouse schema created successfully")
    
    def load_teams(self, teams_data):
        """Load teams into the dimension table"""
        now = datetime.now()
        
        for team_data in teams_data:
            team = DimTeam(
                team_id=team_data['team_id'],
                team_name=team_data['team_name'],
                team_city=team_data['team_city'],
                team_abbreviation=team_data['team_abbreviation'],
                conference=team_data.get('conference'),
                division=team_data.get('division'),
                inserted_at=now,
                updated_at=now
            )
            
            # Check if team already exists
            existing_team = self.session.query(DimTeam).filter_by(team_id=team_data['team_id']).first()
            if existing_team:
                # Update existing team
                existing_team.team_name = team_data['team_name']
                existing_team.team_city = team_data['team_city']
                existing_team.team_abbreviation = team_data['team_abbreviation']
                existing_team.conference = team_data.get('conference')
                existing_team.division = team_data.get('division')
                existing_team.updated_at = now
            else:
                # Add new team
                self.session.add(team)
        
        self.session.commit()
        print(f"Loaded {len(teams_data)} teams into data warehouse")
    
    def load_games(self, games_data):
        """Load games into the dimension table"""
        now = datetime.now()
        
        for game_data in games_data:
            # Parse game date
            if isinstance(game_data.get('game_date'), str):
                try:
                    game_date = datetime.strptime(game_data['game_date'], "%Y-%m-%d").date()
                except ValueError:
                    game_date = None
            else:
                game_date = game_data.get('game_date')
            
            # Create date_id
            date_id = game_date.strftime("%Y%m%d") if game_date else None
            
            # Ensure date exists in dim_dates
            if date_id:
                self._ensure_date_exists(game_date)
            
            game = DimGame(
                game_id=game_data['game_id'],
                game_date=game_date,
                season=game_data.get('season'),
                season_type=game_data.get('season_type'),
                home_team_id=game_data.get('home_team_id'),
                away_team_id=game_data.get('away_team_id'),
                venue_id=game_data.get('venue_id'),
                inserted_at=now,
                updated_at=now
            )
            
            # Check if game already exists
            existing_game = self.session.query(DimGame).filter_by(game_id=game_data['game_id']).first()
            if existing_game:
                # Update existing game
                existing_game.game_date = game_date
                existing_game.season = game_data.get('season')
                existing_game.season_type = game_data.get('season_type')
                existing_game.home_team_id = game_data.get('home_team_id')
                existing_game.away_team_id = game_data.get('away_team_id')
                existing_game.venue_id = game_data.get('venue_id')
                existing_game.updated_at = now
            else:
                # Add new game
                self.session.add(game)
            
            # Add game stats if available
            if 'home_team_score' in game_data or 'away_team_score' in game_data:
                game_stats = FactGameStats(
                    game_id=game_data['game_id'],
                    date_id=date_id,
                    home_team_id=game_data.get('home_team_id'),
                    away_team_id=game_data.get('away_team_id'),
                    home_team_score=game_data.get('home_team_score'),
                    away_team_score=game_data.get('away_team_score'),
                    home_q1_score=game_data.get('home_q1_score'),
                    home_q2_score=game_data.get('home_q2_score'),
                    home_q3_score=game_data.get('home_q3_score'),
                    home_q4_score=game_data.get('home_q4_score'),
                    away_q1_score=game_data.get('away_q1_score'),
                    away_q2_score=game_data.get('away_q2_score'),
                    away_q3_score=game_data.get('away_q3_score'),
                    away_q4_score=game_data.get('away_q4_score'),
                    home_ot_score=game_data.get('home_ot_score'),
                    away_ot_score=game_data.get('away_ot_score'),
                    attendance=game_data.get('attendance'),
                    lead_changes=game_data.get('lead_changes'),
                    times_tied=game_data.get('times_tied'),
                    duration_minutes=game_data.get('duration_minutes'),
                    inserted_at=now
                )
                
                # Check if game stats already exist
                existing_stats = self.session.query(FactGameStats).filter_by(game_id=game_data['game_id']).first()
                if existing_stats:
                    # Update existing stats
                    existing_stats.home_team_score = game_data.get('home_team_score')
                    existing_stats.away_team_score = game_data.get('away_team_score')
                    existing_stats.home_q1_score = game_data.get('home_q1_score')
                    existing_stats.home_q2_score = game_data.get('home_q2_score')
                    existing_stats.home_q3_score = game_data.get('home_q3_score')
                    existing_stats.home_q4_score = game_data.get('home_q4_score')
                    existing_stats.away_q1_score = game_data.get('away_q1_score')
                    existing_stats.away_q2_score = game_data.get('away_q2_score')
                    existing_stats.away_q3_score = game_data.get('away_q3_score')
                    existing_stats.away_q4_score = game_data.get('away_q4_score')
                    existing_stats.home_ot_score = game_data.get('home_ot_score')
                    existing_stats.away_ot_score = game_data.get('away_ot_score')
                    existing_stats.attendance = game_data.get('attendance')
                    existing_stats.lead_changes = game_data.get('lead_changes')
                    existing_stats.times_tied = game_data.get('times_tied')
                    existing_stats.duration_minutes = game_data.get('duration_minutes')
                else:
                    # Add new stats
                    self.session.add(game_stats)
        
        self.session.commit()
        print(f"Loaded {len(games_data)} games into data warehouse")
    
    def load_player_game_stats(self, stats_data):
        """Load player game stats into the fact table"""
        now = datetime.now()
        
        for stat_data in stats_data:
            # Generate player_game_id
            player_game_id = f"{stat_data['game_id']}_{stat_data['player_id']}"
            
            # Get date_id from game
            game = self.session.query(DimGame).filter_by(game_id=stat_data['game_id']).first()
            date_id = game.game_date.strftime("%Y%m%d") if game and game.game_date else None
            
            player_stats = FactPlayerGameStats(
                player_game_id=player_game_id,
                game_id=stat_data['game_id'],
                player_id=stat_data['player_id'],
                team_id=stat_data['team_id'],
                date_id=date_id,
                minutes_played=stat_data.get('minutes_played'),
                points=stat_data.get('points'),
                assists=stat_data.get('assists'),
                rebounds=stat_data.get('rebounds'),
                steals=stat_data.get('steals'),
                blocks=stat_data.get('blocks'),
                turnovers=stat_data.get('turnovers'),
                personal_fouls=stat_data.get('personal_fouls'),
                fg_made=stat_data.get('fg_made'),
                fg_attempted=stat_data.get('fg_attempted'),
                fg_pct=stat_data.get('fg_pct'),
                fg3_made=stat_data.get('fg3_made'),
                fg3_attempted=stat_data.get('fg3_attempted'),
                fg3_pct=stat_data.get('fg3_pct'),
                ft_made=stat_data.get('ft_made'),
                ft_attempted=stat_data.get('ft_attempted'),
                ft_pct=stat_data.get('ft_pct'),
                plus_minus=stat_data.get('plus_minus'),
                inserted_at=now
            )
            
            # Check if player stats already exist
            existing_stats = self.session.query(FactPlayerGameStats).filter_by(player_game_id=player_game_id).first()
            if existing_stats:
                # Update existing stats
                existing_stats.minutes_played = stat_data.get('minutes_played')
                existing_stats.points = stat_data.get('points')
                existing_stats.assists = stat_data.get('assists')
                existing_stats.rebounds = stat_data.get('rebounds')
                existing_stats.steals = stat_data.get('steals')
                existing_stats.blocks = stat_data.get('blocks')
                existing_stats.turnovers = stat_data.get('turnovers')
                existing_stats.personal_fouls = stat_data.get('personal_fouls')
                existing_stats.fg_made = stat_data.get('fg_made')
                existing_stats.fg_attempted = stat_data.get('fg_attempted')
                existing_stats.fg_pct = stat_data.get('fg_pct')
                existing_stats.fg3_made = stat_data.get('fg3_made')
                existing_stats.fg3_attempted = stat_data.get('fg3_attempted')
                existing_stats.fg3_pct = stat_data.get('fg3_pct')
                existing_stats.ft_made = stat_data.get('ft_made')
                existing_stats.ft_attempted = stat_data.get('ft_attempted')
                existing_stats.ft_pct = stat_data.get('ft_pct')
                existing_stats.plus_minus = stat_data.get('plus_minus')
            else:
                # Add new stats
                self.session.add(player_stats)
        
        self.session.commit()
        print(f"Loaded {len(stats_data)} player game stats into data warehouse")
    
    def load_player_advanced_stats(self, player_game_id, advanced_stats):
        """Load advanced player stats into the fact tables"""
        now = datetime.now()
        
        # Get basic info
        player_stat = self.session.query(FactPlayerGameStats).filter_by(player_game_id=player_game_id).first()
        if not player_stat:
            print(f"Cannot load advanced stats: Player game stats not found for {player_game_id}")
            return
        
        # Load shot tracking stats
        if 'shot_tracking' in advanced_stats:
            shot_data = advanced_stats['shot_tracking']
            shot_tracking = FactPlayerShotTracking(
                player_game_id=player_game_id,
                game_id=player_stat.game_id,
                player_id=player_stat.player_id,
                team_id=player_stat.team_id,
                date_id=player_stat.date_id,
                shots_made_0_3ft=shot_data.get('shots_made_0_3ft'),
                shots_attempted_0_3ft=shot_data.get('shots_attempted_0_3ft'),
                shots_pct_0_3ft=shot_data.get('shots_pct_0_3ft'),
                shots_made_3_10ft=shot_data.get('shots_made_3_10ft'),
                shots_attempted_3_10ft=shot_data.get('shots_attempted_3_10ft'),
                shots_pct_3_10ft=shot_data.get('shots_pct_3_10ft'),
                shots_made_10_16ft=shot_data.get('shots_made_10_16ft'),
                shots_attempted_10_16ft=shot_data.get('shots_attempted_10_16ft'),
                shots_pct_10_16ft=shot_data.get('shots_pct_10_16ft'),
                shots_made_16ft_3pt=shot_data.get('shots_made_16ft_3pt'),
                shots_attempted_16ft_3pt=shot_data.get('shots_attempted_16ft_3pt'),
                shots_pct_16ft_3pt=shot_data.get('shots_pct_16ft_3pt'),
                corner_3_made=shot_data.get('corner_3_made'),
                corner_3_attempted=shot_data.get('corner_3_attempted'),
                corner_3_pct=shot_data.get('corner_3_pct'),
                above_break_3_made=shot_data.get('above_break_3_made'),
                above_break_3_attempted=shot_data.get('above_break_3_attempted'),
                above_break_3_pct=shot_data.get('above_break_3_pct'),
                dunk_made=shot_data.get('dunk_made'),
                dunk_attempted=shot_data.get('dunk_attempted'),
                inserted_at=now
            )
            
            # Check if shot tracking stats already exist
            existing_shot = self.session.query(FactPlayerShotTracking).filter_by(player_game_id=player_game_id).first()
            if existing_shot:
                # Update fields
                for key, value in shot_data.items():
                    if hasattr(existing_shot, key):
                        setattr(existing_shot, key, value)
            else:
                # Add new stats
                self.session.add(shot_tracking)
        
        # Load defensive stats
        if 'defensive' in advanced_stats:
            def_data = advanced_stats['defensive']
            defensive = FactPlayerDefensive(
                player_game_id=player_game_id,
                game_id=player_stat.game_id,
                player_id=player_stat.player_id,
                team_id=player_stat.team_id,
                date_id=player_stat.date_id,
                defensive_rebounds=def_data.get('defensive_rebounds'),
                contested_shots=def_data.get('contested_shots'),
                contested_shots_made=def_data.get('contested_shots_made'),
                contested_shots_pct=def_data.get('contested_shots_pct'),
                deflections=def_data.get('deflections'),
                charges_drawn=def_data.get('charges_drawn'),
                box_outs=def_data.get('box_outs'),
                loose_balls_recovered=def_data.get('loose_balls_recovered'),
                screen_assists=def_data.get('screen_assists'),
                defensive_win_shares=def_data.get('defensive_win_shares'),
                defensive_rating=def_data.get('defensive_rating'),
                steals_per_foul=def_data.get('steals_per_foul'),
                blocks_per_foul=def_data.get('blocks_per_foul'),
                block_pct=def_data.get('block_pct'),
                steal_pct=def_data.get('steal_pct'),
                defensive_box_plus_minus=def_data.get('defensive_box_plus_minus'),
                inserted_at=now
            )
            
            # Check if defensive stats already exist
            existing_def = self.session.query(FactPlayerDefensive).filter_by(player_game_id=player_game_id).first()
            if existing_def:
                # Update fields
                for key, value in def_data.items():
                    if hasattr(existing_def, key):
                        setattr(existing_def, key, value)
            else:
                # Add new stats
                self.session.add(defensive)
        
        # Load playmaking stats
        if 'playmaking' in advanced_stats:
            play_data = advanced_stats['playmaking']
            playmaking = FactPlayerPlaymaking(
                player_game_id=player_game_id,
                game_id=player_stat.game_id,
                player_id=player_stat.player_id,
                team_id=player_stat.team_id,
                date_id=player_stat.date_id,
                potential_assists=play_data.get('potential_assists'),
                assist_points_created=play_data.get('assist_points_created'),
                passes_made=play_data.get('passes_made'),
                passes_received=play_data.get('passes_received'),
                secondary_assists=play_data.get('secondary_assists'),
                free_throw_assists=play_data.get('free_throw_assists'),
                assist_to_turnover_ratio=play_data.get('assist_to_turnover_ratio'),
                assist_ratio=play_data.get('assist_ratio'),
                assist_pct=play_data.get('assist_pct'),
                time_of_possession=play_data.get('time_of_possession'),
                avg_dribbles_per_touch=play_data.get('avg_dribbles_per_touch'),
                avg_touch_time=play_data.get('avg_touch_time'),
                usage_pct=play_data.get('usage_pct'),
                front_court_touches=play_data.get('front_court_touches'),
                elbow_touches=play_data.get('elbow_touches'),
                post_touches=play_data.get('post_touches'),
                paint_touches=play_data.get('paint_touches'),
                inserted_at=now
            )
            
            # Check if playmaking stats already exist
            existing_play = self.session.query(FactPlayerPlaymaking).filter_by(player_game_id=player_game_id).first()
            if existing_play:
                # Update fields
                for key, value in play_data.items():
                    if hasattr(existing_play, key):
                        setattr(existing_play, key, value)
            else:
                # Add new stats
                self.session.add(playmaking)
        
        # Load efficiency stats
        if 'efficiency' in advanced_stats:
            eff_data = advanced_stats['efficiency']
            efficiency = FactPlayerEfficiency(
                player_game_id=player_game_id,
                game_id=player_stat.game_id,
                player_id=player_stat.player_id,
                team_id=player_stat.team_id,
                date_id=player_stat.date_id,
                true_shooting_pct=eff_data.get('true_shooting_pct'),
                effective_fg_pct=eff_data.get('effective_fg_pct'),
                offensive_rating=eff_data.get('offensive_rating'),
                offensive_win_shares=eff_data.get('offensive_win_shares'),
                offensive_box_plus_minus=eff_data.get('offensive_box_plus_minus'),
                player_impact_estimate=eff_data.get('player_impact_estimate'),
                game_score=eff_data.get('game_score'),
                points_per_shot=eff_data.get('points_per_shot'),
                points_per_touch=eff_data.get('points_per_touch'),
                points_per_possession=eff_data.get('points_per_possession'),
                offensive_rebound_pct=eff_data.get('offensive_rebound_pct'),
                defensive_rebound_pct=eff_data.get('defensive_rebound_pct'),
                total_rebound_pct=eff_data.get('total_rebound_pct'),
                net_rating=eff_data.get('net_rating'),
                value_added=eff_data.get('value_added'),
                estimated_wins_added=eff_data.get('estimated_wins_added'),
                inserted_at=now
            )
            
            # Check if efficiency stats already exist
            existing_eff = self.session.query(FactPlayerEfficiency).filter_by(player_game_id=player_game_id).first()
            if existing_eff:
                # Update fields
                for key, value in eff_data.items():
                    if hasattr(existing_eff, key):
                        setattr(existing_eff, key, value)
            else:
                # Add new stats
                self.session.add(efficiency)
        
        # Load hustle stats
        if 'hustle' in advanced_stats:
            hustle_data = advanced_stats['hustle']
            hustle = FactPlayerHustle(
                player_game_id=player_game_id,
                game_id=player_stat.game_id,
                player_id=player_stat.player_id,
                team_id=player_stat.team_id,
                date_id=player_stat.date_id,
                contested_shots_2pt=hustle_data.get('contested_shots_2pt'),
                contested_shots_3pt=hustle_data.get('contested_shots_3pt'),
                deflections=hustle_data.get('deflections'),
                loose_balls_recovered=hustle_data.get('loose_balls_recovered'),
                charges_drawn=hustle_data.get('charges_drawn'),
                screen_assists=hustle_data.get('screen_assists'),
                screen_assist_points=hustle_data.get('screen_assist_points'),
                box_outs=hustle_data.get('box_outs'),
                box_outs_offensive=hustle_data.get('box_outs_offensive'),
                box_outs_defensive=hustle_data.get('box_outs_defensive'),
                distance_miles=hustle_data.get('distance_miles'),
                distance_miles_offense=hustle_data.get('distance_miles_offense'),
                distance_miles_defense=hustle_data.get('distance_miles_defense'),
                avg_speed_mph=hustle_data.get('avg_speed_mph'),
                avg_speed_mph_offense=hustle_data.get('avg_speed_mph_offense'),
                avg_speed_mph_defense=hustle_data.get('avg_speed_mph_defense'),
                inserted_at=now
            )
            
            # Check if hustle stats already exist
            existing_hustle = self.session.query(FactPlayerHustle).filter_by(player_game_id=player_game_id).first()
            if existing_hustle:
                # Update fields
                for key, value in hustle_data.items():
                    if hasattr(existing_hustle, key):
                        setattr(existing_hustle, key, value)
            else:
                # Add new stats
                self.session.add(hustle)
        
        self.session.commit()
        print(f"Loaded advanced stats for player game ID {player_game_id}")
    
    def _ensure_date_exists(self, date_obj):
        """Ensure the date exists in the dim_dates table"""
        date_id = date_obj.strftime("%Y%m%d")
        
        # Check if date exists
        existing_date = self.session.query(DimDate).filter_by(date_id=date_id).first()
        if not existing_date:
            now = datetime.now()
            
            # Create date record
            date_record = DimDate(
                date_id=date_id,
                full_date=date_obj,
                day_of_week=date_obj.weekday(),
                day_name=date_obj.strftime("%A"),
                day_of_month=date_obj.day,
                day_of_year=date_obj.timetuple().tm_yday,
                week_of_year=date_obj.isocalendar()[1],
                month_num=date_obj.month,
                month_name=date_obj.strftime("%B"),
                quarter=(date_obj.month - 1) // 3 + 1,
                year=date_obj.year,
                is_weekend=date_obj.weekday() >= 5,  # 5 = Saturday, 6 = Sunday
                inserted_at=now
            )
            
            self.session.add(date_record)
            self.session.commit()
    
    def close(self):
        """Close the database connection"""
        if self.session:
            self.session.close()
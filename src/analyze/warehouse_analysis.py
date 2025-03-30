# src/analyze/warehouse_analysis.py
import pandas as pd
import sqlite3
import os
from pathlib import Path
import sys
import matplotlib.pyplot as plt

# Add project root to path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from config.settings import DATABASE_PATH

class NBAWarehouseAnalyzer:
    """Class for analyzing data in the NBA data warehouse"""
    
    def __init__(self, db_path=DATABASE_PATH):
        """Initialize the analyzer"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
    
    def top_scorers(self, season=None, limit=10):
        """Get the top scorers for a season"""
        query = """
        SELECT 
            p.player_id,
            p.first_name || ' ' || p.last_name AS player_name,
            t.team_name,
            COUNT(s.game_id) AS games_played,
            SUM(s.points) AS total_points,
            ROUND(AVG(s.points), 1) AS ppg,
            ROUND(AVG(s.rebounds), 1) AS rpg,
            ROUND(AVG(s.assists), 1) AS apg
        FROM fact_player_game_stats s
        JOIN dim_players p ON s.player_id = p.player_id
        JOIN dim_teams t ON s.team_id = t.team_id
        JOIN dim_games g ON s.game_id = g.game_id
        WHERE 1=1
        """
        
        if season:
            query += f" AND g.season = '{season}'"
        
        query += """
        GROUP BY p.player_id, player_name, t.team_name
        HAVING COUNT(s.game_id) >= 10
        ORDER BY ppg DESC
        LIMIT ?
        """
        
        return pd.read_sql(query, self.conn, params=(limit,))
    
def team_performance(self, season=None):
        """Get team performance metrics"""
        query = """
        SELECT 
            t.team_name,
            t.conference,
            t.division,
            COUNT(DISTINCT g.game_id) AS games_played,
            SUM(CASE WHEN 
                (s.is_home = 1 AND g.home_team_score > g.away_team_score) OR
                (s.is_home = 0 AND g.away_team_score > g.home_team_score)
                THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN 
                (s.is_home = 1 AND g.home_team_score < g.away_team_score) OR
                (s.is_home = 0 AND g.away_team_score < g.home_team_score)
                THEN 1 ELSE 0 END) AS losses,
            ROUND(AVG(s.points), 1) AS ppg,
            ROUND(AVG(s.rebounds), 1) AS rpg,
            ROUND(AVG(s.assists), 1) AS apg,
            ROUND(AVG(s.steals), 1) AS spg,
            ROUND(AVG(s.blocks), 1) AS bpg,
            ROUND(AVG(s.turnovers), 1) AS topg,
            ROUND(AVG(s.fg_pct) * 100, 1) AS fg_pct,
            ROUND(AVG(s.fg3_pct) * 100, 1) AS fg3_pct,
            ROUND(AVG(s.ft_pct) * 100, 1) AS ft_pct
        FROM fact_team_game_stats s
        JOIN dim_teams t ON s.team_id = t.team_id
        JOIN dim_games g ON s.game_id = g.game_id
        WHERE 1=1
        """
        
        if season:
            query += f" AND g.season = '{season}'"
        
        query += """
        GROUP BY t.team_name, t.conference, t.division
        ORDER BY (wins * 1.0 / NULLIF(games_played, 0)) DESC
        """
        
        return pd.read_sql(query, self.conn)
    
def head_to_head(self, team1_id, team2_id, seasons=None):
        """Get head-to-head matchup stats between two teams"""
        query = """
        SELECT 
            g.game_id,
            g.game_date,
            t1.team_name AS team1_name,
            t2.team_name AS team2_name,
            CASE 
                WHEN g.home_team_id = ? THEN g.home_team_score
                ELSE g.away_team_score
            END AS team1_score,
            CASE 
                WHEN g.home_team_id = ? THEN g.away_team_score
                ELSE g.home_team_score
            END AS team2_score,
            CASE 
                WHEN (g.home_team_id = ? AND g.home_team_score > g.away_team_score) OR
                     (g.away_team_id = ? AND g.away_team_score > g.home_team_score)
                THEN 1
                ELSE 0
            END AS team1_win
        FROM dim_games g
        JOIN dim_teams t1 ON t1.team_id = ?
        JOIN dim_teams t2 ON t2.team_id = ?
        WHERE (g.home_team_id = ? AND g.away_team_id = ?) OR
              (g.home_team_id = ? AND g.away_team_id = ?)
        """
        
        if seasons:
            if isinstance(seasons, list):
                seasons_str = "','".join(str(s) for s in seasons)
                query += f" AND g.season IN ('{seasons_str}')"
            else:
                query += f" AND g.season = '{seasons}'"
        
        query += " ORDER BY g.game_date DESC"
        
        params = (
            team1_id, team2_id, team1_id, team1_id, 
            team1_id, team2_id, team1_id, team2_id, team2_id, team1_id
        )
        
        return pd.read_sql(query, self.conn, params=params)
    
def player_trends(self, player_id, last_n_games=10):
        """Get performance trends for a player over their last N games"""
        query = """
        WITH player_games AS (
            SELECT 
                s.game_id,
                g.game_date,
                s.points,
                s.rebounds,
                s.assists,
                s.steals,
                s.blocks,
                s.turnovers,
                s.minutes_played,
                s.fg_made,
                s.fg_attempted,
                s.fg_pct,
                s.fg3_made,
                s.fg3_attempted,
                s.fg3_pct,
                s.ft_made,
                s.ft_attempted,
                s.ft_pct,
                s.plus_minus,
                ROW_NUMBER() OVER (ORDER BY g.game_date DESC) AS game_num
            FROM fact_player_game_stats s
            JOIN dim_games g ON s.game_id = g.game_id
            WHERE s.player_id = ?
            ORDER BY g.game_date DESC
        )
        SELECT 
            p.first_name || ' ' || p.last_name AS player_name,
            t.team_name,
            pg.game_date,
            pg.points,
            pg.rebounds,
            pg.assists,
            pg.steals,
            pg.blocks,
            pg.turnovers,
            pg.minutes_played,
            pg.fg_made || '/' || pg.fg_attempted AS fg,
            ROUND(pg.fg_pct * 100, 1) AS fg_pct,
            pg.fg3_made || '/' || pg.fg3_attempted AS fg3,
            ROUND(pg.fg3_pct * 100, 1) AS fg3_pct,
            pg.ft_made || '/' || pg.ft_attempted AS ft,
            ROUND(pg.ft_pct * 100, 1) AS ft_pct,
            pg.plus_minus
        FROM player_games pg
        JOIN dim_players p ON p.player_id = ?
        JOIN fact_player_game_stats s ON s.game_id = pg.game_id AND s.player_id = ?
        JOIN dim_teams t ON s.team_id = t.team_id
        WHERE pg.game_num <= ?
        ORDER BY pg.game_date DESC
        """
        
        params = (player_id, player_id, player_id, last_n_games)
        return pd.read_sql(query, self.conn, params=params)
    
def season_standings(self, season, conference=None):
        """Get the standings for a specific season"""
        query = """
        WITH team_records AS (
            SELECT 
                t.team_id,
                t.team_name,
                t.conference,
                t.division,
                COUNT(DISTINCT g.game_id) AS games_played,
                SUM(CASE WHEN 
                    (s.is_home = 1 AND g.home_team_score > g.away_team_score) OR
                    (s.is_home = 0 AND g.away_team_score > g.home_team_score)
                    THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN 
                    (s.is_home = 1 AND g.home_team_score < g.away_team_score) OR
                    (s.is_home = 0 AND g.away_team_score < g.home_team_score)
                    THEN 1 ELSE 0 END) AS losses,
                ROUND(AVG(s.points), 1) AS ppg,
                ROUND(AVG(s.points_allowed), 1) AS papg
            FROM fact_team_game_stats s
            JOIN dim_teams t ON s.team_id = t.team_id
            JOIN dim_games g ON s.game_id = g.game_id
            WHERE g.season = ?
            GROUP BY t.team_id, t.team_name, t.conference, t.division
        )
        SELECT 
            team_name,
            conference,
            division,
            wins,
            losses,
            games_played,
            ROUND(wins * 100.0 / games_played, 1) AS win_pct,
            ppg,
            papg,
            ROUND(ppg - papg, 1) AS point_diff,
            RANK() OVER (PARTITION BY conference ORDER BY wins DESC, point_diff DESC) AS conf_rank
        FROM team_records
        WHERE 1=1
        """
        
        if conference:
            query += f" AND conference = ?"
            params = (season, conference)
        else:
            params = (season,)
        
        query += " ORDER BY conference, conf_rank"
        
        return pd.read_sql(query, self.conn, params=params)
    
def plot_team_trends(self, team_id, stat='points', last_n_games=20):
        """Plot trend of a specific stat for a team over their last N games"""
        query = """
        SELECT 
            g.game_date,
            t.team_name,
            g.game_id,
            s.{stat}
        FROM fact_team_game_stats s
        JOIN dim_teams t ON s.team_id = t.team_id
        JOIN dim_games g ON s.game_id = g.game_id
        WHERE s.team_id = ?
        ORDER BY g.game_date DESC
        LIMIT ?
        """.format(stat=stat)
        
        data = pd.read_sql(query, self.conn, params=(team_id, last_n_games))
        
        if data.empty:
            print(f"No data found for team ID {team_id}")
            return
        
        # Reverse to show chronological order
        data = data.iloc[::-1].reset_index(drop=True)
        
        plt.figure(figsize=(12, 6))
        plt.plot(data.index, data[stat], marker='o', linestyle='-', linewidth=2)
        
        # Add a trend line
        z = np.polyfit(data.index, data[stat], 1)
        p = np.poly1d(z)
        plt.plot(data.index, p(data.index), "r--", linewidth=1)
        
        team_name = data['team_name'].iloc[0]
        plt.title(f'{team_name} {stat.title()} - Last {len(data)} Games')
        plt.xlabel('Game Number')
        plt.ylabel(stat.title())
        plt.grid(True, linestyle='--', alpha=0.7)
        
        # Add game IDs as x-tick labels for reference
        if len(data) <= 10:
            plt.xticks(data.index, [g[-4:] for g in data['game_id']], rotation=45)
        
        plt.tight_layout()
        return plt
    
def player_comparison(self, player_ids, season=None):
        """Compare statistics between multiple players"""
        if not isinstance(player_ids, list):
            player_ids = [player_ids]
        
        query = """
        SELECT 
            p.player_id,
            p.first_name || ' ' || p.last_name AS player_name,
            t.team_name,
            COUNT(s.game_id) AS games_played,
            ROUND(AVG(s.minutes_played), 1) AS mpg,
            ROUND(AVG(s.points), 1) AS ppg,
            ROUND(AVG(s.rebounds), 1) AS rpg,
            ROUND(AVG(s.assists), 1) AS apg,
            ROUND(AVG(s.steals), 1) AS spg,
            ROUND(AVG(s.blocks), 1) AS bpg,
            ROUND(AVG(s.turnovers), 1) AS topg,
            ROUND(AVG(s.fg_pct) * 100, 1) AS fg_pct,
            ROUND(AVG(s.fg3_pct) * 100, 1) AS fg3_pct,
            ROUND(AVG(s.ft_pct) * 100, 1) AS ft_pct,
            ROUND(AVG(s.plus_minus), 1) AS plus_minus
        FROM fact_player_game_stats s
        JOIN dim_players p ON s.player_id = p.player_id
        JOIN dim_teams t ON s.team_id = t.team_id
        JOIN dim_games g ON s.game_id = g.game_id
        WHERE p.player_id IN ({})
        """.format(','.join(['?' for _ in player_ids]))
        
        if season:
            query += f" AND g.season = '{season}'"
        
        query += """
        GROUP BY p.player_id, player_name, t.team_name
        ORDER BY ppg DESC
        """
        
        return pd.read_sql(query, self.conn, params=player_ids)
    
def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
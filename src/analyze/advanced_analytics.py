# src/analyze/advanced_analytics.py
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
import os
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from config.settings import DB_URL, DATABASE_PATH

class NBAAdvancedAnalytics:
    """Class for advanced NBA analytics"""
    
    def __init__(self, db_url=DB_URL):
        """Initialize the analytics engine"""
        # Make sure the database directory exists
        os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
        
        # Create engine for SQLite
        self.engine = create_engine(db_url)
        
        # Create a simple test query
        print(f"Connecting to database at {DATABASE_PATH}")
    
    def player_shot_distribution(self, player_id, season=None, last_n_games=None):
        """Analyze a player's shot distribution by zone"""
        query = """
        WITH player_games AS (
            SELECT
                pg.player_game_id,
                g.game_id,
                g.game_date,
                g.season,
                p.first_name || ' ' || p.last_name AS player_name,
                t.team_name
            FROM fact_player_game_stats pg
            JOIN dim_games g ON pg.game_id = g.game_id
            JOIN dim_players p ON pg.player_id = p.player_id
            JOIN dim_teams t ON pg.team_id = t.team_id
            WHERE pg.player_id = :player_id
            {season_filter}
            ORDER BY g.game_date DESC
            {limit_clause}
        )
        SELECT
            pg.player_name,
            pg.team_name,
            SUM(s.shots_attempted_0_3ft) AS attempts_0_3ft,
            ROUND(SUM(s.shots_made_0_3ft)::numeric / NULLIF(SUM(s.shots_attempted_0_3ft), 0) * 100, 1) AS pct_0_3ft,
            SUM(s.shots_attempted_3_10ft) AS attempts_3_10ft,
            ROUND(SUM(s.shots_made_3_10ft)::numeric / NULLIF(SUM(s.shots_attempted_3_10ft), 0) * 100, 1) AS pct_3_10ft,
            SUM(s.shots_attempted_10_16ft) AS attempts_10_16ft,
            ROUND(SUM(s.shots_made_10_16ft)::numeric / NULLIF(SUM(s.shots_attempted_10_16ft), 0) * 100, 1) AS pct_10_16ft,
            SUM(s.shots_attempted_16ft_3pt) AS attempts_16ft_3pt,
            ROUND(SUM(s.shots_made_16ft_3pt)::numeric / NULLIF(SUM(s.shots_attempted_16ft_3pt), 0) * 100, 1) AS pct_16ft_3pt,
            SUM(s.corner_3_attempted) AS attempts_corner_3,
            ROUND(SUM(s.corner_3_made)::numeric / NULLIF(SUM(s.corner_3_attempted), 0) * 100, 1) AS pct_corner_3,
            SUM(s.above_break_3_attempted) AS attempts_above_break_3,
            ROUND(SUM(s.above_break_3_made)::numeric / NULLIF(SUM(s.above_break_3_attempted), 0) * 100, 1) AS pct_above_break_3
        FROM player_games pg
        JOIN fact_player_shot_tracking s ON pg.player_game_id = s.player_game_id
        GROUP BY pg.player_name, pg.team_name
        """
        
        # Add filters
        season_filter = ""
        limit_clause = ""
        params = {"player_id": player_id}
        
        if season:
            season_filter = "AND g.season = :season"
            params["season"] = season
        
        if last_n_games:
            limit_clause = "LIMIT :limit"
            params["limit"] = last_n_games
        
        # Format the query
        query = query.format(
            season_filter=season_filter,
            limit_clause=limit_clause
        )
        
        # Execute query
        result = pd.read_sql(query, self.engine, params=params)
        
        if result.empty:
            print(f"No shot distribution data found for player ID {player_id}")
            return None
        
        # Create visual representation
        if not result.empty:
            labels = [
                '0-3 ft', '3-10 ft', '10-16 ft', '16ft-3pt', 
                'Corner 3', 'Above Break 3'
            ]
            
            attempts = [
                result['attempts_0_3ft'].iloc[0],
                result['attempts_3_10ft'].iloc[0],
                result['attempts_10_16ft'].iloc[0],
                result['attempts_16ft_3pt'].iloc[0],
                result['attempts_corner_3'].iloc[0],
                result['attempts_above_break_3'].iloc[0]
            ]
            
            percentages = [
                result['pct_0_3ft'].iloc[0],
                result['pct_3_10ft'].iloc[0],
                result['pct_10_16ft'].iloc[0],
                result['pct_16ft_3pt'].iloc[0],
                result['pct_corner_3'].iloc[0],
                result['pct_above_break_3'].iloc[0]
            ]
            
            player_name = result['player_name'].iloc[0]
            team_name = result['team_name'].iloc[0]
            
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
            
            # Shot attempts by zone
            bars = ax1.bar(labels, attempts, color='navy')
            ax1.set_title(f'{player_name} Shot Attempts by Zone')
            ax1.set_ylabel('Number of Attempts')
            ax1.set_ylim(0, max(attempts) * 1.15)
            
            # Add actual numbers on top of bars
            for bar in bars:
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                        f'{int(height)}',
                        ha='center', va='bottom')
            
            # Shot percentage by zone
            bars = ax2.bar(labels, percentages, color='darkgreen')
            ax2.set_title(f'{player_name} FG% by Zone')
            ax2.set_ylabel('Field Goal Percentage')
            ax2.set_ylim(0, 100)
            
            # Add percentages on top of bars
            for bar in bars:
                height = bar.get_height()
                if not np.isnan(height):
                    ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                            f'{height:.1f}%',
                            ha='center', va='bottom')
            
            plt.suptitle(f'{player_name} ({team_name}) Shot Distribution Analysis', fontsize=16)
            plt.tight_layout()
            
        return result

def player_efficiency_analysis(self, player_id, season=None, last_n_games=None):
    """Analyze a player's offensive and defensive efficiency metrics"""
    query = """
    WITH player_games AS (
        SELECT
            pg.player_game_id,
            g.game_id,
            g.game_date,
            g.season,
            p.first_name || ' ' || p.last_name AS player_name,
            t.team_name
        FROM fact_player_game_stats pg
        JOIN dim_games g ON pg.game_id = g.game_id
        JOIN dim_players p ON pg.player_id = p.player_id
        JOIN dim_teams t ON pg.team_id = t.team_id
        WHERE pg.player_id = :player_id
        {season_filter}
        ORDER BY g.game_date DESC
        {limit_clause}
    )
    SELECT
        pg.player_name,
        pg.team_name,
        ROUND(AVG(e.true_shooting_pct) * 100, 1) AS avg_ts_pct,
        ROUND(AVG(e.effective_fg_pct) * 100, 1) AS avg_efg_pct,
        ROUND(AVG(e.offensive_rating), 1) AS avg_off_rating,
        ROUND(AVG(e.defensive_rating), 1) AS avg_def_rating,
        ROUND(AVG(e.net_rating), 1) AS avg_net_rating,
        ROUND(AVG(e.player_impact_estimate), 1) AS avg_pie,
        ROUND(AVG(e.points_per_shot), 1) AS avg_pts_per_shot,
        ROUND(AVG(e.total_rebound_pct) * 100, 1) AS avg_reb_pct,
        ROUND(AVG(pm.assist_pct) * 100, 1) AS avg_ast_pct,
        ROUND(AVG(pm.usage_pct) * 100, 1) AS avg_usg_pct,
        ROUND(AVG(d.steal_pct) * 100, 1) AS avg_stl_pct,
        ROUND(AVG(d.block_pct) * 100, 1) AS avg_blk_pct,
        ROUND(SUM(e.offensive_win_shares), 2) AS total_ows,
        ROUND(SUM(d.defensive_win_shares), 2) AS total_dws,
        ROUND(AVG(s.minutes_played), 1) AS avg_minutes,
        ROUND(AVG(s.points), 1) AS avg_points,
        ROUND(AVG(s.rebounds), 1) AS avg_rebounds,
        ROUND(AVG(s.assists), 1) AS avg_assists,
        ROUND(AVG(s.steals), 1) AS avg_steals,
        ROUND(AVG(s.blocks), 1) AS avg_blocks,
        COUNT(pg.game_id) AS games_played
    FROM player_games pg
    JOIN fact_player_efficiency e ON pg.player_game_id = e.player_game_id
    JOIN fact_player_playmaking pm ON pg.player_game_id = pm.player_game_id
    JOIN fact_player_defensive d ON pg.player_game_id = d.player_game_id
    JOIN fact_player_game_stats s ON pg.player_game_id = s.player_game_id
    GROUP BY pg.player_name, pg.team_name
    """
    
    # Add filters
    season_filter = ""
    limit_clause = ""
    params = {"player_id": player_id}
    
    if season:
        season_filter = "AND g.season = :season"
        params["season"] = season
    
    if last_n_games:
        limit_clause = "LIMIT :limit"
        params["limit"] = last_n_games
    
    # Format the query
    query = query.format(
        season_filter=season_filter,
        limit_clause=limit_clause
    )
    
    # Execute query
    result = pd.read_sql(query, self.engine, params=params)
    
    if result.empty:
        print(f"No efficiency data found for player ID {player_id}")
        return None
    
    # Create visual representation
    if not result.empty:
        player_name = result['player_name'].iloc[0]
        team_name = result['team_name'].iloc[0]
        games_played = result['games_played'].iloc[0]
        
        # Create a radar chart for efficiency metrics
        categories = [
            'TS%', 'eFG%', 'USG%', 'AST%', 'REB%', 'STL%', 'BLK%', 'PIE'
        ]
        
        values = [
            result['avg_ts_pct'].iloc[0],
            result['avg_efg_pct'].iloc[0],
            result['avg_usg_pct'].iloc[0],
            result['avg_ast_pct'].iloc[0],
            result['avg_reb_pct'].iloc[0],
            result['avg_stl_pct'].iloc[0],
            result['avg_blk_pct'].iloc[0],
            result['avg_pie'].iloc[0] * 10  # Scale PIE to fit with percentages
        ]
        
        # Convert to numpy array and handle NaN values
        values = np.array(values)
        values = np.nan_to_num(values)
        
        # Number of categories
        N = len(categories)
        
        # Create angles for each category
        angles = [n / float(N) * 2 * np.pi for n in range(N)]
        angles += angles[:1]  # Close the loop
        
        # Values need to be repeated to close the loop
        values = np.append(values, values[0])
        
        # Create the plot
        fig = plt.figure(figsize=(10, 8))
        ax = fig.add_subplot(111, polar=True)
        
        # Draw the chart
        ax.plot(angles, values, linewidth=2, linestyle='solid', label=player_name)
        ax.fill(angles, values, alpha=0.1)
        
        # Set category labels
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(categories)
        
        # Set y-axis limits
        ax.set_ylim(0, max(100, max(values) * 1.1))
        
        # Add a title
        plt.title(f'{player_name} ({team_name}) Efficiency Profile\n(Based on {games_played} games)', 
                 size=15, y=1.1)
        
        # Add additional statistics as text
        txt = (f"Offensive Rating: {result['avg_off_rating'].iloc[0]:.1f}\n"
               f"Defensive Rating: {result['avg_def_rating'].iloc[0]:.1f}\n"
               f"Net Rating: {result['avg_net_rating'].iloc[0]:.1f}\n"
               f"Offensive Win Shares: {result['total_ows'].iloc[0]:.2f}\n"
               f"Defensive Win Shares: {result['total_dws'].iloc[0]:.2f}\n"
               f"Points Per Shot: {result['avg_pts_per_shot'].iloc[0]:.2f}")
        
        plt.figtext(0.95, 0.15, txt, horizontalalignment='right', 
                    bbox=dict(facecolor='white', alpha=0.5))
        
        plt.tight_layout()
        
    return result

def playmaking_analysis(self, player_id, season=None, last_n_games=None):
    """Analyze a player's playmaking and ball-handling metrics"""
    query = """
    WITH player_games AS (
        SELECT
            pg.player_game_id,
            g.game_id,
            g.game_date,
            g.season,
            p.first_name || ' ' || p.last_name AS player_name,
            t.team_name
        FROM fact_player_game_stats pg
        JOIN dim_games g ON pg.game_id = g.game_id
        JOIN dim_players p ON pg.player_id = p.player_id
        JOIN dim_teams t ON pg.team_id = t.team_id
        WHERE pg.player_id = :player_id
        {season_filter}
        ORDER BY g.game_date DESC
        {limit_clause}
    ),
    game_stats AS (
        SELECT
            pg.player_game_id,
            pg.game_date,
            s.points,
            s.assists,
            pm.assist_points_created,
            pm.potential_assists,
            pm.passes_made,
            pm.passes_received,
            pm.secondary_assists,
            pm.free_throw_assists,
            pm.time_of_possession,
            pm.avg_dribbles_per_touch,
            pm.avg_touch_time,
            pm.front_court_touches,
            pm.elbow_touches,
            pm.post_touches,
            pm.paint_touches
        FROM player_games pg
        JOIN fact_player_game_stats s ON pg.player_game_id = s.player_game_id
        JOIN fact_player_playmaking pm ON pg.player_game_id = pm.player_game_id
        ORDER BY pg.game_date
    )
    SELECT
        pg.player_name,
        pg.team_name,
        ROUND(AVG(s.assists), 1) AS avg_assists,
        ROUND(AVG(pm.assist_points_created), 1) AS avg_assist_points,
        ROUND(AVG(pm.potential_assists), 1) AS avg_potential_assists,
        ROUND(AVG(pm.secondary_assists), 1) AS avg_secondary_assists,
        ROUND(AVG(pm.free_throw_assists), 1) AS avg_ft_assists,
        ROUND(AVG(pm.passes_made), 1) AS avg_passes_made,
        ROUND(AVG(pm.passes_received), 1) AS avg_passes_received,
        ROUND(AVG(pm.assist_to_turnover_ratio), 1) AS avg_ast_to_ratio,
        ROUND(AVG(pm.time_of_possession), 1) AS avg_possession_time,
        ROUND(AVG(pm.avg_dribbles_per_touch), 1) AS avg_dribbles,
        ROUND(AVG(pm.avg_touch_time), 1) AS avg_touch_time,
        ROUND(AVG(pm.front_court_touches), 1) AS avg_frontcourt_touches,
        ROUND(AVG(pm.elbow_touches), 1) AS avg_elbow_touches,
        ROUND(AVG(pm.post_touches), 1) AS avg_post_touches,
        ROUND(AVG(pm.paint_touches), 1) AS avg_paint_touches,
        COUNT(pg.game_id) AS games_played
    FROM player_games pg
    JOIN fact_player_game_stats s ON pg.player_game_id = s.player_game_id
    JOIN fact_player_playmaking pm ON pg.player_game_id = pm.player_game_id
    GROUP BY pg.player_name, pg.team_name
    """
    
    # Add filters
    season_filter = ""
    limit_clause = ""
    params = {"player_id": player_id}
    
    if season:
        season_filter = "AND g.season = :season"
        params["season"] = season
    
    if last_n_games:
        limit_clause = "LIMIT :limit"
        params["limit"] = last_n_games
    
    # Format the query
    query = query.format(
        season_filter=season_filter,
        limit_clause=limit_clause
    )
    
    # Get game-by-game stats for trend analysis
    trend_query = """
    SELECT
        g.game_date,
        pg.assists,
        pm.potential_assists,
        pm.assist_points_created,
        pm.time_of_possession,
        pm.front_court_touches
    FROM fact_player_game_stats pg
    JOIN fact_player_playmaking pm ON pg.player_game_id = pm.player_game_id
    JOIN dim_games g ON pg.game_id = g.game_id
    WHERE pg.player_id = :player_id
    {season_filter}
    ORDER BY g.game_date DESC
    {limit_clause}
    """
    
    trend_query = trend_query.format(
        season_filter=season_filter,
        limit_clause=limit_clause
    )
    
    # Execute queries
    result = pd.read_sql(query, self.engine, params=params)
    trend_data = pd.read_sql(trend_query, self.engine, params=params)
    
    if result.empty:
        print(f"No playmaking data found for player ID {player_id}")
        return None
    
    # Create visual representation
    if not result.empty:
        player_name = result['player_name'].iloc[0]
        team_name = result['team_name'].iloc[0]
        games_played = result['games_played'].iloc[0]
        
        fig = plt.figure(figsize=(15, 12))
        
        # Plot 1: Touches breakdown
        ax1 = fig.add_subplot(221)
        touch_labels = ['Frontcourt', 'Elbow', 'Post', 'Paint']
        touch_values = [
            result['avg_frontcourt_touches'].iloc[0],
            result['avg_elbow_touches'].iloc[0],
            result['avg_post_touches'].iloc[0],
            result['avg_paint_touches'].iloc[0]
        ]
        
        # Handle NaN values
        touch_values = np.nan_to_num(touch_values)
        
        bars = ax1.bar(touch_labels, touch_values, color='cornflowerblue')
        ax1.set_title('Average Touches by Location')
        ax1.set_ylabel('Touches per Game')
        
        # Add values on top of bars
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{height:.1f}',
                    ha='center', va='bottom')
        
        # Plot 2: Assists breakdown
        ax2 = fig.add_subplot(222)
        assist_labels = ['Assists', 'Potential\nAssists', 'Secondary\nAssists', 'FT Assists']
        assist_values = [
            result['avg_assists'].iloc[0],
            result['avg_potential_assists'].iloc[0],
            result['avg_secondary_assists'].iloc[0],
            result['avg_ft_assists'].iloc[0]
        ]
        
        # Handle NaN values
        assist_values = np.nan_to_num(assist_values)
        
        bars = ax2.bar(assist_labels, assist_values, color='forestgreen')
        ax2.set_title('Assists Breakdown')
        ax2.set_ylabel('Per Game')
        
        # Add values on top of bars
        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{height:.1f}',
                    ha='center', va='bottom')
        
        # Plot 3: Time of possession and touch time trends
        ax3 = fig.add_subplot(223)
        
        if not trend_data.empty:
            # Reverse data to show chronological order
            trend_data = trend_data.iloc[::-1].reset_index(drop=True)
            
            # Create x-axis labels (game dates)
            dates = trend_data['game_date']
            
            # Plot time of possession trend
            ax3.plot(range(len(dates)), trend_data['time_of_possession'], 
                    marker='o', linestyle='-', color='purple', label='Time of Possession (min)')
            
            # Set x-axis labels
            if len(dates) > 10:
                # If we have many games, show fewer dates
                step = max(1, len(dates) // 10)
                ax3.set_xticks(range(0, len(dates), step))
                ax3.set_xticklabels([d.strftime('%m/%d') for d in dates[::step]], rotation=45)
            else:
                ax3.set_xticks(range(len(dates)))
                ax3.set_xticklabels([d.strftime('%m/%d') for d in dates], rotation=45)
            
            ax3.set_title('Time of Possession Trend')
            ax3.set_ylabel('Minutes')
            ax3.grid(True, alpha=0.3)
            
        # Plot 4: Pass ratio and touches breakdown
        ax4 = fig.add_subplot(224)
        
        # Create a pie chart of pass ratio
        labels = ['Passes Made', 'Passes Received']
        sizes = [
            result['avg_passes_made'].iloc[0],
            result['avg_passes_received'].iloc[0]
        ]
        
        # Make sure we have valid data
        if sum(sizes) > 0:
            ax4.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, 
                   colors=['lightblue', 'lightgreen'])
            ax4.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
            ax4.set_title('Pass Distribution')
        
        # Add overall title and subtitle with key stats
        plt.suptitle(f'{player_name} ({team_name}) Playmaking Analysis', fontsize=16)
        plt.figtext(0.5, 0.92, 
                   f'Based on {games_played} games | '
                   f'Assists: {result["avg_assists"].iloc[0]:.1f} | '
                   f'Assist Points Created: {result["avg_assist_points"].iloc[0]:.1f} | '
                   f'AST/TO Ratio: {result["avg_ast_to_ratio"].iloc[0]:.1f}',
                   ha='center', fontsize=10)
        
        plt.tight_layout(rect=[0, 0, 1, 0.9])
        
    return result

def defensive_analysis(self, player_id, season=None, last_n_games=None):
    """Analyze a player's defensive metrics and impact"""
    query = """
    WITH player_games AS (
        SELECT
            pg.player_game_id,
            g.game_id,
            g.game_date,
            g.season,
            p.first_name || ' ' || p.last_name AS player_name,
            t.team_name
        FROM fact_player_game_stats pg
        JOIN dim_games g ON pg.game_id = g.game_id
        JOIN dim_players p ON pg.player_id = p.player_id
        JOIN dim_teams t ON pg.team_id = t.team_id
        WHERE pg.player_id = :player_id
        {season_filter}
        ORDER BY g.game_date DESC
        {limit_clause}
    )
    SELECT
        pg.player_name,
        pg.team_name,
        ROUND(AVG(s.steals), 1) AS avg_steals,
        ROUND(AVG(s.blocks), 1) AS avg_blocks,
        ROUND(AVG(s.rebounds), 1) AS avg_rebounds,
        ROUND(AVG(d.defensive_rebounds), 1) AS avg_def_rebounds,
        ROUND(AVG(d.contested_shots), 1) AS avg_contested_shots,
        ROUND(AVG(d.contested_shots_made), 1) AS avg_contested_shots_made,
        ROUND(AVG(d.contested_shots_pct) * 100, 1) AS avg_contested_shots_pct,
        ROUND(AVG(d.deflections), 1) AS avg_deflections,
        ROUND(AVG(d.charges_drawn), 1) AS avg_charges_drawn,
        ROUND(AVG(d.box_outs), 1) AS avg_box_outs,
        ROUND(AVG(d.loose_balls_recovered), 1) AS avg_loose_balls,
        ROUND(AVG(d.defensive_rating), 1) AS avg_def_rating,
        ROUND(SUM(d.defensive_win_shares), 2) AS total_dws,
        ROUND(AVG(d.steals_per_foul), 1) AS avg_stl_per_foul,
        ROUND(AVG(d.blocks_per_foul), 1) AS avg_blk_per_foul,
        ROUND(AVG(d.block_pct) * 100, 1) AS avg_blk_pct,
        ROUND(AVG(d.steal_pct) * 100, 1) AS avg_stl_pct,
        ROUND(AVG(d.defensive_box_plus_minus), 1) AS avg_dbpm,
        ROUND(AVG(h.distance_miles_defense), 1) AS avg_def_miles,
        ROUND(AVG(h.avg_speed_mph_defense), 1) AS avg_def_speed,
        COUNT(pg.game_id) AS games_played
    FROM player_games pg
    JOIN fact_player_game_stats s ON pg.player_game_id = s.player_game_id
    JOIN fact_player_defensive d ON pg.player_game_id = d.player_game_id
    JOIN fact_player_hustle h ON pg.player_game_id = h.player_game_id
    GROUP BY pg.player_name, pg.team_name
    """
    
    # Add filters
    season_filter = ""
    limit_clause = ""
    params = {"player_id": player_id}
    
    if season:
        season_filter = "AND g.season = :season"
        params["season"] = season
    
    if last_n_games:
        limit_clause = "LIMIT :limit"
        params["limit"] = last_n_games
    
    # Format the query
    query = query.format(
        season_filter=season_filter,
        limit_clause=limit_clause
    )
    
    # Execute query
    result = pd.read_sql(query, self.engine, params=params)
    
    if result.empty:
        print(f"No defensive data found for player ID {player_id}")
        return None
    
    # Create visual representation
    if not result.empty:
        player_name = result['player_name'].iloc[0]
        team_name = result['team_name'].iloc[0]
        games_played = result['games_played'].iloc[0]
        
        fig = plt.figure(figsize=(15, 10))
        
        # Plot 1: Defensive stats overview
        ax1 = fig.add_subplot(221)
        stats_labels = ['Steals', 'Blocks', 'Deflections', 'Charges\nDrawn', 'Loose Balls\nRecovered']
        stats_values = [
            result['avg_steals'].iloc[0],
            result['avg_blocks'].iloc[0],
            result['avg_deflections'].iloc[0],
            result['avg_charges_drawn'].iloc[0],
            result['avg_loose_balls'].iloc[0]
        ]
        
        # Handle NaN values
        stats_values = np.nan_to_num(stats_values)
        
        bars = ax1.bar(stats_labels, stats_values, color='firebrick')
        ax1.set_title('Defensive Activity Metrics')
        ax1.set_ylabel('Per Game')
        
        # Add values on top of bars
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.05,
                    f'{height:.1f}',
                    ha='center', va='bottom', fontsize=9)
        
        # Plot 2: Contested shots
        ax2 = fig.add_subplot(222)
        
        # Ensure we have valid data for contested shots
        if result['avg_contested_shots'].iloc[0] > 0:
            # Create a pie chart of contested shots
            labels = ['Made', 'Missed']
            sizes = [
                result['avg_contested_shots_made'].iloc[0],
                result['avg_contested_shots'].iloc[0] - result['avg_contested_shots_made'].iloc[0]
            ]
            
            ax2.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, 
                   colors=['tomato', 'lightgreen'], 
                   explode=(0.1, 0))
            ax2.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
            ax2.set_title(f'Contested Shots Outcome\n{result["avg_contested_shots"].iloc[0]:.1f} per game')

        # Plot 3: Defensive rating vs. league average
        ax3 = fig.add_subplot(223)
        
        # We'll compare the player's defensive rating to the league average (around 110)
        league_avg_def_rating = 110  # This is approximate, would need to be calculated from data
        player_def_rating = result['avg_def_rating'].iloc[0]
        
        # Create a horizontal bar chart
        labels = ['Player', 'League Avg']
        values = [player_def_rating, league_avg_def_rating]
        colors = ['green' if player_def_rating < league_avg_def_rating else 'red', 'gray']
        
        bars = ax3.barh(labels, values, color=colors)
        ax3.set_title('Defensive Rating Comparison')
        ax3.set_xlabel('Defensive Rating (Lower is Better)')
        
        # Add values at the end of bars
        for bar in bars:
            width = bar.get_width()
            ax3.text(width + 0.5, bar.get_y() + bar.get_height()/2,
                    f'{width:.1f}',
                    ha='left', va='center')
        
        # Set x-axis limit
        ax3.set_xlim(0, max(values) * 1.1)
        
        # Plot 4: Advanced defensive metrics
        ax4 = fig.add_subplot(224)
        adv_labels = ['D-BPM', 'DWS', 'STL%', 'BLK%']
        adv_values = [
            result['avg_dbpm'].iloc[0],
            result['total_dws'].iloc[0],
            result['avg_stl_pct'].iloc[0] / 5,  # Scale down percentages to fit with other metrics
            result['avg_blk_pct'].iloc[0] / 5   # Scale down percentages to fit with other metrics
        ]
        
        # Handle NaN values
        adv_values = np.nan_to_num(adv_values)
        
        bars = ax4.bar(adv_labels, adv_values, color='navy')
        ax4.set_title('Advanced Defensive Metrics')
        
        # Add values on top of bars
        for i, bar in enumerate(bars):
            height = bar.get_height()
            if i in [2, 3]:  # Percentages
                label = f'{adv_values[i] * 5:.1f}%'
            else:
                label = f'{adv_values[i]:.1f}'
            ax4.text(bar.get_x() + bar.get_width()/2., height + 0.05,
                    label, ha='center', va='bottom', fontsize=9)
        
        # Add overall title and subtitle with key stats
        plt.suptitle(f'{player_name} ({team_name}) Defensive Analysis', fontsize=16)
        plt.figtext(0.5, 0.92, 
                   f'Based on {games_played} games | '
                   f'STL: {result["avg_steals"].iloc[0]:.1f} | '
                   f'BLK: {result["avg_blocks"].iloc[0]:.1f} | '
                   f'DEF RTG: {result["avg_def_rating"].iloc[0]:.1f} | '
                   f'DEF REB: {result["avg_def_rebounds"].iloc[0]:.1f}',
                   ha='center', fontsize=10)
        
        plt.tight_layout(rect=[0, 0, 1, 0.9])
        
    return result

def hustle_analysis(self, player_id, season=None, last_n_games=None):
    """Analyze a player's hustle stats and activity metrics"""
    query = """
    WITH player_games AS (
        SELECT
            pg.player_game_id,
            g.game_id,
            g.game_date,
            g.season,
            p.first_name || ' ' || p.last_name AS player_name,
            t.team_name
        FROM fact_player_game_stats pg
        JOIN dim_games g ON pg.game_id = g.game_id
        JOIN dim_players p ON pg.player_id = p.player_id
        JOIN dim_teams t ON pg.team_id = t.team_id
        WHERE pg.player_id = :player_id
        {season_filter}
        ORDER BY g.game_date DESC
        {limit_clause}
    )
    SELECT
        pg.player_name,
        pg.team_name,
        ROUND(AVG(h.contested_shots_2pt), 1) AS avg_contested_2pt,
        ROUND(AVG(h.contested_shots_3pt), 1) AS avg_contested_3pt,
        ROUND(AVG(h.deflections), 1) AS avg_deflections,
        ROUND(AVG(h.loose_balls_recovered), 1) AS avg_loose_balls,
        ROUND(AVG(h.charges_drawn), 1) AS avg_charges_drawn,
        ROUND(AVG(h.screen_assists), 1) AS avg_screen_assists,
        ROUND(AVG(h.screen_assist_points), 1) AS avg_screen_assist_pts,
        ROUND(AVG(h.box_outs), 1) AS avg_box_outs,
        ROUND(AVG(h.box_outs_offensive), 1) AS avg_off_box_outs,
        ROUND(AVG(h.box_outs_defensive), 1) AS avg_def_box_outs,
        ROUND(AVG(h.distance_miles), 1) AS avg_distance,
        ROUND(AVG(h.distance_miles_offense), 1) AS avg_off_distance,
        ROUND(AVG(h.distance_miles_defense), 1) AS avg_def_distance,
        ROUND(AVG(h.avg_speed_mph), 1) AS avg_speed,
        ROUND(AVG(h.avg_speed_mph_offense), 1) AS avg_off_speed,
        ROUND(AVG(h.avg_speed_mph_defense), 1) AS avg_def_speed,
        ROUND(AVG(s.minutes_played), 1) AS avg_minutes,
        COUNT(pg.game_id) AS games_played
    FROM player_games pg
    JOIN fact_player_game_stats s ON pg.player_game_id = s.player_game_id
    JOIN fact_player_hustle h ON pg.player_game_id = h.player_game_id
    GROUP BY pg.player_name, pg.team_name
    """
    
    # Add filters
    season_filter = ""
    limit_clause = ""
    params = {"player_id": player_id}
    
    if season:
        season_filter = "AND g.season = :season"
        params["season"] = season
    
    if last_n_games:
        limit_clause = "LIMIT :limit"
        params["limit"] = last_n_games
    
    # Format the query
    query = query.format(
        season_filter=season_filter,
        limit_clause=limit_clause
    )
    
    # Execute query
    result = pd.read_sql(query, self.engine, params=params)
    
    if result.empty:
        print(f"No hustle data found for player ID {player_id}")
        return None
    
    # Create visual representation
    if not result.empty:
        player_name = result['player_name'].iloc[0]
        team_name = result['team_name'].iloc[0]
        games_played = result['games_played'].iloc[0]
        
        fig = plt.figure(figsize=(15, 10))
        
        # Plot 1: Contested shots breakdown
        ax1 = fig.add_subplot(221)
        contest_labels = ['2PT Contested', '3PT Contested']
        contest_values = [
            result['avg_contested_2pt'].iloc[0],
            result['avg_contested_3pt'].iloc[0]
        ]
        
        # Handle NaN values
        contest_values = np.nan_to_num(contest_values)
        
        bars = ax1.bar(contest_labels, contest_values, color=['indianred', 'steelblue'])
        ax1.set_title('Contested Shots per Game')
        ax1.set_ylabel('Shots Contested')
        
        # Add values on top of bars
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{height:.1f}',
                    ha='center', va='bottom')
        
        # Plot 2: Movement stats
        ax2 = fig.add_subplot(222)
        
        # Miles run per game
        dist_labels = ['Total', 'Offense', 'Defense']
        dist_values = [
            result['avg_distance'].iloc[0],
            result['avg_off_distance'].iloc[0],
            result['avg_def_distance'].iloc[0]
        ]
        
        # Handle NaN values
        dist_values = np.nan_to_num(dist_values)
        
        # Plot bars
        bars = ax2.bar(dist_labels, dist_values, color=['purple', 'orangered', 'teal'])
        ax2.set_title('Miles Covered per Game')
        ax2.set_ylabel('Miles')
        
        # Add values on top of bars
        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.05,
                    f'{height:.1f}',
                    ha='center', va='bottom')
        
        # Add miles per 36 minutes as text
        minutes = result['avg_minutes'].iloc[0]
        if minutes > 0:
            miles_per_36 = result['avg_distance'].iloc[0] * (36 / minutes)
            ax2.text(0.5, -0.15, f'Miles per 36 minutes: {miles_per_36:.1f}',
                    ha='center', transform=ax2.transAxes)
        
        # Plot 3: Box outs breakdown
        ax3 = fig.add_subplot(223)
        boxout_labels = ['Total\nBox Outs', 'Offensive\nBox Outs', 'Defensive\nBox Outs']
        boxout_values = [
            result['avg_box_outs'].iloc[0],
            result['avg_off_box_outs'].iloc[0],
            result['avg_def_box_outs'].iloc[0]
        ]
        
        # Handle NaN values
        boxout_values = np.nan_to_num(boxout_values)
        
        bars = ax3.bar(boxout_labels, boxout_values, color=['darkblue', 'crimson', 'darkgreen'])
        ax3.set_title('Box Outs per Game')
        ax3.set_ylabel('Box Outs')
        
        # Add values on top of bars
        for bar in bars:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height + 0.05,
                    f'{height:.1f}',
                    ha='center', va='bottom')
        
        # Plot 4: Other hustle stats
        ax4 = fig.add_subplot(224)
        other_labels = ['Deflections', 'Loose Balls\nRecovered', 'Charges\nDrawn', 'Screen\nAssists']
        other_values = [
            result['avg_deflections'].iloc[0],
            result['avg_loose_balls'].iloc[0],
            result['avg_charges_drawn'].iloc[0],
            result['avg_screen_assists'].iloc[0]
        ]
        
        # Handle NaN values
        other_values = np.nan_to_num(other_values)
        
        bars = ax4.bar(other_labels, other_values, color=['darkorange', 'mediumseagreen', 'mediumslateblue', 'darkkhaki'])
        ax4.set_title('Other Hustle Stats per Game')
        
        # Add values on top of bars
        for bar in bars:
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height + 0.05,
                    f'{height:.1f}',
                    ha='center', va='bottom')
        
        # Add overall title and subtitle with key stats
        plt.suptitle(f'{player_name} ({team_name}) Hustle Analysis', fontsize=16)
        plt.figtext(0.5, 0.92, 
                   f'Based on {games_played} games | '
                   f'Speed: {result["avg_speed"].iloc[0]:.1f} mph | '
                   f'Distance: {result["avg_distance"].iloc[0]:.1f} miles/game | '
                   f'Minutes: {result["avg_minutes"].iloc[0]:.1f} mpg',
                   ha='center', fontsize=10)
        
        plt.tight_layout(rect=[0, 0, 1, 0.9])
        
    return result

def team_comparison(self, team_ids, season=None):
    """Compare advanced metrics between multiple teams"""
    if not isinstance(team_ids, list):
        team_ids = [team_ids]
    
    # Convert team_ids to string format for SQL
    team_ids_str = "','".join([str(tid) for tid in team_ids])
    
    query = """
    WITH team_games AS (
        SELECT
            t.team_id,
            t.team_name,
            t.conference,
            t.division,
            g.game_id,
            g.game_date,
            g.season,
            CASE 
                WHEN g.home_team_id = t.team_id THEN gs.home_team_score
                ELSE gs.away_team_score
            END AS team_score,
            CASE 
                WHEN g.home_team_id = t.team_id THEN gs.away_team_score
                ELSE gs.home_team_score
            END AS opponent_score,
            CASE 
                WHEN (g.home_team_id = t.team_id AND gs.home_team_score > gs.away_team_score) OR
                     (g.away_team_id = t.team_id AND gs.away_team_score > gs.home_team_score)
                THEN 1 ELSE 0
            END AS is_win
        FROM dim_teams t
        JOIN dim_games g ON t.team_id = g.home_team_id OR t.team_id = g.away_team_id
        JOIN fact_game_stats gs ON g.game_id = gs.game_id
        WHERE t.team_id IN ('{team_ids}')
        {season_filter}
    )
    SELECT
        tg.team_id,
        tg.team_name,
        tg.conference,
        tg.division,
        COUNT(tg.game_id) AS games_played,
        SUM(tg.is_win) AS wins,
        COUNT(tg.game_id) - SUM(tg.is_win) AS losses,
        ROUND(SUM(tg.is_win)::numeric / COUNT(tg.game_id) * 100, 1) AS win_pct,
        ROUND(AVG(tg.team_score), 1) AS avg_points,
        ROUND(AVG(tg.opponent_score), 1) AS avg_points_allowed,
        ROUND(AVG(tg.team_score) - AVG(tg.opponent_score), 1) AS point_diff,
        ROUND(AVG(ts.points), 1) AS avg_team_points,
        ROUND(AVG(ts.assists), 1) AS avg_team_assists,
        ROUND(AVG(ts.rebounds), 1) AS avg_team_rebounds,
        ROUND(AVG(ts.offensive_rebounds), 1) AS avg_team_offensive_rebounds,
        ROUND(AVG(ts.defensive_rebounds), 1) AS avg_team_defensive_rebounds,
        ROUND(AVG(ts.steals), 1) AS avg_team_steals,
        ROUND(AVG(ts.blocks), 1) AS avg_team_blocks,
        ROUND(AVG(ts.turnovers), 1) AS avg_team_turnovers,
        ROUND(AVG(ts.personal_fouls), 1) AS avg_team_fouls,
        ROUND(AVG(ts.fg_pct) * 100, 1) AS avg_team_fg_pct,
        ROUND(AVG(ts.fg3_pct) * 100, 1) AS avg_team_fg3_pct,
        ROUND(AVG(ts.ft_pct) * 100, 1) AS avg_team_ft_pct,
        ROUND(AVG(ts.fast_break_points), 1) AS avg_fast_break_points,
        ROUND(AVG(ts.points_in_paint), 1) AS avg_points_in_paint,
        ROUND(AVG(ts.points_off_turnovers), 1) AS avg_points_off_turnovers,
        ROUND(AVG(ts.second_chance_points), 1) AS avg_second_chance_points
    FROM team_games tg
    JOIN fact_team_game_stats ts ON tg.game_id = ts.game_id AND tg.team_id = ts.team_id
    GROUP BY tg.team_id, tg.team_name, tg.conference, tg.division
    ORDER BY win_pct DESC
    """.format(team_ids=team_ids_str)
    
    # Add season filter if provided
    season_filter = ""
    if season:
        season_filter = f"AND g.season = '{season}'"
    
    # Format the query
    query = query.format(season_filter=season_filter)
    
    # Execute query
    result = pd.read_sql(query, self.engine)
    
    if result.empty:
        print("No team data found for the specified teams")
        return None
    
    # Create visual representation
    if not result.empty:
        # Number of teams
        num_teams = len(result)
        
        # Create a figure with multiple comparisons
        fig = plt.figure(figsize=(15, 12))
        
        # Plot 1: Win-Loss record
        ax1 = fig.add_subplot(231)
        
        # Create stacked bars for wins and losses
        team_names = result['team_name']
        wins = result['wins']
        losses = result['losses']
        
        # Create bars
        ax1.bar(team_names, wins, label='Wins', color='forestgreen')
        ax1.bar(team_names, losses, bottom=wins, label='Losses', color='firebrick')
        
        # Add win percentages as text
        for i, (w, l) in enumerate(zip(wins, losses)):
            wp = w / (w + l) * 100
            ax1.text(i, w + l + 1, f"{wp:.1f}%", ha='center')
        
        ax1.set_title('Win-Loss Record')
        ax1.set_ylabel('Games')
        ax1.legend()
        
        # Rotate x-axis labels if we have many teams
        if num_teams > 2:
            plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')
        
        # Plot 2: Offensive comparison
        ax2 = fig.add_subplot(232)
        
        # Bar chart for points per game
        bars = ax2.bar(team_names, result['avg_points'], color='orangered')
        
        # Add values on top of bars
        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{height:.1f}',
                    ha='center', va='bottom')
        
        ax2.set_title('Points Per Game')
        ax2.set_ylabel('Points')
        
        # Rotate x-axis labels if we have many teams
        if num_teams > 2:
            plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')
        
        # Plot 3: Defensive comparison
        ax3 = fig.add_subplot(233)
        
        # Bar chart for points allowed
        bars = ax3.bar(team_names, result['avg_points_allowed'], color='steelblue')
        
        # Add values on top of bars
        for bar in bars:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{height:.1f}',
                    ha='center', va='bottom')
        
        ax3.set_title('Points Allowed Per Game')
        ax3.set_ylabel('Points')
        
        # Rotate x-axis labels if we have many teams
        if num_teams > 2:
            plt.setp(ax3.get_xticklabels(), rotation=45, ha='right')
        
        # Plot 4: Shooting percentages
        ax4 = fig.add_subplot(234)
        
        # Set up grouped bar chart
        x = np.arange(len(team_names))
        width = 0.25
        
        # Create grouped bars for different shooting percentages
        ax4.bar(x - width, result['avg_team_fg_pct'], width, label='FG%', color='purple')
        ax4.bar(x, result['avg_team_fg3_pct'], width, label='3PT%', color='darkorange')
        ax4.bar(x + width, result['avg_team_ft_pct'], width, label='FT%', color='green')
        
        ax4.set_title('Shooting Percentages')
        ax4.set_ylabel('Percentage')
        ax4.set_xticks(x)
        ax4.set_xticklabels(team_names)
        ax4.legend()
        
        # Rotate x-axis labels if we have many teams
        if num_teams > 2:
            plt.setp(ax4.get_xticklabels(), rotation=45, ha='right')
        
        # Plot 5: Assists and Turnovers
        ax5 = fig.add_subplot(235)
        
        # Set up grouped bar chart
        x = np.arange(len(team_names))
        width = 0.35
        
        # Create grouped bars for assists and turnovers
        ax5.bar(x - width/2, result['avg_team_assists'], width, label='Assists', color='mediumseagreen')
        ax5.bar(x + width/2, result['avg_team_turnovers'], width, label='Turnovers', color='salmon')
        
        ax5.set_title('Assists vs Turnovers')
        ax5.set_ylabel('Per Game')
        ax5.set_xticks(x)
        ax5.set_xticklabels(team_names)
        ax5.legend()
        
        # Rotate x-axis labels if we have many teams
        if num_teams > 2:
            plt.setp(ax5.get_xticklabels(), rotation=45, ha='right')
        
        # Plot 6: Additional scoring breakdown
        ax6 = fig.add_subplot(236)
        
        # Set up grouped bar chart for different scoring types
        x = np.arange(len(team_names))
        width = 0.2
        
        # Create grouped bars for different types of points
        ax6.bar(x - 1.5*width, result['avg_fast_break_points'], width, label='Fast Break', color='dodgerblue')
        ax6.bar(x - 0.5*width, result['avg_points_in_paint'], width, label='Paint', color='indigo')
        ax6.bar(x + 0.5*width, result['avg_points_off_turnovers'], width, label='Off TOs', color='crimson')
        ax6.bar(x + 1.5*width, result['avg_second_chance_points'], width, label='2nd Chance', color='gold')
        
        ax6.set_title('Scoring Breakdown')
        ax6.set_ylabel('Points Per Game')
        ax6.set_xticks(x)
        ax6.set_xticklabels(team_names)
        ax6.legend(fontsize='small')
        
        # Rotate x-axis labels if we have many teams
        if num_teams > 2:
            plt.setp(ax6.get_xticklabels(), rotation=45, ha='right')
        
        # Add overall title
        teams_str = ', '.join(result['team_name'])
        plt.suptitle(f'Team Comparison: {teams_str}', fontsize=16)
        
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        
    return result

def close(self):
    """Close the database connection"""
    pass  # SQLAlchemy engine doesn't require explicit closing

if __name__ == "__main__":
    # Example usage
    analytics = NBAAdvancedAnalytics()
    
    # Example: LeBron James player ID
    player_id = "2544"
    
    # Run different analyses
    shot_dist = analytics.player_shot_distribution(player_id, season="2022-23")
    efficiency = analytics.player_efficiency_analysis(player_id, season="2022-23")
    playmaking = analytics.playmaking_analysis(player_id, season="2022-23")
    defense = analytics.defensive_analysis(player_id, season="2022-23")
    hustle = analytics.hustle_analysis(player_id, season="2022-23")
    
    # Team comparison example
    # Lakers, Celtics team IDs
    team_ids = ["1610612747", "1610612738"]
    team_comp = analytics.team_comparison(team_ids, season="2022-23")
    
    plt.show()
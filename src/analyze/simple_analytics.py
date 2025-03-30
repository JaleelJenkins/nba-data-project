import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from config.settings import DATABASE_PATH

class NBASimpleAnalytics:
    """Simple NBA analytics"""
    
    def __init__(self):
        """Initialize the analytics"""
        self.conn = sqlite3.connect(DATABASE_PATH)
    
    def get_team_stats(self):
        """Get teams from the database"""
        query = "SELECT * FROM dim_teams"
        teams = pd.read_sql(query, self.conn)
        return teams
    
    def get_player_stats(self):
        """Get players from the database"""
        query = "SELECT * FROM dim_players"
        players = pd.read_sql(query, self.conn)
        return players
    
    def team_analysis(self):
        """Analyze teams by conference and division"""
        teams = self.get_team_stats()
        
        if teams.empty:
            print("No team data found in the database.")
            return
        
        # Count teams by conference
        conference_counts = teams['conference'].value_counts()
        
        # Create a bar chart
        plt.figure(figsize=(10, 6))
        conference_counts.plot(kind='bar', color=['blue', 'red'])
        plt.title('NBA Teams by Conference')
        plt.xlabel('Conference')
        plt.ylabel('Number of Teams')
        plt.tight_layout()
        
        # Save and show
        plt.savefig('teams_by_conference.png')
        plt.show()
        
        return conference_counts
    
    def player_analysis(self):
        """Analyze players"""
        players = self.get_player_stats()
        
        if players.empty:
            print("No player data found in the database.")
            return
        
        # Count first letter of last name
        players['first_letter'] = players['last_name'].str[0]
        letter_counts = players['first_letter'].value_counts().sort_index()
        
        # Create a bar chart
        plt.figure(figsize=(12, 6))
        letter_counts.plot(kind='bar', color='green')
        plt.title('NBA Players by First Letter of Last Name')
        plt.xlabel('First Letter')
        plt.ylabel('Number of Players')
        plt.tight_layout()
        
        # Save and show
        plt.savefig('players_by_letter.png')
        plt.show()
        
        return letter_counts
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()

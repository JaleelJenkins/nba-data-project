# src/test_extraction.py
from extract.advanced_stats_extractor import NBAAdvancedStatsExtractor

# Example: Extract advanced stats for LeBron James in a specific game
game_id = "0022200063"  # Replace with a real game ID
player_id = "2544"      # LeBron James
team_id = "1610612747"  # Lakers

# Initialize extractor
extractor = NBAAdvancedStatsExtractor()

# Extract and save data
stats = extractor.extract_player_advanced_stats(game_id, player_id, team_id)
print(f"Extracted advanced stats: {stats.keys()}")

# Close connections
extractor.close()
# src/test_analytics.py
import matplotlib.pyplot as plt
from analyze.advanced_analytics import NBAAdvancedAnalytics

# Initialize analytics
analytics = NBAAdvancedAnalytics()

# Example: LeBron James player ID
player_id = "2544"

# Test player shot distribution analysis
print("Running shot distribution analysis...")
shot_dist = analytics.player_shot_distribution(player_id, season="2022-23")

# Test efficiency analysis
print("Running efficiency analysis...")
efficiency = analytics.player_efficiency_analysis(player_id, season="2022-23")

# Display all plots
plt.show()
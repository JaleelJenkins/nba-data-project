# NBA Data Engineering Project

This project extracts, transforms, and loads NBA data using the [nba_api](https://github.com/swar/nba_api) Python package.

## Setup

1. Clone this repository
2. Create a virtual environment:
    - python -m venv nba_env
    - nba_env\Scripts\activate

nba-data-project

Project notes - 3/22/2025 Start of Project
##### Step 1: Choose a REST API data source
**Sports Data**: NBA API, NHL Stats API, ESPN API

- NBA_API (GitHub Project)
	The nba_api package on GitHub is a free, open-source Python client that allows you to access NBA.com APIs. It includes functionality to interact with live game data through endpoints like the ScoreBoard.

[GitHub](https://github.com/swar/nba_api)

This is likely your best free option for live NBA data.

Example of how to use it:
	`from nba_api.live.nba.endpoints import scoreboard`
	
	`# Get today's scoreboard`
	`games = scoreboard.ScoreBoard()` 
	
	`# Access data in different formats` 
	`json_data = games.get_json()`
	`dict_data = games.get_dict()`

##### Step 2: Set up your project environment
Once you've selected an API, let's set up your development environment:

	`# Create a virtual environment`
	`python -m venv data_project_env`
	
	`# Activate it (Windows)`
	`data_project_env\Scripts\activate`
	
	`# Activate it (Mac/Linux)`
	`source data_project_env/bin/activate`
	
	`# Install necessary packages` 
	`pip install requests pandas sqlalchemy python-dotenv jupyter`

 **Step 2.1: Create a Project Directory** 
	 First, let's create a dedicated directory for our project:

	`# Create a project directory`
		`mkdir nba_data_project`
		`cd nba_data_project`

	Gives us a cleas space to organize all our project files.

 **Step 2.2: Set up a Virtual Environment**
	 Virtual environments are crucial for Python projects because they: {Keep dependencies isolated from other projects, make your project portable and reproducible, help avoid version conflicts}.

	`# Create a virtual environment`
	`python -m venv nba_env`

	`# Activate the vitual environment`
	`# On macOS/Linux:`
	`source nba_env/bin/activate` 

**Step 2.3: Install Required Packages** 
	Now let's install the packages we'll need 

	`# Install the nba_api package and other useful libraries` 
	`pip install nba_api pandas matplotlib jupyter requests sqlalchemy python-dotenv`

Here's why we're installing each:
- ***nba_api:*** The core library to access NBA data 
- ***pandas:*** For data manipulation and analysis
- ***matplotlib:*** For creating visualizations 
- ***jupyter:*** For interactive development
- ***requests:*** For making HTTP requests
- ***sqlalchemy:*** For database interactions
- ***python-dotenv:*** For managing environment variables

 **Step 2.4: Create a Project Structure** 
	 Let's create a well-organized project structure:

	# Create directory structure 
	mkdir -p src/extract src/transform src/load src/analyze config data/raw data/processed notebooks 

This structure follows ETL (Extract, Transform, Load) principles:
	- src/extract: Code for getting data from the API
	- src/transform: Code for cleaning and processing data
	- src/load: Code for storing data
	- src/analyze: Code for analyzing and visualizing data
	- config: Configuration files
	- data/raw: Raw data from the API
	- data/processed: Cleaned and processed data 
	- notebooks: Jupyter notebooks for exploration

 **Step 2.5: Create Basic Configuration Files** 
	 Let's create a basic configuration file:

	touch config/settings.py

Edit config/settings.py with:
	 # Database settings
	 DATABASE_PATH = "data/processed/nba_data.db"
	 
	# API settings 
	# The nba_api doesn't require an API key, but we can set other configurations
	API_REQUEST_TIMEOUT = 30 # seconds
	# Data directories
	RAW_DATA_DIR = "data/raw"
	PROCESSED_DATA_DIR = "data/processed"

 **Step 2.6: Create Core Files for the ETL Pipeline** 
	 2.6a - Create an Extractor for the API 
	  - Create a file src/extract/api_client.py
	 2.6b - Create a Transformer for Data Cleaning
	   - Create a file src/transform/data_processor.py
	 2.6c - Create a Data Loader for Storage
	   - Create a file src/load/db_loader.py

**Step 3: Create a Main Script to Run the Pipeline**
	Create a main script src/main.py

**Step 4: Create a Sample Jupyter Notebook for Analysis**
	 Create a notebook notebooks/nba_data_analysis.ipynb

**Step 5: Create a README.md File**

**Step 6: Run the Project**
Now you're ready to run the project:

	# Make sure you're in the project root directory with the virtual environment activated
	# Run the pipeline

	# Then explore the data in Jupyter 
	jupyter notebook notebooks/nba_data_analysis.ipynb

## Explanation of the Architecture

This project follows a standard ETL (Extract, Transform, Load) architecture:

1. **Extract**: We use the nba_api package to get data from the NBA API endpoints. The data is saved as raw JSON files.
2. **Transform**: The raw JSON data is processed into structured pandas DataFrames and saved as CSV files. We clean and reshape the data to make it more usable.
3. **Load**: The processed data is loaded into an SQLite database, making it easy to query and analyze.
4. **Analyze**: We provide a Jupyter notebook for analyzing and visualizing the data.

The modular design makes it easy to:

- Add new data sources (just create a new extractor method)
- Change how data is processed (modify the transformer)
- Use a different database (switch out the loader)


For a basic data engineering project, we'll create:

1. **Data Extraction**: Pull data from the API
2. **Data Transformation**: Clean and structure the data
3. **Data Loading**: Store the transformed data
4. **Basic Analysis**: Analyze the collected data

Here's a simple project structure to start with:
	data_project/
	├── config/
	│   └── config.py     # Configuration settings
	├── extract/
	│   └── api_client.py # API interaction logic
	├── transform/
	│   └── cleaner.py    # Data cleaning and transformation
	├── load/
	│   └── database.py   # Database connection and loading
	├── analysis/
	│   └── basic_analysis.py # Simple analytics
	└── main.py           # Pipeline orchestration

Comprehensive Architecture
The complete data warehouse architecture consists of:

ETL Pipeline:
	Extract data from NBA API
	Transform into dimensional model
	Load into SQLite database (can be upgraded to PostgreSQL for production)


Warehouse Schema:
	Dimension tables: teams, players, games, dates, venues
	Fact tables: game stats, player stats, team stats


Data Update Mechanism:
	Regular scheduled updates
	Historical data loading
	Incremental refresh


Analytics Layer:
	Predefined analytical queries
	Performance metrics
	Trend analysis
	Visualizations


Dashboard/UI:
	Jupyter notebook for visualization
	Statistical comparisons
	Performance charts

import os

# here I get the projectzero directory path
project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# get relative path from projectzero path
DATABASE_CONFIG_FILE = os.path.join(project_folder, 'database_config.ini')
CSV_FILE_PATH = os.path.join(project_folder, 'RawData', 'IMDb_movies.csv')
TOP_RATED_INDIAN_MOVIES_PATH = os.path.join(project_folder, 'RawData', 'top-rated-indian-movies-01.json')
RAW_TMDB_JSON_RAW_PATH = os.path.join(project_folder, 'RawData', 'raw_tmdb_movies.json')
LOGGER_FILE_LOG_PATH = os.path.join(project_folder, 'Logger', 'movies.log')


from DatabaseManagement.db_manager import  DatabaseConnector, MovieDatabase
import json
from datetime import date
import logging
from Logger.movies_logging_config import setup_logging
from Constants.paths import TOP_RATED_INDIAN_MOVIES_PATH
from Constants.queries import CHECK_MOVIE_EXISTENCE_BY_TITLE

setup_logging()
logger = logging.getLogger(__name__)


class IndianMovieHandler:
    def __init__(self, json_file_path=TOP_RATED_INDIAN_MOVIES_PATH):
        self.json_file_path = json_file_path
        self.db_connector = DatabaseConnector()
        self.movies_db = MovieDatabase(self.db_connector)


    def retrieve_json_data(self) -> tuple[str, str, date, str, int, list[str], float, int, list[str]]:
        try:
            # Check if the first movie in the JSON file already exists in the database
            with open(self.json_file_path, 'r') as json_file:
                data = json.load(json_file)

            if data:
                first_movie_title = data[0].get('title')
                # this time I check if the first movie in the json exist in db and if True, I stop
                self.db_connector.connect()
                self.db_connector.cursor.execute(CHECK_MOVIE_EXISTENCE_BY_TITLE, (first_movie_title,))
                count = self.db_connector.cursor.fetchone()[0]

                if count > 0:
                    logger.info(f"Movie '{first_movie_title}' already exists in the database. Skipping insertion.")
                    return  # Stop iteration

        except Exception as e:
            logger.error(f"Error checking for existing movie data: {e}")
        finally:
            pass

        # Continue with data retrieval if no existing data
        with open(self.json_file_path, 'r') as json_file:
            data = json.load(json_file)

        for movie_data in data:
            imdb_id = None
            title = movie_data.get('title')
            genre = movie_data.get('genres')
            release_date = movie_data.get('releaseDate')

            # ratings are given as an array, sum them up and calculate avg
            avg_rating = sum(movie_data.get('ratings'))/len(movie_data.get('ratings'))

            # runtime is given as an ISO standard, strip and leave the numbers in the string
            runtime = self.transform_runtime(movie_data.get('duration'))

            # we do not have information about budget in the data source
            budget = None
            actors = movie_data.get('actors')

            # used generators here as well
            yield imdb_id, title, release_date, "India", runtime, genre, avg_rating, budget, actors

    def insert_indian_movies(self) -> None:
        """Insert into movies table"""
        for movie_data in self.retrieve_json_data():
            self.movies_db.insert_movie(movie_data)

    def transform_runtime(self, duration: str) -> int:
        """the format of runtime is in ISO, function converts it to simple int"""
        try:
            # Extract numeric part from the duration string
            minutes = int(''.join(filter(str.isdigit, duration)))
            return minutes
        except (ValueError, TypeError):
            logger.error(f"Error extracting duration. Invalid format: {duration}")
            return None


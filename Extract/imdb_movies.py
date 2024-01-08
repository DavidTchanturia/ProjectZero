import csv
from DatabaseManagement.db_manager import DatabaseConnector, MovieDatabase
from datetime import date
from Logger.movies_logging_config import setup_logging
import logging
from Constants.paths import CSV_FILE_PATH
from Constants.queries import CHECK_MOVIE_EXISTENCE_BY_IMDB_ID

setup_logging()
logger = logging.getLogger(__name__)

class CSVDataHandler:
    def __init__(self, csv_file_path=CSV_FILE_PATH):
        self.csv_file_path = csv_file_path
        self.db_connector = DatabaseConnector()
        self.movies_db = MovieDatabase(self.db_connector)

    # if data other than the indian movies exists, means I have already inserted csv data in the table
    def has_existing_data(self) -> bool:
        """Check if data from the CSV file already exists in the database."""
        try:
            self.db_connector.connect()

            with open(self.csv_file_path, newline='', encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                header = next(csv_reader)

                imdb_title_id_idx = header.index('imdb_title_id')

                for row in csv_reader:
                    imdb_title_id = row[imdb_title_id_idx]

                    self.db_connector.cursor.execute(CHECK_MOVIE_EXISTENCE_BY_IMDB_ID, (imdb_title_id,))
                    count = self.db_connector.cursor.fetchone()[0]

                    if count > 0:
                        logger.info(f"Movie with imdb_title_id '{imdb_title_id}' already exists in the database. Skipping insertion.")
                        return True  # Data already exists

        except Exception as e:
            logger.error(f"Error checking for existing CSV movie data: {e}")
        finally:
            pass

        return False

    def retrieve_csv_data(self) -> tuple[str, str, date, str, int, dict, float, int, list]:
        """retrieve movies data from csv folder and use generator object to yield
        each row one by one"""
        if not self.has_existing_data():
            with open(self.csv_file_path, newline='', encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                header = next(csv_reader)

                imdb_title_id_idx = header.index('imdb_title_id')
                title_idx = header.index('title')
                date_published_idx = header.index('date_published')
                country_idx = header.index('country')
                duration_idx = header.index('duration')
                genre_idx = header.index('genre')
                avg_vote_idx = header.index('avg_vote')
                budget_idx = header.index('budget')
                actors_idx = header.index('actors')

                for row in csv_reader:
                    imdb_id = row[imdb_title_id_idx]
                    title = row[title_idx]
                    release_date = row[date_published_idx]

                    prod_countries = row[country_idx]
                    countries_list = [country.strip().lower() for country in prod_countries.split(',')]

                    runtime = row[duration_idx]
                    genres = row[genre_idx]
                    genres_list = [genre.strip() for genre in genres.split(',')] if genres else []
                    avg_rating = row[avg_vote_idx]

                    budget = row[budget_idx]
                    if not budget:
                        budget = None
                    else:
                        budget = budget.split(' ')[1]

                    actors = row[actors_idx]
                    actors_list = [actor.strip() for actor in actors.split(',')] if actors else []
                    # decided to go with generators because the sequence is too large
                    yield imdb_id, title, release_date, countries_list, runtime, genres_list, avg_rating, budget, actors_list
        else:
            logger.info("CSV movie data already exists in the database. Skipping insertion.")
            return  # Stop iteration in CSV so program can move to sending request to API

    def process_and_insert_data(self):
        """Insert into movies database"""
        for movie_data in self.retrieve_csv_data():
            self.movies_db.insert_movie(movie_data)

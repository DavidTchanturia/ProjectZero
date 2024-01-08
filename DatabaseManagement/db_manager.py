import psycopg2
import logging
from DatabaseManagement.db_config import config
from datetime import datetime
from Logger.movies_logging_config import setup_logging
from Constants.queries import CREATE_MOVIES_TABLE, INSERT_MOVIE, CHECK_MOVIE_EXISTENCE

# basic logging setup
setup_logging()
logger = logging.getLogger(__name__)


# to manage connection opening and closing
class DatabaseConnector:
    def __init__(self):
        self.connection = None
        self.cursor = None

    # connect to the database
    def connect(self):
        try:
            params = config()
            self.connection = psycopg2.connect(**params)  # db parameters extracted from database_config.ini
            self.cursor = self.connection.cursor()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def close_connection(self):
        try:
            if self.connection:
                self.connection.close()
            if self.cursor:
                self.cursor.close()
            logger.info("connection closed")
        except Exception as error:
            logger.error(f"{error} encountered while closing connection")


class MovieDatabase:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def is_valid_date(self, date_string: str) -> bool:
        """some dates are just YYYY, this method validates that
        dates are YYYY-MM-DD format"""
        try:
            # Attempt to parse the string as a date
            datetime.strptime(date_string, '%Y-%m-%d')
            return True
        except ValueError:
            # If the parsing fails, the string is not in the correct format
            return False

    def create_table(self):
        """create movies table if it does not exist"""
        try:
            self.db_connector.connect()
            self.db_connector.cursor.execute(CREATE_MOVIES_TABLE)
            self.db_connector.connection.commit()
        finally:
            pass

    def movie_exists(self, imdb_id):
        """Check if a movie with the given imdb_id already exists in the table"""
        try:
            self.db_connector.cursor.execute(CHECK_MOVIE_EXISTENCE, (imdb_id,))
            count = self.db_connector.cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            logger.error(f'Error: {e} while checking if movie exists in movies table')
            return False

    def insert_movie(self, movie_data):
        try:
            imdb_id = movie_data[0]

            if self.is_valid_date(movie_data[2]):
                self.db_connector.connect()

                if not self.movie_exists(imdb_id):
                    self.db_connector.cursor.execute(INSERT_MOVIE, movie_data)
                    self.db_connector.connection.commit()
                else:
                    logger.info(f'Movie with imdb_id {imdb_id} already exists, skipping insertion')
            else:
                logger.info(f'Skipping inserting movie: "{movie_data[1]}" with imdb_id: {imdb_id} because of the invalid format')
        except Exception as e:
            logger.error(f'Error: {e} while inserting data to movies table')

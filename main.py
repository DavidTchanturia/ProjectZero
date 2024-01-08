from DatabaseManagement.db_manager import DatabaseConnector, MovieDatabase
from Extract.tmdb_api import get_movie_details, get_movies_list
from Extract.imdb_movies import CSVDataHandler
from Extract.indian_movies import IndianMovieHandler


def main():
    # establish connection and create tables if they don't exist
    db_connector = DatabaseConnector()
    movies_db = MovieDatabase(db_connector)
    movies_db.create_table()

    # extracting from IMDB_movies.csv and insert into movies
    data_handler = CSVDataHandler()
    data_handler.process_and_insert_data()

    # extracting from top_rated_indian_movies_01.json and insert into movies
    indian_movie = IndianMovieHandler()
    indian_movie.insert_indian_movies()

    # this will send requests to TMDB API, extract, transform and insert into movies
    for page_number in range(1, 100):  # each page contains multiple movies
        for movie_id in get_movies_list(page_number):
            movie_details = get_movie_details(movie_id)
            movies_db.insert_movie(movie_details)

    # finally close the connection
    db_connector.close_connection()


if __name__ == '__main__':
    main()

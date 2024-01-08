MOVIES_TABLE_SCHEMA = {
    'id': 'SERIAL PRIMARY KEY',
    'imdb_id': 'VARCHAR(15)',
    'title': 'VARCHAR(150)',
    'release_date': 'DATE',
    'country': 'VARCHAR(255)',
    'runtime': 'INT',
    'genre': 'VARCHAR(500)',
    'avg_rating': 'FLOAT CHECK (avg_rating >= 0.0 AND avg_rating <= 10.0)',
    'budget': 'BIGINT NULL',
    'actors': 'VARCHAR(500)'
}


CREATE_MOVIES_TABLE = f"""
CREATE TABLE IF NOT EXISTS movies (
    {', '.join(f'{column} {data_type}' for column, data_type in MOVIES_TABLE_SCHEMA.items())}
);
"""

INSERT_MOVIE = """
INSERT INTO movies (imdb_id, title, release_date, country, runtime, genre, avg_rating, budget, actors)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


CHECK_MOVIE_EXISTENCE = """
SELECT COUNT(*) FROM movies WHERE imdb_id = %s
"""

CHECK_MOVIE_EXISTENCE_BY_TITLE = """
SELECT COUNT(*) FROM movies WHERE title = %s
"""

CHECK_MOVIE_EXISTENCE_BY_IMDB_ID = """
SELECT COUNT(*) FROM movies WHERE imdb_id = %s
"""
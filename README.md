# ETL Pipeline
**ProjectZero** is an ETL pipeline that works with three
different sources of data, extracts them from the original
sources, transforms and loads into postgres database.

### Extract
The extraction process happens from three different sources
- TMDB API
  - Data from this API is saved in a json file, as is, to simulate data lake architecture.
  - during the next steps, this data is used as a batch to process and load into database.
- imdb_movies.csv file containing  > 80 000 imdb movies
- json file containing top Indian movies

### Transform
Even tho the three different sources of data are about the same Topic,
They have different fields, data types, styles. during the transformation process
all three types of data are concatenated into one with the common schema pre-defined.

### Load
after the transformation has been complete, the data is then loaded into Postgres database.


## Setting Up
In order to run the program on your local machine, follow the steps

- After cloning the repository, make sure to install requirements in your virtual enviroment

```
pip3 install -r requirements.txt
```

- make sure to change database_config.ini configuration file, to match your database configuration

```
[database]
host=localhost
database=my_movies
user=postgres
password=postgres
```

- In Extract/tmdb_api.py make sure to use your bearer tocken to be able to send API calls to TMDB API.

```
HEADERS = {
    "accept": "application/json",
    "Authorization": "Bearer <your token goes here>"
}
```

- after completing these steps, you are ready to run the program. either from your IDE,
or running the following command in terminal.

```
python3 main.py
```

# How does the final table look:

### Schema

```
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
```

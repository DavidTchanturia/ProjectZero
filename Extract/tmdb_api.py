import requests
import json
import time
from Constants.paths import RAW_TMDB_JSON_RAW_PATH

HEADERS = {
    "accept": "application/json",
    "Authorization": "Bearer <your token goes here>"
}


def delay_decorator(func):
    """delays function by 0.5 seconds"""
    def wrapper(*args, **kwargs):
        time.sleep(0.5)
        return func(*args, **kwargs)
    return wrapper


def save_raw_movie_response(response):
    # Read existing data from the file
    try:
        with open(RAW_TMDB_JSON_RAW_PATH, 'r') as json_file:
            data = json.load(json_file)
    except FileNotFoundError:
        data = []

    # Append the new response to the existing data
    data.append(response)

    # Write the updated data back to the file
    with open('RawData/raw_tmdb_movies.json', 'w') as json_file:
        json.dump(data, json_file, indent=2)


def get_production_countries_list(response):
    if not response.get('production_countries'):
        countries = None
    else:
        countries = [country['name'] for country in response.get('production_countries')]

    return countries


# using delay_decorator to avoid reaching api calls limit per 10 seconds
@delay_decorator
def get_movie_details(movie_id: int) -> tuple:
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"

    response = requests.get(url, headers=HEADERS).json()

    # save response as a raw data, in JSON folder
    save_raw_movie_response(response)

    imdb_id = response['imdb_id']
    title = response['title']
    release_date = response['release_date']
    countries = get_production_countries_list(response)  # some movies have multiple countries, so use list
    runtime = response['runtime']
    genres = [each['name'] for each in response['genres']]  # extract the list of genres
    avg_rating = response['vote_average']
    budget = response['budget']
    actors = None  # movie info form tmdb_api does not contain actors

    return imdb_id, title, release_date, countries, runtime, genres, avg_rating, budget, actors


def get_movies_list(page_number: int) -> list[int]:
    url = f"https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=en-US&page={page_number}&sort_by=popularity.desc"

    response = requests.get(url, headers=HEADERS)
    contents = response.json()
    movie_ids = []
    for content in contents['results']:
        movie_ids.append(content['id'])

    return movie_ids
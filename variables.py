from dotenv import load_dotenv
from os import getenv
from json import loads
load_dotenv()

TOTAL_PAGES = 10
MAX_WORKERS = 10
OMDB_API_KEY = getenv('OMDB_API_KEY')
TMDB_HEADERS = loads(getenv('TMDB_HEADERS'))

CLOUDKARAFKA_HOSTNAME = getenv('CLOUDKARAFKA_HOSTNAME')
CLOUDKARAFKA_USERNAME = getenv('CLOUDKARAFKA_USERNAME')
CLOUDKARAFKA_PASSWORD = getenv('CLOUDKARAFKA_PASSWORD')
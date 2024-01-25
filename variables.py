from dotenv import load_dotenv
from os import getenv
from json import loads
load_dotenv()

TOTAL_PAGES = 50
MAX_WORKERS = 10
RAPID_HEADERS = loads(getenv('RAPID_HEADERS'))
TMDB_HEADERS = loads(getenv('TMDB_HEADERS'))
from os import getenv
from json import loads
from dotenv import load_dotenv

load_dotenv()

TMDB_HEADERS = loads(getenv('TMDB_HEADERS'))
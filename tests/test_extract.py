import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))

from src.extract.extract import Extract
from utils.config import Config

if __name__ == "__main__":
    Config.validate_config()

    e = Extract(5)
    e.start()
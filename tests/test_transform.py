import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))

from src.transform.transform import Transform
from utils.config import Config

if __name__ == "__main__":
    Config.validate_config()

    t = Transform()
    t.start()
    
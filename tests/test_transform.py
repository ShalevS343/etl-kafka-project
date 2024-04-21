
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))

from utils.config import Config
from utils.interfaces.kafka_interface import KafkaInterface
from src.transform.transformer import Transformer

if __name__ == "__main__":
    Config.validate_config()
    
    # k = KafkaInterface()
    # k.clean_buffer()
    
    t = Transformer()
    t.transform()
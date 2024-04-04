import redis
import json

from utils.config import Config

# Base class for now will work on redis soon
class RedisInterface:
    def __init__(self):
        self._redis = redis.from_url(Config.REDIS_URI)

    def set_value(self, key, value):
        """
        Sets a key-value pair in Redis.

        Parameters:
        - key: The key to set.
        - value: The value to set for the key.
        """
        self._redis.set(key, json.dumps(value))

    def get_value(self, key):
        """
        Gets the value associated with a given key.

        Parameters:
        - key: The key to retrieve the value for.

        Returns:
        The value associated with the key (decoded from JSON if applicable).
        """
        value = self._redis.get(key)
        if value is not None:
            return json.loads(value)
        return None

    def delete_key(self, key):
        """
        Deletes a key from Redis.

        Parameters:
        - key: The key to delete.
        """
        self._redis.delete(key)

    def get_keys(self, pattern='*'):
        """
        Gets all keys matching a given pattern.

        Parameters:
        - pattern: The pattern to match (default is '*').

        Returns:
        A list of keys matching the specified pattern.
        """
        return self._redis.keys(pattern)

    def incr_counter(self, key, amount=1):
        """
        Increments a counter stored in Redis.

        Parameters:
        - key: The key of the counter.
        - amount: The amount by which to increment the counter (default is 1).
        """
        self._redis.incr(key, amount)

redis_interface = RedisInterface()
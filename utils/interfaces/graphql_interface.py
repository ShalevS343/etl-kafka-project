import graphene
import redis

import asyncio
import json
import logging
from typing import List

from utils.config import Config
from utils.data_structures.movie import Movie
from utils.interfaces.redis_interface import RedisInterface
from utils.schemas import Schemas

logging.basicConfig(level=logging.INFO)
    

class Query(graphene.ObjectType):
    movies = graphene.List(
        Schemas.MovieSchema, 
        imdb_id=graphene.String(), 
        movie_name=graphene.String(),
        genres=graphene.String(),
        directors=graphene.String(),
        lead_actors=graphene.String(),
        rating=graphene.String(),
        awards=graphene.String(),
        release_date=graphene.String())
    
    def resolve_movies(self, info, **kwargs):
        _redis = RedisInterface()
        _query = []
        
        if kwargs:
            for key, value in kwargs.items():
                for sub_value in value.split(', '):
                    _query.append(f'@{key}:{sub_value}' 
                                  if key not in ['genres', 'directors', 'lead_actors', 'awards'] 
                                  else f'@{key}:{{{sub_value}}}')
        return _redis.movie_search('*' if not _query else ' '.join(_query))

class SubscriptionSchema(graphene.Schema):
    movie_updates = graphene.List(Schemas.MovieSchema)

class Subscription(graphene.ObjectType):
    movie_updates = graphene.List(Schemas.MovieSchema)
    
    async def subscribe_movie_updates(root, info):
        _redis = redis.StrictRedis.from_url(Config.REDIS_URI)
        _pubsub = _redis.pubsub()
        _pubsub.subscribe('movie_updates')
        
        try:
            movies: List = []            
            
            for message in _pubsub.listen():
                if message['type'] == 'message':
                    movie_data = message['data'].decode('utf-8')
                    movies.append(json.loads(movie_data))
                    yield movies
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            _pubsub.unsubscribe()
            _pubsub.close()
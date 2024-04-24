import sys
import os
import logging
logging.basicConfig(level=logging.INFO)

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))

import graphene
from starlette.applications import Starlette
from starlette_graphene3 import GraphQLApp, make_graphiql_handler

from utils.interfaces.redis_interface import RedisInterface

class Movie(graphene.ObjectType):
    imdb_id = graphene.String()
    movie_name = graphene.String()
    genres = graphene.String()
    directors = graphene.String()
    lead_actors = graphene.String()
    rating = graphene.String()
    awards = graphene.String()
    release_date = graphene.String()

class Query(graphene.ObjectType):
    movies = graphene.List(
        Movie, 
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
        
        if kwargs:
            for key, value in kwargs.items():
                if hasattr(_redis, f'get_by_{key}'):                   
                    return getattr(_redis, f'get_by_{key}')(value)
        return _redis.get_all()

app = Starlette()
schema = graphene.Schema(query=Query)
app.mount("/", GraphQLApp(schema=schema, on_get=make_graphiql_handler()))

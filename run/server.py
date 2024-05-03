import uvicorn

import sys
import os
import logging
logging.basicConfig(level=logging.INFO)

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))

import graphene
from starlette.applications import Starlette
from starlette_graphene3 import GraphQLApp, make_graphiql_handler

from utils.interfaces.graphql_interface import Query, Subscription

app = Starlette()
schema = graphene.Schema(query=Query, subscription=Subscription)
app.mount("/", GraphQLApp(schema=schema, on_get=make_graphiql_handler()))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
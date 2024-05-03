# etl-kafka-project

## Overview
This project is an ETL process project that revolves around Extract, Transform and Load. It uses Kafka as its message broker and a variety of data fetchers to fetch data from the APIs, enrich it with CSV files and store it in Redis and GraphQL.

In addition, there is a user interface that allows the user to interact with the project.

## Dependencies
In order to run this project, you will need to install the dependancies listed in the requirements.txt file. for that you will have to run the following command in your terminal:

`pip install -r requirements.txt`

## Running the project
To run the project, you will first need to get the API keys from OMDB and TMDB and replace the OMDB_API_KEY and TMDB_API_KEY in the config.py file. Then you will need to do the same for the Kafka configurations and the Redis configurations in the config.py file.

To run the project, you will need to run the following command in your terminal:

`python3 etl_process.py`

## Running the user interface
To run the user interface, you will need to run the following command in your terminal:

`python3 server.py`

### Notes
Please make sure that you read the config.py and edit it to your needs.
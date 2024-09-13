import datetime
import io
import json
import traceback
import urllib

from pconf import Pconf
import os
import pandas as pd
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects import postgresql
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from contextlib import contextmanager
from sqlalchemy import create_engine, MetaData, Table, tuple_, or_
from sqlalchemy.engine import reflection
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, Session
from database.base_model import BaseModel

class DatabaseUtil:
    __engine = None
    __Session = None
    __base = None
    __client = None
    temp_session = None

    def __init__(self):
        pass

    # This method initializes Global Session, Engine
    @classmethod
    def get_config(cls):
        # Read Current Directory relative to config files' path
        Pconf.env()
        current_file_path = os.path.realpath(__file__)
        current_directory = os.path.dirname(current_file_path)
        destination_directory = os.path.join(current_directory, '..', 'config')
        destination_file = os.path.join(destination_directory, "config.json")
        print(destination_file)
        # Populate Pconf with json key-value pairs
        Pconf.file(destination_file, encoding='json')
        # Assign json key-value pairs to class variable
        config = Pconf.get() #Pconf used to read config file
        return config

    @classmethod
    def initialize_postgres_database(cls, db_config=None):
        config = cls.get_config()
        if db_config is None:
            db_config = config['postgres']
        db_uri = DatabaseUtil.get_db_uri(db_config)
        pool_size = db_config['pool_size'] if 'pool_size' in db_config else 50
        DatabaseUtil.__engine = create_engine(db_uri, echo=False, pool_size=pool_size, max_overflow=50)
        DatabaseUtil.__Session = sessionmaker(bind=DatabaseUtil.__engine)
        DatabaseUtil.__create_postgres_tables()

    @classmethod
    def initialize_mongodb(cls):
        config = DatabaseUtil.get_config()['mongodb']
        username = config['username']
        password = config['password']
        database = config['database']
        port = config['port']
        # uri = f'mongodb+srv://{username}:{password}@motor.3w7ehsz.mongodb.net/?w=majority&connectTimeoutMS=36000000&wtimeoutMS=0&socketTimeoutMS=3600000'
        uri = f'mongodb://{username}:{password}@localhost:{port}'
        DatabaseUtil.__client = MongoClient(uri, server_api=ServerApi('1'), retryWrites=True,
                             serverSelectionTimeoutMS=5000,
                             waitQueueTimeoutMS=200,
                             waitQueueMultiple=10,
                             connect=True,
                             retryReads=True,
                             socketTimeoutMS=360000,
                             maxConnecting=100,
                             maxPoolSize=400,
                             minPoolSize=200)

    @staticmethod
    def __create_postgres_tables():
        try:
            BaseModel.base.metadata.create_all(DatabaseUtil.__engine)
        except Exception as e:
            print(str(e))


    # This method will return global object of session maker, engine
    @staticmethod
    def get_postgres_session():
        if DatabaseUtil.__Session:
            return DatabaseUtil.__Session()
        else:
            DatabaseUtil.initialize_postgres_database()
            return DatabaseUtil.__Session()

    @staticmethod
    def get_mongo_client():
        if DatabaseUtil.__client:
            return DatabaseUtil.__client
        else:
            DatabaseUtil.initialize_mongodb()
            return DatabaseUtil.__client


    @staticmethod
    def get_postgres_engine():
        if DatabaseUtil.__engine:
            return DatabaseUtil.__engine
        else:
            DatabaseUtil.initialize_postgres_database()
            return DatabaseUtil.__engine

    @staticmethod
    def drop_postgres_table(table_name):
        try:
            engine = DatabaseUtil.get_postgres_engine()
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=engine)
            # Drop the table if it already exists
            table.drop(engine, checkfirst=True)
            BaseModel.base.metadata.create_all(DatabaseUtil.__engine) # recreate all tables from data models
        except Exception as e:
            print(str(e))

    @staticmethod
    def commit_postgres_session(session):
        if session:
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                raise Exception(e)
            finally:
                session.close()

    @staticmethod
    def close_postgres_session(session):
        if session:
            session.close()


    @staticmethod
    def get_postgres_connection():
        if DatabaseUtil.__engine:
            return DatabaseUtil.__engine.connect()
        else:
            DatabaseUtil.initialize_postgres_database()
            return DatabaseUtil.__engine.connect()

    @staticmethod
    def close_postgres_connection(connection):
        if connection:
            connection.close()

    @staticmethod
    def close_mongo_client(client):
        if client:
            client.close()


    @classmethod
    def get_db_uri(cls, db_config, mongo=False):
        driver = db_config['driver']
        username = urllib.parse.quote_plus(db_config['username'])
        password = urllib.parse.quote_plus(db_config['password'])
        host = db_config['host']
        port = db_config['port']
        database = db_config['database']
        schema = db_config['schema'] if 'schema' in db_config else 'public'

        mongo_uri = f"{driver}:///?Server={host}&Port={port}&Database={database}&User={username}&Password={password}"
        uri = f"{driver}://{username}:{password}@{host}:{port}/{database}?options=--search_path%3D{schema}"

        if "msdriver" in db_config:
            uri = "{}?driver={}".format(uri, db_config['msdriver'])
        return mongo_uri if mongo else uri



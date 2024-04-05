import io
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
from sqlalchemy import create_engine, MetaData, Table, tuple_, or_, URL
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
    def initialize_database(cls, db_config=None):
        config = cls.get_config()
        if db_config is None:
            db_config = config['postgres']
        db_uri = DatabaseUtil.get_db_uri(db_config)
        pool_size = db_config['pool_size'] if 'pool_size' in db_config else 50
        DatabaseUtil.__engine = create_engine(db_uri, echo=False, pool_size=pool_size, max_overflow=50)
        DatabaseUtil.__Session = sessionmaker(bind=DatabaseUtil.__engine)
        DatabaseUtil.temp_session = DatabaseUtil.__Session
        DatabaseUtil.__base = automap_base()
        DatabaseUtil.__base.prepare(DatabaseUtil.__engine, reflect=True)
        DatabaseUtil.__create_tables()

    @classmethod
    def initialize_mongodb(cls):
        config = DatabaseUtil.get_config()['mongodb']
        username = config['username']
        password = config['password']
        uri = f'mongodb+srv://{username}:{password}@motor.3w7ehsz.mongodb.net/?w=majority&connectTimeoutMS=36000000&wtimeoutMS=0&socketTimeoutMS=3600000'
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
    def __create_tables():
        try:
            BaseModel.base.metadata.create_all(DatabaseUtil.__engine)
            if DatabaseUtil.archive_engine:
                BaseModel.base.metadata.create_all(DatabaseUtil.archive_engine)
        except Exception as e:
            print(str(e))

    # This method will return global object of session maker, engine
    @staticmethod
    def get_session():
        if DatabaseUtil.__Session:
            return DatabaseUtil.__Session()
        else:
            DatabaseUtil.initialize_database()
            return DatabaseUtil.__Session()

    @staticmethod
    def get_mongo_client():
        if DatabaseUtil.__client:
            return DatabaseUtil.__client
        else:
            DatabaseUtil.initialize_mongodb()
            return DatabaseUtil.__client


    @staticmethod
    def get_engine():
        if DatabaseUtil.__engine:
            return DatabaseUtil.__engine
        else:
            DatabaseUtil.initialize_database()
            return DatabaseUtil.__engine

    @staticmethod
    def commit_session(session):
        if session:
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                raise Exception(e)
            finally:
                session.close()

    @staticmethod
    def close_session(session):
        if session:
            session.close()

    @staticmethod
    def get_query_df(session, sql_orm):
        sq = session.query(sql_orm)
        df = pd.read_sql(sq.statement, session.bind)
        return df

    @staticmethod
    def bulk_insert_orm(session, mapper, mappings):
        session.bulk_insert_mappings(mapper, mappings)
        session.commit()

    @staticmethod
    def bulk_insert_core(engine, mapper, mappings):
        # engine.execute(mapper.__table__.insert(), mappings)
        with engine.connect() as conn:
            result = conn.execute(mapper.__table__.insert(), mappings)

    @staticmethod
    def get_connection():
        if DatabaseUtil.__engine:
            return DatabaseUtil.__engine.connect()
        else:
            DatabaseUtil.initialize_database()
            return DatabaseUtil.__engine.connect()

    @staticmethod
    def close_connection(connection):
        if connection:
            connection.close()


    @staticmethod
    def get_table_columns(table_name, schema_name='public'):
        """Return table columns from specified schema.

        :param table_name: String with name of table.
        :param schema_name: String with name of database schema.
        :return: Columns or None.
        """
        columns = []
        try:
            inspector = reflection.Inspector.from_engine(DatabaseUtil.__engine)
            columns = inspector.get_columns(table_name, schema=schema_name)
        except Exception as e:
            print(e)
            DatabaseUtil.__logger.error(e)
            columns = []
        return columns

    @staticmethod
    def delete_table_data(table_name, schema_name='public', filter_column=None, filter_value=None,
                          session=None):
        # connection = DatabaseUtil.__engine.connect()
        session_f = DatabaseUtil.get_session() if session is None else session
        try:
            metadata = MetaData(schema=schema_name)
            table = Table(table_name, metadata, autoload_with=DatabaseUtil.__engine)
            if filter_column is not None:
                # query = delete(table).where((table.c[filter_column] == filter_value))
                session_f.query(table).filter((table.c[filter_column] == filter_value)).delete(
                    synchronize_session=False)
            else:
                # query = delete(table)
                session_f.query(table).delete(synchronize_session=False)
            # connection.execute(query)
            if session is None:
                session_f.commit()
        except Exception as e:
            print(traceback.format_exc())
            session_f.rollback()
            raise Exception(e)
        finally:
            # connection.close()
            if session is None:
                session_f.close()

    @staticmethod
    def save_df_to_DB_with_session(df, table_name, schema='public', session=None, append=False):
        session_f = DatabaseUtil.get_session() if session is None else session
        cur = session_f.connection().connection.cursor()
        metadata = MetaData(schema=schema)
        table = Table(table_name, metadata, autoload_with=DatabaseUtil.__engine)
        output = io.StringIO()
        success = True
        common_cols = {i.name for i in table.columns}.intersection(set(df.columns))
        cols = [col for col in df.columns if col in common_cols]
        try:
            if not append:
                DatabaseUtil.delete_table_data(table_name=table_name, schema_name=schema, session=session)
            columns_with_quotes = [f"{col}" for col in cols]
            df[cols].to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            cur.copy_from(output, table_name, sep='\t', columns=columns_with_quotes, null="")  # null values become ''
            if session is None:
                session_f.commit()

        except Exception as e:
            success = False  # Failed
            print(traceback.format_exc())
            raise Exception(e)
        finally:
            if session is None:
                session_f.close()
            return success


    @classmethod
    def insert_data(cls, table, data, session=None):
        commit = False
        if session is None:
            commit = True
            session = DatabaseUtil.get_session()
        session.bulk_insert_mappings(table, data.to_dict(orient='records'))
        print("insert complete")
        if commit:
            session.commit()
            DatabaseUtil.close_session(session)

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
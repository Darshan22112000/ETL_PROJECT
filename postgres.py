import pandas as pd
import numpy as np
from sqlalchemy import MetaData, Table

from database.DatabaseUtil import DatabaseUtil
from database.models import Crash, map_crash_columns, map_person_columns, map_vehicle_columns


def insert_to_postgres(collection_name=None):
    client = DatabaseUtil.get_mongo_client()
    db = client.dap
    # fetch all collections
    collection = db[collection_name]
    df = pd.DataFrame(list(collection.find()))

    # data cleaning/processing
    df.replace({np.nan: None}, inplace=True)
    # Load your table model
    table_name = collection_name
    meta = MetaData()
    table = Table(table_name, meta, auto_load=True, autoload_with=DatabaseUtil.get_postgres_engine())
    # Get column names and data types
    column_data_types = {column.name: str(column.type) for column in table.columns}
    int_cols = []
    for key, value in column_data_types.items():
        if column_data_types[key] == 'INTEGER':
            int_cols.append(key)
    if collection_name == 'crash':
        df['CRASH TIME'] = pd.to_datetime(df['CRASH TIME'], format='mixed')
        # df = df.loc[~df['CONTRIBUTING FACTOR VEHICLE 1'].isna()]
        df.rename(columns=map_crash_columns, inplace=True)
        df = df[map_crash_columns.values()]
    elif collection_name == 'person':
        df['CRASH_TIME'] = pd.to_datetime(df['CRASH_TIME'], format='mixed')
        df.rename(columns=map_person_columns, inplace=True)
        df = df[map_person_columns.values()]
    elif collection_name == 'vehicle':
        df['CRASH_TIME'] = pd.to_datetime(df['CRASH_TIME'], format='mixed')
        df.rename(columns=map_vehicle_columns, inplace=True)
        df = df[map_vehicle_columns.values()]

    df[int_cols] = df[int_cols].replace({None: 0}).astype(int)
    p_key = [str(x).split('.')[1] for x in list(table.primary_key)]
    for col in p_key:
        df = df[df[col].notna()]
        df = df[~df[col].isnull()]
    df = df.drop_duplicates()

    try:
        session = DatabaseUtil.get_postgres_session()
        DatabaseUtil.save_to_postgres(df, collection_name, append=False, session=session)  # truncate and insert
        session.commit()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        DatabaseUtil.close_postgres_session(session)


insert_to_postgres(collection_name='crash')
insert_to_postgres(collection_name='person')

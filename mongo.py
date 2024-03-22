import json
import os
import asyncio
import time
import datetime

import pandas as pd
import numpy as np
from multiprocessing import Pool

from database.DatabaseUtil import DatabaseUtil
from database.models import map_collision_columns
from pymongo import InsertOne, WriteConcern

def truncate_n_insert_chunk_records(db, records):
    db.motor_collisions.delete_many({})
    if len(records) > 99999:
        # records = np.array_split(records, 999)
        records = [records[i: i+50000] for i in range(0, len(records), 50000)]
        for record in list(records):
            db.motor_collisions.insert_many(list(record), ordered=False)
            print(datetime.datetime.now())                           #10 mins 30 secs (7.3 lakh records)
    else:
        db.motor_collisions.insert_many(records)

async def truncate_n_insert_async_loop(db, collection_name, records):
    db[collection_name].delete_many({})
    # insert chunk of records asynchronously using async loop executer
    loop = asyncio.get_running_loop()
    tasks = []
    if len(records) > 99999:
        records = [records[i: i + 10000] for i in range(0, len(records), 10000)]
        for record in list(records):
            tasks.append(
                loop.run_in_executor(None, db[collection_name].insert_many, list(record), False)
            )
        response, pending = await asyncio.wait(tasks)
        # response_dict = {}
        # [response_dict.update(t.result()) for t in list(response)]
        return response
    else:
        db.motor_collisions.insert_many(records)


def to_dict_custom(df):
    cols = list(df)
    col_arr_map = {col: df[col].astype(object).to_numpy() for col in cols}
    records = []
    for i in range(len(df)):
        record = {col: col_arr_map[col][i] for col in cols}
        records.append(record)
    return records

async def csv_handler():
    try:
        begin = datetime.datetime.now()
        print(begin)
        # read csv
        cwd = os.getcwd()
        # dir = os.path.dirname(cwd)
        path = os.path.join(cwd, 'Data', 'Motor_Vehicle_Collisions_-_Crashes.csv').replace("\\", '/')
        filename = path
        collisions = pd.read_csv(filename, usecols=map_collision_columns.keys())

        # data cleaning/processing
        collisions.replace({np.nan: None}, inplace=True)
        collisions['CRASH TIME'] = pd.to_datetime(collisions['CRASH TIME'], format='mixed')
        collisions = collisions.loc[~collisions['CONTRIBUTING FACTOR VEHICLE 1'].isna()]
        collisions.rename(columns=map_collision_columns, inplace=True)
        collisions_df = collisions[map_collision_columns.values()]

        # convert df to json and insert to mongodb
        # records = collisions_df.to_dict(orient='records') #slow
        records = to_dict_custom(collisions_df) #3 times faster
        client = DatabaseUtil.get_mongo_client()
        db = client.motor_db
        # truncate_n_insert_chunk_records(db, records)
        response_dict = await truncate_n_insert_async_loop(db, 'motor_collisions', records)
        client.close()
        print(f'insert done in: {begin - datetime.datetime.now()}')
    except OSError as error:
        print(error)
    except FileNotFoundError:
        print(f'File not found on path: {filename}')
    except IOError:
        print(f'Could not read from the file: {filename}')
    finally:
        pass

asyncio.run(csv_handler())



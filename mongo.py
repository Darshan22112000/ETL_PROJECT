import json
import os
import asyncio
import time
import datetime

import pandas as pd
import numpy as np
from multiprocessing import Pool

from database.DatabaseUtil import DatabaseUtil
from database.models import map_crash_columns, map_person_columns, map_vehicle_columns
from pymongo import InsertOne, WriteConcern

def truncate_n_insert_chunk_records(db, records, collection_name):
    collection = db[collection_name]
    collection.delete_many({})
    if len(records) > 99999: # cant insert more than 1 lakh records in a session/ also cant use insert_many more than 100 times in single session
        # records = np.array_split(records, 999)
        records = [records[i: i+50000] for i in range(0, len(records), 50000)]
        for record in list(records):
            collection.insert_many(list(record), ordered=False)
        print(datetime.datetime.now())           #10 mins 30 secs (7.3 lakh records) / 3 mins 30 sec(20 Lakh records(docker))
    else:
        collection.insert_many(records)

def insert_chunk_records(db, records, collection_name):
    collection = db[collection_name]
    if len(records) > 99999:
        # records = np.array_split(records, 999)
        records = [records[i: i+50000] for i in range(0, len(records), 50000)]
        for record in list(records):
            collection.insert_many(list(record), ordered=False)
            print(datetime.datetime.now())                           #10 mins 30 secs (7.3 lakh records)
    else:
        collection.insert_many(records)

async def truncate_n_insert_async_loop(db, records, collection_name):
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
        db[collection_name].insert_many(records)


def to_dict_custom(df):
    cols = list(df)
    col_arr_map = {col: df[col].astype(object).to_numpy() for col in cols}
    records = []
    for i in range(len(df)):
        record = {col: col_arr_map[col][i] for col in cols}
        records.append(record)
    return records
def insert_crash():
    begin = datetime.datetime.now()
    print(begin)
    # read csv
    cwd = os.getcwd()
    # dir = os.path.dirname(cwd)
    path = os.path.join(cwd, 'Data', 'crash.json').replace("\\", '/')
    filename = path
    # collisions = pd.read_csv(filename, usecols=map_collision_columns.keys())

    # loading json
    data = json.load(open(filename))
    collisions = pd.DataFrame(data)

    # collisions = pd.DataFrame(data["data"])
    # new_names = pd.DataFrame(data['meta']['view']['columns'])['name'].tolist()
    # old_names = collisions.keys().tolist()
    # col_rename = list(zip(old_names, new_names))
    # col_rename = {key: value for key, value in col_rename}
    # collisions.rename(columns=col_rename, inplace=True)
    # collisions = collisions[list(map_crash_columns.keys())]

    # data cleaning/processing
    collisions.replace({np.nan: None}, inplace=True)
    collisions['CRASH TIME'] = pd.to_datetime(collisions['CRASH TIME'], format='mixed')
    collisions = collisions.loc[~collisions['CONTRIBUTING FACTOR VEHICLE 1'].isna()]
    collisions.rename(columns=map_crash_columns, inplace=True)
    collisions_df = collisions[map_crash_columns.values()]

    # convert df to json and insert to mongodb
    # records = collisions_df.to_dict(orient='records') #slow
    records = to_dict_custom(collisions_df)  # 3 times faster
    client = DatabaseUtil.get_mongo_client()
    db = client.dap
    response_dict = truncate_n_insert_chunk_records(db, records, 'crash')
    client.close()
    print(f'insert done in: {begin - datetime.datetime.now()}')

def insert_person():
    begin = datetime.datetime.now()
    print(begin)
    # read csv
    cwd = os.getcwd()
    # dir = os.path.dirname(cwd)
    path = os.path.join(cwd, 'Data', 'person.json').replace("\\", '/')
    filename = path
    # df = pd.read_csv(filename, usecols=map_collision_columns.keys())
    # loading json
    data = json.load(open(filename))
    df = pd.DataFrame(data)
    # data cleaning/processing
    df.replace({np.nan: None}, inplace=True)
    df['CRASH_TIME'] = pd.to_datetime(df['CRASH_TIME'], format='mixed')
    df.rename(columns=map_person_columns, inplace=True)
    df = df[map_person_columns.values()]

    records = to_dict_custom(df)  # 3 times faster
    client = DatabaseUtil.get_mongo_client()
    db = client.dap
    response_dict = truncate_n_insert_chunk_records(db, records, 'person')
    client.close()
    print(f'insert done in: {begin - datetime.datetime.now()}')
def insert_to_mongo(collection_name=None):
    # read csv
    cwd = os.getcwd()
    # dir = os.path.dirname(cwd)
    path = os.path.join(cwd, 'Data', f'{collection_name}.json').replace("\\", '/')
    filename = path
    # loading json
    data = json.load(open(filename))
    df = pd.DataFrame(data)
    if collection_name == 'crash':
        df = df[list(map_crash_columns.keys())]
    elif collection_name == 'person':
        df = df[list(map_person_columns.keys())]
    elif collection_name == 'vehicle':
        df = df[list(map_vehicle_columns.keys())]
    records = to_dict_custom(df)  # 3 times faster
    client = DatabaseUtil.get_mongo_client()
    db = client.dap
    response_dict = truncate_n_insert_chunk_records(db, records, collection_name)
    client.close()
async def json_handler():
    try:
        insert_to_mongo('crash')
        insert_to_mongo('person')

    except OSError as error:
        print(error)
    except FileNotFoundError:
        print(f'File not found on path:')
    except IOError:
        print(f'Could not read from the file:')
    finally:
        pass

asyncio.run(json_handler())



import json
import luigi
import os
import datetime
import pandas as pd
import numpy as np
import asyncio

from sqlalchemy import MetaData, Table

from Analysis.luigi_analyse import Visualize_run_model
from database.DatabaseUtil import DatabaseUtil
from database.models import map_crash_columns, Crash, Person, Vehicle, map_person_columns, map_vehicle_columns
from database.IO_ops import IO_ops


class Extract_data(luigi.Task):
    collection_name = luigi.Parameter()

    def output(self):
        cwd = os.getcwd()
        csv_path = os.path.join(cwd, 'Data', f'{self.collection_name}_data.csv').replace("\\", '/')
        return luigi.LocalTarget(csv_path)

    def run(self):
        # read csv
        cwd = os.getcwd()
        path = os.path.join(cwd, 'Data', f'{self.collection_name}.json').replace("\\", '/')
        file_path = path
        data = json.load(open(file_path))
        df = pd.DataFrame(data)

        if self.collection_name == 'crash':
            df = df[list(map_crash_columns.keys())]
        elif self.collection_name == 'person':
            df = df[list(map_person_columns.keys())]
        elif self.collection_name == 'vehicle':
            df = df[list(map_vehicle_columns.keys())]

        # Save DataFrame as CSV
        csv_path = os.path.join(cwd, 'Data', f'{self.collection_name}_data.csv').replace("\\", '/')
        df.to_csv(csv_path, index=False)
        print("extract ran")


class Save_to_mongo(luigi.Task):
    collection_name = luigi.Parameter()
    def requires(self):
        return Extract_data(collection_name=self.collection_name)

    def output(self):
        cwd = os.getcwd()
        csv_path = os.path.join(cwd, 'Data', f'{self.collection_name}_data.csv').replace("\\", '/')
        return luigi.LocalTarget(csv_path)

    def run(self):
        data = pd.read_csv(self.input().path)
        #insert records to mongo
        client = DatabaseUtil.get_mongo_client()
        db = client.dap
        data = IO_ops.df_to_dict_custom(data)
        IO_ops.insert_mongo_chunk_records(db, self.collection_name, data)
        print(f"{self.collection_name} task done")

class fetch_from_mongo_and_save_to_postgres(luigi.Task):
    collection_name = luigi.Parameter()

    def requires(self):
        return Save_to_mongo(collection_name=self.collection_name)

    def run(self):
        client = DatabaseUtil.get_mongo_client()
        db = client.dap
        # fetch all collections
        collection = db[self.collection_name]
        df = pd.DataFrame(list(collection.find()))

        # print(df.summary())
        print(df.head())
        print(df.columns)
        print(df.isnull())
        print(df.describe())
        print(df.shape)
        print(df.count())

        # data cleaning/processing
        df.replace({np.nan: None}, inplace=True)
        # Load your table model
        table_name = self.collection_name
        meta = MetaData()
        table = Table(table_name, meta, auto_load=True, autoload_with=DatabaseUtil.get_postgres_engine())
        # Get column names and data types
        column_data_types = {column.name: str(column.type) for column in table.columns}
        int_cols = []
        for key, value in column_data_types.items():
            if column_data_types[key] == 'INTEGER':
                int_cols.append(key)
        if self.collection_name == 'crash':
            df['CRASH TIME'] = pd.to_datetime(df['CRASH TIME'], format='mixed')
            # df = df.loc[~df['CONTRIBUTING FACTOR VEHICLE 1'].isna()]
            df.rename(columns=map_crash_columns, inplace=True)
            df = df[map_crash_columns.values()]
        elif self.collection_name == 'person':
            df['CRASH_TIME'] = pd.to_datetime(df['CRASH_TIME'], format='mixed')
            df.rename(columns=map_person_columns, inplace=True)
            df = df[map_person_columns.values()]
        elif self.collection_name == 'vehicle':
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
            IO_ops.save_to_postgres(df, self.collection_name, append=False, session=session)  # truncate and insert
            session.commit()
        except Exception as err:
            session.rollback()
            raise err
        finally:
            DatabaseUtil.close_postgres_session(session)
            # remove temp file
            cwd = os.getcwd()
            csv_path = os.path.join(cwd, 'Data', f'{self.collection_name}_data.csv').replace("\\", '/')
            try:
                os.remove(csv_path)
            except:
                pass
            finally:
                print(f"fetch ran, removed {self.collection_name} temp files")


# if __name__ == '__main__':
#     luigi.build([Save_to_mongo(collection_name='crash'),
#                  Save_to_mongo(collection_name='person'),
#                  Save_to_mongo(collection_name='vehicle')], local_scheduler=True)


# run asynchronously
async def build_luigi_task(task):
    luigi.build([task], local_scheduler=True)

async def run_luigi_tasks():
    tasks = [
        fetch_from_mongo_and_save_to_postgres(collection_name='crash'),
        fetch_from_mongo_and_save_to_postgres(collection_name='person'),
        fetch_from_mongo_and_save_to_postgres(collection_name='vehicle')
    ]
    await asyncio.gather(*[build_luigi_task(task) for task in tasks])

begin=datetime.datetime.now()
asyncio.run(run_luigi_tasks())
luigi.build([Visualize_run_model()], local_scheduler=True)
# print(f'start_time: {begin}, end_time: {datetime.datetime.now()}')
print(f'run_time: {datetime.datetime.now() - begin} seconds')

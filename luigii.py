import json

import luigi
import pymongo
import os
import datetime
import pandas as pd
import numpy as np

from database.DatabaseUtil import DatabaseUtil
from database.models import map_collision_columns


class SaveToMongoDBTask(luigi.Task):
    """Task to save data to MongoDB."""

    # read csv
    cwd = os.getcwd()
    # dir = os.path.dirname(cwd)
    path = os.path.join(cwd, 'Data', 'crash_json.json').replace("\\", '/')
    file_path = path

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(self.file_path)

    def run(self):
        # Connect to MongoDB
        client = DatabaseUtil.get_mongo_client()
        db = client.motor_db
        collection = db['motor_collisions']

        data = json.load(self.file_path)
        df = pd.DataFrame(data["data"])

        new_names = pd.DataFrame(data['meta']['view']['columns'])['name'].tolist()
        old_names = df.keys().tolist()
        col_rename = list(zip(old_names, new_names))
        col_rename = {key: value for key, value in col_rename}
        df.rename(columns=col_rename, inplace=True)
        df = df[list(map_collision_columns.keys())]

        #insert records to mongo
        response_dict = DatabaseUtil.insert_mongo_chunk_records(None, db, 'motor_collisions', data)
        DatabaseUtil.close_mongo_client_connection(client)


if __name__ == '__main__':
    luigi.build([SaveToMongoDBTask()], local_scheduler=True)
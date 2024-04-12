import datetime

from database.DatabaseUtil import DatabaseUtil


class IO_ops():

    def df_to_dict_custom(df):
        cols = list(df)
        col_arr_map = {col: df[col].astype(object).to_numpy() for col in cols}
        records = []
        for i in range(len(df)):
            record = {col: col_arr_map[col][i] for col in cols}
            records.append(record)
        return records

    def truncate_mongo_doc(self, db, collection_name):
        db[collection_name].delete_many({})

    def insert_mongo_chunk_records(self, db, collection_name, records):
        IO_ops.truncate_mongo_doc(None, db, collection_name)
        collection = db[collection_name]
        if len(records) > 99999:
            records = [records[i: i + 50000] for i in range(0, len(records), 50000)]
            for record in list(records):
                collection.insert_many(list(record), ordered=False)
                print(datetime.datetime.now())
        else:
            collection.insert_many(records)
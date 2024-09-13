import datetime
import traceback
import io
from database.DatabaseUtil import DatabaseUtil
from sqlalchemy import MetaData, Table, tuple_, or_


class IO_ops():

    @staticmethod
    def df_to_dict_custom(df):
        cols = list(df)
        col_arr_map = {col: df[col].astype(object).to_numpy() for col in cols}
        records = []
        for i in range(len(df)):
            record = {col: col_arr_map[col][i] for col in cols}
            records.append(record)
        return records

    @staticmethod
    def truncate_mongo_doc(db, collection_name):
        db[collection_name].delete_many({})

    @staticmethod
    def insert_mongo_chunk_records(db, collection_name, records):
        IO_ops.truncate_mongo_doc(db, collection_name)
        collection = db[collection_name]
        if len(records) > 99999:
            records = [records[i: i + 50000] for i in range(0, len(records), 50000)]
            for record in list(records):
                collection.insert_many(list(record), ordered=False)
                print(datetime.datetime.now())
        else:
            collection.insert_many(records)

    @staticmethod
    def delete_table_data(table_name, schema_name='public', filter_column=None, filter_value=None,
                          session=None):
        # connection = DatabaseUtil.__engine.connect()
        session_f = DatabaseUtil.get_postgres_session() if session is None else session
        try:
            metadata = MetaData(schema=schema_name)
            table = Table(table_name, metadata, autoload_with=DatabaseUtil.get_postgres_engine())
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
    def save_to_postgres(df, table_name, schema='public', session=None, append=False):
        session_f = DatabaseUtil.get_postgres_session() if session is None else session
        cur = session_f.connection().connection.cursor()
        metadata = MetaData(schema=schema)
        table = Table(table_name, metadata, autoload_with=DatabaseUtil.get_postgres_engine())
        output = io.StringIO()
        success = True
        common_cols = {i.name for i in table.columns}.intersection(set(df.columns))
        cols = [col for col in df.columns if col in common_cols]
        try:
            if not append:
                IO_ops.delete_table_data(table_name=table_name, schema_name=schema, session=session)
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

    @staticmethod
    def insert_postgres_data(table, data, session=None):
        commit = False
        if session is None:
            commit = True
            session = DatabaseUtil.get_postgres_session()
        session.bulk_insert_mappings(table, data.to_dict(orient='records'))
        print("insert complete")
        if commit:
            session.commit()
            DatabaseUtil.close_postgres_session(session)
import numpy as np
import os

import pandas as pd
from dagster import op, Out, In, get_dagster_logger
from sqlalchemy import MetaData, Table

from database.DagsterDataFrames import *
from database.DatabaseUtil import DatabaseUtil
from database.IO_ops import IO_ops
from database.models import map_person_columns, Person

logger = get_dagster_logger()


# Define functions for each step

# Get Data from JSON and save to MongoDB
# read_person_saveToMogo: This function is used to Read data from JSON and load to MongoDB
@op(
    # out=Out(pd.DataFrame)
    out=Out(bool)
)
def read_person_saveToMogo() -> bool:
    result = True
    cwd = os.getcwd()
    path = os.path.join(cwd, 'Data', 'person.json').replace("\\", '/')
    file_path = path
    db_name = "dap"
    collection_name = "person"
    try:
        # Connect to the MongoDB database
        client = DatabaseUtil.get_mongo_client()
        persons_db = client[db_name]
        collection = persons_db[collection_name]

        # Discard collection if it already exists
        persons_db.drop_collection(collection_name)
        logger.info(f"Dropped already existing collection '{collection_name}'.")

        with open(file_path, 'r', encoding="utf-8") as file:
            data = pd.read_json(file)
            data = IO_ops.df_to_dict_custom(data)
            # v_json = json.load(file)

            logger.info("File loaded successfully")
            # logger.info(data.shape)

            # Connect to the Person database
            try:
                # Insert data into MongoDB collection
                IO_ops.insert_mongo_chunk_records(persons_db, collection_name, data)
                logger.info("Data Successfully Inserted to MongoDB.")
            except Exception as e:
                logger.error(f"Error: {e}")
                result = False
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False

    return result


# Data Extraction from MongoDB
# get_person_fromMogo : This function is used to read Data from MongoDB and perform initial tranformation if requried.
@op(
    ins={"start": In(bool)},
    out=Out(pd.DataFrame)
)
def get_person_fromMogo(start):  # > pd.DataFrame
    client = DatabaseUtil.get_mongo_client()
    db = client.dap
    collection_name = 'person'
    person_collection = db[collection_name]
    df = pd.DataFrame()
    try:
        cursor = person_collection.find({})
        df = pd.DataFrame(list(cursor))
        logger.info("Data Successfully Read from MongoDB.")
        return df
    except Exception as err:
        logger.error("Error: %s" % err)
        return df


# Data Transformation:
# This operation performs data transformation as required
@op(
    ins={"person_df": In(pd.DataFrame)},
    out=Out(PersonDataFrame)
)
def tranform_person(person_df):
    try:
        df = person_df
        # Transformation 1: Converting column names to lower case
        # df.columns = df.columns.str.lower()

        # transformation 1: convert crash time to datetime format, mapping right col headers
        df['CRASH_TIME'] = pd.to_datetime(df['CRASH_TIME'], format='mixed')
        df.rename(columns=map_person_columns, inplace=True)
        df = df[map_person_columns.values()]

        # Transformation 2:Converting temporal columns to correct data type
        df['crash_date'] = pd.to_datetime(df['crash_date'], format='mixed')

        # Transformation 5 : Remove rows with NaN values in IDs columns
        columns_to_check = ['collision_id', 'person_id']
        df.dropna(subset=columns_to_check, inplace=True)

        # transformation 6: replace nan with none
        df.replace({np.nan: None}, inplace=True)
        # Load your table model
        table_name = 'person'
        meta = MetaData()
        table = Table(table_name, meta, auto_load=True, autoload_with=DatabaseUtil.get_postgres_engine())
        # Get column names and data types
        column_data_types = {column.name: str(column.type) for column in table.columns}

        # transformation 7: convert int cols to int type
        int_cols = []
        for key, value in column_data_types.items():
            if column_data_types[key] == 'INTEGER':
                int_cols.append(key)
        df[int_cols] = df[int_cols].replace({None: 0}).astype(int)

        # transformation 8: remove null values in primary keys
        p_key = [str(x).split('.')[1] for x in list(table.primary_key)]
        for col in p_key:
            df = df[df[col].notna()]
            df = df[~df[col].isnull()]

        # transformation 9: drop duplicate rows
        df = df.drop_duplicates()

        logger.info("Data is transformed")
        return df
    except Exception as err:
        logger.error("Error: %s" % err)
        return df


# Export Data to Postgres
@op(
    ins={"person_df": In(PersonDataFrame)},
    out=Out(bool)
)
def save_person_to_postgresql(person_df) -> bool:
    result = True
    try:
        df = person_df
        table_name = 'person'
        DatabaseUtil.drop_postgres_table(table_name)  # drop table if it exists and recreate table based on data model
        session = DatabaseUtil.get_postgres_session()
        IO_ops.save_to_postgres(df, table_name, append=False, session=session)  # truncate and insert
        session.commit()

        result = True
        print("Data is successfully saved to PostgreSQL")
    except Exception as err:
        session.rollback()
        print("Error:", err)
        result = False
    finally:
        DatabaseUtil.close_postgres_session(session)

    return result


@op(
    ins={"start": In(bool)},
    out=Out(PersonDataFrame)
)
def get_person_from_postgresql(start):
    df = PersonDataFrame
    if not start:  # Check if the previous operation was successful
        logger.error("Data not loaded into PostgreSQL. Cannot fetch data.")
        return pd.DataFrame()  # Return an empty DataFrame or handle as appropriate
    table_name = 'person'
    # If data_loaded is True, proceed to fetch data
    try:
        session = DatabaseUtil.get_postgres_session()
        sq = session.query(Person)
        df = pd.read_sql(sq.statement, session.bind)
        logger.info(df.shape)


        df['crash_time'] = df['crash_time'].astype(str).astype('datetime64[ns]')
        df['crash_date'] = df['crash_date'].astype(str).astype('datetime64[ns]')

        logger.info("Data is successfully fetched from PostgreSQL")
        person_df = df
        return person_df
    except Exception as err:
        logger.error("Error: %s" % err)
        return pd.DataFrame()
    finally:
        DatabaseUtil.close_postgres_session(session)


'''def visualize(df):
    # Create a bokeh figure
    TOOLS = """hover,crosshair,pan,wheel_zoom,zoom_in,zoom_out,
    box_zoom,reset,tap,save,box_select,poly_select,
    lasso_select
    """

    p = figure(
        title='Figure 1',
        x_axis_label='Deaths',
        y_axis_label='Population',
        tools=TOOLS
    )
    p.scatter(
        df["deaths"],
        df["population"]
    )
    show(p)

    return True'''
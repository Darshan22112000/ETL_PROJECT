import pandas as pd
import numpy as np

from database.DatabaseUtil import DatabaseUtil
from database.models import Crash, map_crash_columns


# Get reference to 'motor_db' database
client = DatabaseUtil.get_mongo_client()
db = client.motor_db

# fetch all collections
collisions_collection = db.motor_collisions
collisions = pd.DataFrame(list(collisions_collection.find()))
# client.close()

#data cleaning/processing
collisions.replace({np.nan: None}, inplace=True)
collisions['CRASH TIME'] = pd.to_datetime(collisions['CRASH TIME'], format='mixed')
# collisions = collisions.loc[~collisions['CONTRIBUTING FACTOR VEHICLE 1'].isna()]
collisions.rename(columns=map_crash_columns, inplace=True)
collisions_df = collisions[map_crash_columns.values()]

try:
    session = DatabaseUtil.get_postgres_session()
    DatabaseUtil.save_to_postgres(collisions_df, str(Crash.metadata.sorted_tables[0]).split('.')[1],
                                            append=False, session=session) #truncate and insert
    # DatabaseUtil.insert_data(Collisions, collisions_df, session)
    session.commit()
except Exception as err:
    session.rollback()
    raise err
finally:
    DatabaseUtil.close_postgres_session(session)


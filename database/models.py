from sqlalchemy import Date, Column, String, Integer, TIMESTAMP, TIME

from database.DatabaseUtil import DatabaseUtil
# Project Imports
from database.base_model import BaseModel

config = DatabaseUtil.get_config()
BaseModel.init_base(config)
class Collisions(BaseModel.base):
    __tablename__ = 'motor_collisions'

    collision_id = Column(Integer, primary_key=True)
    crash_date = Column(Date)
    crash_time = Column(TIME)
    on_street_name = Column(String)
    off_street_name = Column(String)
    cross_street_name = Column(String)
    borough = Column(String)
    zip_code = Column(String)
    contributing_factor_vehicle_1 = Column(String)
    contributing_factor_vehicle_2 = Column(String)
    contributing_factor_vehicle_3 = Column(String)
    contributing_factor_vehicle_4 = Column(String)
    vehicle_type_code_1 = Column(String)
    vehicle_type_code_2 = Column(String)
    vehicle_type_code_3 = Column(String)
    vehicle_type_code_4 = Column(String)
    persons_injured = Column(Integer)
    persons_killed = Column(Integer)
    motorist_injured = Column(Integer)
    motorist_killed = Column(Integer)
    pedestrians_injured = Column(Integer)
    pedestrians_killed = Column(Integer)
    cyclist_injured = Column(Integer)
    cyclist_killed = Column(Integer)


map_collision_columns = {
    'COLLISION_ID': 'collision_id',
    'CRASH DATE': 'crash_date',
    'CRASH TIME': 'crash_time',
    'ON STREET NAME': 'on_street_name',
    'OFF STREET NAME': 'off_street_name',
    'CROSS STREET NAME': 'cross_street_name',
    'BOROUGH': 'borough',
    'ZIP CODE': 'zip_code',
    'CONTRIBUTING FACTOR VEHICLE 1': 'contributing_factor_vehicle_1',
    'CONTRIBUTING FACTOR VEHICLE 2': 'contributing_factor_vehicle_2',
    'CONTRIBUTING FACTOR VEHICLE 3': 'contributing_factor_vehicle_3',
    'CONTRIBUTING FACTOR VEHICLE 4': 'contributing_factor_vehicle_4',
    'VEHICLE TYPE CODE 1': 'vehicle_type_code_1',
    'VEHICLE TYPE CODE 2': 'vehicle_type_code_2',
    'VEHICLE TYPE CODE 3': 'vehicle_type_code_3',
    'VEHICLE TYPE CODE 4': 'vehicle_type_code_4',
    'NUMBER OF PERSONS INJURED': 'persons_injured',
    'NUMBER OF PERSONS KILLED': 'persons_killed',
    'NUMBER OF MOTORIST INJURED': 'motorist_injured',
    'NUMBER OF MOTORIST KILLED': 'motorist_killed',
    'NUMBER OF PEDESTRIANS INJURED': 'pedestrians_injured',
    'NUMBER OF PEDESTRIANS KILLED': 'pedestrians_killed',
    'NUMBER OF CYCLIST INJURED': 'cyclist_injured',
    'NUMBER OF CYCLIST KILLED': 'cyclist_killed'
}
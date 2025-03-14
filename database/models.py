from sqlalchemy import Date, Column, String, Integer, TIMESTAMP, TIME

from database.DatabaseUtil import DatabaseUtil
# Project Imports
from database.base_model import BaseModel

config = DatabaseUtil.get_config()
BaseModel.init_base(config)
class Crash(BaseModel.base):
    __tablename__ = 'crash'

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

class Person(BaseModel.base):
    __tablename__ = 'person'

    # person_id = Column(String, primary_key=True)
    person_id = Column(Integer, primary_key=True)
    collision_id = Column(Integer)
    crash_date = Column(Date)
    crash_time = Column(TIME)
    person_type = Column(String)
    person_injury = Column(String)
    vehicle_id = Column(Integer)
    person_age = Column(Integer)
    ejection = Column(String)
    emotional_status = Column(String)
    bodily_injury = Column(String)
    position_in_vehicle = Column(String)
    safety_equipment = Column(String)
    ped_location = Column(String)
    ped_action = Column(String)
    complaint = Column(String)
    ped_role = Column(String)
    contributing_factor_1 = Column(String)
    contributing_factor_2 = Column(String)
    person_sex = Column(String)


class Vehicle(BaseModel.base):
    __tablename__ = 'vehicle'

    collision_id = Column(Integer)
    crash_date = Column(Date)
    crash_time = Column(TIME)
    # vehicle_id = Column(String, primary_key=True)
    vehicle_id = Column(Integer, primary_key=True)
    state_registration = Column(String)
    vehicle_type = Column(String)
    vehicle_make = Column(String)
    vehicle_model = Column(String)
    vehicle_year = Column(Integer)
    travel_direction = Column(String)
    vehicle_occupants = Column(Integer)
    driver_sex = Column(String)
    driver_license_status = Column(String)
    driver_license_jurisdiction = Column(String)
    pre_crash = Column(String)
    point_of_impact = Column(String)
    vehicle_damage = Column(String)
    vehicle_damage_1 = Column(String)
    vehicle_damage_2 = Column(String)
    vehicle_damage_3 = Column(String)
    public_property_damage = Column(String)
    public_property_damage_type = Column(String)
    contributing_factor_1 = Column(String)
    contributing_factor_2 = Column(String)


map_crash_columns = {
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

map_person_columns = {
    'COLLISION_ID': 'collision_id',
    'CRASH_DATE': 'crash_date',
    'CRASH_TIME': 'crash_time',
    # 'PERSON_ID': 'person_id',
    'UNIQUE_ID': 'person_id',
    'PERSON_TYPE': 'person_type',
    'PERSON_INJURY': 'person_injury',
    'VEHICLE_ID': 'vehicle_id',
    'PERSON_AGE': 'person_age',
    'EJECTION': 'ejection',
    'EMOTIONAL_STATUS': 'emotional_status',
    'BODILY_INJURY': 'bodily_injury',
    'POSITION_IN_VEHICLE': 'position_in_vehicle',
    'SAFETY_EQUIPMENT': 'safety_equipment',
    'PED_LOCATION': 'ped_location',
    'PED_ACTION': 'ped_action',
    'COMPLAINT': 'complaint',
    'PED_ROLE': 'ped_role',
    'CONTRIBUTING_FACTOR_1': 'contributing_factor_1',
    'CONTRIBUTING_FACTOR_2': 'contributing_factor_2',
    'PERSON_SEX': 'person_sex'
}

map_vehicle_columns = {
    'COLLISION_ID': 'collision_id',
    'CRASH_DATE': 'crash_date',
    'CRASH_TIME': 'crash_time',
    # 'VEHICLE_ID': 'vehicle_id',
    'UNIQUE_ID': 'vehicle_id',
    'STATE_REGISTRATION': 'state_registration',
    'VEHICLE_TYPE': 'vehicle_type',
    'VEHICLE_MAKE': 'vehicle_make',
    'VEHICLE_MODEL': 'vehicle_model',
    'VEHICLE_YEAR': 'vehicle_year',
    'TRAVEL_DIRECTION': 'travel_direction',
    'VEHICLE_OCCUPANTS': 'vehicle_occupants',
    'DRIVER_SEX': 'driver_sex',
    'DRIVER_LICENSE_STATUS': 'driver_license_status',
    'DRIVER_LICENSE_JURISDICTION': 'driver_license_jurisdiction',
    'PRE_CRASH': 'pre_crash',
    'POINT_OF_IMPACT': 'point_of_impact',
    'VEHICLE_DAMAGE': 'vehicle_damage',
    'VEHICLE_DAMAGE_1': 'vehicle_damage_1',
    'VEHICLE_DAMAGE_2': 'vehicle_damage_2',
    'VEHICLE_DAMAGE_3': 'vehicle_damage_3',
    'PUBLIC_PROPERTY_DAMAGE': 'public_property_damage',
    'PUBLIC_PROPERTY_DAMAGE_TYPE': 'public_property_damage_type',
    'CONTRIBUTING_FACTOR_1': 'contributing_factor_1',
    'CONTRIBUTING_FACTOR_2': 'contributing_factor_2'
}
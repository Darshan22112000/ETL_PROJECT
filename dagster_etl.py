from dagster import job

from Analysis.analyse import *
from load_and_transform.vehicle_module import *
from load_and_transform.crash_module import *
from load_and_transform.person_module import *

@job
def etl():
    visualize_run_model(
   get_vehicle_from_postgresql(
       save_vehicle_to_postgresql(
           tranform_vehicle(
               get_vehicle_fromMogo(
                   read_vehicle_saveToMogo()
               )
           )
       )
   ),

   get_crash_from_postgresql(
       save_crash_to_postgresql(
           transform_crash_data(
               get_crash_fromMogo(
                   read_crash_saveToMogo()
               )
           )
       )
   ),

   get_person_from_postgresql(
       save_person_to_postgresql(
           tranform_person(
               get_person_fromMogo(
                   read_person_saveToMogo()
               )
           )
       )
   )

)
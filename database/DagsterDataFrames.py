from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type

VehicleDataFrame = create_dagster_pandas_dataframe_type(
name="VehicleDataFrame",
columns=[
        PandasColumn.integer_column(
            name="collision_id",
            non_nullable=True
        ),
        PandasColumn.datetime_column(
            name="crash_date",

        ),
        PandasColumn.datetime_column(
            name="crash_time",

        ),
        PandasColumn.integer_column(
            name="vehicle_id",

        ),
        PandasColumn.string_column(
            name="state_registration",

        ),
        PandasColumn.string_column(
            name="vehicle_type",

        ),
        PandasColumn.string_column(
            name="vehicle_make",

        ),
        PandasColumn.string_column(
            name="vehicle_model",

        ),
        PandasColumn.integer_column(
            name="vehicle_year",

        ),
        PandasColumn.string_column(
            name="travel_direction",

        ),
        PandasColumn.integer_column(
            name="vehicle_occupants",

        ),
        PandasColumn.string_column(
            name="driver_sex",

        ),
        PandasColumn.string_column(
            name="driver_license_status",

        ),
        PandasColumn.string_column(
            name="driver_license_jurisdiction",

        ),
        PandasColumn.string_column(
            name="pre_crash",

        ),
        PandasColumn.string_column(
            name="point_of_impact",

        ),
        PandasColumn.string_column(
            name="vehicle_damage",

        ),
        PandasColumn.string_column(
            name="vehicle_damage_1",

        ),
        PandasColumn.string_column(
            name="vehicle_damage_2",

        ),
        PandasColumn.string_column(
            name="vehicle_damage_3",

        ),
        PandasColumn.string_column(
            name="public_property_damage",

        ),
        PandasColumn.string_column(
            name="public_property_damage_type",

        ),
        PandasColumn.string_column(
            name="contributing_factor_1",

        ),
        PandasColumn.string_column(
            name="contributing_factor_2",

        )
    ]
)

CrashDataFrame = create_dagster_pandas_dataframe_type(
    name="CrashDataFrame",
    columns=[
        PandasColumn.integer_column(name="collision_id", non_nullable=True),
        PandasColumn.datetime_column(name="crash_date"),
        PandasColumn.datetime_column(name="crash_time"),
        PandasColumn.string_column(name="on_street_name"),
        PandasColumn.string_column(name="off_street_name"),
        PandasColumn.string_column(name="cross_street_name"),
        PandasColumn.string_column(name="borough"),
        PandasColumn.string_column(name="zip_code"),
        PandasColumn.string_column(name="contributing_factor_vehicle_1"),
        PandasColumn.string_column(name="contributing_factor_vehicle_2"),
        PandasColumn.string_column(name="contributing_factor_vehicle_3"),
        PandasColumn.string_column(name="contributing_factor_vehicle_4"),
        PandasColumn.string_column(name="vehicle_type_code_1"),
        PandasColumn.string_column(name="vehicle_type_code_2"),
        PandasColumn.string_column(name="vehicle_type_code_3"),
        PandasColumn.string_column(name="vehicle_type_code_4"),
        PandasColumn.integer_column(name="persons_injured"),
        PandasColumn.integer_column(name="persons_killed"),
        PandasColumn.integer_column(name="motorist_injured"),
        PandasColumn.integer_column(name="motorist_killed"),
        PandasColumn.integer_column(name="pedestrians_injured"),
        PandasColumn.integer_column(name="pedestrians_killed"),
        PandasColumn.integer_column(name="cyclist_injured"),
        PandasColumn.integer_column(name="cyclist_killed"),
    ]
)


PersonDataFrame = create_dagster_pandas_dataframe_type(
    name="PersonDataFrame",
    columns=[
        PandasColumn.integer_column(
            name="person_id",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="collision_id"
        ),
        PandasColumn.datetime_column(
            name="crash_date"
        ),
        PandasColumn.datetime_column(
            name="crash_time"
        ),
        PandasColumn.string_column(
            name="person_type"
        ),
        PandasColumn.string_column(
            name="person_injury"
        ),
        PandasColumn.integer_column(
            name="vehicle_id"
        ),
        PandasColumn.integer_column(
            name="person_age"
        ),
        PandasColumn.string_column(
            name="ejection"
        ),
        PandasColumn.string_column(
            name="emotional_status"
        ),
        PandasColumn.string_column(
            name="bodily_injury"
        ),
        PandasColumn.string_column(
            name="position_in_vehicle"
        ),
        PandasColumn.string_column(
            name="safety_equipment"
        ),
        PandasColumn.string_column(
            name="ped_location"
        ),
        PandasColumn.string_column(
            name="ped_action"
        ),
        PandasColumn.string_column(
            name="complaint"
        ),
        PandasColumn.string_column(
            name="ped_role"
        ),
        PandasColumn.string_column(
            name="contributing_factor_1"
        ),
        PandasColumn.string_column(
            name="contributing_factor_2"
        ),
        PandasColumn.string_column(
            name="person_sex"
        )
    ]
)

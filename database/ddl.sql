CREATE DATABASE dap;
CREATE USER dap WITH PASSWORD 'dap';
GRANT ALL PRIVILEGES ON DATABASE dap TO dap;

-- public.crash definition
 
-- Drop table
 
-- DROP TABLE public.crash;
 
CREATE TABLE public.crash (
	collision_id serial4 NOT NULL,
	crash_date date NULL,
	crash_time time NULL,
	on_street_name varchar NULL,
	off_street_name varchar NULL,
	cross_street_name varchar NULL,
	borough varchar NULL,
	zip_code varchar NULL,
	contributing_factor_vehicle_1 varchar NULL,
	contributing_factor_vehicle_2 varchar NULL,
	contributing_factor_vehicle_3 varchar NULL,
	contributing_factor_vehicle_4 varchar NULL,
	vehicle_type_code_1 varchar NULL,
	vehicle_type_code_2 varchar NULL,
	vehicle_type_code_3 varchar NULL,
	vehicle_type_code_4 varchar NULL,
	persons_injured int4 NULL,
	persons_killed int4 NULL,
	motorist_injured int4 NULL,
	motorist_killed int4 NULL,
	pedestrians_injured int4 NULL,
	pedestrians_killed int4 NULL,
	cyclist_injured int4 NULL,
	cyclist_killed int4 NULL
);
 
 
-- public.crash foreign keys
-- public.person definition   -- Drop table


-- public.person definition
 
-- Drop table
 
-- DROP TABLE public.person;
 
CREATE TABLE public.person (
	person_id serial4 NOT NULL,
	collision_id int4 NULL,
	crash_date date NULL,
	crash_time time NULL,
	person_type varchar NULL,
	person_injury varchar NULL,
	vehicle_id int4 NULL,
	person_age int4 NULL,
	ejection varchar NULL,
	emotional_status varchar NULL,
	bodily_injury varchar NULL,
	position_in_vehicle varchar NULL,
	safety_equipment varchar NULL,
	ped_location varchar NULL,
	ped_action varchar NULL,
	complaint varchar NULL,
	ped_role varchar NULL,
	contributing_factor_1 varchar NULL,
	contributing_factor_2 varchar NULL,
	person_sex varchar NULL
);
 
 
-- public.person foreign keys
-- public.vehicle definition   -- Drop tabl... by Darshan Vetal


-- public.vehicle definition
 
-- Drop table
 
-- DROP TABLE public.vehicle;
 
CREATE TABLE public.vehicle (
	collision_id int4 NULL,
	crash_date date NULL,
	crash_time time NULL,
	vehicle_id serial4 NOT NULL,
	state_registration varchar NULL,
	vehicle_type varchar NULL,
	vehicle_make varchar NULL,
	vehicle_model varchar NULL,
	vehicle_year int4 NULL,
	travel_direction varchar NULL,
	vehicle_occupants int4 NULL,
	driver_sex varchar NULL,
	driver_license_status varchar NULL,
	driver_license_jurisdiction varchar NULL,
	pre_crash varchar NULL,
	point_of_impact varchar NULL,
	vehicle_damage varchar NULL,
	vehicle_damage_1 varchar NULL,
	vehicle_damage_2 varchar NULL,
	vehicle_damage_3 varchar NULL,
	public_property_damage varchar NULL,
	public_property_damage_type varchar NULL,
	contributing_factor_1 varchar NULL,
	contributing_factor_2 varchar NULL
);
 
 
-- public.vehicle foreign keys

# DAP Project : Motor Collison

Dataset URLs:
https://catalog.data.gov/dataset/motor-vehicle-collisions-crashes
https://catalog.data.gov/dataset/motor-vehicle-collisions-vehicles
https://catalog.data.gov/dataset/motor-vehicle-collisions-person
or 
https://github.com/Darshan22112000/etl_project/tree/main/Data

Steps to be followed for code execution:

1.Installation Docker
  https://docs.docker.com/get-docker/

2. Create a folder in your local system where you run docker containers

3. Put 3 files in a folder given below (from docker_files):
  i. mongoDB.env
 ii. postgresql.env
iii. docker-compose.yml

4. Go to folder path in command prompt in above created folder

5. Run below command
	docker-compose up

6. Download postgreSQL(server) , pgadmin and mongodbcompass in your local system.

7. Create a new database in your PostgresSQL and in MongoDB using ddl.sql

8. Git clone project:- motocollision in your system, run below cmd
git clone https://github.com/Darshan22112000/etl_project.git

9. Open project in IDE

10. Goto terminal,run following command to download libraries

	pip install -r requirements.txt

11. Once all dependencied are installed, run dagster pipeline using below cmd
 dagit -f .\dagster_etl.py  
 
12. Goto Web dagster server using http://127.0.0.1:3000/  

13. Navigate to Launchpad--> Click on the Launchpad tab, then click on the Launch Run button located at the bottom right of the page.

14. When the job is completed, a new tab will open containing the Bokeh visualisation.

15. To Trigger Luigi job run below command in the terminal
python .\luigi_etl.py

16. When the job is completed, a new tab will open containing the Bokeh visualisation.




---
# ETL Project: Motor Collision Analysis

### Dataset URLs:
- [Motor Vehicle Collisions - Crashes](https://catalog.data.gov/dataset/motor-vehicle-collisions-crashes)
- [Motor Vehicle Collisions - Vehicles](https://catalog.data.gov/dataset/motor-vehicle-collisions-vehicles)
- [Motor Vehicle Collisions - Person](https://catalog.data.gov/dataset/motor-vehicle-collisions-person)

### Steps for Code Execution:

1. **Install Docker**: [Docker Installation Guide](https://docs.docker.com/get-docker/)
   
2. **Setup Docker Environment**:
   - Create a folder on your system for Docker containers.
   - Add the following files (from `docker_files`):
     - `mongoDB.env`
     - `postgresql.env`
     - `docker-compose.yml`
   - Open the folder in your terminal and run:
     ```
     docker-compose up
     ```

3. **Install PostgreSQL, pgAdmin, and MongoDB Compass**:
   - Create databases using the `ddl.sql` file.

4. **Clone Project**:
   ```
   git clone https://github.com/Darshan22112000/etl_project.git
   ```
   - Open the project in your IDE.

5. **Install Python Dependencies**:
   ```
   pip install -r requirements.txt
   ```

6. **Run Dagster Pipeline**:
   - Start the Dagster server:
     ```
     dagit -f .\dagster_etl.py
     ```
   - Access the server: [http://127.0.0.1:3000](http://127.0.0.1:3000/)
   - Navigate to "Launchpad", and click the **Launch Run** button.
   - A new tab will display Bokeh visualizations upon completion.

7. **Run Luigi Job**:
   ```
   python .\luigi_etl.py
   ```
   - A new tab will open with the Bokeh visualizations after job completion.

---

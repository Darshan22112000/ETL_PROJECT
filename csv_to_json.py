import csv
import json
import os
import pandas as pd

def csv_to_json(csv_file, json_file):
    # Read data from CSV file
    with open(csv_file, mode='r', encoding="UTF-8") as csv_file:
        csv_reader = pd.read_csv(csv_file)
    # Write data to JSON file
    csv_reader.to_json(json_file, orient='records')
    print("done")

# Example usage
cwd = os.getcwd()
csv_path = os.path.join(cwd, 'Data', f'crash.csv').replace("\\", '/')
json_path = os.path.join(cwd, 'Data', f'crash.json').replace("\\", '/')
csv_to_json(csv_path, json_path)
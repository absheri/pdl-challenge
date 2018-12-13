import json
import glob
from pprint import pprint


# How many of each are data type are there?
data_count = {}


def parse_data(data_count, json_data):
    if not isinstance(json_data, dict):
        if isinstance(json_data, list):
            return len(json_data)
        else:
            return 1
    for key in json_data:
        if key not in data_count:
            if isinstance(json_data[key], dict):
                data_count[key] = {}
            else:
                data_count[key] = 0
        data_count = parse_data(data_count[key], json_data[key])


for file in glob.glob('./data/*'):
    with open(file) as f:
        for line in f.readlines():
            json_data = json.loads(line.strip())
            parse_data(data_count, json_data)

pprint(data_count)
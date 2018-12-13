import recordlinkage
import pandas as pd
import json
import glob
from pandas.io.json import json_normalize


flattening_key = {"phone_numbers": "number",
                  "names": "clean",
                  "locations": "name",
                  "emails": "address",
                  "education": ['school', 'name'],
                  "gender": 'gender'}


def flatten(data, key):
    flattened = []
    for element in data[key]:
        if isinstance(flattening_key[key], list):
            flattened.append(element[flattening_key[key][0]][flattening_key[key][1]])
        else:
            flattened.append(element[flattening_key[key]])
    data[key] = flattened


def read_file(file):
    file_data = []
    with open(file) as f:
        for line in f.readlines():

            # Process the data into something that will fit into a dataframe
            data = json.loads(line.strip())

            flatten(data, 'phone_numbers')
            flatten(data, 'locations')
            flatten(data, 'emails')
            flatten(data, 'names')
            flatten(data, 'education')
            flatten(data, 'gender')
            profiles = {}
            for x in data['profiles']:
                profiles[x['network']] = x
                profiles[x['network']].pop('network', None)

            data['profiles'] = profiles

            file_data.append(data)

    return file_data


for file in glob.glob('./data/*'):
    print(read_file(file))

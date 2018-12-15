import recordlinkage
import pandas as pd
import json
import glob
from pandas.io.json import json_normalize
from recordlinkage.index import Full

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
            try:
                flattened.append(element[flattening_key[key][0]][flattening_key[key][1]])
            except:
                pass
        else:
            flattened.append(element[flattening_key[key]])
    if len(flattened) > 0:
        data[key] = flattened[0]
    else:
        data[key] = ''


def read_file(file):
    file_data = []
    with open(file) as f:
        line_num = 0
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
            data['identifier'] = "(" + file + "," + str(line_num) + ")"

            file_data.append(data)
            line_num += 1

    return file_data


def merge_data(df_a, df_b, indexes_in_b_to_drop):
    df_dropped_b = df_b.drop(df_b.index[indexes_in_b_to_drop])
    df_merged = df_a.append(df_dropped_b, ignore_index=True)

    return df_merged


files = glob.glob('./data/test-*')
df_a = json_normalize(read_file(files[0]))

# Keep a list of (file_name,lines) so that we can clean the data once we have found the duplicates.
lines_to_be_dropped = []

# Compare each of the files.
for file in files[1:]:
    df_b = json_normalize(read_file(file))

    indexer = recordlinkage.Index()

    # TODO: Need to decide on what values to block on.
    # indexer.block('names')
    indexer.add(Full())
    candidate_links = indexer.index(df_a, df_b)

    compare = recordlinkage.Compare()

    # TODO: Look at attributes to compare. Drop what is not needed.
    # TODO: Look at methods for comparing elements.
    compare.string('names', 'names', method='levenshtein', threshold=0.85)

    # The comparison vectors
    compare_vectors = compare.compute(candidate_links, df_a, df_b)

    # Classification step
    matches = compare_vectors[compare_vectors.sum(axis=1) >= 1]

    indexes_in_b_to_drop = [y for (x, y) in matches.index.values]

    # Keep a list of everything to drop for later.
    lines_to_be_dropped.append(df_b.ix[indexes_in_b_to_drop]['identifier'].tolist())

    # Merge the datasets so that we can compare to the next file.
    df_a = merge_data(df_a, df_b, indexes_in_b_to_drop)

# Print out all the names that were unique
print(df_a['names'].to_string())
print(lines_to_be_dropped)

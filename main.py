import recordlinkage
import pandas as pd
import numpy as np
import json
import glob
from pandas.io.json import json_normalize

flattening_key = {"phone_numbers": "number",
                  "names": "clean",
                  "locations": "name",
                  "emails": "address",
                  "education": ['school', 'name'],
                  "gender": "gender",
                  "birth_date": "date"}


def flatten(data, cleaned_data, key):
    # Flattens JSON data into one element.
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
        #TODO: I am taking only the first element here because this needs to fit into a  dataframe. Need to figure out how to handel multiple attributes.
        cleaned_data[key] = flattened[0]
    else:
        cleaned_data[key] = ''


def print_file_stats(file_data):
    # Prints out stats on parsed file info.
    aggregation = {}

    for entity in file_data:
        for key in entity:
            if key not in aggregation:
                aggregation[key] = {"count": 0, "unique": set()}
            if entity[key]:
                aggregation[key]["count"] += 1
                aggregation[key]["unique"].add(entity[key])
    print("File stats: ")
    for key in aggregation:
        print("{} -- Count: {}, Unique: {}".format(key, aggregation[key]["count"], len(aggregation[key]["unique"])))


def read_file(file):
    print("Reading file {}".format(file))
    file_data = []
    with open(file) as f:
        line_num = 0
        for line in f.readlines():
            # Process the data into something that will fit into a dataframe
            data = json.loads(line.strip())
            cleaned_data = {}

            # flatten(data, cleaned_data, 'phone_numbers')
            # flatten(data, cleaned_data, 'locations')
            # flatten(data, cleaned_data, 'emails')
            # flatten(data, cleaned_data, 'education')
            # flatten(data, cleaned_data, 'gender')
            flatten(data, cleaned_data, 'names')
            flatten(data, cleaned_data, 'birth_date')
            # profiles = {}
            # for x in data['profiles']:
            #     profiles[x['network']] = x
            #     profiles[x['network']].pop('network', None)
            #
            # cleaned_data['profiles'] = profiles
            cleaned_data['identifier'] = "(" + file + "," + str(line_num) + ")"

            file_data.append(cleaned_data)
            line_num += 1

    print_file_stats(file_data)
    return file_data


def write_cleaned_data(file, matches_to_keep):
    lines_to_keep = [int(x.replace(")", "").split(",")[1]) for x in matches_to_keep]
    file_data = []
    with open(file) as f:
        line_num = 0
        for line in f.readlines():
            if line_num in lines_to_keep:
                file_data.append(line)
            line_num += 1

    with open(file + "-cleaned", 'w') as f:
        for item in file_data:
            f.write(item)

def merge_data(df_a, df_b, indexes_in_a_to_drop):
    df_dropped_a = df_a.drop(df_a.index[indexes_in_a_to_drop])
    df_merged = df_b.append(df_dropped_a, ignore_index=True)

    return df_merged


files = glob.glob('./data/part-*')

# Link records within each of the files and write out the cleaned data.
for file in files:
    file_data = read_file(file)
    chunk_size = 100
    chunked_file_data = [file_data[i:i + chunk_size] for i in range(0, len(file_data), chunk_size)]

    df_a = json_normalize(chunked_file_data[0])
    for chunk in chunked_file_data[1:]:
        # Print out the identifier for the fist element so that we know how far along we are.
        print("Comparing {}".format(chunk[0]['identifier']))
        df_b = json_normalize(chunk)

        indexer = recordlinkage.Index()

        # Blocking on birth data because its a value that does not change over a persons life like name or address.
        indexer.block('birth_date')
        # indexer.add(Full())
        candidate_links = indexer.index(df_a, df_b)

        compare = recordlinkage.Compare()

        compare.string('names', 'names', method='damerau_levenshtein', threshold=0.85)
        # compare.string('phone_numbers', 'phone_numbers', method='damerau_levenshtein', threshold=0.85)
        # compare.string('emails', 'emails', method='damerau_levenshtein', threshold=0.85)
        # # Looks like there is a geographic comparer that could be used here if the locations are converted into WGS84 coordinate values.
        # compare.string('locations', 'locations', method='damerau_levenshtein', threshold=0.85)
        # compare.string('education', 'education', method='damerau_levenshtein', threshold=0.85)
        # compare.string('gender', 'gender', method='damerau_levenshtein', threshold=0.85)
        # compare.date('birth_date', 'birth_date')

        # The comparison vectors
        compare_vectors = compare.compute(candidate_links, df_a, df_b)

        # Classification step
        matches = compare_vectors[compare_vectors.sum(axis=1) >= 1]
        print("Found {} matches".format(len(matches)))

        # Use KMeansClassifier to pick matches
        # km = recordlinkage.KMeansClassifier()
        # matches = km.fit_predict(compare_vectors)

        matches_in_b = [y for (x, y) in matches.index.values]
        matches_in_a = [x for (x, y) in matches.index.values]

        # Merge the datasets so that we can compare to the next chunk.
        df_a = merge_data(df_a, df_b, matches_in_a)

        print("There are {} elements after merging".format(df_a.shape[0]))
        print()

    write_cleaned_data(file, df_a['identifier'].tolist())


# Once we have cleaned all of the files we can compare each of the files.
files = glob.glob('./data/part-*cleaned')
#TODO: Merge all of the files together. There is likly too much data here to use the aggregation method like above. Comparing all combinations of files might work better.
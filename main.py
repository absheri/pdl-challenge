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
    file_data = []
    with open(file) as f:
        line_num = 0
        for line in f.readlines():

            # Process the data into something that will fit into a dataframe
            data = json.loads(line.strip())
            cleaned_data = {}

            flatten(data, cleaned_data, 'phone_numbers')
            flatten(data, cleaned_data, 'locations')
            flatten(data, cleaned_data, 'emails')
            flatten(data, cleaned_data, 'names')
            flatten(data, cleaned_data, 'education')
            flatten(data, cleaned_data, 'gender')
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


def merge_data(df_a, df_b, indexes_in_b_to_drop):
    df_dropped_b = df_b.drop(df_b.index[indexes_in_b_to_drop])
    df_merged = df_a.append(df_dropped_b, ignore_index=True)

    return df_merged


files = glob.glob('./data/part-*')
df_a = json_normalize(read_file(files[0]))

# Keep a list of (file_name,lines) so that we can clean the data once we have found the duplicates.
lines_to_be_dropped = []

# Compare each of the files.
for file in files[1:]:
    df_b = json_normalize(read_file(file))

    indexer = recordlinkage.Index()

    # Blocking on birth data because its a value that does not change over a persons life like name or address.
    indexer.block('birth_date')
    candidate_links = indexer.index(df_a, df_b)

    compare = recordlinkage.Compare()

    # TODO: Look at attributes to compare. Drop what is not needed.
    # TODO: Look at methods for comparing elements.
    compare.string('names', 'names', method='damerau_levenshtein', threshold=0.85)
    compare.string('phone_numbers', 'phone_numbers', method='damerau_levenshtein', threshold=0.85)
    compare.string('emails', 'emails', method='damerau_levenshtein', threshold=0.85)
    # Looks like there is a geographic comparer that could be used here if the locations are converted into WGS84 coordinate values.
    compare.string('locations', 'locations', method='damerau_levenshtein', threshold=0.85)
    compare.string('education', 'education', method='damerau_levenshtein', threshold=0.85)
    compare.string('gender', 'gender', method='damerau_levenshtein', threshold=0.85)
    compare.date('birth_date', 'birth_date')

    # The comparison vectors
    compare_vectors = compare.compute(candidate_links, df_a, df_b)

    # Classification step
    matches = compare_vectors[compare_vectors.sum(axis=1) >= 1]

    # Use KMeansClassifier to pick matches
    # km = recordlinkage.KMeansClassifier()
    # matches = km.fit_predict(compare_vectors)

    indexes_in_b_to_drop = [y for (x, y) in matches.index.values]

    # Keep a list of everything to drop for later.
    lines_to_be_dropped.append(df_b.ix[indexes_in_b_to_drop]['identifier'].tolist())

    # Merge the datasets so that we can compare to the next file.
    df_a = merge_data(df_a, df_b, indexes_in_b_to_drop)

# Print out all the names that were unique
print(df_a['names'].to_string())
print(lines_to_be_dropped)

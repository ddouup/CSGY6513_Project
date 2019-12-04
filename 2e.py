import json

from dateutil import parser
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions, types

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("proj").getOrCreate()


######################## Main ########################

semantic_types = [
    'person_name',
    'business_name',
    'phone_number',
    'address',
    'street_name',
    'city',
    'neighborhood',
    'coordinates',
    'zip',
    'borough',
    'school_name',
    'color',
    'car_make',
    'city_agency',
    'areas_of_study',
    'subjects_in_school',
    'university_names',
    'websites',
    'building_classification',
    'vehicle_type',
    'type_of_location',
    'parks_or_playgrounds'
]

# 1. list the working subset
with open('./cluster2.txt') as f:
    cluster = json.loads(f.read().replace("'", '"'))

with open('result') as f:
    ground_truth = None  # dict

# 2. for each working dataset
for filename in cluster:
    # filename = cluster[0]
    print('>> entering {}'.format(filename).encode('utf-8'))

    # 2.1 load the corresponding dataset profile
    [dataset_name, column_name] = filename.split('.').slice(2)
    with open('{}.spec.json'.format(dataset_name), 'r') as f:
        output = json.load(f)

    # 2.2 load the corresponing column profile
    for column in output['columns']:
        if column['column_name'] == column_name:
            label_threshold = 0.6 * (column['number_non_empty_cells'] + column['number_empty_cells'])

            # 2.4 load the corresponing column semantic profile
            label = []
            for x in column['semantic_types']:
                semantic_type = x['semantic_type']
                count = x['count']

                # 2.5 exceeds threshold => attach label
                if count > label_threshold:
                    label.append(semantic_type)
                
                # TODO: 2.6 stats 

            # 
            break # 2.2 corresponing column profile
    else:
        raise ValueError

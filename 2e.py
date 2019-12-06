import csv
import json

from dateutil import parser
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("proj").getOrCreate()


######################## Util ########################

def filter_ground_truth(semantic_type):
    return {k: v for [k, v] in ground_truth.items() if semantic_type in v}


######################## Main ########################

semantic_types_threshold = {
    'person_name': 0.5,
    'business_name': 0.5,
    'phone_number': 0.5,
    'address': 0.5,
    'street_name': 0.5,
    'city': 0.5,
    'neighborhood': 0.5,
    'coordinates': 0.5,
    'zip': 0.5,
    'borough': 0.5,
    'school_name': 0.5,
    'color': 0.5,
    'car_make': 0.5,
    'city_agency': 0.5,
    'areas_of_study': 0.5,
    'subjects_in_school': 0.5,
    'university_names': 0.5,
    'websites': 0.5,
    'building_classification': 0.5,
    'vehicle_type': 0.5,
    'type_of_location': 0.5,
    'parks_or_playgrounds': 0.5,
}

correct_postive = {semantic_type: 0 for semantic_type in semantic_types_threshold}
predict_postive = {semantic_type: 0 for semantic_type in semantic_types_threshold}
truth_postive = {semantic_type: 0 for semantic_type in semantic_types_threshold}

# 1. list the working subset
with open('./cluster2.txt') as f:
    cluster = json.loads(f.read().replace("'", '"'))

with open('./DF_Label.csv') as f:
    ground_truth = {
        filename: {
            semantic_type
            for semantic_type in labels if semantic_type is not ''
        }
        for [filename, *labels] in csv.reader(f)
    }

# 2. for each working dataset
for filename in cluster:
    # filename = cluster[0]
    [dataset_name, column_name] = filename.split('.').slice(2)
    print(u'>> entering {}'.format(filename))

    # 2.1 load dataset
    dataset = (spark.read.format('csv')
               .options(inferschema='true', sep='\t')
               .load('/user/hm74/NYCColumns/{}'.format(filename))
               .toDF('value', 'count'))

    # 2.2 count dataset rows
    dataset_count = dataset.select(F.sum('count')).collect()[0][0]

    # 2.3 load the corresponding semantic profile
    with open('task2.{}.json'.format(filename)) as f:
        output = json.load(f)

    # 2.4 exceeds threshold => attach label
    labels = []
    for entry in output['semantic_types']:
        semantic_type = entry['semantic_type']
        count = entry['count']
        if count > semantic_types_threshold[semantic_type] * dataset_count:
            labels.append(semantic_type)

    # 2.5 evaluate
    for semantic_type in labels:
        predict_postive[semantic_type] += 1
        if semantic_type in ground_truth[filename]:
            correct_postive[semantic_type] += 1
    for semantic_type in ground_truth[filename]:
        truth_postive[semantic_type] += 1

json.dumps(correct_postive)
json.dumps(predict_postive)
json.dumps(truth_postive)

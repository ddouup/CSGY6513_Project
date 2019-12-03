import json

from dateutil import parser
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions, types

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("proj").getOrCreate()


######################## Utils ########################

def count_person_name(dataset):
    ret = count


def count_business_name(dataset):
    ret = count


def count_phone_number(dataset):
    ret = count


def count_address(dataset):
    ret = count


def count_street_name(dataset):
    ret = count


def count_city(dataset):
    ret = count


def count_neighborhood(dataset):
    ret = count


def count_coordinates(dataset):
    ret = count


def count_zip(dataset):
    ret = count


def count_borough(dataset):
    ret = count


def count_school_name(dataset):
    ret = count


def count_color(dataset):
    ret = count


def count_car_make(dataset):
    ret = count


def count_city_agency(dataset):
    ret = count


def count_areas_of_study(dataset):
    ret = count


def count_subjects_in_school(dataset):
    ret = count


def count_university_names(dataset):
    ret = count


def count_websites(dataset):
    ret = count


def count_building_classification(dataset):
    ret = count


def count_vehicle_type(dataset):
    ret = count


def count_type_of_location(dataset):
    ret = count


def count_parks_or_playgrounds(dataset):
    ret = count


######################## Main ########################

semantic_types = {
    'person_name': count_person_name,
    'business_name': count_business_name,
    'phone_number': count_phone_number,
    'address': count_address,
    'street_name': count_street_name,
    'city': count_city,
    'neighborhood': count_neighborhood,
    'coordinates': count_coordinates,
    'zip': count_zip,
    'borough': count_borough,
    'school_name': count_school_name,
    'color': count_color,
    'car_make': count_car_make,
    'city_agency': count_city_agency,
    'areas_of_study': count_areas_of_study,
    'subjects_in_school': count_subjects_in_school,
    'university_names': count_university_names,
    'websites': count_websites,
    'building_classification': count_building_classification,
    'vehicle_type': count_vehicle_type,
    'type_of_location': count_type_of_location,
    'parks_or_playgrounds': count_parks_or_playgrounds
}


def profile_semantic(dataset):
    confirm_threshold = 0.95 * dataset.count()
    ret = []
    for semantic_type in semantic_types:
        count = semantic_types[semantic_type](dataset)
        ret.append({'semantic_type': semantic_type, 'count': count})
        if count > confirm_threshold:
            break
    return ret


# 1. list the working subset
with open('./cluster2.txt') as f:
    cluster = json.loads(f.read().replace("'", '"'))

# 2. for each working dataset
for filename in cluster:
    # filename = cluster[0]
    print('>> entering {}'.format(filename).encode('utf-8'))

    # 2.1 load dataset
    dataset = (spark.read.format('csv')
               .options(inferschema='true', sep='\t')
               .load('/user/hm74/NYCColumns/{}'.format(filename))
               .toDF('value', 'count'))

    # 2.2 load the corresponding dataset profile
    [dataset_name, column_name] = filename.split('.').slice(2)
    with open('{}.spec.json'.format(dataset_name), 'r') as f:
        output = json.load(f)

    # 2.3 load the corresponing column profile
    for column in output['columns']:
        if column['column_name'] == column_name:
            # 2.4 create column semantic profile
            column['semantic_types'] = profile_semantic(dataset)
            break

    # 2.5 dump updated dataset profile as json
    with open('{}.spec.json'.format(dataset_name), 'w') as f:
        json.dump(output, f, indent=2)

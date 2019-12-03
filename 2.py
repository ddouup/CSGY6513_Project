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

def count_business_name(dataset):
    ret = count

def count_business_name(dataset):
    ret = count

def count_business_name(dataset):
    ret = count

def count_business_name(dataset):
    ret = count

def count_business_name(dataset):
    ret = count

def count_business_name(dataset):
    ret = count

def count_business_name(dataset):
    ret = count

with open('./cluster2.txt') as f:
    cluster = json.loads(f.read().replace("'", '"'))

for filename in cluster:
    print('>> entering {}'.format(filename).encode('utf-8'))

    # 2.1 load dataset
    dataset = (spark.read.format('csv')
               .options(inferschema='true', sep='\t')
               .load('/user/hm74/NYCColumns/{}'.format(filename))
               .toDF('value', 'count'))

    xxxx = foo(dataset)

def foo(dataset):
    if is_person_name() / num_rows > threshold:
        person_name= True
    if is_business_name() /  num_rows > threshold:
        person_name= True
    

import json

from dateutil import parser
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions, types

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("proj").getOrCreate()


######################## Utils ########################

with open('./cluster2.txt') as f:
    cluster = json.loads(f.read().replace("'", '"'))

for filename in cluster:
    print('>> entering {}'.format(filename).encode('utf-8'))

    # 2.1 load dataset
    dataset = (spark.read.format('csv')
               .options(inferschema='true', sep='\t')
               .load('/user/hm74/NYCColumns/{}'.format(filename))
               .toDF('value', 'count'))


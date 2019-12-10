import csv
import datetime
import json
import os
import re

from dateutil import parser
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("proj").getOrCreate()


def task1():
    ret = {}
    for filename in os.listdir('/scratch/ql1045/'):
        if filename.startswith('task1.'):
            with open(f'/scratch/ql1045/{filename}') as f:
                parsed = json.load(f)
                for column in j['columns']:
                    for data_type in list(column['data_types']):
                        if data_type['type'] == 'INTEGER (LONG)':
                            int_cap = data_type['count']
                        elif data_type['type'] == 'REAL':
                            double_cap = data_type['count']
                            if double_cap == int_cap:
                                column['data_types'].remove(data_type)
                        elif data_type['type'] == 'DATE/TIME':
                            date_cap = data_type['count']
                            if date_cap == int_cap or date_cap == double_cap:
                                column['data_types'].remove(data_type)
                ret[re.search(r'[\d\w]{4}[-][\d\w]{4}', filename).group(0)] = parsed
    json.dump(ret, f, indent=2)


def task2_manual_label():
    ground_truth = {
        filename: [
            # label.filter(e => e)
            semantic_type
            for semantic_type in labels if semantic_type
        ]
        # for each line
        for [filename, *labels] in (spark.read.format('csv')
                                    .options(header='true', inferschema='true')
                                    .load('/user/ql1045/proj-in/DF_Label.csv')
                                    .toLocalIterator())
    }
    with open('task2-manual-labels.json', 'w') as f:
        json.dump(ground_truth, f, indent=2)


import json

from dateutil import parser
from pyspark import SparkContext
from pyspark.sql import functions, types

from to_date_robust import to_date_robust

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("proj").getOrCreate()

######################## Utils ########################


def to_iso_string(x):
    try:
        return parser.parse(x).isoformat()
    except:
        return None


to_iso_string_udf = functions.udf(to_iso_string)

# converts a string column to date column with best effort
def to_date_robust(x): return functions.to_date(to_iso_string_udf(x))

######################## Main ########################


# 1. list all filenames and titles
datasets = (spark.read.format('csv')
            .options(inferschema='true', sep='\t')
            .load('/user/hm74/NYCOpenData/datasets.tsv')
            .toDF('filename', 'title'))

# 2. for each dataset
for filename, title in datasets.toLocalIterator():
# filename, title = next(datasets.toLocalIterator())
    print('>> entering {}.tsv.gz: {}'.encode('utf-8'), filename, title)

    # 2.1 load dataset
    dataset = (spark.read.format('csv')
               .options(header='true', inferschema='true', sep='\t')
               .load('/user/hm74/NYCOpenData/{}.tsv.gz'.format(filename)))

    # 2.2 create dataset profile
    output = {'dataset_name': '', 'columns': [], 'key_column_candidates': []}
    output['dataset_name'] = filename

    # 2.3 for each column
    for column in dataset.schema:
    # schema = iter(dataset.schema)
    # column = next(schema)

        # 2.3.1 create column profile
        name = column.name
        dataType = column.dataType
        column_output = {
            'column_name': '',
            'number_non_empty_cells': 0,
            'number_empty_cells': 0,
            'number_distinct_values': 0,
            'frequent_values': [],
            'data_types': [],
            'semantic_types': [
                {'semantic_type': '',  'count': 0},
            ],
        }
        column_output['column_name'] = name
        column_output['number_non_empty_cells'] = dataset.filter(dataset[name].isNotNull()).count()
        column_output['number_empty_cells'] = dataset.filter(dataset[name].isNull()).count()
        column_output['number_distinct_values'] = dataset.select(dataset[name]).distinct().count()
        column_output['frequent_values'] = [x[0] for x in (dataset.groupBy(dataset[name]).count()
                                                        .orderBy(functions.desc('count'))
                                                        .select(dataset[name])
                                                        .take(5))]

        # 2.3.2 for each dataType
        # 2.3.2.1 create integer profile
        if dataType is types.IntegerType or dataType is types.LongType:
            data_output = {
                "type": "INTEGER (LONG)",
                "count": 0,
                "max_value": 0,
                "min_value": 0,
                "mean": 0,
                "stddev": 0,
            }
            data_output['count'] = dataset.select(dataset[name]).count()
            data_output['min_value'] = dataset.select(functions.min(dataset[name])).collect()[0][0]
            data_output['max_value'] = dataset.select(functions.max(dataset[name])).collect()[0][0]
            data_output['mean'] = dataset.select(functions.mean(dataset[name])).collect()[0][0]
            data_output['stddev'] = dataset.select(functions.stddev(dataset[name])).collect()[0][0]
            
        # 2.3.2.2 create real profile
        elif dataType is types.DoubleType or dataType is types.FloatType:
            data_output = {
                "type": "REAL",
                "count": 0,
                "max_value": 0,
                "min_value": 0,
                "mean": 0,
                "stddev": 0,
            }
            data_output['count'] = dataset.select(dataset[name]).count()
            data_output['min_value'] = dataset.select(functions.min(dataset[name])).collect()[0][0]
            data_output['max_value'] = dataset.select(functions.max(dataset[name])).collect()[0][0]
            data_output['mean'] = dataset.select(functions.mean(dataset[name])).collect()[0][0]
            data_output['stddev'] = dataset.select(functions.stddev(dataset[name])).collect()[0][0]

        # 2.3.2.3 create date profile
        elif dataType is types.DateType or dataType is types.TimestampType:
            data_output = {
                "type": "DATE/TIME",
                "count": 0,
                "max_value": '',
                "min_value": '',
            }
            data_output['count'] = dataset.select(dataset[name]).count()
            data_output['min_value'] = dataset.select(functions.min(dataset[name])).collect()[0][0]
            data_output['max_value'] = dataset.select(functions.max(dataset[name])).collect()[0][0]
        
        # 2.3.2.4 create string profile
        elif dataType is types.StringType:
            data_output = {
                "type": "TEXT",
                "count": 0,
                "shortest_values": [],
                "longest_values": [],
                "average_length": 0,
            }

            # 2.3.2.4.1 convert to string length
            # TODO: fill profile

            # 2.3.2.4.2 try to cast to int
            data_int = dataset.select(dataset[name].cast(types.LongType()).alias(name))
            # TODO: extract 2.3.2 as a separate method. call that function here.

            # 2.3.2.4.3 try to cast to real
            data_double = dataset.select(dataset[name].cast(types.DoubleType()).alias(name))

            # 2.3.2.4.4 try to cast to date
            data_date = dataset.select(to_date_robust(dataset[name]).alias(name))
        else:
            raise NotImplementedError

        # 2.3.3 add column to output
        output['columns'].append(column_output)

    # 2.4 dump dataset profile as json
    json.dump(output, open('{}.spec.json'.format(filename)))


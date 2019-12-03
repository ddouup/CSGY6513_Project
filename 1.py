import json

from dateutil import parser
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions, types

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


def to_date_robust(x):
    '''converts a string column to date column with best effort'''
    return functions.to_date(to_iso_string_udf(x))

######################## Main ########################


def profile_datatype(dataset, name):
    dataType = dataset.schema[name].dataType
    if isinstance(dataType, types.IntegerType) or isinstance(dataType, types.LongType):
        ret = {
            "type": "INTEGER (LONG)",
            "count": 0,
            "max_value": 0,
            "min_value": 0,
            "mean": 0,
            "stddev": 0,
        }
        ret['count'] = dataset.select(dataset[name]).count()
        ret['min_value'] = dataset.select(functions.min(dataset[name])).collect()[0][0]
        ret['max_value'] = dataset.select(functions.max(dataset[name])).collect()[0][0]
        ret['mean'] = dataset.select(functions.mean(dataset[name])).collect()[0][0]
        ret['stddev'] = dataset.select(functions.stddev(dataset[name])).collect()[0][0]
    elif isinstance(dataType, types.DoubleType) or isinstance(dataType, types.FloatType):
        ret = {
            "type": "REAL",
            "count": 0,
            "max_value": 0,
            "min_value": 0,
            "mean": 0,
            "stddev": 0,
        }
        ret['count'] = dataset.select(dataset[name]).count()
        ret['min_value'] = dataset.select(functions.min(dataset[name])).collect()[0][0]
        ret['max_value'] = dataset.select(functions.max(dataset[name])).collect()[0][0]
        ret['mean'] = dataset.select(functions.mean(dataset[name])).collect()[0][0]
        ret['stddev'] = dataset.select(functions.stddev(dataset[name])).collect()[0][0]
    elif isinstance(dataType, types.DateType) or isinstance(dataType, types.TimestampType):
        ret = {
            "type": "DATE/TIME",
            "count": 0,
            "max_value": '',
            "min_value": '',
        }
        ret['count'] = dataset.select(dataset[name]).count()
        ret['min_value'] = str(dataset.select(functions.min(dataset[name])).collect()[0][0])
        ret['max_value'] = str(dataset.select(functions.max(dataset[name])).collect()[0][0])
    elif isinstance(dataType, types.StringType):
        ret = {
            "type": "TEXT",
            "count": 0,
            "shortest_values": [],
            "longest_values": [],
            "average_length": 0,
        }
        data_str_length = dataset.select(dataset[name], functions.length(dataset[name]).alias('_len'))
        ret['count'] = dataset.select(dataset[name]).count()
        ret['shortest_values'] = [x for [x] in (data_str_length
                                                .filter(data_str_length['_len'] == data_str_length.select(functions.min(data_str_length['_len'])).collect()[0][0])
                                                .select(data_str_length[name])
                                                .distinct()
                                                .collect())]
        ret['longest_values'] = [x for [x] in (data_str_length
                                               .filter(data_str_length['_len'] == data_str_length.select(functions.max(data_str_length['_len'])).collect()[0][0])
                                               .select(data_str_length[name])
                                               .distinct()
                                               .collect())]
        ret['average_length'] = data_str_length.select(functions.mean(data_str_length['_len'])).collect()[0][0]
    else:
        raise NotImplementedError
    return ret


# 1. list all filenames and titles
datasets = (spark.read.format('csv')
            .options(inferschema='true', sep='\t')
            .load('/user/hm74/NYCOpenData/datasets.tsv')
            .toDF('filename', 'title'))

# 2. for each dataset
for filename, title in datasets.toLocalIterator():
    # filename, title = next(datasets.toLocalIterator())
    print('>> entering {}.tsv.gz: {}'.format(filename, title).encode('utf-8'))

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
        column_output['frequent_values'] = [x for [x] in (dataset.groupBy(dataset[name]).count()
                                                          .orderBy(functions.desc('count'))
                                                          .select(dataset[name])
                                                          .take(5))]

        # 2.3.2 create datatype profile
        column_output['data_types'] = [profile_datatype(dataset, name)]

        # 2.3.3 datatype indefinite => try other types
        if isinstance(column.dataType, types.StringType):
            cast = dataset.select(dataset[name].cast(types.LongType()).alias(name))
            if cast.filter(cast[name].isNotNull()).count():
                column_output['data_types'].append(profile_datatype(cast, name))
            cast = dataset.select(dataset[name].cast(types.DoubleType()).alias(name))
            if cast.filter(cast[name].isNotNull()).count():
                column_output['data_types'].append(profile_datatype(cast, name))
            cast = dataset.select(to_date_robust(dataset[name]).alias(name))
            if cast.filter(cast[name].isNotNull()).count():
                column_output['data_types'].append(profile_datatype(cast, name))

        # 2.3.4 all distinct => key candidate
        if column_output['number_distinct_values'] == dataset.count():
            output['key_column_candidates'].append(name)

        # 2.3.5 add column to output
        output['columns'].append(column_output)

    # 2.4 dump dataset profile as json
    json.dump(output, open('{}.spec.json'.format(filename), 'w'), indent=2)

from to_date_robust import to_date_robust
from pyspark.sql import functions, types
import json

datasets = (spark.read.format('csv')
            .options(inferschema='true', sep='\t')
            .load('/user/hm74/NYCOpenData/datasets.tsv')
            .toDF('filename', 'title'))

number_non_empty_cells = dataset.select([count(c) for c in dataset.columns])
number_empty_cells = dataset.select([count(when (col(c).isNull(), c)).alias(c) for c in dataset.columns])
number_distinct_values = dataset.select([countDistinct(c).alias(c) for c in dataset.columns])
frequent_values = [dataset.groupBy(c).count().orderBy(desc('count')).limit(5).select(c) for c in dataset.columns]

ret = dataset.describe().collect()
ret = dataset.select(dataset.xx).distinct().count()

filename = 'sm48-t3s6'
for filename, title in datasets.rdd.toLocalIterator():
    print('>> entering {}.tsv.gz: {}'.encode('utf-8'), filename, title)
    dataset = (spark.read.format('csv')
               .options(header='true', inferschema='true', sep='\t')
               .load('/user/hm74/NYCOpenData/{}.tsv.gz'.format(filename)))
    output = {'dataset_name': '', 'columns': [], 'key_column_candidates': []}
    output['dataset_name'] = filename
    for column in dataset.schema:
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
                                                           .limit(5).collect())]
        if dataType is types.IntegerType or dataType is types.LongType:
            data_output = {
                "type": "INTEGER (LONG)",
                "count": 0,
                "max_value": 0,
                "min_value": 0,
                "mean": 0,
                "stddev": 0,
            }
        elif dataType is types.DoubleType or dataType is types.FloatType:
            data_output = {
                "type": "REAL",
                "count": 0,
                "max_value": 0,
                "min_value": 0,
                "mean": 0,
                "stddev": 0,
            }
        elif dataType is types.DateType or dataType is types.TimestampType:
            data_output = {
                "type": "DATE/TIME",
                "count": 0,
                "max_value": '',
                "min_value": '',
            }
        elif dataType is types.StringType:
            data_output = {
                "type": "TEXT",
                "count": 0,
                "shortest_values": [],
                "longest_values": [],
                "average_length": 0,
            }
        else:
            raise NotImplementedError
        output['columns'].append(column_output)
    json.dump(output, open('{}.spec.json'.format(filename)))

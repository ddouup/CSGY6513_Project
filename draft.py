from pyspark.sql import functions
from dateutil import parser
datasets = spark.read.format('csv').options(inferschema='true', sep='\t').load(
    '/user/hm74/NYCOpenData/datasets.tsv').toDF('filename', 'title')

filename = '7299-2etw'
dataset = spark.read.format('csv').options(header='true', inferschema='true', sep='\t').load(
    '/user/hm74/NYCOpenData/{}.tsv.gz'.format(filename))
print(filename.encode('utf-8'), title.encode('utf-8'))

number_non_empty_cells = dataset.select([count(c) for c in dataset.columns])
number_empty_cells = dataset.select([count(when (col(c).isNull(), c)).alias(c) for c in dataset.columns])
number_distinct_values = dataset.select([countDistinct(c).alias(c) for c in dataset.columns])
frequent_values = [dataset.groupBy(c).count().orderBy(desc('count')).limit(5).select(c) for c in dataset.columns]


def to_iso_string(x):
    try:
        return parser.parse(x).isoformat()
    except:
        return None


to_iso_string_udf = functions.udf(to_iso_string)


def to_date_robust(x): return functions.to_date(to_iso_string_udf(x))


dataset.select(to_date_robust(dataset['Day & Date'])).show()

# to_date_robust

ret = dataset.describe().collect()
ret = dataset.select(dataset.xx).distinct().count()

for filename, title in datasets.rdd.toLocalIterator():
    output = {'dataset_name': '', 'columns': [], 'key_column_candidates': []}
    column = {
        'column_name': '',
        'number_non_empty_cells': 0,
        'number_empty_cells': 0,
        'number_distinct_values': 0,
        'frequent_values': [],
        "data_types": [
            {
                "type": "INTEGER (LONG)",
                "count": 0,
                "max_value": 0,
                "min_value": 0,
                "mean": 0,
                "stddev": 0,
            },
            {
                "type": "REAL",
                "count": 0,
                "max_value": 0,
                "min_value": 0,
                "mean": 0,
                "stddev": 0,
            },
            {
                "type": "DATE/TIME",
                "count": 0,
                "max_value": '',
                "min_value": '',
            },
            {
                "type": "TEXT",
                "count": 0,
                "shortest_values": [],
                "longest_values": [],
                "average_length": 0,
            },
        ],
        "semantic_types": [
            {"semantic_type": '',  "count": 0},
        ],
    }

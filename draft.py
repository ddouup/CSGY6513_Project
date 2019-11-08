datasets = spark.read.format('csv').options(inferschema='true', sep='\t').load(
    '/user/hm74/NYCOpenData/datasets.tsv').toDF('filename', 'title')

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
    dataset = spark.read.format('csv').options(header='true', inferschema='true', sep='\t').load(
        '/user/hm74/NYCOpenData/{}.tsv.gz'.format(filename))
    print(filename.encode('utf-8'), title.encode('utf-8'))

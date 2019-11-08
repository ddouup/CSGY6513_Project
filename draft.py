from to_date_robust import to_date_robust

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
            "data_types": [],
            "semantic_types": [
                {"semantic_type": '',  "count": 0},
            ],
        }
        column_output['column_name'] =name
        column_output['number_non_empty_cells'] = None
        column_output['number_empty_cells'] = None
        column_output['number_distinct_values'] = None
        column_output['frequent_values'] = None
        output['columns'].append(column_output)

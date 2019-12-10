import re


street_noun_regex = re.compile(r'\W*(?:STREET|AVENUE|ST|PLACE|AVE|ROAD|COURT|LANE|DRIVE|PARK|BOULEVARD|RD|BLVD|PARKWAY|PL|TERRACE|EXIT|LOOP|EXPRESSWAY|PKWY|PLAZA|BRIDGE|EN|ENTRANCE|DR|ET|BROADWAY|FLOOR|added_by_us|TUNNEL|ROUTE|CIRCLE|WAY|SQUARE|XPWY|EXPY|CRCL|WALK|PKW|CONCOURSE|BOARDWALK|FREEWAY|CHANNEL)\W*$')
door_number_regex = re.compile(r'\d+')

def is_address(data):
    if not data:
        return None
    split = [word for word in data.split(' ') if word]
    for street_noun_index in range(len(split)):
        if street_noun_regex.match(split[street_noun_index]):
            break
    else:
        return None
    for door_number_index in range(street_noun_index - 1):
        if door_number_regex.search(split[door_number_index]):
            return True
    return False



for e in ['WEST 33RD  ST']:
    is_address(e)


datasets = [
    spark.read.format('csv')
               .options(inferschema='true', sep='\t')
               .load('/user/hm74/NYCColumns/{}'.format(filename))
               .toDF('value', 'count')
    for filename in [k for [k, v] in ground_truth.items() if 'person_name' in v]
]
dataset = datasets[0]
for d in datasets[1:]:
    dataset = dataset.union(d)

d = dataset.select('value', is_address_udf('value').alias('is_business_name'))
d.filter(~d['is_business_name']).sample(fraction=0.1).show()
d.filter(d['is_business_name']).sample(fraction=0.1).show()
d.filter(F.isnull(d['is_business_name'])).sample(fraction=0.1).show()
d.select(F.count(F.when(d['is_business_name'], 1)).alias('addr'), F.count(F.when(~d['is_business_name'], 1)).alias('st'), F.count(F.when(F.isnull(d['is_business_name']), 1)).alias('unk'), F.count('*').alias('total')).collect()

datasetb.select('value').rdd.map(lambda x: x[0]).filter(lambda x: x).flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], False).take(50)


datasetb.toPandas().to_csv('b.csv')
datasetp.toPandas().to_csv('p.csv')

for filename in (filename for filename in [k for [k, v] in ground_truth.items() if 'person_name' in v]):
    [dataset_name, column_name] = filename.split('.')[0:2]
    print(u'>> entering {}.{}'.format(dataset_name, column_name))
    dataset = (spark.read.format('csv')
               .options(inferschema='true', sep='\t')
               .load('/user/hm74/NYCColumns/{}'.format(filename))
               .toDF('value', 'count'))
    print(f'TOTAL = {dataset.select(F.sum("count")).collect()[0][0]}')
    print(f'TRUE POSITIVE = {count_person_name(dataset)}')

for filename in (filename for filename in [k for [k, v] in ground_truth.items() if 'business_name' in v]):
    [dataset_name, column_name] = filename.split('.')[0:2]
    print(u'>> entering {}.{}'.format(dataset_name, column_name))
    dataset = (spark.read.format('csv')
               .options(inferschema='true', sep='\t')
               .load('/user/hm74/NYCColumns/{}'.format(filename))
               .toDF('value', 'count'))
    print(f'TOTAL = {dataset.select(F.sum("count")).collect()[0][0]}')
    print(f'TRUE POSITIVE = {count_business_name(dataset)}')



datasets = (
    spark.read.format('csv')
               .options(inferschema='true', sep='\t')
               .load('/user/hm74/NYCColumns/{}'.format(filename))
               .toDF('value', 'count')
    for filename in [k for [k, v] in ground_truth.items() if 'person_name' in v]
)
datasetp = next(datasets)
for d in datasets:
    datasetp = datasetp.union(d)



datasets = (
    spark.read.format('csv')
               .options(inferschema='true', sep='\t')
               .load('/user/hm74/NYCColumns/{}'.format(filename))
               .toDF('value', 'count')
    for filename in [k for [k, v] in ground_truth.items() if 'business_name' in v]
)
datasetb = next(datasets)
for d in datasets:
    datasetb = datasetb.union(d)


############################################################

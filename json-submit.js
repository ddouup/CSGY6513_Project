const fs = require('fs');

// task2-manual-labels.json
csv = fs.readFileSync('./DF_Label.csv', 'utf8').split('\n').map(e => e.split(',').map(e => e.trim()).filter(e => e));
csv = csv.slice(1);
ret = {
    actual_types: csv.map(([column_name, ...labels]) => ({
        column_name,
        manual_labels: labels.map(semantic_type => ({ semantic_type })),
    }))
}
fs.writeFileSync('task2-manual-labels.json', JSON.stringify(ret, null, 2));

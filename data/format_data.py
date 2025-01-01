"""
Process all the data we have

"""

import pickle

DATA_FILE = 'perf_test_256.pkl'

with open(DATA_FILE, 'rb') as f:
    raw_data = pickle.load(f)

print(raw_data)

# we need to output csv to be imported into sheets
OUTPUT_FILE = 'perf_test_256.csv'
with open(OUTPUT_FILE, 'w') as out_file:
    tmp = [f'seconds per {op},{op} per second,max time of a single {op}' for op in raw_data[1]]
    out_file.write(f"num_clients,{','.join(tmp)}\n")
    for num_clients in raw_data:
        # we have time, count, and max
        outlines = []
            
        for op, opdata in raw_data[num_clients].items():
            outlines.append(str(opdata['time']/opdata['count']))
            outlines.append(str(opdata['count']/opdata['time']))
            outlines.append(str(opdata['max']))

        out_file.write(f"{num_clients},{','.join(outlines)}\n")

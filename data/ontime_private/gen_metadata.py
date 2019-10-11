#!/usr/bin/env python3
import csv
import json
import itertools

infile = '2018_1.csv'

string_fields = ['UniqueCarrier', 'Origin', 'OriginCityName', 'OriginState', 'Dest', 'DestState']

string_buckets = [chr(x) for x in list(range(ord('A'), ord('Z') + 1))] + [chr(x) for x in list(range(ord('a'), ord('z') + 1))]

def get_double_metadata(e, g, gMin, gMax):
    return {'type': "DoubleColumnPrivacyMetadata",
            'epsilon': e,
            'granularity': g,
            'globalMin': gMin,
            'globalMax': gMax}

def get_string_metadata(e):
    return {'type':'StringColumnPrivacyMetadata',
            'epsilon': e,
            'globalMax': chr(ord('z') + 1),
            'leftBoundaries': string_buckets
    }        

def concat_colnames(colnames):
    return '+'.join(colnames)

def main():
    colnames = []
    with open(infile, 'r') as f:
        colnames = f.readline().strip().split(',')

    length2 = itertools.combinations(colnames, 2)
    length2 = [sorted(x) for x in length2]
    print(list(length2))

    with open('privacy_metadata.json', 'w') as f:
        metadata = {}
        for cn in colnames:
            if cn in string_fields:
                metadata[cn] = get_string_metadata(0.1)
            else:
                metadata[cn] = get_double_metadata(0.1, 1.0, -100.0, 100.0)
        for cn in length2:
            concat_cn = concat_colnames(cn)
            metadata[concat_cn] = {'type':'ColumnPrivacyMetadata','epsilon':0.1}
        output = {'metadata':metadata}
        f.write(json.dumps(output))

if __name__=='__main__':
    main()

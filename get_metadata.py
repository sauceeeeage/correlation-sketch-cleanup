import sys
import pandas as pd
from sodapy import Socrata
import pprint
import requests
import json
import glob
import os
from json import dumps, loads, JSONEncoder, JSONDecoder
import pickle

class PythonObjectEncoder(JSONEncoder):
    def default(self, obj):
        try:
            return {'_python_object': pickle.dumps(obj).decode('latin-1')}
        except pickle.PickleError:
            return super().default(obj)

def as_python_object(dct):
    if '_python_object' in dct:
        return pickle.loads(dct['_python_object'].encode('latin-1'))
    return dct


def read_back_serialized_object(file_name):
    with open(file_name, 'r') as fp:
        full_description = json.load(fp, object_hook=as_python_object)
        fp.close()
    return full_description

def main():
    LOAD_FROM_API = False
    full_description: set(str, [str, set(str, str)]) = {}
    table_description = ''
    col_description = {}

    if LOAD_FROM_API:
        get_metadata()

    for file_name in glob.glob('metadata/*.json'):
        data = json.load(open(file_name))
        table_name = file_name.split('/')[-1][0:-5]
        if 'columns' in data.keys():
            col = pd.DataFrame(data['columns'])
        if 'description' in data.keys():
            table_description = data['description']

        for i in range(len(col)):
            if 'description' in col.keys():
                col_description[col['name'][i]] = col['description']

        full_description[table_name] = [table_description, col_description]

    json_str = json.dumps(full_description, indent=4, cls=PythonObjectEncoder)
    with open('full_description.json', 'w') as fp:
        fp.write(json_str)
        fp.close()


def get_metadata():
    if not os.path.isfile('metadata_hash.csv'):
        sys.exit('metadata_hash.csv does not exist')
    metadata_hash = pd.read_csv('metadata_hash.csv')
    client = Socrata("data.cityofchicago.org", None)
    if not os.path.exists('metadata/'):
        os.makedirs('metadata/')
    for i in range(len(metadata_hash)):
        first_row_values = metadata_hash.iloc[i]
        name_value = first_row_values['name']
        name_value = name_value[0:-5]
        hash_value = first_row_values['hash']
        result = client.get_metadata(hash_value)
        with open('metadata/' + name_value + '.json', 'w') as fp:
            json.dump(result, fp, indent=4)
            fp.close()


if __name__ == '__main__':
    main()

import pandas as pd
from sodapy import Socrata
import pprint
import requests

def main():
    # get_metadata()
    metadatas = pd.read_json('/home/shaotong/VsCodeProjects/graph_sparsity/metadata/Business_Owners.json')
    pprint(metadatas)

def get_metadata(hash):
    metadata_hash = pd.read_csv('metadata_hash.csv')
    print(metadata_hash)
    client = Socrata("data.cityofchicago.org", None)
    for i in range(len(metadata_hash)):
        first_row_values = metadata_hash.iloc[i]
        name_value = first_row_values['name']
        name_value = name_value[0:-4]
        hash_value = first_row_values['hash']
        result = client.get_metadata(hash_value)
        with open('metadata/' + name_value + 'json', 'w') as f:
            f.write(str(result))

if __name__ == '__main__':
    main()

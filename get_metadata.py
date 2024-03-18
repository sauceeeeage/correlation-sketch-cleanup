import pandas as pd
from sodapy import Socrata
import pprint
import requests

def main():
    client = Socrata("data.cityofchicago.org", None)
    results = client.get("xzkq-xp2w")
    results_df = pd.DataFrame.from_records(results)
    metadata = client.get_metadata("3h7q-7mdb")
    print(results_df.head())
    # store metadata in a json file
    with open('metadata.json', 'w') as f:
        f.write(str(metadata))
    pprint.pprint(metadata)

if __name__ == '__main__':
    main()

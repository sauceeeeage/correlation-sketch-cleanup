file_path = '/home/shaotong/IdeaProjects/corrsketches2/datas/test_results/test3/test3_sketch-params=kmv:256.csv'

# import networkx as nx
# import matplotlib.pyplot as plt
import re
import pandas as pd
from pprint import pprint
# from tqdm import tqdm
# from pyvis.network import Network
import csv
from collections import defaultdict
import json
import string


def keep_dataset(name_list: [str], x_name: str, y_name: str) -> bool:
    if x_name in name_list and y_name in name_list:
        return True
    return False

def elimate_duplicates(out, result, counter, skip_file_counter, previous_pairs, mi_threshold):
    out.write(f'X,Y\n')
    all_csv = set()
    for column_pair in result['column']:
        # print(f'column_pair: {column_pair}')
        counter += 1
        
        two_csv = column_pair.split(') Y(')
        csv_x_name = two_csv[0].split('X(')[1].split(',')[2]
        csv_y_name = two_csv[1].split(',')[2].split(')')[0]
        all_csv.add(csv_x_name)
        all_csv.add(csv_y_name)

        if csv_x_name == csv_y_name:
            print("Same file correlation")
            print(f"Skipping... {csv_x_name} == {csv_y_name}")
            skip_file_counter += 1
            # print("Skipping...")
            continue

        # if "Employee_Overtime_and_Supplemental_Earnings" in csv_x_name or "Employee_Overtime_and_Supplemental_Earnings" in csv_y_name:
            # print("COVID-19 in file name")
            # print(f"Skipping... {csv_x_name} == {csv_y_name}")
            # skip_file_counter += 1
            # print("Skipping...")
            # continue

        # if "Budget" in csv_x_name or "Budget" in csv_y_name:
        #     skip_file_counter += 1
        #     continue

        x_num_col = two_csv[0].split('X(')[1].split(',')[1]
        y_num_col = two_csv[1].split(',')[1]

        x_cate_col = two_csv[0].split('X(')[1].split(',')[0]
        y_cate_col = two_csv[1].split(',')[0]

        if x_num_col.casefold() == y_num_col.casefold():
            print("Same column correlation")
            print(f"Skipping... {x_num_col} == {y_num_col}")
            skip_file_counter += 1
            continue

        x_key = str(x_num_col + " | " + csv_x_name)
        y_key = str(y_num_col + " | " + csv_y_name)
        whole_key = str(x_key + "," + y_key)
        alt_key = str(y_key + "," + x_key)

        if whole_key in previous_pairs or alt_key in previous_pairs:
            print(f'{whole_key} already in result')
            print("Skipping...")
            skip_file_counter += 1
            continue
        previous_pairs.append(whole_key)
        out.write(f'{x_key},{y_key}\n')
    
    print(f"Done. with {counter} pairs of columns with mi > {mi_threshold}")
    print(f"Skipped {skip_file_counter} pairs of columns")
    print(f"Total {counter - skip_file_counter} pairs of columns")
    print(f"Tables are: ")
    pprint(all_csv)



def main():
# X(Administrator,Student_Count_Total,Chicago_Public_Schools_-_School_Profile_Information_SY2122.csv) Y(Administrator,Student_Count_Hispanic,Chicago_Public_Schools_-_School_Profile_Information_SY2122.csv)
# X\(([^)]+),([^)]+),([^)]+)\) Y\(([^)]+),([^)]+),([^)]+)\)
    mi_threshold = 1.1
    counter = 0
    valid_pairs = []
    skip_file_counter = 0
    previous_pairs = []
    with open(file_path, 'r', newline='') as f:
        data = pd.read_csv(f, delimiter=',')
        # list_of_columns = data['column']
        # list_of_mi_actuals = data['mi_actual']
        data.sort_values(by=["mi_actual"], ascending=False, inplace=True)
        result = data[data['mi_actual'] > mi_threshold]
        easy_negatives = data[data['mi_actual'] <= mi_threshold]
        # fin_result: [[[string, string], [string, string]]] = [[]]
        # fin_result = defaultdict(list) # {[x_num_col, csv_x_name]: [[y_num_col, csv_y_name], ...]}
        simple_result = dict()
        output_file = open("benchmark_output.csv", "a")
        neg_file = open("easy_negatives.csv", "a")
        elimate_duplicates(output_file, result, counter, skip_file_counter, previous_pairs, mi_threshold)
        # reset everything
        counter = 0
        skip_file_counter = 0
        previous_pairs = []
        elimate_duplicates(neg_file, easy_negatives, counter, skip_file_counter, previous_pairs, mi_threshold)
        output_file.close()
        neg_file.close()
        

if __name__ == "__main__":
    main()


from TableReader import *
from utils import *
from ExecutionNode import *
from ExecutionGraph import *
from Optimizer import *
import polars as pl
import argparse
import os
import math

# Script for converting TPC-DS tables from .dat files to parquet.
def read_columns():
    column_dict = defaultdict(list)
    column_file = open("tpcds/column_list.txt")
    for line in column_file.readlines():
        if line[0].isalpha():
            col_table = line.split()
            table = col_table[1].lower()
            col = col_table[0].lower()
            column_dict[col_table[1].lower()].append(col_table[0].lower())

    return column_dict

def read_table(table_name, column_dict):
    # test database
    f = open("tpcds/" + table_name + ".dat", "r", encoding = "utf-8",
             errors = 'replace')
    df_str = f.read()
    column_count = df_str.split('\n')[0].count('|')
    return pl.read_csv(io.StringIO(df_str), has_header = False, sep = '|',
                       new_columns = column_dict[table_name][:column_count])

if __name__ == '__main__':
    column_dict = read_columns()
    
    for column in column_dict.keys():
        print(column)
        table = read_table(column, column_dict)

        p_size = 5
        num_partitions = int(math.ceil(len(column_dict[column]) / p_size))
        for i in range(num_partitions):
            if (i * p_size > len(column_dict[column])):
                print(table[column_dict[column][i*p_size:])
                parquet_result(table[column_dict[column][i*p_size:]],
                               column + "_" + str(i), location = 'tpcds/')
            else:
                print(column_dict[column][i*p_size:(i+1)*p_size-1])
                parquet_result(table[column_dict[column]
                                     [i*p_size:(i+1)*p_size-1]],
                               column + "_" + str(i), location = 'tpcds/') 

        df = unparquet_result(column + "_0", location = 'tpcds/')
        for i in range(1, num_partitions):
            df = df.hstack(unparquet_result(column + "_" + str(i),
                                            location = 'tpcds/'))

        parquet_result(df, column, location = 'tpcds/')

        for i in range(num_partitions):
            os.remove("tpcds/" + column + "_" + str(i) + ".parquet")

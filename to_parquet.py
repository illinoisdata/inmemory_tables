from TableReader import *
from utils import *
from ExecutionNode import *
from ExecutionGraph import *
from Optimizer import *
from tpcds_queries_new import *
import polars as pl
import argparse

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
        table = read_table(column, column_dict)
        parquet_result(table, column, location = 'tpcds/')

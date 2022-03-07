from TableReader import *
from utils import *
from ExecutionNode import *
from ExecutionGraph import *
from Optimizer import *
from tpcds_queries_new import *
import polars as pl
import argparse

if __name__ == '__main__':
    tablereader = TableReader()
    tablereader.read_columns()
    
    for column in tablereader.column_dict.keys():
        table = tablereader.read_table(column)
        parquet_result(table, column, location = 'tpcds/')

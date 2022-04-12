from TableReader import *
from utils import *
from ExecutionNode import *
from ExecutionGraph import *
from Optimizer import *
import polars as pl
import pandas as pd
import argparse
import os
import math

if __name__ == '__main__':
    tablereader = TableReader()

    store_sales = tablereader.read_table("store_sales")
    catalog_sales = tablereader.read_table("catalog_sales")
    web_sales = tablereader.read_table("web_sales")

    date = tablereader.read_table("date")

    for i in range(1999, 2003):
        table1 = store_sales.join(date, left_on = "ss_sold_date_sk",
                                  right_on = "d_date_sk").filter(pl.col("d_year") == i)

        table2 = store_sales.filter(pl.col("ss_sold_date_sk")
                                    .is_in(table1["ss_sold_date_sk"].to_list()))

        parquet_result(table2, "store_sales_" + str(i), location = 'tpcds/')

        table1 = catalog_sales.join(date, left_on = "cs_sold_date_sk",
                                  right_on = "d_date_sk").filter(pl.col("d_year") == i)

        table2 = catalog_sales.filter(pl.col("cs_sold_date_sk")
                                    .is_in(table1["cs_sold_date_sk"].to_list()))

        parquet_result(table2, "catalog_sales_" + str(i), location = 'tpcds/')

        table1 = web_sales.join(date, left_on = "ws_sold_date_sk",
                                  right_on = "d_date_sk").filter(pl.col("d_year") == i)

        table2 = web_sales.filter(pl.col("ws_sold_date_sk")
                                    .is_in(table1["ws_sold_date_sk"].to_list()))

        parquet_result(table2, "web_sales_" + str(i), location = 'tpcds/')
                           
        
    

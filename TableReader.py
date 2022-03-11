import io
import polars as pl
import time
from utils import *
from collections import defaultdict

class TableReader(object):

    """
    execution_graph: the ExecutionGraph object to perform optimization on.
    memory_limit: Desired peak memory consumption limit during execution.
    max_iters: Maximum number of iterations of EM algorithm to run
    """
    def __init__(self):
        self.table_read_times = defaultdict(int)
        self.table_read_num_times = defaultdict(int)

    def read_table(self, table_name):
        start = time.time()

        table = unparquet_result(table_name, location = 'tpcds/')
        
        table_read_time = time.time() - start
        self.table_read_times[table_name] += table_read_time
        self.table_read_num_times[table_name] += 1

        return table

    def report(self):
        print("table read times:")
        for k, v in self.table_read_times.items():
            print("Table:" + str(k) + " times read: " +
                  str(self.table_read_num_times[k]) + " total time: " +
                  str(v))
        print("total table read time:",
              sum(list(self.table_read_times.values())))

        return sum(list(self.table_read_times.values())))

    def clear(self):
        self.table_read_times = defaultdict(int)
        self.table_read_num_times = defaultdict(int)

from ExecutionNode import *
from ExecutionGraph import *
from Optimizer import *
from tpcds_queries import *
import polars as pl

import threading, queue
import time
import matplotlib.pyplot as plt
import sns
import copy
import utils
import os.path
import os
import cProfile, pstats
import psutil
import scipy.stats

if __name__ == '__main__':   
    # Get node representation of a TPC-DS query 
    execution_nodes = get_tpcds_query_nodes(query_num = 5)

    # Create graph & add nodes
    execution_graph = ExecutionGraph()
    execution_graph.add_nodes(execution_nodes)
    execution_graph.build_graph(draw_graph = True)

    # Dry run; store no nodes in memory
    execution_graph.execute(debug = True)
    
    # Create optimizer to jointly optimize nodes to store in memory & execution
    # order
    optimizer = Optimizer(execution_graph, memory_limit = 200000000)
    optimizer.optimize(debug = True)

    # Evaluate efficiency after optimization
    execution_graph.execute(debug = True, save_inmemory_tables = True)

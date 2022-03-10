from ExecutionNode import *
from ExecutionGraph import *
from Optimizer import *
from tpcds_queries import *
import polars as pl
import argparse
import glob
import os
import time
import gc
from collections import Counter

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-Q", "--job", help="TPC-DS job number")
    parser.add_argument("-M", "--memory", help="Memory container size")
    args = parser.parse_args()
    
    # Get node representation of a TPC-DS query 
    execution_nodes, tablereader = get_tpcds_query_nodes(
        job_num = int(args.job))

    print("number of nodes:", len(execution_nodes))

    # Create graph & add nodes
    execution_graph = ExecutionGraph()
    execution_graph.add_nodes(execution_nodes)
    execution_graph.build_graph(draw_graph = False)

    # Dry run; store no nodes in memory
    execution_graph.execute(debug = False)

    tablereader.report()
    tablereader.clear()

    files = glob.glob('disk/*')
    for f in files:
        os.remove(f)
    
    # Create optimizer to jointly optimize nodes to store in memory & execution
    # order
    optimizer = Optimizer(execution_graph,
                          memory_limit = float(args.memory) * 1000000000)
    optimizer.optimize(execution_order_method = "both", debug = True)

    gc.collect()
    time.sleep(2)

    # Evaluate efficiency after optimization
    execution_graph.execute(debug = False, save_inmemory_tables = False)

    tablereader.report()

    files = glob.glob('disk/*')
    for f in files:
        os.remove(f)

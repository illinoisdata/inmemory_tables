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
import pickle
from collections import Counter

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-Q", "--job", help="TPC-DS job number")
    parser.add_argument("-M", "--memory", help="Memory container size")
    args = parser.parse_args()
    
    # Get node representation of a TPC-DS query 
    execution_nodes, tablereader = get_tpcds_query_nodes(
        job_num = int(args.job))

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
    with open("results/baselines.txt") as file:
        settings = [line.rstrip().split() for line in file]
    file.close()

    default_order = execution_graph.execution_order

    optimizer = Optimizer(execution_graph,
                          memory_limit = float(args.memory) * 1000000000)

    for setting in settings:
        store_nodes_method = setting[0]
        execution_order_method = setting[1]

        print("running", store_nodes_method, execution_order_method)

        execution_graph.execution_order = default_order
        execution_graph.store_in_memory = set()

        _, _, computation_time = optimizer.optimize(store_nodes_method = store_nodes_method,
                           execution_order_method = execution_order_method,
                           debug = False)

        myfile = open("results/result.txt", "a")
        myfile.write(str(args.job) + " " + str(args.memory) + " " +
                     str(args.store) + " " + str(args.top) + ": " +
                     str(computation_time) + "\n")
        myfile.close()

        pickle.dump([execution_graph.store_in_memory,
                    execution_graph.execution_order],
                    open("configs/job_" + str(args.job) +
                         "_mem_" + str(args.memory) +
                         "_s_" + str(store_nodes_method) +
                         "_t_" + str(execution_order_method), "wb"))
    

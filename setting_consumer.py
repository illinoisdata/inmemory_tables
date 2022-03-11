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
    parser.add_argument("-S", "--store", help="Method for flagging nodes")
    parser.add_argument("-T", "--top", help="Method for topological order")
    args = parser.parse_args()

    print("running", args.job, args.memory, args.store, args.top)
    
    # Get node representation of a TPC-DS query 
    execution_nodes, tablereader = get_tpcds_query_nodes(
        job_num = int(args.job))

    # Create graph & add nodes
    execution_graph = ExecutionGraph()
    execution_graph.add_nodes(execution_nodes)
    execution_graph.build_graph(draw_graph = False)

    settings_list = pickle.load(open("configs/job_" + str(args.job) +
                     "_mem_" + str(args.memory) +
                     "_s_" + str(args.store) +
                     "_t_" + str(args.top), "rb"))

    execution_graph.store_in_memory = settings_list[0]
    execution_graph.execution_order = settings_list[1]

    # Dry run; store no nodes in memory
    execution_time_counter, time_to_deserialize_counter, \
    time_to_serialize_counter, peak_memory_usage_counter = \
    execution_graph.execute(debug = False)

    total_read_time = tablereader.report()

    files = glob.glob('disk/*')
    for f in files:
        os.remove(f)

    myfile = open("results/result.txt", "a")
    myfile.write(str(args.job) + " " + str(args.memory) + " " +
                 str(args.store) + " " + str(args.top) + ": " +
                 str(execution_time_counter) + " " +
                 str(time_to_deserialize_counter) + " " +
                 str(time_to_serialize_counter) + " " +
                 str(total_read_time) + " " +
                 str(peak_memory_usage_counter) + "\n")
    myfile.close()

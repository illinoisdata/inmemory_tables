from ExecutionNode import *
from ExecutionGraph import *
from Optimizer import *
from tpcds_queries import *
from dag_generator.dag_generator import *
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
    parser.add_argument("-N", "--nodes", help="Number of nodes in generated graph")
    parser.add_argument("-M", "--memory", help="Memory container size")
    parser.add_argument("-I", "--iters", help="Number of iterations to run")
    args = parser.parse_args()

    with open("results/baselines.txt") as file:
        settings = [line.rstrip().split() for line in file]
    file.close()

    # Initialize result storage
    result_dicts = {}
    for setting in settings:
        result_dicts[setting[0] + ' ' + setting[1]] = defaultdict(list)

    nx_graphs = run_dag_experiments(int(args.nodes), int(args.iters))
        
    for i in range(len(nx_graphs)):
        if i % 1 == 0:
            print(i, "---------------------------------")
        nx_graph = nx_graphs[i]
        execution_graph = ExecutionGraph()
        for node_id in range(len(nx_graph["parents"])):
            execution_graph.add_nodes(ExecutionNode(
                str(node_id),
                lambda x: x,
                [str(parent_id) for parent_id in nx_graph["parents"][node_id]]))
                                      
            execution_graph.node_dict[str(node_id)].result_size_history \
                .append(nx_graph["size_out"][node_id])
            execution_graph.node_dict[str(node_id)].time_to_serialize_history \
                .append(nx_graph["serialize_time"][node_id])
            execution_graph.node_dict[str(node_id)].time_to_deserialize_history\
                .append(nx_graph["deserialize_time"][node_id])
        execution_graph.build_graph(draw_graph = False)

        default_order = execution_graph.execution_order

        optimizer = Optimizer(execution_graph,
                          memory_limit = float(args.memory) * 1000000000)
        
        for setting in settings:
            store_nodes_method = setting[0]
            execution_order_method = setting[1]

            execution_graph.execution_order = default_order
            execution_graph.store_in_memory = set()

            """
            if store_nodes_method == "mkp" and execution_order_method == "both":
                debug = True
            else:
                debug = False
            """
                
            prev_time_save, _, computation_time = optimizer.optimize(
                store_nodes_method = store_nodes_method,
                execution_order_method = execution_order_method,
                debug = False)

            result_dicts[setting[0] + ' ' + setting[1]]["computation_time"]\
                                    .append(computation_time)
            result_dicts[setting[0] + ' ' + setting[1]]["time save score"]\
                                    .append(prev_time_save)
            result_dicts[setting[0] + ' ' + setting[1]]["save time"].append(
                sum([execution_graph.node_dict[n].get_time_to_serialize()
                     for n in execution_graph.store_in_memory]))
            result_dicts[setting[0] + ' ' + setting[1]]["load time"].append(
                sum([execution_graph.node_dict[n].get_time_to_deserialize()
                     for n in execution_graph.store_in_memory]))

            print(setting, computation_time, prev_time_save)

    pickle.dump(result_dicts, "results/result_dicts_" + args.nodes +
                "_" + args.memory + ".pickle", "wb")

        

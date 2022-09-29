from core.algorithm.optimizer import *
from experiment.tpcds_queries import *
import argparse
import glob
import os
import time
import gc

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-Q", "--job", help="TPC-DS job number")
    parser.add_argument("-M", "--memory", help="Memory container size")
    parser.add_argument("-S", "--store", help="Method for flagging nodes")
    parser.add_argument("-T", "--top", help="Method for topological order")
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

    tablereader.report(debug = False)
    tablereader.clear()

    files = glob.glob('disk/*')
    for f in files:
        os.remove(f)
    
    # Create optimizer to jointly optimize nodes to store in memory & execution
    # order
    optimizer = Optimizer(execution_graph,
                          memory_limit = float(args.memory) * 1000000000)
    optimizer.optimize(store_nodes_method = args.store,
        execution_order_method = args.top, debug = False)

    del optimizer
    gc.collect()
    time.sleep(2)

    # Evaluate efficiency after optimization
    execution_graph.execute(debug = False, save_inmemory_tables = True)

    tablereader.report()

    files = glob.glob('disk/*')
    for f in files:
        os.remove(f)

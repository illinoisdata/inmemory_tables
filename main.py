from ExecutionNode import *
from ExecutionGraph import *
from Optimizer import *
from tpcds_queries import *
import polars as pl
import argparse
import glob
import os

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-Q", "--query", help="TPC-DS query number")
    parser.add_argument("-M", "--memory", help="Memory container size")
    args = parser.parse_args()
    
    # Get node representation of a TPC-DS query 
    execution_nodes, tablereader = get_tpcds_query_nodes(
        job_num = int(args.query))

    print("number of nodes:", len(execution_nodes))

    # Create graph & add nodes
    execution_graph = ExecutionGraph()
    execution_graph.add_nodes(execution_nodes)
    execution_graph.build_graph(draw_graph = False)

    # Dry run; store no nodes in memory
    execution_graph.execute(debug = True)

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

    # Evaluate efficiency after optimization
    execution_graph.execute(debug = False, save_inmemory_tables = True)

    tablereader.report()

    files = glob.glob('disk/*')
    for f in files:
        os.remove(f)

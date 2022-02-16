from ExecutionNode import *
from ExecutionGraph import *
from Optimizer import *
from tpcds_queries import *
import polars as pl
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-Q", "--query", help="TPC-DS query number")
    parser.add_argument("-M", "--memory", help="Memory container size")
    args = parser.parse_args()
    
    # Get node representation of a TPC-DS query 
    execution_nodes = get_tpcds_query_nodes(query_num = int(args.query))

    # Create graph & add nodes
    execution_graph = ExecutionGraph()
    execution_graph.add_nodes(execution_nodes)
    execution_graph.build_graph(draw_graph = False)

    # Dry run; store no nodes in memory
    execution_graph.execute(debug = True)
    
    # Create optimizer to jointly optimize nodes to store in memory & execution
    # order
    optimizer = Optimizer(execution_graph, memory_limit = int(args.memory))
    optimizer.optimize(debug = True)

    # Evaluate efficiency after optimization
    execution_graph.execute(debug = True, save_inmemory_tables = True)

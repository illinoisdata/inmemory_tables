from ExecutionNode import *
from ExecutionGraph import *
from Optimizer import *
from tpcds_queries_new import *
import polars as pl
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-Q", "--query", help="TPC-DS query number")
    parser.add_argument("-M", "--memory", help="Memory container size")
    args = parser.parse_args()
    
    # Get node representation of a TPC-DS query 
    # execution_nodes = get_tpcds_query_nodes(query_num = int(args.query))
    execution_nodes, tablereader = get_tpcds_query_nodes(job_num = 2)

    # Create graph & add nodes
    execution_graph = ExecutionGraph()
    execution_graph.add_nodes(execution_nodes)
    execution_graph.build_graph(draw_graph = True)

    # Dry run; store no nodes in memory
    execution_graph.execute(debug = False)

    tablereader.report()
    tablereader.clear()
    
    # Create optimizer to jointly optimize nodes to store in memory & execution
    # order
    # optimizer = Optimizer(execution_graph, memory_limit = int(args.memory))
    optimizer = Optimizer(execution_graph, memory_limit = 2000000000)
    optimizer.optimize(execution_order_method = "both",
                       debug = True)

    # Evaluate efficiency after optimization
    execution_graph.execute(debug = False, save_inmemory_tables = True)
    #recursive_min_cut(execution_graph.graph,
    #                  {node.name: node.get_result_size()
    #                    for node in execution_graph.node_dict.values()},
    #                  set(execution_graph.graph.nodes()), debug = True)

    tablereader.report()

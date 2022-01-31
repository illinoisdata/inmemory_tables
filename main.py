from ExecutionNode import *
from ExecutionGraph import *
from Optimizer import *
from tpcds_queries import *
import polars as pl

if __name__ == '__main__':
    execution_nodes = get_tpcds_query_nodes(query_num = 5)

    # Create graph & add nodes
    execution_graph = ExecutionGraph()
    execution_graph.add_nodes(execution_nodes)
    execution_graph.build_graph()

    # Dry run; store no nodes in memory
    execution_graph.execute(debug = True)

    # Create optimizer to jointly optimize nodes to store in memory & execution
    # order
    
    optimizer = Optimizer(execution_graph, memory_limit = 200000000)
    optimizer.optimize(debug = True)

    # Evaluate efficiency after optimization
    execution_graph.execute(debug = True, save_inmemory_tables = True)

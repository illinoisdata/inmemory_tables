from core.algorithm.optimizer import *
from core.algorithm.optimize_nodes.heuristic import FlagNodesHeuristic
from core.algorithm.optimize_order.ma_dfs import OptimizeOrderMADFS
from core.graph.ExecutionGraph import ExecutionGraph
from core.graph.ExecutionNode import ExecutionNode
from dag_generator.dag_generator import *
import argparse

# Run scalability experiments on the generated DAGs.
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-S", "--size", help="Number of nodes in generated graph")
    parser.add_argument("-M", "--memory", help="Memory container size")
    parser.add_argument("-I", "--iters", help="Number of iterations to run")
    args = parser.parse_args()

    results = []
        
    for i in range(int(args.iters)):
        if i % 1 == 0:
            print(i, "---------------------------------")

        # Generate a random graph
        nx_graph = run_dag_experiments(int(args.size), 1)[0]

        # Manually construct the execution graph
        execution_graph = ExecutionGraph(None, None, None, "", "")
        for node_id in range(len(nx_graph["parents"])):
            node = ExecutionNode("CREATE TABLE xxx AS (SELECT * FROM yyy);")
            node.node_name = str(node_id)
            node.table_size = nx_graph["size_out"][node_id]
            node.time_save = nx_graph["speedup_score"][node_id]

            execution_graph.node_dict[str(node_id)] = node
            execution_graph.graph.add_node(str(node_id))

        execution_graph.build_graph()

        # Construct the optimizer of choice
        optimizer_nodes = FlagNodesHeuristic()
        optimizer_order = OptimizeOrderMADFS()
        optimizer = Optimizer(float(args.memory) * 1000000000, optimizer_nodes, optimizer_order, debug=False)

        # Time the optimization
        start = time.time()
        execution_graph.optimize(optimizer)
        end = time.time()

        results.append(end - start)

    print("Average optimization time:", sum(results) / len(results))

        

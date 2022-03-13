import time
import copy
import networkx as nx 
from ExecutionGraph import *
from store_nodes import *
from optimize_order import *

class Optimizer(object):

    """
    execution_graph: the ExecutionGraph object to perform optimization on.
    memory_limit: Desired peak memory consumption limit during execution.
    max_iters: Maximum number of iterations of EM algorithm to run
    """
    def __init__(self, execution_graph, memory_limit, max_iters = 100):
        self.execution_graph = execution_graph
        self.memory_limit = memory_limit
        self.max_iters = max_iters

        # Optimization algorithm inputs computed from input execution graph
        # at runtime.
        self.node_sizes = None
        self.node_scores = None

        # Nodes to trivially exclude from storing in memory computed at runtime.
        self.nodes_to_exclude = None
        
    """
    Function for choosing method to compute set of nodes to store in memory.
    """
    def store_nodes(self, method = "mkp", debug = False):
        if method == "all":
            return store_nodes_all(
                graph = self.execution_graph.graph,
                execution_order = self.execution_graph.execution_order,
                node_scores = self.node_scores,
                node_sizes = self.node_sizes,
                nodes_to_exclude = self.execution_graph.outputs,
                debug = debug)

        if method == "none":
            return store_nodes_none(debug = debug)

        if method == "greedy":
            return store_nodes_greedy(
                graph = self.execution_graph.graph,
                execution_order = self.execution_graph.execution_order,
                node_scores = self.node_scores,
                node_sizes = self.node_sizes,
                nodes_to_exclude = self.nodes_to_exclude,
                memory_limit = self.memory_limit,
                debug = debug)

        if method == "random":
            return store_nodes_random(
                graph = self.execution_graph.graph,
                execution_order = self.execution_graph.execution_order,
                node_scores = self.node_scores,
                node_sizes = self.node_sizes,
                nodes_to_exclude = self.nodes_to_exclude,
                memory_limit = self.memory_limit,
                debug = debug)

        if method == "mkp":
            return store_nodes_mkp(
                graph = self.execution_graph.graph,
                execution_order = self.execution_graph.execution_order,
                node_scores = self.node_scores,
                node_sizes = self.node_sizes,
                nodes_to_exclude = self.nodes_to_exclude,
                memory_limit = self.memory_limit,
                debug = debug)

        # no valid choice made
        return store_nodes_none(debug = debug)

    """
    Function for choosing method to improve execution order with.
    """
    def improve_execution_order(self, method = "both", sa_iters = 10000,
                                   debug = False):
        if method == "both":
            new_execution_order, _ = dfs_topological(
                graph = self.execution_graph.graph,
                node_sizes = self.node_sizes,
                store_in_memory = self.execution_graph.store_in_memory,
                debug = debug)
            
            return simulated_annealing(
                graph = self.execution_graph.graph,
                node_sizes = self.node_sizes,
                store_in_memory = self.execution_graph.store_in_memory,
                memory_limit = self.memory_limit,
                new_execution_order = new_execution_order,
                n_iters = sa_iters, debug = debug)

        if method == "dfs":
            return dfs_topological(
                graph = self.execution_graph.graph,
                node_sizes = self.node_sizes,
                store_in_memory = self.execution_graph.store_in_memory,
                debug = debug)

        if method == "sa":
            return simulated_annealing(graph = self.execution_graph.graph,
                node_sizes = self.node_sizes,
                store_in_memory = self.execution_graph.store_in_memory,
                memory_limit = self.memory_limit,
                new_execution_order = \
                    copy.deepcopy(self.execution_graph.execution_order),
                n_iters = sa_iters, debug = debug)

        if method == "recursive_min_cut":
            return recursive_min_cut(
                graph = self.execution_graph.graph,
                node_sizes = self.node_sizes,
                store_in_memory = self.execution_graph.store_in_memory,
                debug = debug)

        if method == "recursive_min_cut2":
            return recursive_min_cut2(
                graph = self.execution_graph.graph,
                node_sizes = self.node_sizes,
                store_in_memory = self.execution_graph.store_in_memory,
                debug = debug)



        # no valid choice made
        return self.execution_graph.execution_order, self.memory_limit
        
        
    """
    Run the EM algorithm for joint optimization of execution order & nodes
    to store in memory.
    Should only be called after info on node sizes/costs are available from dry
    running the execution graph once.
    debug: show EM iteration count & info during optimization.
    store_nodes_method: method for selecting nodes to store in memory. See
    the store_nodes function for a list of available methods.
    execution_order_method: method for optimizing the execution order. See
    the optimize_execution_order function for a list of available methods.
    sa_iters: Number of simulated annealing iterations to run per EM iteration.
    """
    def optimize(self, store_nodes_method = "mkp",
                 execution_order_method = "both",
                 sa_iters = 10000, debug = False):
        
        # Retrieve node sizes & scores for optimization algorithm
        self.node_sizes = {node.name: node.get_result_size()
                        for node in self.execution_graph.node_dict.values()}
        self.node_scores = {node.name: node.get_time_to_serialize() +
            len(list(self.execution_graph.graph.successors(node.name))) *
            node.get_time_to_deserialize()
                         for node in self.execution_graph.node_dict.values()}

        # A node is trivially excluded from storing in memory if its size
        # exceeds the memory limit, if its score is 0, or if it is an output.
        self.nodes_to_exclude = set()
        for name in self.execution_graph.graph.nodes:
            if (self.node_sizes[name] > self.memory_limit or
                self.node_scores[name] == 0 or
                name in self.execution_graph.outputs):
                self.nodes_to_exclude.add(name)
        
        prev_time_save = 0
        prev_peak_memory_usage = self.memory_limit

        counter = 0
        start = time.time()

        # EM algorithm
        for i in range(self.max_iters):
            # Find optimal nodes to store in memory given topological order
            if debug:
                print("Iteration " + str(i + 1) + ":---------------------")
                print("Finding nodes to store.............")

            new_store_in_memory, new_time_save, new_peak_memory_usage = \
                                 self.store_nodes(
                                     method = store_nodes_method, debug = debug)
            
            prev_peak_memory_usage = new_peak_memory_usage
            
            # Early stop if time save cannot be improved
            if new_time_save <= prev_time_save:
                if debug:
                    print("New solution is worse, stopping EM............")
                break
            
            self.execution_graph.store_in_memory = new_store_in_memory

            # Find optimal topological order given nodes to store
            if debug:
                print("Optimizing topological order.............")
                        
            new_execution_order, new_peak_memory_usage = \
                                 self.improve_execution_order(
                                     method = execution_order_method,
                                     sa_iters = sa_iters, debug = debug)
            
            prev_time_save = new_time_save

            # Early stop if peak memory usage violates memory limit
            if new_peak_memory_usage > self.memory_limit:
                break

            self.execution_graph.execution_order = new_execution_order

        computation_time = time.time() - start
        if debug:
            print("Algorithm computation time:", computation_time)

        return prev_time_save, prev_peak_memory_usage, computation_time

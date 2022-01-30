import time
import copy
import networkx as nx
import random
import math
import matplotlib as plt
from ortools.algorithms import pywrapknapsack_solver      
from ExecutionGraph import *
from utils import *

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

        # Maximal sets acting as constraints for the MKP.
        self.maximal_sets = None
        
    """
    Run the EM algorithm for joint optimization of execution order & nodes
    to store in memory.
    Should only be called after info on node sizes/costs are available from dry
    running the execution graph once.
    debug: show EM iteration count & info during optimization.
    """
    def optimize(self, debug = False):
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

            self.find_maximal_sets(debug = debug)
            new_store_in_memory, new_time_save, new_peak_memory_usage = \
                                 self.mkp(debug = debug)
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
                        
            new_execution_order, new_peak_memory_usage = self.dfs_topological(
                debug = debug)
            new_execution_order, new_peak_memory_usage = \
                self.simulated_annealing(new_execution_order, debug = debug)
            prev_time_save = new_time_save

            # Early stop if peak memory usage violates memory limit
            if new_peak_memory_usage > self.memory_limit:
                break

            self.execution_graph.execution_order = new_execution_order
            
        if debug:
            print("Algorithm computation time:", time.time() - start)

        return prev_time_save, prev_peak_memory_usage

    """
    Step 1: find all relevant maximal sets of results which act as constraints
    for the multidimensional knapsack problem.
    debug: show maximal set count.
    """
    def find_maximal_sets(self, debug = False):
        # Keep track of when to simulate garbage collection of results
        num_successors = [len(list(self.execution_graph.graph.successors(i)))
                          for i in self.execution_graph.execution_order]
        num_successors_dict = dict(zip(self.execution_graph.execution_order,
                                       num_successors))
        self.maximal_sets = []
        current_set = set()
        current_memory_usage = 0
        
        for name in self.execution_graph.execution_order:
            if name not in self.nodes_to_exclude:
                current_set.add(name)
                current_memory_usage += self.node_sizes[name]

            # Find dependencies to garbage collect
            dependencies_to_free = set()
            for parent_name in self.execution_graph.graph.predecessors(name):
                num_successors_dict[parent_name] -= 1
                if (num_successors_dict[parent_name] == 0 and
                    parent_name not in self.nodes_to_exclude):
                    dependencies_to_free.add(parent_name)

            # If there are dependencies to remove, current set must be maximal.
            # Add the set if it is a nontrivial constraint (i.e. the combined
            # size exceeds the memory limit)
            if (len(dependencies_to_free) > 0 and
                current_memory_usage > self.memory_limit):
                self.maximal_sets.append(copy.deepcopy(current_set))

            # Simulate garbage collection of dependencies
            for parent_name in dependencies_to_free:
                current_memory_usage -= self.node_sizes[parent_name]
            current_set = current_set.difference(dependencies_to_free)

        # Edge case for adding last set
        if (current_memory_usage > self.memory_limit and (
            not current_set.issubset(maximal_sets[-1]) or
            len(maximal_sets) == 0)):
            self.maximal_sets.append(copy.deepcopy(current_set))

        if debug:
            print("number of maximal sets:", len(self.maximal_sets))

    """
    Step 2: Run a multidimensional 0-1 knapsack solver using branch & bound to
    find the optimal set of nodes to store in memory given memory size
    constraint.
    Returns the nodes to store in memory, best time save and peak memory usage.
    debug: Print time saved/memory usage given set of nodes to store in memory.
    """
    def mkp(self, debug = False):
        new_store_in_memory = set()
        
        # Trivial computation where there are no maximal sets; store all nodes
        # which are not trivially excluded.
        if len(self.maximal_sets) == 0:
            for name in self.execution_graph.execution_order:
                if name not in self.nodes_to_exclude:
                    new_store_in_memory.add(name)

            # Compute time save (score) & peak memory usage
            new_time_save = sum([self.node_scores[name]
                              for name in new_store_in_memory])
            new_peak_memory_usage = compute_peak_memory_usage(
                self.execution_graph.graph,
                self.execution_graph.execution_order, self.node_sizes,
                new_store_in_memory)

            if debug:
                print("Time save:", new_time_save)
                print("Peak memory usage:", new_peak_memory_usage)
            
            return new_store_in_memory, new_time_save, new_peak_memory_usage
        
        # The vector consists of nodes which appear in at least 1 maximal set.
        vector_nodes = set()
        for maximal_set in self.maximal_sets:
            vector_nodes = vector_nodes.union(maximal_set)
        vector_nodes = list(vector_nodes)

        # Scale scores to transform problem into integer programming
        multiplier = 1 / min([self.node_scores[name] for name in vector_nodes])
                
        profits = [self.node_scores[name] * multiplier for name in vector_nodes]
        weights = []
        for maximal_set in self.maximal_sets:
            weights.append([int(name in maximal_set) * self.node_sizes[name]
                            for name in vector_nodes])
        capacities = [self.memory_limit for i in range(len(self.maximal_sets))]

        # Initialize Google MKP solver
        solver = pywrapknapsack_solver.KnapsackSolver(
              pywrapknapsack_solver.KnapsackSolver
                  .KNAPSACK_MULTIDIMENSION_BRANCH_AND_BOUND_SOLVER,
              'Multi-dimensional solver')
        solver.Init(profits, weights, capacities)
        time_save = solver.Solve()

        # Gather nodes to store in memory
        for i in range(len(vector_nodes)):
            if solver.BestSolutionContains(i):
                new_store_in_memory.add(vector_nodes[i])

        # Add nodes not in any valid maximal set due to all member maximal sets
        # which it is a part of being under the size limit.
        for name in self.execution_graph.graph.nodes:
            if name not in vector_nodes and name not in self.nodes_to_exclude:
                new_store_in_memory.add(name)

        # Compute time save & peak memory usage
        new_time_save = time_save / multiplier
        new_peak_memory_usage = compute_peak_memory_usage(
                self.execution_graph.graph,
                self.execution_graph.execution_order, self.node_sizes,
                new_store_in_memory)

        if debug:
            print("Time save:", new_time_save)
            print("Max memory usage:", new_peak_memory_usage)
        
        return new_store_in_memory, new_time_save, new_peak_memory_usage

    """
    Step 3: Find a better execution order by running DFS with a heuristic of
    minimizing the amount of time large results stay in memory.
    Returns the new execution order and its peak memory usage.
    debug: Print memory usage given execution order.
    """
    def dfs_topological(self, debug = False):
        # Compute the actual memory usage of nodes based on its size and whether
        # it is stored in memory or not (i.e. if its not, then the usage is 0)
        memory_usage = {name: int(name in self.execution_graph.store_in_memory) *
                        self.node_sizes[name]
                        for name in self.execution_graph.graph.nodes}

        # Prioritize adding nodes with large memory usage to the stack first.
        sorted_memory_usage = [k for k, v in sorted(memory_usage.items(),
                                                    key = lambda item: -item[1])]
        
        visited = set()
        stack = []
        
        for name in sorted_memory_usage:
            if name not in visited:
                self.dfs_topological_helper(name, visited, stack, memory_usage)

        new_execution_order = stack[::-1]

        # Compute peak memory usage
        new_peak_memory_usage = compute_peak_memory_usage(
                    self.execution_graph.graph, new_execution_order,
                    self.node_sizes, self.execution_graph.store_in_memory)
        
        if debug:
            print("Peak memory usage:", new_peak_memory_usage)

        return new_execution_order, new_peak_memory_usage

    """
    Helper recursive function for step 3 DFS.
    """
    def dfs_topological_helper(self, name, visited, stack, memory_usage):
        visited.add(name)

        children = self.execution_graph.graph.successors(name)
        sorted_children = sorted(children,
                                 key = lambda item: -memory_usage[item])

        for child_name in sorted_children:
            if child_name not in visited:
                self.dfs_topological_helper(child_name, visited, stack,
                                            memory_usage)

        stack.append(name)

    """
    Step 4: Further improve the execution order by optimizing it in terms of
    weighted minimum linear arrangement via simulated annealing.
    new_execution_order: a default execution order (passed from step 3)
    """
    def simulated_annealing(self, new_execution_order, debug = False):
        memory_usage = {name: int(name in self.execution_graph.store_in_memory) *
                        self.node_sizes[name]
                        for name in self.execution_graph.graph.nodes}

        # Set up dictionaries for fast lookup
        node_name_to_idx = {}
        node_idx_to_name = {}
        for i in range(len(new_execution_order)):
            node_name_to_idx[new_execution_order[i]] = i
            node_idx_to_name[i] = new_execution_order[i]

        best_score = 0
        best_order = new_execution_order
        cur_score = 0
        scores = []

        # Controls for simulated annealing. Higher temperatire means the
        # algorithm is more likely to pick a worse solution at any time step.
        n_iterations = 1000
        temperature = \
            max(list(memory_usage.values())) / len(list(memory_usage.values()))
        
        for i in range(n_iterations):
            # Select some random node
            u = random.randint(0, len(new_execution_order) - 1)
            max_parent = max([node_name_to_idx[p]
                for p in self.execution_graph.graph
                              .predecessors(node_idx_to_name[u])],
                             default = 0)
            min_child = min([node_name_to_idx[c]
                for c in self.execution_graph.graph
                              .successors(node_idx_to_name[u])],
                            default = len(new_execution_order) - 1)

            # Current node cannot be swapped
            if min_child - max_parent <= 2:
                continue

            # Randomly select a valid position for swapping
            v = random.randint(max_parent + 1, min_child - 1)

            # Evaluate swap
            score_change = minLA_change(self.execution_graph.graph,
                                        node_idx_to_name, node_name_to_idx,
                                        memory_usage, node_idx_to_name[u], v)

            # Metropolis
            t = temperature / float(i + 1)
            
            if (score_change > 0 or
                random.random() < math.exp(score_change / t)):
                minLA_apply(node_idx_to_name, node_name_to_idx,
                            node_idx_to_name[u], v)

                # Revert change if the order violates memory limit
                cur_order = [v for k, v in sorted(node_idx_to_name.items())]
                if compute_peak_memory_usage(self.execution_graph.graph,
                    cur_order, self.node_sizes,
                    self.execution_graph.store_in_memory) > self.memory_limit:
                    minLA_apply(node_idx_to_name, node_name_to_idx,
                            node_idx_to_name[v], u)
                    
                else:
                    cur_score += score_change
                    if cur_score > best_score:
                        best_score = cur_score
                        best_order = [v for
                                  k, v in sorted(node_idx_to_name.items())]

        # Compute peak memory usage
        new_peak_memory_usage = compute_peak_memory_usage(
                    self.execution_graph.graph, best_order,
                    self.node_sizes, self.execution_graph.store_in_memory)
        
        if debug:
            print("Peak memory usage:", new_peak_memory_usage)
            
        return best_order, new_peak_memory_usage

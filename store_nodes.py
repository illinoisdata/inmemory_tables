import networkx as nx
import numpy as np
import copy
from ortools.algorithms import pywrapknapsack_solver      
from utils import *

"""
Baseline method of storing all nodes in memory.
"""
def store_nodes_all(graph, execution_order,
                    node_scores, node_sizes, nodes_to_exclude, debug = False):
    
    new_store_in_memory = set()
    for node in graph.nodes:
        if node not in nodes_to_exclude:
            new_store_in_memory.add(node)

    new_time_save = sum([node_scores[i] for i in new_store_in_memory])
    new_peak_memory_usage = compute_peak_memory_usage(
            graph, execution_order, node_sizes,
            new_store_in_memory)

    if debug:
        print("Time save:", new_time_save)
        print("Peak memory usage:", new_peak_memory_usage)

    return new_store_in_memory, new_time_save, new_peak_memory_usage

"""
Baseline method of storing no nodes in memory.
"""
def store_nodes_none(debug = False):
    if debug:
        print("Time save: 0")
        print("Peak memory usage: 0")
            
    return set(), 0, 0

"""
Baseline greedy node selector
"""
def store_nodes_greedy_forward(graph, execution_order, node_scores, node_sizes,
                               nodes_to_exclude, memory_limit):
    
    # Keep track of when to simulate garbage collection of results
    num_successors = [len(list(graph.successors(i))) for i in execution_order]
    num_successors_dict = dict(zip(execution_order, num_successors))
    
    new_store_in_memory = set()
    current_memory_usage = 0
    
    for name in execution_order:
        # Store node if possible
        if (node_sizes[name] <= memory_limit - current_memory_usage and
            name not in nodes_to_exclude):
            new_store_in_memory.add(name)
            current_memory_usage += node_sizes[name]

        # Find dependencies to garbage collect
        dependencies_to_free = set()
        for parent_name in graph.predecessors(name):
            num_successors_dict[parent_name] -= 1
            if (num_successors_dict[parent_name] == 0 and
                parent_name in new_store_in_memory):
                current_memory_usage -= node_sizes[parent_name]

    new_time_save = sum([node_scores[i] for i in new_store_in_memory])
    new_peak_memory_usage = compute_peak_memory_usage(
            graph, execution_order, node_sizes, new_store_in_memory)

    return new_store_in_memory, new_time_save, new_peak_memory_usage

"""
Baseline greedy node selector, but goes through the execution order in
reverse
"""
def store_nodes_greedy_backward(graph, execution_order, node_scores, node_sizes,
                                nodes_to_exclude, memory_limit):
    
    # Keep track of when to simulate garbage collection of results
    num_successors = [0 for i in execution_order]
    num_successors_dict = dict(zip(execution_order, num_successors))
    
    new_store_in_memory = set()
    current_memory_usage = 0
    
    for name in reversed(execution_order):
        # Find dependencies freed here
        for parent_name in graph.predecessors(name):
            if (num_successors_dict[parent_name] == 0 and
                node_sizes[parent_name] <= memory_limit - current_memory_usage
                and parent_name not in nodes_to_exclude):
                new_store_in_memory.add(parent_name)
                current_memory_usage += node_sizes[parent_name]
                
            num_successors_dict[parent_name] += 1
        
        # node execution
        if name in new_store_in_memory:
            current_memory_usage -= node_sizes[name]
            
    new_time_save = sum([node_scores[i] for i in new_store_in_memory])
    new_peak_memory_usage = compute_peak_memory_usage(
            graph, execution_order, node_sizes, new_store_in_memory)

    return new_store_in_memory, new_time_save, new_peak_memory_usage

"""
Baseline greedy node selector, choosing the better result from iterating
forward or backward
"""
def store_nodes_greedy(graph, execution_order, node_scores, node_sizes,
                       nodes_to_exclude, memory_limit, debug = False):
    
    s_forward, t_forward, m_forward = \
               store_nodes_greedy_forward(graph = graph,
                                          execution_order = execution_order,
                                          node_scores = node_scores,
                                          node_sizes = node_sizes,
                                          nodes_to_exclude = nodes_to_exclude,
                                          memory_limit = memory_limit)

    s_backward, t_backward, m_backward = \
                store_nodes_greedy_backward(graph = graph,
                                            execution_order = execution_order,
                                            node_scores = node_scores,
                                            node_sizes = node_sizes,
                                            nodes_to_exclude = nodes_to_exclude,
                                            memory_limit = memory_limit)

    if t_forward >= t_backward:
        if debug:
            print("Time save:", t_forward)
            print("Peak memory usage:", m_forward)
            
        return s_foward, t_forward, m_forward

    if debug:
        print("Time save:", t_backward)
        print("Peak memory usage:", m_backward)

    return s_backward, t_backward, m_backward
    

"""
Randomly select nodes to store given memory limit.
"""
def store_nodes_random(graph, execution_order, node_scores, node_sizes,
                       nodes_to_exclude, memory_limit, debug = False):
    
    for name in np.random.permutation(graph.nodes):
        new_peak_memory_usage = compute_peak_memory_usage(graph,
            execution_order, node_sizes,
            new_store_in_memory.union({name}))

        if (new_peak_memory_usage <= memory_limit and
            name not in nodes_to_exclude):
            new_store_in_memory.add(name)
            
    new_time_save = sum([node_scores[i] for i in new_store_in_memory])
    new_peak_memory_usage = compute_peak_memory_usage(
            graph, execution_order, node_sizes, new_store_in_memory)

    if debug:
        print("Time save:", new_time_save)
        print("Peak memory usage:", new_peak_memory_usage)

    return new_store_in_memory, new_time_save, new_peak_memory_usage

"""
Step 1: find all relevant maximal sets of results which act as constraints
for the multidimensional knapsack problem.
debug: show maximal set count.
"""
def find_maximal_sets(graph, execution_order,node_scores, node_sizes,
                      nodes_to_exclude, memory_limit, debug = False):
    
    # Keep track of when to simulate garbage collection of results
    num_successors = [len(list(graph.successors(i))) for i in execution_order]
    num_successors_dict = dict(zip(execution_order, num_successors))
    
    maximal_sets = []
    current_set = set()
    current_memory_usage = 0
    
    for name in execution_order:
        if name not in nodes_to_exclude:
            current_set.add(name)
            current_memory_usage += node_sizes[name]

        # Find dependencies to garbage collect
        dependencies_to_free = set()
        for parent_name in graph.predecessors(name):
            num_successors_dict[parent_name] -= 1
            if (num_successors_dict[parent_name] == 0 and
                parent_name not in nodes_to_exclude):
                dependencies_to_free.add(parent_name)

        # If there are dependencies to remove, current set must be maximal.
        # Add the set if it is a nontrivial constraint (i.e. the combined
        # size exceeds the memory limit)
        if (len(dependencies_to_free) > 0 and
            current_memory_usage > memory_limit):
            maximal_sets.append(copy.deepcopy(current_set))

        # Simulate garbage collection of dependencies
        for parent_name in dependencies_to_free:
            current_memory_usage -= node_sizes[parent_name]
        current_set = current_set.difference(dependencies_to_free)

    # Edge case for adding last set
    if (current_memory_usage > memory_limit and (
        not current_set.issubset(maximal_sets[-1]) or len(maximal_sets) == 0)):
        maximal_sets.append(copy.deepcopy(current_set))

    if debug:
        print("number of maximal sets:", len(maximal_sets))

    return maximal_sets

"""
Step 2: Run a multidimensional 0-1 knapsack solver using branch & bound to
find the optimal set of nodes to store in memory given memory size
constraint.
Returns the nodes to store in memory, best time save and peak memory usage.
debug: Print time saved/memory usage given set of nodes to store in memory.
"""
def mkp(graph, execution_order, node_scores, node_sizes, nodes_to_exclude,
        memory_limit, maximal_sets, debug = False):
    
    new_store_in_memory = set()
    
    # Trivial computation where there are no maximal sets; store all nodes
    # which are not trivially excluded.
    if len(maximal_sets) == 0:
        for name in execution_order:
            if name not in nodes_to_exclude:
                new_store_in_memory.add(name)

        # Compute time save (score) & peak memory usage
        new_time_save = sum([node_scores[name] for name in new_store_in_memory])
        new_peak_memory_usage = compute_peak_memory_usage(
            graph, execution_order, node_sizes, new_store_in_memory)

        if debug:
            print("Time save:", new_time_save)
            print("Peak memory usage:", new_peak_memory_usage)
        
        return new_store_in_memory, new_time_save, new_peak_memory_usage
    
    # The vector consists of nodes which appear in at least 1 maximal set.
    vector_nodes = set()
    for maximal_set in maximal_sets:
        vector_nodes = vector_nodes.union(maximal_set)
    vector_nodes = list(vector_nodes)

    # Scale scores to transform problem into integer programming
    multiplier = 1 / min([node_scores[name] for name in vector_nodes])
            
    profits = [node_scores[name] * multiplier for name in vector_nodes]
    weights = []
    for maximal_set in maximal_sets:
        weights.append([int(name in maximal_set) * node_sizes[name]
                        for name in vector_nodes])
    capacities = [memory_limit for i in range(len(maximal_sets))]

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

    new_time_save = time_save / multiplier

    # Add nodes not in any valid maximal set due to all member maximal sets
    # which it is a part of being under the size limit.
    for name in graph.nodes:
        if name not in vector_nodes and name not in nodes_to_exclude:
            new_store_in_memory.add(name)
            new_time_save += node_scores[name]

    # Compute time save & peak memory usage
    new_peak_memory_usage = compute_peak_memory_usage(
            graph, execution_order, node_sizes, new_store_in_memory)

    if debug:
        print("Time save:", new_time_save)
        print("Max memory usage:", new_peak_memory_usage)
    
    return new_store_in_memory, new_time_save, new_peak_memory_usage

"""
Select nodes to store using the MKP solver.
"""
def store_nodes_mkp(graph, execution_order, node_scores, node_sizes,
                    nodes_to_exclude, memory_limit, debug = False):
    
    maximal_sets = find_maximal_sets(graph = graph,
                                     execution_order = execution_order,
                                     node_scores = node_scores,
                                     node_sizes = node_sizes,
                                     nodes_to_exclude = nodes_to_exclude,
                                     memory_limit = memory_limit,
                                     debug = debug)
    
    new_store_in_memory, new_time_save, new_peak_memory_usage = \
                        mkp(graph = graph,
                            execution_order = execution_order,
                            node_scores = node_scores,
                            node_sizes = node_sizes,
                            nodes_to_exclude = nodes_to_exclude,
                            memory_limit = memory_limit,
                            maximal_sets = maximal_sets,
                            debug = debug)

    return new_store_in_memory, new_time_save, new_peak_memory_usage

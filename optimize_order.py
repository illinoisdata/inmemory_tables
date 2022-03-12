import networkx as nx
import random
import math
import numpy as np
from ExecutionGraph import *
from utils import *

"""
Step 3: Find a better execution order by running DFS with a heuristic of
minimizing the amount of time large results stay in memory.
Returns the new execution order and its peak memory usage.
debug: Print memory usage given execution order.
"""
def dfs_topological(graph, node_sizes, store_in_memory, debug = False):
    # Compute the actual memory usage of nodes based on its size and whether
    # it is stored in memory or not (i.e. if its not, then the usage is 0)
    memory_usage = {name: int(name in store_in_memory) * node_sizes[name]
                    for name in graph.nodes}

    # Prioritize adding nodes with large memory usage to the stack first.
    sorted_memory_usage = [k for k, v in sorted(memory_usage.items(),
                                                key = lambda item: -item[1])]
    
    visited = set()
    stack = []
    
    for name in sorted_memory_usage:
        if name not in visited:
            dfs_topological_helper(name, graph, visited, stack, memory_usage)

    new_execution_order = stack[::-1]

    # Compute peak memory usage
    new_peak_memory_usage = compute_peak_memory_usage(
                graph, new_execution_order, node_sizes, store_in_memory)
    
    if debug:
        print("Peak memory usage:", new_peak_memory_usage)

    return new_execution_order, new_peak_memory_usage

"""
Helper recursive function for step 3 DFS.
"""
def dfs_topological_helper(name, graph, visited, stack, memory_usage):
    visited.add(name)

    children = graph.successors(name)
    sorted_children = sorted(children, key = lambda item: -memory_usage[item])

    for child_name in sorted_children:
        if child_name not in visited:
            dfs_topological_helper(child_name, graph, visited, stack,
                                        memory_usage)

    stack.append(name)

"""
Step 4: Further improve the execution order by optimizing it in terms of
weighted minimum linear arrangement via simulated annealing.
new_execution_order: a default execution order (passed from step 3)
"""
def simulated_annealing(graph, node_sizes, store_in_memory, memory_limit,
                        new_execution_order, n_iters = 10000, debug = False):
    # Compute the actual memory usage of nodes based on its size and whether
    # it is stored in memory or not (i.e. if its not, then the usage is 0)
    memory_usage = {name: int(name in store_in_memory) * node_sizes[name]
                    for name in graph.nodes}

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

    # Higher temperatire leads to higher probability of picking a worse
    # solution at any time step.
    temperature = \
        max(list(memory_usage.values())) / len(list(memory_usage.values()))
    
    for i in range(n_iters):
        # Select some random node
        u = random.randint(0, len(new_execution_order) - 1)
        max_parent = max([node_name_to_idx[p]
            for p in graph.predecessors(node_idx_to_name[u])], default = 0)
        min_child = min([node_name_to_idx[c]
            for c in graph.successors(node_idx_to_name[u])],
                        default = len(new_execution_order) - 1)

        # Current node cannot be swapped
        if min_child - max_parent <= 2:
            continue

        # Randomly select a valid position for swapping
        v = random.randint(max_parent + 1, min_child - 1)

        # Evaluate swap
        score_change = minLA_change(graph, node_idx_to_name, node_name_to_idx,
                                    memory_usage, node_idx_to_name[u], v)

        # Metropolis
        t = temperature / float(i + 1)
        
        if (score_change > 0 or
            random.random() < math.exp(score_change / t)):
            minLA_apply(node_idx_to_name, node_name_to_idx,
                        node_idx_to_name[u], v)

            # Revert change if the order violates memory limit
            cur_order = [v for k, v in sorted(node_idx_to_name.items())]
            if compute_peak_memory_usage(graph, cur_order, node_sizes,
                store_in_memory) > memory_limit:
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
                graph, best_order, node_sizes, store_in_memory)
    
    if debug:
        print("Peak memory usage:", new_peak_memory_usage)
        
    return best_order, new_peak_memory_usage

def recursive_min_cut(graph, node_sizes, store_in_memory, debug = False):
    # Compute the actual memory usage of nodes based on its size and whether
    # it is stored in memory or not (i.e. if its not, then the usage is 0)
    memory_usage = {name: int(name in store_in_memory) * node_sizes[name]
                    for name in graph.nodes}

    max_capacity = sum(memory_usage.values())

    # Augment graph
    new_graph = nx.DiGraph()
    
    for node in graph.nodes():
        new_graph.add_node(node)
        new_graph.add_node(node + "_done")
        new_graph.add_edge(node, node + "_done", capacity = memory_usage[node])

    for edge in graph.edges():
        new_graph.add_edge(edge[0], edge[1], capacity = 0)
        new_graph.add_edge(edge[1], edge[0] + "_done", capacity = 0)

    raw_execution_order = recursive_min_cut_helper(new_graph, memory_usage,
                                               max_capacity, debug = debug)

    # Clean execution order
    execution_order = []
    for name in raw_execution_order:
        if name in graph.nodes():
            execution_order.append(name)

    # Compute peak memory usage
    new_peak_memory_usage = compute_peak_memory_usage(
                graph, execution_order, node_sizes, store_in_memory)

    if debug:
        print("Peak memory usage:", new_peak_memory_usage)

    return execution_order, new_peak_memory_usage

def recursive_min_cut_helper(new_graph, memory_usage, max_capacity,
                             debug = False):
    if debug:
        #print(new_graph.edges())
        pass
        
    # Base case
    if new_graph.number_of_nodes() == 1 or new_graph.number_of_edges() == 0:
        return list(new_graph.nodes())
    
    # Add aggregate source/sink
    sources = []
    sinks = []
    
    for node in new_graph.nodes():
        if len(list(new_graph.predecessors(node))) == 0:
            sources.append(node)  
        if len(list(new_graph.successors(node))) == 0:
            sinks.append(node)

    new_graph.add_node("source")
    new_graph.add_node("sink")

    for node in sources:
        new_graph.add_edge("source", node, capacity = max_capacity + 1)
    for node in sinks:
        new_graph.add_edge(node, "sink", capacity = max_capacity + 1)

    cut_value, partition = nx.minimum_cut(new_graph, "source", "sink")

    while True:
        changed = False
        for edge in new_graph.edges:
            if edge[1] in partition[0] and edge[0] in partition[1]:
                partition[0].remove(edge[1])
                partition[1].add(edge[1])
                changed = True
                
        if not changed:
            break
    
    if debug:
        print(cut_value)
        #print("partition 0:", new_graph.subgraph(partition[0]).nodes())
        #print("partition 1:", new_graph.subgraph(partition[1]).nodes())
        pass

    partition_0 = nx.DiGraph(new_graph.subgraph(partition[0]))
    partition_1 = nx.DiGraph(new_graph.subgraph(partition[1]))

    partition_0.remove_node("source")
    partition_1.remove_node("sink")

    source_half = recursive_min_cut_helper(partition_0,
                                           memory_usage, max_capacity, debug)
    sink_half = recursive_min_cut_helper(partition_1,
                                         memory_usage, max_capacity, debug)

    return source_half + sink_half    


def recursive_min_cut2(graph, node_sizes, store_in_memory, debug = False):
    # Compute the actual memory usage of nodes based on its size and whether
    # it is stored in memory or not (i.e. if its not, then the usage is 0)
    memory_usage = {name: int(name in store_in_memory) * node_sizes[name]
                    for name in graph.nodes}

    raw_execution_order = recursive_min_cut_helper2(graph, memory_usage,
                                               max_capacity, debug)

    new_graph = nx.DiGraph(graph)

    # Compute peak memory usage
    new_peak_memory_usage = compute_peak_memory_usage(
                new_graph, execution_order, node_sizes, store_in_memory)

    if debug:
        print("Peak memory usage:", new_peak_memory_usage)

    return execution_order, new_peak_memory_usage

def recursive_min_cut_helper2(new_graph, memory_usage, max_capacity,
                             debug = False):
    if debug:
        #print(new_graph.edges())
        pass
        
    # Base case
    if new_graph.number_of_nodes() == 1:
        return list(new_graph.nodes())
    
    sep, part1, part2 = nxmetis.vertex_separator(new_graph,
                                                 weight = memory_usage)
    if debug:
        print(cut_value)
        #print("partition 0:", new_graph.subgraph(partition[0]).nodes())
        #print("partition 1:", new_graph.subgraph(partition[1]).nodes())
        pass

    partition_0 = nx.DiGraph(new_graph.subgraph(part1 + sep))
    partition_1 = nx.DiGraph(new_graph.subgraph(part2))

    source_half = recursive_min_cut_helper2(partition_0,
                                           memory_usage, max_capacity, debug)
    sink_half = recursive_min_cut_helper2(partition_1,
                                         memory_usage, max_capacity, debug)

    return source_half + sink_half    

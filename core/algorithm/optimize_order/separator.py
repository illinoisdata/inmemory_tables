#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois
from typing import List

from core.algorithm.optimize_order.order_optimizer import OrderOptimizer
import networkx as nx


class OptimizeOrderSeparator(OrderOptimizer):
    """
        Computes the new execution order via 'separator', i.e. recursively finding minimum cuts and ordering the
        resulting subgraphs.
        The minimum cut value conceptually is the peak memory usage.

        Args:
            debug (bool):
                whether to print debug messages during optimization.
    """
    def __init__(self, debug=False):
        super().__init__(debug)

    """
        Helper function for recursively finding minimum cut in a graph.
    """
    def recursive_min_cut_helper(self, subgraph, max_capacity):
        # Base case
        if subgraph.number_of_nodes() == 1 or subgraph.number_of_edges() == 0:
            return list(subgraph.nodes())

        # Add aggregate source/sink
        sources = []
        sinks = []

        for node in subgraph.nodes():
            if len(list(subgraph.predecessors(node))) == 0:
                sources.append(node)
            if len(list(subgraph.successors(node))) == 0:
                sinks.append(node)

        subgraph.add_node("source")
        subgraph.add_node("sink")

        for node in sources:
            subgraph.add_edge("source", node, capacity=max_capacity + 1)
        for node in sinks:
            subgraph.add_edge(node, "sink", capacity=max_capacity + 1)

        cut_value, partition = nx.minimum_cut(subgraph, "source", "sink")

        while True:
            changed = False
            for edge in subgraph.edges:
                if edge[1] in partition[0] and edge[0] in partition[1]:
                    partition[0].remove(edge[1])
                    partition[1].add(edge[1])
                    changed = True

            if not changed:
                break

        if self.debug:
            print("cut value:", cut_value)
            pass

        partition_0 = nx.DiGraph(subgraph.subgraph(partition[0]))
        partition_1 = nx.DiGraph(subgraph.subgraph(partition[1]))

        partition_0.remove_node("source")
        partition_1.remove_node("sink")

        source_half = self.recursive_min_cut_helper(partition_0, max_capacity)
        sink_half = self.recursive_min_cut_helper(partition_1, max_capacity)

        return source_half + sink_half

    def optimize_order(self) -> List[str]:
        max_capacity = sum(self.memory_usage.values())

        # Augment graph
        new_graph = nx.DiGraph()

        for node in self.graph.nodes():
            new_graph.add_node(node)
            new_graph.add_node(node + "_done")
            new_graph.add_edge(node, node + "_done", capacity=self.memory_usage[node])

        for edge in self.graph.edges():
            new_graph.add_edge(edge[0], edge[1], capacity=0)
            new_graph.add_edge(edge[1], edge[0] + "_done", capacity=0)

        raw_execution_order = self.recursive_min_cut_helper(new_graph, max_capacity)

        # Clean execution order
        execution_order = []
        for name in raw_execution_order:
            if name in self.graph.nodes():
                execution_order.append(name)

        return execution_order

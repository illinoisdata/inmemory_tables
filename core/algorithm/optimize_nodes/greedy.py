#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois
from core.algorithm.optimize_nodes.nodes_optimizer import NodesOptimizer


class FlagNodesGreedy(NodesOptimizer):
    """
        Greedy node selector; simulates executing the nodes in the specified execution order, and always flags a node
        if current memory usage allows.

        Args:
            debug (bool):
                whether to print debug messages during optimization.
    """
    def __init__(self, debug=False):
        super().__init__(debug)

    def flag_nodes(self) -> set(str):
        # Keep track of when to simulate garbage collection of results
        num_successors = [len(list(self.graph.successors(i))) for i in self.execution_order]
        num_successors_dict = dict(zip(self.execution_order, num_successors))

        nodes_to_flag_names = set()
        current_memory_usage = 0

        for name in self.execution_order:
            if name in self.nodes_to_exclude:
                continue

            # Store node if possible
            if self.node_sizes[name] <= self.memory_limit - current_memory_usage and name not in self.nodes_to_exclude:
                nodes_to_flag_names.add(name)
                current_memory_usage += self.node_sizes[name]

            # Find dependencies to garbage collect
            dependencies_to_free = set()
            for parent_name in self.graph.predecessors(name):
                num_successors_dict[parent_name] -= 1
                if (num_successors_dict[parent_name] == 0 and
                        parent_name in nodes_to_flag_names):
                    current_memory_usage -= self.node_sizes[parent_name]

        return nodes_to_flag_names

#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois
from core.algorithm.optimize_nodes.nodes_optimizer import NodesOptimizer
from core.utils import compute_peak_memory_usage


class FlagNodesHeuristic(NodesOptimizer):
    """
        Heuristic node selector; flags nodes (if memory allows) in the order of ratio of speedup score to node size.

        Args:
            debug (bool):
                whether to print debug messages during optimization.
    """
    def __init__(self, debug=False):
        super().__init__(debug)

    def flag_nodes(self) -> set:
        nodes_to_flag_names = set()

        # Compute ratio for each node
        heuristic_dict = {}
        for name in self.execution_order:
            try:
                heuristic_dict[name] = self.node_scores[name] / self.node_sizes[name]
            except:
                heuristic_dict[name] = self.node_scores[name]

        # iterate through nodes in random order
        for name in [k for k, v in sorted(heuristic_dict.items(), key=lambda item:item[1])]:
            if name in self.nodes_to_exclude:
                continue

            peak_memory_usage = compute_peak_memory_usage(self.graph, self.execution_order, self.node_sizes,
                                                          nodes_to_flag_names.union({name}))

            # flag node if current memory usage allows
            if (peak_memory_usage <= self.memory_limit and
                    name not in self.nodes_to_exclude):
                nodes_to_flag_names.add(name)

        return nodes_to_flag_names


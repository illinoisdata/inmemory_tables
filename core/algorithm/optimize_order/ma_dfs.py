#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois
from typing import List

from core.algorithm.optimize_order.order_optimizer import OrderOptimizer
from core.utils import compute_peak_memory_usage


class OptimizeOrderMADFS(OrderOptimizer):
    """
        Computes the new execution order via memory-aware DFS (MA-DFS), i.e. tie-breaking based on actual memory usage
        of nodes.

        Args:
            debug (bool):
                whether to print debug messages during optimization.
    """
    def __init__(self, debug=False):
        super().__init__(debug)

    """
    Helper recursive function for MA-DFS.
    """
    def ma_dfs_helper(self, node_name, visited, stack):
        visited.add(node_name)

        children = self.graph.successors(node_name)
        sorted_children = sorted(children, key=lambda item: -self.memory_usage[item])

        for child_name in sorted_children:
            if child_name not in visited:
                self.ma_dfs_helper(child_name, visited, stack)

        stack.append(node_name)

    def optimize_order(self) -> List[str]:
        # Prioritize adding nodes with large memory usage to the stack first.
        sorted_memory_usage = [k for k, v in sorted(self.memory_usage.items(), key=lambda item: -item[1])]

        visited = set()
        stack = []

        for name in sorted_memory_usage:
            if name not in visited:
                self.ma_dfs_helper(name, visited, stack)

        execution_order = stack[::-1]

        return execution_order


#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois
from typing import List


class OrderOptimizer(object):
    """
        The OrderOptimizer class picks the execution order given a set of flagged nodes and the memory limit.

        Args:
            debug (bool):
                whether to print debug messages during optimization.
    """
    def __init__(self, debug=False):
        self.graph = None
        self.flagged_nodes_names = None
        self.cur_execution_order = None
        self.memory_limit = None
        self.node_scores = None
        self.node_sizes = None
        self.debug = debug

        self.memory_usage = None

    """
        Initializes the OrderOptimizer with the execution graph and related information.

        Args:
            graph (ExecutionGraph):
                the execution graph to compute the nodes to flag for
            memory_limit (int):
                the spare memory size in bytes
            node_scores (Dict):
                the estimated time savings of flagging each node
            node_sizes (Dict):
                the estimated sizes of each node
        """
    def set_graph(self, graph, cur_execution_order, memory_limit, node_scores, node_sizes):
        self.graph = graph
        self.cur_execution_order = cur_execution_order
        self.memory_limit = memory_limit
        self.node_scores = node_scores
        self.node_sizes = node_sizes

    """
        Sets the flagged nodes for the current iteration.
    """
    def set_flagged_nodes(self, flagged_nodes_names):
        self.flagged_nodes_names = flagged_nodes_names

        # Compute the actual memory usage of nodes based on its size and whether
        # it is stored in memory or not (i.e. if it iss not, then the usage is 0)
        self.memory_usage = {name: int(name in self.flagged_nodes_names) * self.node_sizes[name]
                             for name in self.graph.nodes}

    """
        Sets the current execution order (i.e. order prior to optimization). Used by certain optimization methods.
    """
    def set_cur_execution_order(self, cur_execution_order):
        self.cur_execution_order = cur_execution_order

    def optimize_order(self) -> List[str]:
        """
            Classes that inherit from the `OrderOptimizer` class (such as `MA-DFS` and various baselines) should
            override `optimize_order`.

        Returns:
            execution_order: the order to execute the nodes in the execution graph in.
        """
        raise NotImplementedError()

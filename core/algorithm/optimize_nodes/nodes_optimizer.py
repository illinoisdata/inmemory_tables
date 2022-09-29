#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois
from typing import List
from networkx import DiGraph

import networkx


class NodesOptimizer(object):
    """
        The NodesOptimizer class picks the nodes to flag given a fixed execution order and memory limit.

        Args:
            debug (bool):
                whether to print debug messages during optimization.
    """
    def __init__(self, debug=False):
        self.graph = None
        self.execution_order = None
        self.memory_limit = None
        self.node_scores = None
        self.node_sizes = None
        self.nodes_to_exclude = None
        self.debug = debug

    """
        Initializes the NodesOptimizer with the execution graph and related information.

        Args:
            graph (networkx.DiGraph):
                the execution graph to compute the nodes to flag for
            execution_order (List):
                the order to execute nodes of the execution graph in
            memory_limit (int):
                the spare memory size in bytes
            node_scores (Dict):
                the estimated time savings of flagging each node
            node_sizes (Dict):
                the estimated sizes of each node
            nodes_to_exclude (List):
                a list of nodes to trivially not flag.
        """
    def set_graph(self, graph: DiGraph, memory_limit: int, node_scores: dict, node_sizes: dict, nodes_to_exclude: List):
        self.graph = graph
        self.memory_limit = memory_limit
        self.node_scores = node_scores
        self.node_sizes = node_sizes
        self.nodes_to_exclude = nodes_to_exclude

    """
        Sets the execution order for the current iteration.
    """
    def set_execution_order(self, execution_order: List):
        self.execution_order = execution_order

    def flag_nodes(self) -> set:
        """
            Classes that inherit from the `NodesOptimizer` class (such as `MKP` and various baselines) should override
            `flag_nodes`.

        Returns:
            nodes_to_flag_names: a subset of nodes to store in memory.
        """
        raise NotImplementedError()

#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois
from core.algorithm.optimize_nodes.nodes_optimizer import NodesOptimizer


class FlagAllBaseline(NodesOptimizer):
    """
        Flags all nodes in the graph disregarding the memory limit.

        Args:
            debug (bool):
                whether to print debug messages during optimization.
    """

    def __init__(self, debug=False):
        super().__init__(debug)

    def flag_nodes(self) -> set:
        nodes_to_flag_names = set()
        for node in self.graph.nodes:
            nodes_to_flag_names.add(node)

        return nodes_to_flag_names


class FlagNoneBaseline(NodesOptimizer):
    """
        Flags all nodes in the graph disregarding the memory limit.

        Args:
            debug (bool):
                whether to print debug messages during optimization.
    """

    def __init__(self, debug=False):
        super().__init__(debug)

    def flag_nodes(self) -> set:
        return set()

#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois

from core.algorithm.optimize_nodes.nodes_optimizer import NodesOptimizer
from core.utils import compute_peak_memory_usage

import numpy as np


class FlagNodesRandom(NodesOptimizer):
    """
        Random node selector; randomly iterates through nodes, and always flags a node if current memory usage allows.

        Args:
            debug (bool):
                whether to print debug messages during optimization.
    """
    def __init__(self, debug=False):
        super().__init__(debug)

    def flag_nodes(self) -> set(str):
        nodes_to_flag_names = set()
        # iterate through nodes in random order
        for name in np.random.permutation(self.execution_order):
            if name in self.nodes_to_exclude:
                continue

            peak_memory_usage = compute_peak_memory_usage(self.graph, self.execution_order, self.node_sizes,
                                                          nodes_to_flag_names.union({name}))

            # flag node if current memory usage allows
            if (peak_memory_usage <= self.memory_limit and
                    name not in self.nodes_to_exclude):
                nodes_to_flag_names.add(name)

        return nodes_to_flag_names

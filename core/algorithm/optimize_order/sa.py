#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois
from typing import List

from core.algorithm.optimize_order.order_optimizer import OrderOptimizer
from core.utils import compute_peak_memory_usage
import random
import math


class OptimizeOrderSA(OrderOptimizer):
    """
        Computes the new execution order via simulated annealing (SA), i.e. temperature-based hill climbing.
        The metric used here is the storage-time product with unit execution time assumption.

        Args:
            debug (bool):
                whether to print debug messages during optimization.
            num_iters (int):
                number of iterations of simulated annealing to run.
    """
    def __init__(self, debug=False, num_iters=10000):
        super().__init__(debug)
        self.num_iters = num_iters

    """
    Computes the change in storage-time product resulting from moving node_to_move to new_node_pos.
    """
    def stp_change(self, node_idx_to_name, node_name_to_idx, node_to_move, new_node_pos):
        old_node_pos = node_name_to_idx[node_to_move]
        change = 0

        # Effect on neighbors
        for parent_name in self.graph.predecessors(node_to_move):
            change += (new_node_pos - old_node_pos) * self.memory_usage[parent_name]
        for child_name in self.graph.successors(node_to_move):
            change -= (new_node_pos - old_node_pos) * self.memory_usage[child_name]

        # Compute change caused by moving node
        for i in range(min(new_node_pos, old_node_pos) + int(old_node_pos < new_node_pos),
                       max(new_node_pos, old_node_pos) + int(old_node_pos < new_node_pos)):
            for parent_name in self.graph.predecessors(node_idx_to_name[i]):
                if node_name_to_idx[parent_name] < old_node_pos:
                    change -= self.memory_usage[parent_name]
            for child_name in self.graph.successors(node_idx_to_name[i]):
                if node_name_to_idx[child_name] > new_node_pos:
                    change += self.memory_usage[child_name]

        return change

    """
    Applies the specified node movement by moving node_to_move to new_node_pos and
    updates the 2 dictionaries accordingly.
    """
    @staticmethod
    def stp_apply(node_idx_to_name, node_name_to_idx, node_to_move, new_node_pos):
        old_node_pos = node_name_to_idx[node_to_move]

        if new_node_pos > old_node_pos:
            for i in range(old_node_pos + 1, new_node_pos + 1):
                i_name = node_idx_to_name[i]
                node_name_to_idx[i_name] = i - 1
                node_idx_to_name[i - 1] = i_name

        else:
            for i in range(old_node_pos - 1, new_node_pos - 1, -1):
                i_name = node_idx_to_name[i]
                node_name_to_idx[i_name] = i + 1
                node_idx_to_name[i + 1] = i_name

        node_name_to_idx[node_to_move] = new_node_pos
        node_idx_to_name[new_node_pos] = node_to_move

    def optimize_order(self) -> List[str]:
        # Set up dictionaries for fast lookup
        node_name_to_idx = {}
        node_idx_to_name = {}
        for i in range(len(self.cur_execution_order)):
            node_name_to_idx[self.cur_execution_order[i]] = i
            node_idx_to_name[i] = self.cur_execution_order[i]

        best_score = 0
        best_order = self.cur_execution_order
        cur_score = 0
        scores = []

        # Higher temperature leads to higher probability of picking a worse
        # solution at any time step.
        temperature = \
            max(list(self.memory_usage.values())) / len(list(self.memory_usage.values()))

        for i in range(self.num_iters):
            # Select some random node
            u = random.randint(0, len(self.cur_execution_order) - 1)
            max_parent = max([node_name_to_idx[p] for p in self.graph.predecessors(node_idx_to_name[u])], default=0)
            min_child = min([node_name_to_idx[c] for c in self.graph.successors(node_idx_to_name[u])],
                            default=len(self.cur_execution_order) - 1)

            # Current node cannot be swapped
            if min_child - max_parent <= 2:
                continue

            # Randomly select a valid position for swapping
            v = random.randint(max_parent + 1, min_child - 1)

            # Evaluate swap
            score_change = self.stp_change(node_idx_to_name, node_name_to_idx, node_idx_to_name[u], v)

            # Metropolis
            t = temperature / float(i + 1)

            if (score_change > 0 or
                    random.random() < math.exp(score_change / t)):
                self.stp_apply(node_idx_to_name, node_name_to_idx, node_idx_to_name[u], v)

                # Revert change if the order violates memory limit
                cur_order = [v for k, v in sorted(node_idx_to_name.items())]
                if compute_peak_memory_usage(self.graph, cur_order, self.node_sizes, self.flagged_nodes_names) \
                        > self.memory_limit:
                    self.stp_apply(node_idx_to_name, node_name_to_idx, node_idx_to_name[v], u)

                else:
                    cur_score += score_change
                    if cur_score > best_score:
                        best_score = cur_score
                        best_order = [v for k, v in sorted(node_idx_to_name.items())]

        return best_order

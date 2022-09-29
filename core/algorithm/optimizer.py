#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois


from core.algorithm.optimize_nodes.nodes_optimizer import NodesOptimizer
from core.algorithm.optimize_order.order_optimizer import OrderOptimizer
from core.utils import compute_peak_memory_usage
import time


class Optimizer(object):
    """
        The Optimizer class performs EM-like optimization alternating between optimizing the set of nodes to flag and
        the execution order.

        Args:
            memory_limit (int):
                The size of spare memory.
            nodes_optimizer (NodesOptimizer):
                The method to use for flagging nodes.
            order_optimizer (OrderOptimizer):
                The method to use for optimizing execution order.
            max_iters (int):
                Maximum iterations of alternating optimization to perform.
    """

    def __init__(self, memory_limit: int, nodes_optimizer: NodesOptimizer, order_optimizer: OrderOptimizer,
                 max_iters=100, debug=False):
        self.memory_limit = memory_limit
        self.nodes_optimizer = nodes_optimizer
        self.order_optimizer = order_optimizer
        self.max_iters = max_iters
        self.debug = debug

        # ExecutionGraph object to optimize.
        self.execution_graph = None

        # Optimization algorithm inputs computed after ExecutionGraph initialization.
        self.node_sizes = None
        self.node_scores = None

        # Nodes to trivially exclude from storing in memory computed after ExecutionGraph initialization.
        self.nodes_to_exclude = None

    """
        Adds the ExecutionGraph to optimize to the optimizer, then computes the node sizes and scores necessary for
        optimization.
    """

    def add_graph(self, execution_graph):
        self.execution_graph = execution_graph

        # Retrieve node sizes & scores for optimization algorithm
        self.node_sizes = {node.get_node_name(): node.get_table_size()
                           for node in self.execution_graph.node_dict.values()}
        self.node_scores = {node.get_node_name(): node.get_time_save()
                            for node in self.execution_graph.node_dict.values()}

        # A node is trivially excluded from storing in memory if its size
        # exceeds the memory limit, if its score is 0, or if it is an output.
        self.nodes_to_exclude = set()
        for name in self.execution_graph.graph.nodes:
            if (self.node_sizes[name] > self.memory_limit or
                    self.node_scores[name] == 0):
                self.nodes_to_exclude.add(name)

        # Sets the graph into the 2 subproblem optimizers.
        self.nodes_optimizer.set_graph(
            self.execution_graph.graph, self.memory_limit, self.node_scores, self.node_sizes, self.nodes_to_exclude)
        self.order_optimizer.set_graph(
            self.execution_graph.graph, self.memory_limit, self.node_scores, self.node_sizes)

    """
        Run the EM algorithm for joint optimization of execution order & nodes to store in memory.
        Should only be called after info on node sizes/costs are available from dry running the execution graph once.
    """

    def optimize(self):
        prev_time_save = 0
        prev_peak_memory_usage = self.memory_limit

        optimization_start = time.time()

        # EM algorithm
        for i in range(self.max_iters):
            if self.debug:
                print("Iteration " + str(i + 1) + ":---------------------")

            # Find optimal nodes to store in memory given topological order
            if self.debug:
                print("Finding nodes to store.............")

            # Use the nodes optimizer to find nodes to flag
            self.nodes_optimizer.set_execution_order(self.execution_graph.execution_order)
            nodes_to_flag_names = self.nodes_optimizer.flag_nodes()

            # Compute time save & peak memory usage of the flagged nodes
            time_save = sum([self.node_scores[i] for i in nodes_to_flag_names])
            peak_memory_usage = compute_peak_memory_usage(
                self.execution_graph.graph, self.execution_graph.execution_order, self.node_sizes, nodes_to_flag_names)

            if self.debug:
                print("Nodes to flag:", nodes_to_flag_names)
                print("Time save:", time_save)
                print("Peak memory usage:", peak_memory_usage)

            prev_peak_memory_usage = peak_memory_usage

            # Early stop if time save cannot be improved
            if time_save <= prev_time_save:
                if self.debug:
                    print("New solution is worse, stopping EM............")
                break

            self.execution_graph.flagged_node_names = nodes_to_flag_names

            # Find optimal topological order given nodes to store
            if self.debug:
                print("Optimizing topological order.............")

            # Use the order optimizer to find execution order
            self.order_optimizer.set_flagged_nodes(self.execution_graph.flagged_node_names)
            self.order_optimizer.set_cur_execution_order(self.execution_graph.execution_order)
            execution_order = self.order_optimizer.optimize_order()

            # Compute peak memory usage
            peak_memory_usage = compute_peak_memory_usage(
                self.execution_graph.graph, execution_order, self.node_sizes, self.execution_graph.flagged_node_names)

            if self.debug:
                print("Execution order:", execution_order)
                print("Peak memory usage:", peak_memory_usage)

            prev_time_save = time_save

            # Early stop if peak memory usage violates memory limit
            if peak_memory_usage > self.memory_limit:
                if self.debug:
                    print("Execution order violates memory limit, stopping EM..........")
                break

            self.execution_graph.execution_order = execution_order

        computation_time = time.time() - optimization_start
        if self.debug:
            print("--------------------------------")
            print("Summary:")
            print("Algorithm computation time:", computation_time)
            print("Flagged nodes:", self.execution_graph.flagged_node_names)
            print("Execution order:", self.execution_graph.execution_order)
            print("Time save:", prev_time_save)
            print("Peak memory usage:", prev_peak_memory_usage)

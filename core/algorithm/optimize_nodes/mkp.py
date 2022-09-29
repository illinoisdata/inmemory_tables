#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois

from core.algorithm.optimize_nodes.nodes_optimizer import NodesOptimizer

from typing import List
from ortools.algorithms import pywrapknapsack_solver
import copy


class FlagNodesMkp(NodesOptimizer):
    """
        Flag nodes by converting the optimization problem to a multidimensional knapsack problem (MKP) and solving it using
        branch & bound.

        Args:
            debug (bool):
                whether to print debug messages during optimization.
    """
    def __init__(self, debug=False):
        super().__init__(debug)

    """
        Find all relevant maximal sets of results which act as constraints
        for the MKP.
    """
    def find_maximal_sets(self) -> List[set]:
        # Keep track of when to simulate garbage collection of results
        num_successors = [len(list(self.graph.successors(i))) for i in self.execution_order]
        num_successors_dict = dict(zip(self.execution_order, num_successors))

        maximal_sets = []
        current_set = set()
        current_memory_usage = 0

        for name in self.execution_order:
            if name not in self.nodes_to_exclude:
                current_set.add(name)
                current_memory_usage += self.node_sizes[name]

            # Find dependencies to garbage collect
            dependencies_to_free = set()
            for parent_name in self.graph.predecessors(name):
                num_successors_dict[parent_name] -= 1
                if num_successors_dict[parent_name] == 0 and parent_name not in self.nodes_to_exclude:
                    dependencies_to_free.add(parent_name)

            # If there are dependencies to remove, current set must be maximal.
            # Add the set if it is a nontrivial constraint (i.e. the combined
            # size exceeds the memory limit)
            if len(dependencies_to_free) > 0 and current_memory_usage > self.memory_limit:
                maximal_sets.append(copy.deepcopy(current_set))

            # Simulate garbage collection of dependencies
            for parent_name in dependencies_to_free:
                current_memory_usage -= self.node_sizes[parent_name]
            current_set = current_set.difference(dependencies_to_free)

        # Edge case for adding last set
        if (current_memory_usage > self.memory_limit and (
                not current_set.issubset(maximal_sets[-1]) or len(maximal_sets) == 0)):
            maximal_sets.append(copy.deepcopy(current_set))

        if self.debug:
            print("number of maximal sets:", len(maximal_sets))

        return maximal_sets

    def flag_nodes(self) -> set(str):
        # Find maximal sets.
        maximal_sets = self.find_maximal_sets()

        nodes_to_flag_names = set()

        # Trivial computation where there are no maximal sets; store all nodes
        # which are not trivially excluded.
        if len(maximal_sets) == 0:
            for name in self.execution_order:
                if name not in self.nodes_to_exclude:
                    nodes_to_flag_names.add(name)

            return nodes_to_flag_names

        # The vector consists of nodes which appear in at least 1 maximal set.
        vector_nodes = set()
        for maximal_set in maximal_sets:
            vector_nodes = vector_nodes.union(maximal_set)
        vector_nodes = list(vector_nodes)

        # Scale scores to transform problem into integer programming
        multiplier = 1 / min([self.node_scores[name] for name in vector_nodes])

        profits = [self.node_scores[name] * multiplier for name in vector_nodes]
        weights = []
        for maximal_set in maximal_sets:
            weights.append([int(name in maximal_set) * self.node_sizes[name]
                            for name in vector_nodes])
        capacities = [self.memory_limit for i in range(len(maximal_sets))]

        # Initialize Google MKP solver
        solver = pywrapknapsack_solver.KnapsackSolver(
            pywrapknapsack_solver.KnapsackSolver.KNAPSACK_MULTIDIMENSION_SCIP_MIP_SOLVER,
            'Multi-dimensional solver')
        solver.Init(profits, weights, capacities)
        solver.set_time_limit(10)
        solver.Solve()

        # Gather nodes to store in memory
        for i in range(len(vector_nodes)):
            if solver.BestSolutionContains(i):
                nodes_to_flag_names.add(vector_nodes[i])

        # Add nodes not in any valid maximal set due to all member maximal sets
        # which it is a part of being under the size limit.
        for name in self.graph.nodes:
            if name not in vector_nodes and name not in self.nodes_to_exclude:
                nodes_to_flag_names.add(name)

        return nodes_to_flag_names

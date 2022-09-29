#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois

"""
Compute the peak memory usage of a configuration of execution plan and set of
nodes to store in memory.
"""


def compute_peak_memory_usage(graph, execution_order, node_sizes,
                              store_in_memory):
    num_successors = [len(list(graph.successors(i))) for i in execution_order]
    num_successors_dict = dict(zip(execution_order, num_successors))
    current_memory_usage = 0
    max_memory_usage = 0

    for name in execution_order:
        if name in store_in_memory:
            current_memory_usage += node_sizes[name]

        if current_memory_usage > max_memory_usage:
            max_memory_usage = current_memory_usage

        for parent_name in graph.predecessors(name):
            num_successors_dict[parent_name] -= 1
            if (num_successors_dict[parent_name] == 0 and
                    parent_name in store_in_memory):
                current_memory_usage -= node_sizes[parent_name]

    return max_memory_usage
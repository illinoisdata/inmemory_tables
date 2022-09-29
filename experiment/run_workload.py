#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois

from core.algorithm.optimizer import Optimizer
from core.algorithm.optimize_order.ma_dfs import OptimizeOrderMADFS
from core.algorithm.optimize_nodes.mkp import FlagNodesMkp
from core.graph.ExecutionGraph import ExecutionGraph
import prestodb

if __name__ == '__main__':
    # Read workload
    f = open("workloads/workload1.txt", "r")
    workload = f.read()

    # Create 2 Presto connections
    cursor_main = prestodb.dbapi.connect(host='localhost', port=8090, user='guest', catalog='hive',
                                         schema='tpcds_10_new_parquet').cursor()
    cursor_materialization = prestodb.dbapi.connect(host='localhost', port=8090, user='guest', catalog='hive',
                                                    schema='tpcds_10_new_parquet').cursor()

    # Create the execution graph
    execution_graph = ExecutionGraph(cursor_main, cursor_materialization, 'memory.default.', workload, debug=True)

    # Run the workload without any optimization.
    runtime = execution_graph.execute()
    print("runtime without optimization (seconds): ", runtime)

    # Dry run workload to collect statistics.
    execution_graph.dry_run(runs=3)

    # Jointly optimize nodes to flag and execution order.
    nodes_optimizer = FlagNodesMkp(debug=True)
    order_optimizer = OptimizeOrderMADFS(debug=True)
    optimizer = Optimizer(400000000, nodes_optimizer, order_optimizer, debug=True)
    execution_graph.optimize(optimizer)

    # Run the workload after optimization.
    runtime = execution_graph.execute()
    print("runtime with optimization (seconds): ", runtime)
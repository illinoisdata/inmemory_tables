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
    cursor_main = prestodb.dbapi.connect(host='localhost', port=8090, user='zl20', catalog='hive',
                                         schema='tpcds_10_rc').cursor()
    cursor_materialization = prestodb.dbapi.connect(host='localhost', port=8090, user='zl20', catalog='hive',
                                                    schema='tpcds_10_rc').cursor()
    cursor_gc = prestodb.dbapi.connect(host='localhost', port=8090, user='zl20', catalog='hive',
                                       schema='tpcds_10_rc').cursor()

    # Create the execution graph
    execution_graph = ExecutionGraph(cursor_main, cursor_materialization, cursor_gc, 'memory.default.', workload, debug=True)

    # Cleanup
    execution_graph.cleanup()

    # Run the workload without any optimization.
    runtime = execution_graph.execute()
    print("runtime without optimization (seconds): ", runtime)

    # Cleanuo
    execution_graph.cleanup()

    # Dry run workload to collect statistics.
    execution_graph.dry_run(runs=1)

    # Jointly optimize nodes to flag and execution order.
    nodes_optimizer = FlagNodesMkp(debug=True)
    order_optimizer = OptimizeOrderMADFS(debug=True)
    optimizer = Optimizer(200000000, nodes_optimizer, order_optimizer, debug=True)
    execution_graph.optimize(optimizer)

    # Run the workload after optimization.
    runtime = execution_graph.execute()
    print("runtime with optimization (seconds): ", runtime)

    # Cleanup
    execution_graph.cleanup()

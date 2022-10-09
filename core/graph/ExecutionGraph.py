#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois
import time

from prestodb.dbapi import Cursor

from core.algorithm.optimizer import Optimizer
from core.graph.ExecutionNode import ExecutionNode
import networkx as nx
import threading
import queue


class ExecutionGraph(object):

    """
        The ExecutionGraph represents the workload of MVs to refresh.

    Args:
        cursor_main (prestodb.Cursor): a cursor for executing Presto queries.
        cursor_materialization (prestodb.Cursor): a cursor given to the materialization queue to materialize
            tables in a separate thread.
        cursor_gc (prestodb.Cursor): a cursor given to the garbage collection queue to drop in-memory tables
            when (i) all of its downstream tables have been computed and (ii) it has been materialized to disk.
        inmemory_prefix (str): the prefix for the schema of the in-memory catalog to keep tables in.
        workload (str): A set of ';' delimited SQL DDL statements for creating tables/MVs.
        debug (bool): whether to print debug message during execution.
    """
    def __init__(self, cursor_main: Cursor, cursor_materialization: Cursor, cursor_gc: Cursor, inmemory_prefix: str,
                 workload: str, debug=False):
        self.cursor_main = cursor_main
        self.cursor_materialization = cursor_materialization
        self.cursor_gc = cursor_gc
        self.inmemory_prefix = inmemory_prefix

        # The graph representation of the current workload.
        self.graph = nx.DiGraph()

        # A mapping from node (table) name to ExecutionNode object for fast lookup.
        self.node_dict = {}

        self.debug = debug
        if self.debug:
            print("Creating nodes...................................")

        self.execution_order = None
        self.flagged_node_names = None

        # Queue & thread for multithreaded materialization of in-memory tables.
        self.materialization_queue = None
        self.materialization_thread = None

        # Queue & thread for multithreaded garbage collection of in-memory tables.
        self.gc_queue = None
        self.gc_thread = None

        # Split the workload into individual SQL statements
        sqls = workload.replace('\n', ' ').split(';')
        for sql in sqls:
            if sql.isspace() or sql == "":
                continue

            # Create an execution node for each SQL statement
            node = ExecutionNode(sql, self.debug)
            self.node_dict[node.get_node_name()] = node
            self.graph.add_node(node.get_node_name())

            if self.debug:
                print("Created node for table " + node.get_node_name())

            self.build_graph()

    def build_graph(self):
        # Remove base table names from dependencies
        for node in self.node_dict.values():
            node.input_node_names = node.get_input_node_names().intersection(set(self.node_dict.keys()))

        # Build dependencies; add directed edges between dependencies.
        for node_name, node in self.node_dict.items():
            for input_node_name in node.get_input_node_names():
                self.graph.add_edge(input_node_name, node_name)
                self.node_dict[input_node_name].add_downstream_node(node)

        # Joint optimization outputs; use a default topological order as the execution order and flag no nodes.
        self.execution_order = list(nx.topological_sort(self.graph))
        self.flagged_node_names = set()

        # Queue & thread for multithreaded materialization of in-memory tables.
        self.materialization_queue = queue.Queue()
        self.materialization_thread = None

        # Queue & thread for multithreaded garbage collection of in-memory tables.
        self.gc_queue = queue.Queue()
        self.gc_thread = None

    """
        Dry run the workload to collect statistics on estimated table sizes and time savings.
        
        Args:
            runs (int): number of runs to perform to reduce variance.
    """
    def dry_run(self, runs=1):
        if self.debug:
            print("Dry running. Creating tables..........................")
        # Create all tables
        for node_name in self.execution_order:
            self.node_dict[node_name].create_table(self.cursor_main)

        if self.debug:
            print("Collecting statistics..........................")

        # Collect statistics
        for node_name in self.execution_order:
            self.node_dict[node_name].compute_table_size(self.cursor_main)
            self.node_dict[node_name].compute_time_save(self.cursor_main, self.inmemory_prefix, runs=runs)

        if self.debug:
            print("Cleaning up tables..........................")

        # Cleanup tables
        for node_name in self.execution_order:
            self.node_dict[node_name].drop_table(self.cursor_main)
            self.node_dict[node_name].drop_table(self.cursor_main, self.inmemory_prefix, on_disk=False)

        if self.debug:
            print("Dry run complete.")

    """
        Jointly optimize the nodes to flag and execution order using the provided optimizer.
        
        Args:
            optimizer (Optimizer): the optimizer to use for joint optimization.
    """
    def optimize(self, optimizer: Optimizer):
        optimizer.add_graph(self)
        optimizer.optimize()


    """
        Separate materialization thread for parallel materialization of in-memory tables.
    """
    def materialization_func(self):
        for node in iter(self.materialization_queue.get, None):
            node.materialize_table(self.cursor_materialization, self.inmemory_prefix)

        # Block & wait for materialization to finish
        while True:
            try:
                self.cursor_materialization.fetchall()
            except:
                break

    """
            Separate materialization thread for parallel materialization of in-memory tables.
        """

    def gc_func(self):
        for node in iter(self.gc_queue.get, None):
            node.drop_table(self.cursor_gc, self.inmemory_prefix, on_disk=False, block=False)

        # Block & wait for garbage collection to finish
        while True:
            try:
                self.cursor_gc.fetchall()
            except:
                break

    """
        Run the workload and refresh the MVs.
    """
    def execute(self):
        if self.debug:
            print("Starting workload execution.........................")

        execution_start_time = time.time()

        # Start multithreaded table materializer
        self.materialization_thread = threading.Thread(target=self.materialization_func)
        self.materialization_thread.start()

        # Start multithreaded table garbage collector
        self.gc_thread = threading.Thread(target=self.gc_func)
        self.gc_thread.start()

        # Maintain a list of successors yet to be computed for each node; garbage collect an in-memory table
        # When all of its downstream tables are computed.
        num_successors_dict = {node_name: len(node.downstream_nodes) for node_name, node in self.node_dict.items()}

        # Run nodes in given execution order
        for node_name in self.execution_order:
            node = self.node_dict[node_name]

            # Execute current node; create table in memory if node is flagged, create on disk otherwise.
            node.create_table(self.cursor_main, self.inmemory_prefix, self.flagged_node_names,
                              node_name not in self.flagged_node_names)

            # Concurrent materialization of in-memory table
            if node_name in self.flagged_node_names:
                self.materialization_queue.put(node)

            # Concurrent garbage collection of in-memory table
            num_successors_dict[node_name] -= 1
            if num_successors_dict[node_name] == 0 and node_name in self.flagged_node_names:
                self.gc_queue.put(node)

        if self.debug:
            print("waiting for materialization thread to finish. Time elapsed:", time.time() - execution_start_time)

        # Join multithreaded writer
        self.materialization_queue.put(None)
        self.materialization_thread.join()

        # Join multithreaded garbage collector
        self.gc_queue.put(None)
        self.gc_thread.join()

        # Compute execution time breakdown
        execution_end_time = time.time()

        if self.debug:
            print("total execution time:", execution_end_time - execution_start_time)

        return execution_end_time - execution_start_time

    """
       Drop all tables in the workload.
    """
    def cleanup(self):
        if self.debug:
            print("Cleaning up tables.................................")

        for node_name in self.execution_order:
            self.node_dict[node_name].drop_table(self.cursor_main)
            self.node_dict[node_name].drop_table(self.cursor_main, inmemory_prefix = self.inmemory_prefix, on_disk=False)

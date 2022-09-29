#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois
from sql_metadata import Parser
from prestodb.dbapi import Cursor


class ExecutionNode(object):

    """
        An execution node represents an intermediate table/MV to update.

    Args:
        sql (str): The DDL for creating the table/MV.
        debug (bool): whether to print debug message during execution.
    """
    def __init__(self, sql: str, debug=False):
        self.sql = sql

        # Parse table name and input tables names (dependencies).
        tokens = Parser(sql).tables
        self.node_name = tokens[0]
        self.input_node_names = set(tokens[1:])

        self.debug = debug

        # Downstream tables/MVs which uses this node as an input.
        self.downstream_nodes = set()

        # Estimated size of the table in bytes.
        self.table_size = 0

        # Estimated time saving for workload by keeping this table in memory.
        self.time_save = 0

    def get_sql(self) -> str:
        return self.sql

    def get_node_name(self) -> str:
        return self.node_name

    def get_input_node_names(self) -> set:
        return self.input_node_names

    def add_downstream_node(self, node):
        self.downstream_nodes.add(node)

    """
        Estimate the table size by sending an ANALYZE query to presto. This is a slight overestimation (< 105%) of
        the actual table size.
        
        Args:
            cursor (prestodb.Cursor): a cursor for executing Presto queries.
    """
    def compute_table_size(self, cursor: Cursor):
        cursor.execute('ANALYZE ' + self.node_name)
        cursor.fetchone()
        self.table_size = int(cursor.stats['processedBytes'])

        if self.debug:
            print("Est. table size of " + self.node_name + ": " + str(self.table_size))

    def get_table_size(self) -> int:
        return self.table_size

    """
        Computes the estimated time save for the workload of keeping this table in memory. Time save is estimated as
        the sum of time saved by (i) each downstream table reading this table from memory instead of from disk and
        (ii) parallel materialization of this table with subsequent operations.
        
        Args:
            cursor (prestodb.Cursor): a cursor for executing Presto queries.
            inmemory_prefix (str): the prefix for the schema of the in-memory catalog to keep tables in.
            runs (int): compute the time save as the average of a given number of runs to reduce variance.
    """
    def compute_time_save(self, cursor: Cursor, inmemory_prefix: str, runs=1):
        if self.debug:
            print("Estimating time save for table " + self.node_name + ":---------------------")

        self_test_node_name = self.node_name + '_test'
        self_inmemory_test_node_name = inmemory_prefix + self.node_name + '_test'

        time_save_history = []
        for i in range(runs):
            time_save = 0

            # Time of creating this table on disk
            cursor.execute(self.sql.replace(self.node_name, self_test_node_name))
            cursor.fetchone()
            time_save += int(cursor.stats['elapsedTimeMillis']) / 1000

            # Time of creating this table in memory
            cursor.execute(self.sql.replace(self.node_name, self_inmemory_test_node_name))
            cursor.fetchone()
            time_save -= int(cursor.stats['elapsedTimeMillis']) / 1000

            for table in self.downstream_nodes:
                downstream_test_node_name = table.get_node_name() + '_test'

                # Time of constructing downstream table as is
                cursor.execute(table.get_sql().replace(table.get_node_name(), downstream_test_node_name)
                               .replace(' ' + self.node_name + ' ', ' ' + self_test_node_name + ' '))
                cursor.fetchone()
                time_save += int(cursor.stats['elapsedTimeMillis']) / 1000

                # Cleanup downstream table
                cursor.execute("DROP TABLE IF EXISTS " + downstream_test_node_name)
                cursor.fetchone()

                # Time of constructing downstream table given current table is in memory
                cursor.execute(table.get_sql().replace(table.get_node_name(), downstream_test_node_name)
                               .replace(' ' + self.node_name + ' ', ' ' + self_inmemory_test_node_name + ' '))
                cursor.fetchone()
                time_save -= int(cursor.stats['elapsedTimeMillis']) / 1000

                # Cleanup downstream table
                cursor.execute("DROP TABLE IF EXISTS " + downstream_test_node_name)
                cursor.fetchone()

            # Cleanup test tables
            cursor.execute("DROP TABLE IF EXISTS " + self_test_node_name)
            cursor.fetchone()
            cursor.execute("DROP TABLE IF EXISTS " + self_inmemory_test_node_name)
            cursor.fetchone()

            time_save_history.append(time_save)

        self.time_save = max(sum(time_save_history) / runs, 0)

        if self.debug:
            print("Est. time save (seconds) of keeping " + self.node_name + " in memory: " + str(self.time_save))

    def get_time_save(self):
        return self.time_save

    """
        Create this table/MV on disk by executing the contained SQL statement.
        
        Args:
            cursor (prestodb.Cursor): a cursor for executing Presto queries.
            inmemory_prefix (str): the prefix for the schema of the in-memory catalog.
            flagged_node_names (List(str)): flagged nodes.
            on_disk (int): whether to create this table on disk. If false, this table is created in memory.
    """
    def create_table(self, cursor: Cursor, inmemory_prefix="", flagged_node_names=set(), on_disk=True):
        if self.debug:
            print("Start executing node " + self.name + ":")
            
        # Append the in-memory prefix to input tables in memory.
        sql_command = self.sql
        for input_name in flagged_node_names.intersection(self.input_node_names):
            sql_command = sql_command.replace(' ' + input_name + ' ', ' ' + inmemory_prefix + input_name + ' ')

        # Append the in-memory prefix to the name of this table if creating in memory.
        if not on_disk:
            sql_command = sql_command.replace(self.node_name, inmemory_prefix + self.node_name)

        # Execute the SQL statement.
        cursor.execute(sql_command)
        cursor.fetchone()

        if self.debug:
            time_elapsed = int(cursor.stats['elapsedTimeMillis']) / 1000
            print("Finished executing node " + self.name + ". Time elapsed: " + str(time_elapsed))

    """
        Materialize the in-memory table to disk.
        
        Args:
            cursor (prestodb.Cursor): a cursor for executing Presto queries.
            inmemory_prefix (str): the prefix for the schema of the in-memory catalog.
    """
    def materialize_table(self, cursor: Cursor, inmemory_prefix: str):
        sql_command = "CREATE TABLE " + self.node_name + " WITH (format = 'PARQUET') AS (SELECT * FROM " + \
            inmemory_prefix + self.node_name + ")"

        # Execute the SQL statement.
        cursor.execute(sql_command)
        cursor.fetchone()

    """
        Drop the created table.
        
        Args:
            cursor (prestodb.Cursor): a cursor for executing Presto queries.
            inmemory_prefix (str): the prefix for the schema of the in-memory catalog.
            on_disk (int): whether the table to drop is on disk. If false, the table to drop is in memory.
    """
    def drop_table(self, cursor: Cursor, inmemory_prefix="", on_disk=True):
        if on_disk:
            cursor.execute("DROP TABLE IF EXISTS " + self.node_name)
        else:
            cursor.execute("DROP TABLE IF EXISTS " + inmemory_prefix + self.node_name)
        cursor.fetchone()

        if self.debug:
            print("Dropped node " + self.name)

import time
import networkx as nx
import matplotlib.pyplot as plt
import threading
import queue
from ExecutionNode import *

class ExecutionGraph_lru(object):

    """
    Class variables:
    graph: A networkx graph representing tasks (stored as names) &
        dependencies.
    node_dict: A mapping of node names to the ExecutionNode objects.
    execution_order: A total ordering to execute the nodes serially in.
    store_in_memory: Nodes to store in memory during execution instead of on
        disk.
    outputs: Sinks of the graph (nodes with no successors) are assumed
        to be the desired outputs of the job and thus always serialized.
        (i.e. never picked for in-memory storage)
    """
    def __init__(self):
        self.graph = nx.DiGraph()
        self.node_dict = {}
        self.execution_order = None
        self.store_in_memory = set()
        self.outputs = set()

        # Metadata for reporting memory footprint and execution time breakdown.
        self.memory_history = []
        self.current_memory_usage = 0
        self.peak_memory_usage_counter = 0
        self.total_time_to_serialize_counter = 0
        self.total_time_to_deserialize_counter = 0
        self.total_execution_time_counter = 0

        # Queue & thread for multithreaded serialization of in-memory tables.
        self.mt_queue = queue.Queue()
        self.use_pyarrow = [True]
        self.deepcopy_dict = {}
        self.mt_thread = None
        
    """
    Add nodes to the graph.
    """
    def add_nodes(self, nodes):
        if isinstance(nodes, list):
            for node in nodes:
                self.graph.add_node(node.name)
                self.node_dict[node.name] = node
        else:
            self.graph.add_node(nodes.name)
            self.node_dict[nodes.name] = nodes

    """
    Build graph by adding edges based on dependencies, should be called after
    adding nodes to the graph.
    draw_graph: Whether to show the resulting dependency graph using matplotlib.
    """
    def build_graph(self, draw_graph = False):
        # build dependencies
        for name, obj in self.node_dict.items():
            for dependency in obj.dependencies:
                self.graph.add_edge(dependency, name)

        # Use a default topological order as the execution order
        self.execution_order = list(nx.topological_sort(self.graph))

        # Find sinks (outputs)
        for name in self.execution_order:
            if len(list(self.graph.successors(name))) == 0:
                self.outputs.add(name)

        if draw_graph:
            nx.draw(self.graph, with_labels = True)
            plt.show()

    """
    Run program and return results from sinks.
    debug: Show memory footprint during execution.
    save_inmemory_tables: Serialize and write to disk tables flagged for
    in-memory storage in parallel before garage collecting them.
    """
    def execute(self, debug = False, save_inmemory_tables = False, memory_limit = 400000000):
        #print("store in memory:", self.store_in_memory, len(self.store_in_memory))
        # Reset counters
        self.peak_memory_usage_counter = 0
        self.serialize_time_counter = 0
        self.deserialize_time_counter = 0
        self.execution_time_counter = 0
        self.actual_execution_time_counter = 0
        
        execution_start_time = time.time()

        # Keep track of number of successors for each node yet to be executed;
        # When a node stored in memory's successors have all been executed,
        # its result is garbage collected.
        self.cache = {}
        current_memory_usage = 0

        # Start multithreaded table serializer
        if save_inmemory_tables:
            for name in self.execution_order:
                self.deepcopy_dict[name] = True
            self.use_pyarrow[0] = True
            self.mt_thread = threading.Thread(target = mt_writer,
                                             args = [self.mt_queue, 
                                                     self.use_pyarrow,
                                                     self.deepcopy_dict])
            self.mt_thread.start()

        # Run nodes in given execution order
        for name in self.execution_order:
            node = self.node_dict[name]

            # Populate dependencies
            for parent_name in node.dependencies.keys():
                parent_node = self.node_dict[parent_name]

                # if node in cache, refresh access time
                if parent_name in self.cache:
                    status = node.populate_dependency(parent_node, True)
                    if status == -1:
                        return
                    self.cache[parent_name] = time.time()
                # otherwise, free some nodes and add to cache
                else:
                    status = node.populate_dependency(parent_node, False)
                    if status == -1:
                        return
                    if parent_node.get_result_size() < memory_limit:
                        current_memory_usage += parent_node.get_result_size()
                        self.cache[parent_name] = time.time()
                        while current_memory_usage > memory_limit:
                            node_to_evict_name = max(self.cache, key=self.cache.get)
                            self.cache.pop(node_to_evict_name)
                            current_memory_usage -= self.node_dict[node_to_evict_name].get_result_size()

            # Execute current node
            status = node.execute(debug = debug)
            if status == -1:
                return
            if node.get_result_size() < memory_limit:
                current_memory_usage += node.get_result_size()
                self.cache[name] = time.time()
                while current_memory_usage > memory_limit:
                    node_to_evict_name = max(self.cache, key=self.cache.get)
                    self.cache.pop(node_to_evict_name)
                    current_memory_usage -= self.node_dict[node_to_evict_name].get_result_size()
        
            node.serialize_result()                      

        # Compute execution time breakdown
        execution_end_time = time.time()
        self.execution_time_counter = execution_end_time - execution_start_time
        self.time_to_serialize_counter = sum([
            node.get_time_to_serialize() *
            int(node.name not in self.store_in_memory)
            for node in self.node_dict.values()])
        self.time_to_deserialize_counter = sum([
            node.get_time_to_deserialize() *
            len(list(self.graph.successors(node.name))) *
            int(node.name not in self.store_in_memory)
            for node in self.node_dict.values()])

        # Print metadata
        print("total execution time:", self.execution_time_counter)
        print("total load time:", self.time_to_deserialize_counter)
        print("total save time:", self.time_to_serialize_counter)
        print("maximum memory usage:", self.peak_memory_usage_counter)

        # Join multithreaded writer
        if save_inmemory_tables:
            self.mt_queue.put(None)
            self.use_pyarrow[0] = False
            self.mt_thread.join()

        self.actual_execution_time_counter = time.time() - execution_start_time

        if debug:
            print("actual total execution time:", self.actual_execution_time_counter)

        return self.execution_time_counter, self.time_to_deserialize_counter, \
               self.time_to_serialize_counter, self.peak_memory_usage_counter, \
               self.actual_execution_time_counter

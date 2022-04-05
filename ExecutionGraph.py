import time
import networkx as nx
import matplotlib.pyplot as plt
import threading
import queue
from ExecutionNode import *

class ExecutionGraph(object):

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
    def execute(self, debug = False, save_inmemory_tables = False):
        #print("store in memory:", self.store_in_memory, len(self.store_in_memory))
        # Reset counters
        self.peak_memory_usage_counter = 0
        self.serialize_time_counter = 0
        self.deserialize_time_counter = 0
        self.execution_time_counter = 0
        
        execution_start_time = time.time()

        for name in self.execution_order:
            self.node_dict[name].timestamp = execution_start_time

        # Keep track of number of successors for each node yet to be executed;
        # When a node stored in memory's successors have all been executed,
        # its result is garbage collected.
        num_successors = [len(list(self.graph.successors(i)))
                          for i in self.execution_order]
        num_successors_dict = dict(zip(self.execution_order, num_successors))

        # Start multithreaded table serializer
        if save_inmemory_tables:
            self.mt_thread = threading.Thread(target = mt_writer,
                                             args = [self.mt_queue, execution_start_time])
            self.mt_thread.start()

        # Run nodes in given execution order
        for name in self.execution_order:
            node = self.node_dict[name]

            # Populate dependencies
            for parent_name in node.dependencies.keys():
                parent_node = self.node_dict[parent_name]
                status = node.populate_dependency(parent_node,
                                            parent_name in self.store_in_memory)
                if status == -1:
                    return
                
                num_successors_dict[parent_name] -= 1

            # Execute current node
            status = node.execute(debug = debug)
            if status == -1:
                return
            #if name in self.store_in_memory and save_inmemory_tables:
            #    self.mt_queue.put((node.result, name))
            # Serialize current node to disk if not flagged for in memory
            # storage
            if name not in self.store_in_memory:
                node.serialize_result()                      
            else:
                self.current_memory_usage += node.get_result_size()

            # Garbage collect dependencies when all children have been computed
            for parent_name in node.dependencies.keys():
                parent_node = self.node_dict[parent_name]
                if (num_successors_dict[parent_name] == 0 and
                        parent_name in self.store_in_memory):
                    
                    # Multithreaded serialization if enabled
                    if save_inmemory_tables:
                        self.mt_queue.put((parent_node.result, parent_name))
                        
                    parent_node.result = None
                    
                    self.current_memory_usage -= parent_node.get_result_size()

            if debug:
                #print("current memory usage:", self.current_memory_usage)
                pass

            # Keep track of current memory usage
            self.memory_history.append(self.current_memory_usage)
            if self.current_memory_usage > self.peak_memory_usage_counter:
                self.peak_memory_usage_counter = self.current_memory_usage

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
        if debug:
            pass
        print("total execution time:", self.execution_time_counter)
        print("total load time:", self.time_to_deserialize_counter)
        print("total save time:", self.time_to_serialize_counter)
        print("maximum memory usage:", self.peak_memory_usage_counter)

        # Join multithreaded writer
        if save_inmemory_tables:
            self.mt_queue.put(None)
            self.mt_thread.join()

        print("actual total execution time:", time.time() - execution_start_time)

        return self.execution_time_counter, self.time_to_deserialize_counter, \
               self.time_to_serialize_counter, self.peak_memory_usage_counter

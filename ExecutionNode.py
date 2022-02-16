import time
from utils import *

class ExecutionNode(object):

    """
    name: Name of the node
    instructions: Some function which takes in the dependencies as arguments in
        the order provided.
    dependencies: Ordered list of dependency names.
    result: Item(s) returned from executing instructions using the dependencies.
    size_function: Function for estimate node result size; should take an object as input
        and return estimated size. if not provided, use default polars table
        size estimation in utils.
    serialize_function: Function for serializing the result. Should take an
        object to store and the file name to store the object under as input.
        If not provided, use default pickle function in utils.
    deserialize_function: Function for deserializing the result from disk.
        should take a file name to read from and return an object. If not
        provided, use default unpickle function in utils.
    """
    def __init__(self, name, instructions, dependencies,
                 size_function = None,
                 serialize_function = None,
                 deserialize_function = None):
        
        self.name = name
        self.instructions = instructions
        self.dependencies = {dependency: None for dependency in dependencies}
        self.result = None

        if size_function:
            self.size_function = size_function
        else:
            self.size_function = estimate_polars_table_size

        if serialize_function:
            self.serialize_function = serialize_function
        else:
            self.serialize_function = parquet_result

        if deserialize_function:
            self.deserialize_function = deserialize_function
        else:
            self.deserialize_function = unparquet_result

        # Number of results to keep track of when computing rolling averages.
        self.ROLLING_AVG_WINDOW_SIZE = 10

        # Metadata for estimating the information needed by the algorithm.
        self.result_size_history = []
        self.time_to_serialize_history = []
        self.time_to_deserialize_history = []

    """
    Returns average result size if available.
    """
    def get_result_size(self):
        if len(self.result_size_history):
            return list_avg(self.result_size_history)
        return 0

    """
    Returns average time to serialize if available.
    """
    def get_time_to_serialize(self):
        if len(self.time_to_serialize_history):
            return list_avg(self.time_to_serialize_history)
        return 0

    """
    Returns average time to deserialize if available.
    """
    def get_time_to_deserialize(self):
        if len(self.time_to_deserialize_history):
            return list_avg(self.time_to_deserialize_history)
        return 0

    """
    Add result for in-memory dependency from another ExecutionNode.
    If the dependency is in memory, read it directly from the node.
    Else, retrieve it using the node's deserialization function.
    """
    def populate_dependency(self, parent_node, dependency_in_memory = True):
        if parent_node.name not in self.dependencies.keys():
            print("Invalid dependency")
            return -1
        
        if dependency_in_memory:
            self.dependencies[parent_node.name] = parent_node.result
        else:
            self.dependencies[parent_node.name] = \
                                                parent_node.deserialize_result()

        return 0

    """
    Execute the instructions stored in the node and store to result.
    debug: print execution/error messages.
    """
    def execute(self, debug = False):
        if debug:
            print("Executing node " + self.name)
            
        # Check for dependencies to be fully loaded
        for k, v in self.dependencies.items():
            if not v:
                print("Dependency", k, "not found")
                return -1

        # Execute
        """
        try:
            self.result = self.instructions(*list(self.dependencies.values()))
        except Exception as inst:
            if debug:
                print("Execution of", self.name, "failed:", inst)
                return -1
        """
        self.result = self.instructions(*list(self.dependencies.values()))

        # Flush dependencies
        for k in self.dependencies.keys():
            self.dependencies[k] = None

        if debug:
            print("Node size:", self.size_function(self.result))

        # Record result size
        self.result_size_history.append(self.size_function(self.result))
        if len(self.result_size_history) > self.ROLLING_AVG_WINDOW_SIZE:
            self.result_size_history.pop(0)

        return 0

    """
    Serializes current result and frees result from memory.
    """
    def serialize_result(self):
        start = time.time()
        self.serialize_function(self.result, self.name)
        time_to_serialize = time.time() - start
        
        self.result = None

        # Record time to serialize
        self.time_to_serialize_history.append(time_to_serialize)
        if len(self.time_to_serialize_history) > self.ROLLING_AVG_WINDOW_SIZE:
            self.time_to_serialize_history.pop(0)

    """
    Deserializes and returns current result from disk.
    """
    def deserialize_result(self):
        start = time.time()
        result_from_disk = self.deserialize_function(self.name)
        time_to_deserialize = time.time() - start

        # Record time to serialize
        self.time_to_deserialize_history.append(time_to_deserialize)
        if len(self.time_to_deserialize_history) > self.ROLLING_AVG_WINDOW_SIZE:
            self.time_to_deserialize_history.pop(0)

        return result_from_disk

import io
import polars as pl
import networkx as nx
import time
import copy
from collections import defaultdict

"""
Default size function for ExecutionNode if none is provided.
Estimates the size of a polars dataframe.
"""
def estimate_polars_table_size(table):
    # Hardcoded data type sizes.
    dtypes_sizes = {
        pl.datatypes.Int64: 8,
        pl.datatypes.Float64: 8,
        pl.datatypes.Int32: 4,
        pl.datatypes.Float32: 4,
        pl.datatypes.Datetime: 8,
        pl.datatypes.Utf8: 100
    }
    
    table_size = 0
    for dtype in table.dtypes:
        if dtype in dtypes_sizes:
            table_size += dtypes_sizes[dtype] * table.shape[0]
        else:
            table_size += 100 * table.shape[0]
    return table_size

"""
Default serialization function for ExecutionNode if none is provided.
Stores a polars table using parquet.
"""
def parquet_result(result, filename, location = 'disk/', use_pyarrow = False):
    result.write_parquet(open(location + filename + '.parquet', 'wb'),
                          statistics = False,
                          use_pyarrow = use_pyarrow)
    
"""
Default deserialization function for ExecutionNode if none is provided.
Loads a polars table using parquet.
"""
def unparquet_result(filename, location = 'disk/', columns = None):
    return pl.read_parquet(open(location + filename + '.parquet', 'rb'),
                           columns = columns)    

"""
Average of a list.
"""
def list_avg(input_list):
    return sum(input_list) / len(input_list)

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
    
"""
Computes the change in minimum linear arrangement score resulting from moving
node_to_move to new_node_pos.
"""
def minLA_change(graph, node_idx_to_name, node_name_to_idx,
                 memory_usage, node_to_move, new_node_pos):
    old_node_pos = node_name_to_idx[node_to_move]
    change = 0

    # Effect on neighbors
    for parent_name in graph.predecessors(node_to_move):
        change += (new_node_pos - old_node_pos) * memory_usage[parent_name]
    for child_name in graph.successors(node_to_move):
        change -= (new_node_pos - old_node_pos) * memory_usage[child_name]

    # Forward movement
    if new_node_pos > old_node_pos:
        for i in range(old_node_pos + 1, new_node_pos + 1):
            for parent_name in graph.predecessors(node_idx_to_name[i]):
                if node_name_to_idx[parent_name] < old_node_pos:
                    change -= memory_usage[parent_name]
            for child_name in graph.successors(node_idx_to_name[i]):
                if node_name_to_idx[child_name] > new_node_pos:
                    change += memory_usage[child_name]

    # Backward movement
    else:
        for i in range(new_node_pos, old_node_pos):
            for parent_name in graph.predecessors(node_idx_to_name[i]):
                if node_name_to_idx[parent_name] < new_node_pos:
                    change -= memory_usage[parent_name]
            for child_name in graph.successors(node_idx_to_name[i]):
                if node_name_to_idx[child_name] > old_node_pos:
                    change += memory_usage[child_name]

    return change

"""
Applies the specified node movement by moving node_to_move to new_node_pos and
updates the 2 dictionaries accordingly.
"""
def minLA_apply(node_idx_to_name, node_name_to_idx, node_to_move, new_node_pos):
    old_node_pos = node_name_to_idx[node_to_move]

    if new_node_pos > old_node_pos:
        for i in range(old_node_pos + 1, new_node_pos + 1):
            i_name = node_idx_to_name[i]
            node_name_to_idx[i_name] = i - 1
            node_idx_to_name[i - 1] = i_name

    else:
        for i in range(old_node_pos - 1, new_node_pos - 1, -1):
            i_name = node_idx_to_name[i]
            node_name_to_idx[i_name] = i + 1
            node_idx_to_name[i + 1] = i_name

    node_name_to_idx[node_to_move] = new_node_pos
    node_idx_to_name[new_node_pos] = node_to_move

"""
Writer for concurrent serialization of in-memory tables.
use_pyarrow is set to True to reduce the memory consumption of the thread
calling this method.
"""
def mt_writer(task_queue, use_pyarrow, deepcopy_dict, timestamp = 0):
    for result, name in iter(task_queue.get, None):
        if deepcopy_dict[name]:
            parquet_result(copy.deepcopy(result), name, 
                    use_pyarrow = False)
        else:
        #print("mt_writer start " + name + ": " + str(time.time() - timestamp))
            parquet_result(result, name, use_pyarrow = False)
        #print("mt_writer end " + name + ": " + str(time.time() - timestamp))

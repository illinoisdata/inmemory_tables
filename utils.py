import pickle
import io
import polars as pl
import networkx as nx
from collections import defaultdict

"""
Reads a TPC-DS table into a polars dataframe.
"""
def read_table(table_name):
    # test database
    column_dict = defaultdict(list)
    column_file = open("tpcds/column_list.txt")
    for line in column_file.readlines():
        if line[0].isalpha():
            col_table = line.split()
            table = col_table[1].lower()
            col = col_table[0].lower()
            column_dict[col_table[1].lower()].append(col_table[0].lower())
            
    f = open("tpcds/" + table_name + ".dat", "r", encoding = "utf-8",
             errors = 'replace')
    df_str = f.read()
    column_count = df_str.split('\n')[0].count('|')
    return pl.read_csv(io.StringIO(df_str), has_header = False, sep = '|',
                       new_columns = column_dict[table_name][:column_count])

"""
Default size function for ExecutionNode if none is provided.
Estimates the size of a polars dataframe.
"""
def estimate_polars_table_size(table):
    # Hardcoded data type sizes.
    dtypes_sizes = {
        pl.datatypes.Int64: 8,
        pl.datatypes.Float64: 8,
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
Stores a file using pickle.
"""
def pickle_result(result, filename):
    pickle.dump(result, open('disk/' + filename + '.pickle', 'wb'))

"""
Default deserialization function for ExecutionNode if none is provided.
Loads a file using pickle.
"""
def unpickle_result(filename):
    return pickle.load(open('disk/' + filename + '.pickle', 'rb'))

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

from core.algorithm.optimizer import *
import polars as pl


# Script for converting TPC-DS tables from .dat files to parquet.
def read_columns():
    column_dict = defaultdict(list)
    column_file = open("tpcds/column_list.txt")
    for line in column_file.readlines():
        if line[0].isalpha():
            col_table = line.split()
            table = col_table[1].lower()
            col = col_table[0].lower()
            column_dict[col_table[1].lower()].append(col_table[0].lower())

    return column_dict

def read_table(table_name, column_dict, dtype_list):
    # test database
    f = open("tpcds/" + table_name + ".dat", "r", encoding = "utf-8",
             errors = 'replace')
    df_str = f.read()
    column_count = df_str.split('\n')[0].count('|')
    return pl.read_csv(io.StringIO(df_str), has_header = False, sep = '|',
                       new_columns = column_dict[table_name][:column_count],
                       dtypes = dtype_list)

if __name__ == '__main__':
    column_dict = read_columns()
    
    for column in column_dict.keys():
        print(column)
        table = read_table(column, column_dict, None)
        
        a = list(table.columns)
        b = list(table.dtypes)
        dtype_list = []
        for i in range(len(a)):
            if b[i] == pl.datatypes.Int64:
                dtype_list.append(pl.datatypes.Int32)
            elif b[i] == pl.datatypes.Float64:
                dtype_list.append(pl.datatypes.Float32)
            else:
                dtype_list.append(b[i])

        table = read_table(column, column_dict, dtype_list)
        #pd_df = table.to_pandas()
        #pd_df.to_parquet("tpcds/" + column + ".parquet")
        parquet_result(table, column, location = 'tpcds/', use_pyarrow = True)

        """
        column_dict[column] = column_dict[column][:len(table.columns)]

        p_size = 5
        num_partitions = int(math.ceil(len(column_dict[column]) / p_size))
        for i in range(num_partitions):
            if (i * p_size > len(column_dict[column])):
                print(column_dict[column][i*p_size:])
                parquet_result(table[column_dict[column][i*p_size:]],
                               column + "_" + str(i), location = 'tpcds/')
            else:
                print(column_dict[column][i*p_size:(i+1)*p_size])
                parquet_result(table[column_dict[column]
                                     [i*p_size:(i+1)*p_size-1]],
                               column + "_" + str(i), location = 'tpcds/') 

        df = unparquet_result(column + "_0", location = 'tpcds/')
        for i in range(1, num_partitions):
            df = df.hstack(unparquet_result(column + "_" + str(i),
                                            location = 'tpcds/'))

        parquet_result(df, column, location = 'tpcds/')

        for i in range(num_partitions):
            os.remove("tpcds/" + column + "_" + str(i) + ".parquet")
        """

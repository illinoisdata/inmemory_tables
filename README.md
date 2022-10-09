# S/C: Speeding up Data Materialization with Bounded Memory
Speed up MV refresh workloads by keeping intermediate data in memory.

## How to:

- Install Presto on your machine.
- Create the Hive and Memory connectors for your Presto server.
- Load the TPC-DS tables into the Hive catalog.
- Modify the connectors in `experiment/run_workload.py` to connect to your Presto server.
- Experiment with different optimizers and workloads. Enjoy!

## Workloads:

All workloads can be found in `experiment/workloads`:
- For TPC-DS dataset:
  - Workload 1 -> I/O 1
  - Workload 2 -> Compute 1
  - Workload 3 -> I/O 2
  - Workload 4 -> Compute 2
  - Workload 5 -> I/O 3
- For TPC-DS date-partitioned dataset:
  - Workload 6 -> I/O 1
  - Workload 7 -> Compute 1
  - Workload 8 -> I/O 2
  - Workload 9 -> Compute 2
  - Workload 10 -> I/O 3

for iter in 1 2
do 
for job in 1 2 3 4 5
do
for scale in 10 25 50 100
do
echo $job
echo $scale
time ./presto --server localhost:8090 --catalog hive --schema tpcds_${scale} --file /home/zl20/inmemory_tables/inmemory_tables/sql_jobs/job${job}.txt 2> /home/zl20/inmemory_tables/inmemory_tables/sql_jobs/job${job}_results.txt
wait
sleep 1
./presto --server localhost:8090 --catalog hive --schema tpcds_${scale} --file /home/zl20/inmemory_tables/inmemory_tables/sql_jobs/job${job}_cleanup.txt 2> /home/zl20/inmemory_tables/inmemory_tables/sql_jobs/job${job}_results.txt
wait
sleep1
done
done
done
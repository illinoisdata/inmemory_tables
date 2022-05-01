for iter in 1 2
do 
for job in 1 2 6 4 5
do
for scale in 25 50 100
do
echo $job
echo $scale
time psql zl20 -d tpcds_${scale} -f job${job}_partitioned.txt > job${job}_results.txt
wait
sleep 1
psql zl20 -d tpcds_${scale} -f job${job}_cleanup.txt > job${job}_results.txt
wait
sleep 1
done
done
done

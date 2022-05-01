for scale in 10 25 50 100
do
echo $scale
psql zl20 -d tpcds -f table_partitioner_${scale}G.txt
wait
sleep 1
done

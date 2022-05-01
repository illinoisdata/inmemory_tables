for iter in 1
do
for i in 1 2 3 4 5
	do
	for j in 32
		do
			python3 setting_generator.py --job $i --memory $j
			wait
			sleep 1
			python3 setting_consumer.py --job $i --memory $j --store mkp --top dfs
			wait
			sleep 1
		done
		python3 setting_consumer.py --job $i --memory 32 --store all --top none
		wait
		sleep 1
	done
done

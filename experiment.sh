for iter in 1 2 3 4 5
do
for i in 1 2 3 4 5
	do
	for j in 2 4 8
		do
			python3 setting_generator.py --job $i --memory $j
			wait
			sleep 1
			python3 setting_consumer.py --job $i --memory $j --store all --top none
			wait
			sleep 1
			python3 setting_consumer.py --job $i --memory $j --store none --top none
			wait
			sleep 1
			python3 setting_consumer.py --job $i --memory $j --store greedy --top none
			wait
			sleep 1
			python3 setting_consumer.py --job $i --memory $j --store random --top none
			wait
			sleep 1
			python3 setting_consumer.py --job $i --memory $j --store mkp --top none
			wait
			sleep 1
			python3 setting_consumer.py --job $i --memory $j --store greedy --top both
			wait
			sleep 1
			python3 setting_consumer.py --job $i --memory $j --store random --top both
			wait
			sleep 1
			python3 setting_consumer.py --job $i --memory $j --store mkp --top dfs
			wait
			sleep 1
			python3 setting_consumer.py --job $i --memory $j --store mkp --top sa
			wait
			sleep 1
			python3 setting_consumer.py --job $i --memory $j --store mkp --top recursive_min_cut
			wait
			sleep 1
			python3 setting_consumer.py --job $i --memory $j --store mkp --top both
			wait
			sleep 1
		done
	done
done

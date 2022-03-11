for iter in 1 2 3 4 5
do
for i in 1 2 3 4 5
	do
	for j in 2 5
		do
			python3 setting_generator.py --job $i --memory $j
			wait
			python3 setting_consumer.py --job $i --memory $j --store mkp --top both
			python3 setting_consumer.py --job $i --memory $j --store all --top none
			python3 setting_consumer.py --job $i --memory $j --store none --top none
			python3 setting_consumer.py --job $i --memory $j --store greedy --top none
			python3 setting_consumer.py --job $i --memory $j --store random --top none
			python3 setting_consumer.py --job $i --memory $j --store mkp --top none
			python3 setting_consumer.py --job $i --memory $j --store greedy --top both
			python3 setting_consumer.py --job $i --memory $j --store random --top both
			python3 setting_consumer.py --job $i --memory $j --store mkp --top dfs
			python3 setting_consumer.py --job $i --memory $j --store mkp --top sa
			python3 setting_consumer.py --job $i --memory $j --store mkp --top recursive_min_cut
			wait
		done
	done
done
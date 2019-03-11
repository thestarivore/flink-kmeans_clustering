#!/bin/bash

read -p "Execution num of times: " EXEC_TIMES
read -p "Parallelism: " PARALLELISM
read -p "Num of centroids min: " MIN_CENTROIDS
read -p "Num of centroids max: " MAX_CENTROIDS
read -p "Recompute nearest centroids, num of times: " RECOMP_NEAREST
read -p "Iterations: " ITERATIONS
read -p "Custom convergence [true or false]: " CUSTOM_CONV
read -p "Base file path: " BASE_PATH

echo "INPUT PARAMS"
echo "Exec times: $EXEC_TIMES"
echo "Centroids: $MIN_CENTROIDS"
echo "RecompNearest: $RECOMP_NEAREST"
echo "Iterations: $ITERATIONS"
echo "Custom convergence: $CUSTOM_CONV"
echo "Base file path: $BASE_PATH"

for j in $(seq $MIN_CENTROIDS $MAX_CENTROIDS);
do
	echo "---STARTING with $j centroids"
	mkdir "$BASE_PATH/output/$j"
	echo "iter,objval" >> "$BASE_PATH/results_objfun_$j.csv"
	echo "iter,time" >> "$BASE_PATH/results_time_$j.csv"
	for i in $(seq 1 $EXEC_TIMES);
	do
		echo "Start execution num $i"
	   	RESULT=$(flink run -p $PARALLELISM -q ../flink-project-1.7.0.jar -numcentroids $j -recompnearest $RECOMP_NEAREST -iterations $ITERATIONS -custconvergence $CUSTOM_CONV -points "$BASE_PATH/input/points.csv" -centroids "$BASE_PATH/input/$j/centroids_$i.csv" -pointsout "$BASE_PATH/output/$j/new_points_$i.csv" -centroidsout "$BASE_PATH/output/$j/new_centroids_$i.csv" -objfunout "$BASE_PATH/output/$j/objfun_$i.csv")
	   	echo "$RESULT"

		OBJ_VALUE=$(head "$BASE_PATH/output/$j/objfun_$i.csv")
		echo "$i,$OBJ_VALUE" >> "$BASE_PATH/results_objfun_$j.csv"

		TIME=$(echo "$RESULT" | head -n 3 | tail -n 1)
		echo "$i,$TIME" >> "$BASE_PATH/results_time_$j.csv"
	done
done
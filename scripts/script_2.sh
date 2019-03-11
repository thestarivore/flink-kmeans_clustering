#!/bin/bash

read -p "Execution num of times: " EXEC_TIMES
read -p "Parallelism: " PARALLELISM
read -p "Num of centroids: " CENTROIDS
read -p "Recompute nearest centroids, num of times: " RECOMP_NEAREST_TIMES
read -p "Iterations: " ITERATIONS
read -p "Custom convergence [true or false]: " CUSTOM_CONV
read -p "Base file path: " BASE_PATH

echo "INPUT PARAMS"
echo "Exec times: $EXEC_TIMES"
echo "Centroids: $CENTROIDS"
echo "RecompNearest times: $RECOMP_NEAREST_TIMES"
echo "Iterations: $ITERATIONS"
echo "Custom convergence: $CUSTOM_CONV"
echo "Base file path: $BASE_PATH"

recomp_nearest=0
for j in $(seq 0 $RECOMP_NEAREST_TIMES);
do
	echo "---STARTING with $recomp_nearest recomp nearest"
	mkdir "$BASE_PATH/output/$j"
	echo "iter,objval" >> "$BASE_PATH/results_objfun_$j.csv"
	for i in $(seq 1 $EXEC_TIMES);
	do
		echo "Start execution num $i"
	   	flink run -p $PARALLELISM -q ../flink-project-1.7.0.jar -numcentroids $CENTROIDS -recompnearest $recomp_nearest -iterations $ITERATIONS -custconvergence $CUSTOM_CONV -points "$BASE_PATH/input/points.csv" -centroids "$BASE_PATH/input/$j/centroids_$i.csv" -pointsout "$BASE_PATH/output/$j/new_points_$i.csv" -centroidsout "$BASE_PATH/output/$j/new_centroids_$i.csv" -objfunout "$BASE_PATH/output/$j/objfun_$i.csv"

		OBJ_VALUE=$(head "$BASE_PATH/output/$j/objfun_$i.csv")
		echo "$i,$OBJ_VALUE" >> "$BASE_PATH/results_objfun_$j.csv"
	done
	recomp_nearest=$((recomp_nearest+10))
done


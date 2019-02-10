# README

- **flink-project**: it contains the source code and the jar file (that can be submitted to the Flink cluster). The project can also be runned without the cluster, directly from the IDE
- **results**: it contains the result of some runs of the algorithm (more info in the Jupyter notebook)
- **flink_k-means.ipynb**: it is a Jupyter notebook used to show data, plots and results
    + openable with [Jupyter](https://jupyter.org/)
    - it requires [tabulate](https://pypi.org/project/tabulate/)


### Program arguments

- files:
    + points: string, path of the input file containing the points
    + centroids: string, path of the input file containing the centroids, (if any)
    + pointsout: string, path of the output file containing the points with the associated cluster
    + centroidsout: string, path of the output file containing the computed centroids
    + objfunout: string, path of the output file containing the value of the objective function
- iteration params:
    + iterations: int, max number of iterations
    + custconvergence: boolean, if custom convergence is used
- centroids:
    + numcentroids: int number of centroids to generate. If specified, centroids input file is ignored
    + minc: int, min value for centroid x, y
    + maxc: int, max value for centroid x, y
    + recompnearest: int, number of centroids nearest centroids are recomputed

Example:
```
-numcentroids 8 -recompnearest 3 -iterations 10 -custconvergence false
-points "points.csv" -centroids "centroids.csv"
-pointsout "new_points.csv" -centroidsout "new_centroids.csv" -objfunout "objfun.csv"
```


### Setup Flink cluster

Download flink [here](https://flink.apache.org/downloads.html), follow [this](https://ci.apache.org/projects/flink/flink-docs-release-1.7/tutorials/local_setup.html) to start the cluster and submit the Jar file.
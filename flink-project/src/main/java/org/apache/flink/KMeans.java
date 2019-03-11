/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class KMeans {
    private static final double EPSILON = 0.1;

    public static void main(String[] args) throws Exception {
        Logger LOG = LoggerFactory.getLogger(KMeans.class);

        final ParameterTool params = ParameterTool.fromArgs(args);
        final String pointsInFile = params.get("points");           // (in) file with points
        final String centroidsInFile = params.get("centroids");     // (in) file with centroids
        final String pointsOutFile = params.get("pointsout");       // (out) file with points
        final String centroidsOutFile = params.get("centroidsout"); // (out) file with centroids
        final String objFunctionOutFile = params.get("objfunout");  // (out) file with objective function value

        final int maxIterations = params.getInt("iterations", 100);
        final boolean customConvergence = params.getBoolean("custconvergence", false);

        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

        // read points (represented as Point)
        final DataSet<Point> points = env.readCsvFile(pointsInFile)
                .ignoreFirstLine()
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .pojoType(Point.class, "x", "y");

        final DataSet<Centroid> centroids;
        if (params.get("numcentroids") != null) {
            // generate centroids randomly
            final int numCentroids = params.getInt("numcentroids", 6);
            LOG.debug("KMEANS: Generating: " + numCentroids + " centroids");

            int rangeMin = params.getInt("minc", -15); // min x,y value centroid
            int rangeMax = params.getInt("maxc", 15);  // max x,y value centroid

            Random r = new Random();
            Collection<Centroid> randomCentroids = new ArrayList<Centroid>();
            for (int i = 0; i < numCentroids; i++) {
                double x = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
                double y = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
                Centroid centroid = new Centroid(i, x, y);
                randomCentroids.add(centroid);
            }

            // recompute the nearest centroids recompnearest times
            if (params.get("recompnearest") != null){
                int recomputeNearest = params.getInt("recompnearest", 0);
                LOG.debug("KMEANS: Recomputing nearest centroids: " + recomputeNearest + " times");
                for (int k = 0; k < recomputeNearest; k++) {
                    double minDistance = Double.MAX_VALUE;
                    int nearest = 0;
                    for (int i = 0; i < numCentroids; i++) {
                        Centroid c = ((ArrayList<Centroid>) randomCentroids).get(i);
                        for (int j = i + 1; j < numCentroids; j++) {
                            double distance = c.distance(((ArrayList<Centroid>) randomCentroids).get(j));
                            if (distance < minDistance) {
                                minDistance = distance;
                                nearest = j;
                            }
                        }
                    }
                    Centroid c = ((ArrayList<Centroid>) randomCentroids).get(nearest);
                    c.x = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
                    c.y = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
                }
            }

            centroids = env.fromCollection(randomCentroids);

            // generate tuples from dataset to save them to the file as id, x, y
            DataSet<Tuple3<Integer, Double, Double>> tCentroidsIn = centroids
                    .map(new MapFunction<Centroid, Tuple3<Integer, Double, Double>>() {
                        @Override
                        public Tuple3<Integer, Double, Double> map(Centroid centroid) throws Exception {
                            return new Tuple3<>(centroid.id, centroid.x, centroid.y);
                        }
                    });

            // save tuples generated centroid to file
            tCentroidsIn.writeAsCsv(centroidsInFile, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        } else {
            // Read centroids (represented as Centroid) from file
            LOG.debug("KMEANS: Reading centroids from file");

            centroids = env.readCsvFile(centroidsInFile)
                    .ignoreFirstLine()
                    .lineDelimiter("\n")
                    .fieldDelimiter(",")
                    .pojoType(Centroid.class, "id", "x", "y");
            centroids.print();
        }

        // ---
        // Create initial IterativeDataSet
        IterativeDataSet<Centroid> iterationCentroids = centroids.iterate(maxIterations);

        /*
         * 1st step: for every point compute the nearest centroid
         * 2nd step: add a count field to the tuple
         * 3rd step: count the number of points assigned to a centroid and add them (used in the Centroid Formula)
         * 4th step: compute the new centroids
         */
        DataSet<Centroid> newCentroids = points
                // 1st step
                .map(new ComputeCentroidsDistance())                        // => (CentroidId, Point)
                .withBroadcastSet(iterationCentroids, "centroids")    // set the centroids shared variable
                // 2nd step
                .map(new PointCounterFieldAppend())                         // append the count field to the tuple (init to 1)
                // 3rd step
                .groupBy(0).reduce(new CentroidReducer())            // group by centroidId and reduce, sum two points and the counter (for each centroid)
                // 4th step
                .map(new ComputeNewCentroids());                            // compute the new centroids

        DataSet<Centroid> finalCentroids;

        if (customConvergence) { // stop the iteration when the centroids do not move more than EPSILON, or after max iterations
            LOG.debug("KMEANS: Custom convergence active");

            finalCentroids = iterationCentroids.closeWith(newCentroids,
                    // stop the iteration when the following set is empty
                    newCentroids.join(iterationCentroids).where("id").equalTo("id") // join the new set with the old one on the id of the cluster
                            .filter(new FilterFunction<Tuple2<Centroid, Centroid>>() {           // filter out centroids that did not move more than EPSILON
                                @Override
                                public boolean filter(Tuple2<Centroid, Centroid> tuple) throws Exception {
                                    return tuple.f0.distance(tuple.f1) > EPSILON;
                                }
                            }));
        } else { // stop the iteration after max iterations
            LOG.debug("KMEANS: Custom convergence NOT active");

            finalCentroids = iterationCentroids.closeWith(newCentroids);
        }
        // ---


        // compute the cluster for the given points with final centroids and transform the dataset into tuples
        DataSet<Tuple2<Integer, Point>> finalPoints = points
                .map(new ComputeCentroidsDistance()).withBroadcastSet(finalCentroids, "centroids");

        // transform the points dataset into tuples
        DataSet<Tuple3<Integer, Double, Double>> tPoints = finalPoints
                .map(new MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Double, Double>>() {
                    @Override
                    public Tuple3<Integer, Double, Double> map(Tuple2<Integer, Point> tuple) throws Exception {
                        return new Tuple3<>(tuple.f0, tuple.f1.x, tuple.f1.y);
                    }
                });


        // transform the centroids dataset into tuples
        DataSet<Tuple3<Integer, Double, Double>> tCentroids = finalCentroids
                .map(new MapFunction<Centroid, Tuple3<Integer, Double, Double>>() {
                    @Override
                    public Tuple3<Integer, Double, Double> map(Centroid centroid) throws Exception {
                        return new Tuple3<>(centroid.id, centroid.x, centroid.y);
                    }
                });

        // compute the objective function
        DataSet<Tuple1<Double>> objFunctionValue = finalPoints
                .map(new ComputeObjFunction())                          // compute the distances for each point
                .withBroadcastSet(finalCentroids, "centroids")
                .reduce(new ReduceFunction<Tuple1<Double>>() {          // sum the distances to get the obj fun value
                    @Override
                    public Tuple1<Double> reduce(Tuple1<Double> t1, Tuple1<Double> t2) throws Exception {
                        Double total = t1.f0 + t2.f0;
                        return new Tuple1<>(total);
                    }
                });


        // emit result
        if (params.has("pointsout") && params.has("centroidsout") && params.has("objfunout")) {
            LOG.debug("KMEANS: Printing to file");

            tPoints.writeAsCsv(pointsOutFile, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            tCentroids.writeAsCsv(centroidsOutFile, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            objFunctionValue.writeAsCsv(objFunctionOutFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

            // execute program
            JobExecutionResult result =env.execute("Flink Project");
            System.out.println("The job took ");
            System.out.println(result.getNetRuntime(TimeUnit.MILLISECONDS));
            System.out.println(" ms to execute");
        } else {
            System.out.println("Printing result to stdout.");
            tPoints.print();
            tCentroids.print();
            objFunctionValue.print();
        }
    }

    // *************************************************************************
    //     FUNCTIONS
    // *************************************************************************

    /*
     * For a given point, compute the distance from all the centroids.
     * In: point p
     * Out: (Id of the nearest centroid, point p)
     *
     * RichMapFunction: Rich variant of the MapFunction. As a RichFunction, it gives access to the RuntimeContext
     * and provides setup and teardown methods: RichFunction.open(org.apache.flink.configuration.Configuration)
     * and RichFunction.close().
     *
     * https://cwiki.apache.org/confluence/display/FLINK/Variables+Closures+vs.+Broadcast+Variables
     */
    public static final class ComputeCentroidsDistance extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private Collection<Centroid> centroids;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point point) throws Exception {

            double minDistance = Double.MAX_VALUE;
            int centroidId = -1;

            for (Centroid centroid : centroids) {
                double distance = point.distance(centroid);

                if (distance < minDistance) {
                    minDistance = distance;
                    centroidId = centroid.id;
                }

            }

            return new Tuple2<>(centroidId, point);
        }
    }

    /*
     * For a given tuple, add the count field
     * In: (Id of the nearest centroid, point p)
     * Out: (Id of the nearest centroid, point p, count field = 1)
     */
    public static final class PointCounterFieldAppend implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> tuple) throws Exception {
            return new Tuple3<>(tuple.f0, tuple.f1, 1L);
        }
    }

    /*
     * For given tuples corresponding to point assigned to one centroid,
     * sum the two points (then used by in the Centroid Formula) and sum the counters
     * In: (centroidId, point p, counter)
     * Out: (centroidId, point p1 + p2, counter)
     */
    public static final class CentroidReducer implements ReduceFunction<Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> tuple1, Tuple3<Integer, Point, Long> tuple2) {
            return new Tuple3<>(
                    tuple1.f0,                  // the id of the centroid
                    tuple1.f1.sum(tuple2.f1),   // sum the coordinates of the two points
                    tuple1.f2 + tuple2.f2);     // sum the counters
        }
    }


    /*
     * Compute the new centroid with the Centroid Formula
     * In: (centroidId, point p, counter)
     * Out: Centroid
     */
    public static final class ComputeNewCentroids implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

        @Override
        public Centroid map(Tuple3<Integer, Point, Long> tuple) throws Exception {
            return new Centroid(
                    tuple.f0,                   // centroidId
                    tuple.f1.div(tuple.f2));    // Point (x1 +...+ xn / n), (y1 +...+ yn / n)
        }
    }

    /*
     * Compute the distance for each point
     * In: (centroidId, point p)
     * Out: (distance)
     */
    public static final class ComputeObjFunction extends RichMapFunction<Tuple2<Integer, Point>, Tuple1<Double>> {
        private Collection<Centroid> centroids;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple1<Double> map(Tuple2<Integer, Point> tuple) throws Exception {
            double distance = 0;

            for (Centroid centroid : centroids) {
                if (centroid.id == tuple.f0) {
                    distance = Math.pow(tuple.f1.distance(centroid), 2);
                }
            }
            return new Tuple1<>(distance);
        }
    }


    // *************************************************************************
    //     CLASSES
    // *************************************************************************
    public static class Point implements Serializable {
        public double x;
        public double y;

        public Point() {
        }

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public double distance(Point p2) {
            return Math.sqrt((x - p2.x) * (x - p2.x) + (y - p2.y) * (y - p2.y));
        }

        public Point sum(Point p2) {
            x += p2.x;
            y += p2.y;
            return this;
        }

        public Point div(long v) {
            x /= v;
            y /= v;
            return this;
        }

        @Override
        public String toString() {
            return "Point{" +
                    "x=" + x +
                    ", y=" + y +
                    '}';
        }
    }


    public static class Centroid extends Point {
        public int id;

        public Centroid() {
        }

        public Centroid(int id, double x, double y) {
            super(x, y);
            this.id = id;
        }

        public Centroid(int id, Point p) {
            super(p.x, p.y);
            this.id = id;
        }

        @Override
        public String toString() {
            return "Centroid{" +
                    "x=" + x +
                    ", y=" + y +
                    ", id=" + id +
                    '}';
        }
    }
}


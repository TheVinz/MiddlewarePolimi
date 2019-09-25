package Kmeans_clustering_algorithm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.utils.RequiredParametersException;
import org.apache.flink.configuration.Configuration;

import java.io.*;
import java.util.*;
import java.util.zip.DataFormatException;

public class BatchJob {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		final String filename = params.get("filename", null);   //Path to the file with Data
		final String output = params.get("output", null);   //Path to the output file for result
		int iterations = params.getInt("iterations", 0);  //Max iterations set by User

		if(filename == null) {
			throw new RequiredParametersException
					("Required parameter --filename with the path to the file with data");
		}
		if(output == null){
		    System.out.println("\nIf in a future you want to save your result and see the clusters add the parameter\n"
					+ "--output with the path to the file on which you want the data to be printed");
        }
		if(iterations == 0) {
			System.out.println("\nIf in a future you want to set the maximum value of iterations different from the "
					+ "defatult value of '100' add the parameter\n" + "--iterations numberOfMaxIt");
			iterations = 100;
		}

		Data data = new Data();
		readData(data, filename);   //Function to read Data from the file
        data.createCentroidsAndClusters(); //Creation of random Centroid and relatives clusters

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(24,5000));

		DataSet<Point> points = env.fromCollection(data.getPoints());
		DataSet<Cluster> clustersDataSet = env.fromCollection(data.getClusters());

		IterativeDataSet<Tuple4<Integer, Point, Boolean, Vector<Point>>> clusterLoop = clustersDataSet
				.map(computeTupleFromCluster)
				.returns(TypeInformation.of(new TypeHint<Tuple4<Integer, Point, Boolean, Vector<Point>>>() {}))
				.iterate(iterations);

		DataSet<Tuple4<Integer, Point, Boolean, Vector<Point>>> newClusters = clusterLoop
                .map(updateClusters)
				.returns(TypeInformation.of(new TypeHint<Tuple4<Integer, Point, Boolean, Vector<Point>>>() {}))
                .withBroadcastSet(
                        points
                        .map(findNearestCentroid).withBroadcastSet(clusterLoop, "clusters")
						.returns(TypeInformation.of(new TypeHint<Tuple4<Integer, Point, Vector<Point>, Long>>() {}))
                        .groupBy(0)
                                .reduce(sumCoordinatesAndPoints)
                                .map(averageCoordinates)
								.returns(TypeInformation.of(new TypeHint<Tuple3<Integer, Point, Vector<Point>>>() {})),
						"clusters");

        DataSet<Tuple4<Integer, Point, Boolean, Vector<Point>>> termination = clusterLoop
                .filter(onlyChanged);

		DataSet<Tuple4<Integer, Point, Boolean, Vector<Point>>> finalClusters = clusterLoop
				.closeWith(newClusters, termination);

		List<Cluster> result = finalClusters
                .sortPartition(0, Order.ASCENDING)
                .map(computeNewClusters)
				.returns(TypeInformation.of(new TypeHint<Cluster>() {}))
				.collect();

		printResult(result);

		if(output != null){
		    printResultOnOutput(result, output, data.getPoints(), data.getClusters());
        }
	}

	private static void readData(Data data, String filename) throws DataFormatException{

		File file = new File(filename);

		try (Scanner scan = new Scanner(file)) {
			if (scan.hasNextLong()) {
				data.setDimension(scan.nextLong());
			} else {
				throw new DataFormatException("File with not correct Data");
			}
			if (scan.hasNextLong()) {
				data.setNumberOfPoints(scan.nextLong());
			} else {
				throw new DataFormatException("File with not correct Data");
			}
			if (scan.hasNextLong()) {
				data.setNumberOfCentroids(scan.nextLong());
			} else {
				throw new DataFormatException("File with not correct Data");
			}

			int dimension = 0;
			long numberOfPoints = 0;
			Vector<Double> coordinate = new Vector<>();
			String num;

			while (scan.hasNext()) {
				if (numberOfPoints < data.getNumberOfPoints()) {
					num = scan.next();
					coordinate.add(Double.valueOf(num));
					dimension++;

					if (dimension == data.getDimension()) {
						Point point = new Point(coordinate);
						data.addPoint(point);
						numberOfPoints++;
						coordinate.clear();
						dimension = 0;
					}
				} else {
					throw new DataFormatException("File with not correct Data: more point than expected");
				}
			}
			if (numberOfPoints != data.getNumberOfPoints()) {
				throw new DataFormatException("File with not correct Data: less point than expected\n" +
						"numberOfPoint: " + numberOfPoints + "\ndata.getNumberOfPoints: " + data.getNumberOfPoints());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void printResult (List<Cluster> result) {
		for(Cluster cluster : result){
            System.out.println(
                    "\nCluster id: " + cluster.getId() +
                    "\nCentroid: " + cluster.getCentroidCoordinates() +
                    "\nNumber of points: " + cluster.getPoints().size());
        }
        System.out.println("\n");
    }

    private static void printResultOnOutput
			(List<Cluster> result, String output, Set<Point> points, Set<Cluster> initialClusters) throws Exception{

	    FileWriter fw = new FileWriter(output);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter out = new PrintWriter(bw);

        out.println("Points:");
        for(Point point : points){
            out.println(" " + point.getCoordinates());
        }

        out.println("\nRandom Centroids:");
        for(Cluster cluster : initialClusters){
            out.println(" " + cluster.getCentroidCoordinates());
        }

        out.println("\n\n========= RESULTS =========");
        out.println("\nFinal Clusters");
        for(Cluster cluster : result){
            out.println("\nCluster id: " + cluster.getId());
            out.println("Centroid: " + cluster.getCentroidCoordinates());
            out.println("Number of points: " + cluster.getPoints().size());
            out.println("Points:");
            for(Point point : cluster.getPoints()){
                out.println(" " + point.getCoordinates());
            }
        }
        out.close();
        bw.close();
        fw.close();
    }


    private static final RichMapFunction<Point, Tuple4<Integer, Point, Vector<Point>, Long>> findNearestCentroid =
			new RichMapFunction<Point, Tuple4<Integer, Point, Vector<Point>, Long>>(){
				private Collection<Tuple4<Integer, Point, Boolean, Vector<Point>>> clusters;

				@Override
				public void open(Configuration parameters) {
					this.clusters = getRuntimeContext().getBroadcastVariable("clusters");
				}

				@Override
				public Tuple4<Integer, Point, Vector<Point>, Long> map(Point point) throws Exception {
					int idClosest = -1;
					double minDistance = Double.MAX_VALUE;

					for (Tuple4<Integer, Point, Boolean, Vector<Point>> cluster : clusters) {
						double distance = point.distanceFromCentroid(cluster.f1.getCoordinates());
						if (distance < minDistance) {
							minDistance = distance;
							idClosest = cluster.f0;
						}
					}

					if(idClosest == -1) throw new DataFormatException("Not found a centroid for a point, impossible situation");

					Vector<Point> pointVector = new Vector<>();
					pointVector.add(point);

					return new Tuple4<>(idClosest, point, pointVector, 1L);
				}
			};


	private static final ReduceFunction<Tuple4<Integer, Point, Vector<Point>, Long>> sumCoordinatesAndPoints =
			(ReduceFunction<Tuple4<Integer, Point, Vector<Point>, Long>>) (data1, data2) -> {
				data1.f2.addAll(data2.f2);
				data1.f3 += data2.f3;
				return new Tuple4<>(data1.f0, data1.f1.sumPoint(data2.f1), data1.f2, data1.f3);
			};

	private static final MapFunction<Tuple4<Integer, Point, Vector<Point>, Long>, Tuple3<Integer, Point, Vector<Point>>>
		averageCoordinates =
			(MapFunction<Tuple4<Integer, Point, Vector<Point>, Long>, Tuple3<Integer, Point, Vector<Point>>>) value -> {
				value.f1.dividePoint(value.f3);
				return new Tuple3<>(value.f0, value.f1, value.f2);
			};

	private static final MapFunction<Tuple4<Integer, Point, Boolean, Vector<Point>>, Cluster> computeNewClusters =
			(MapFunction<Tuple4<Integer, Point, Boolean, Vector<Point>>, Cluster>) value -> {
				Cluster cluster = new Cluster(value.f0, value.f1.getCoordinates());
				cluster.setPoints(value.f3);
				return cluster;
			};

	private static final RichMapFunction<Tuple4<Integer, Point, Boolean, Vector<Point>>, Tuple4<Integer, Point, Boolean, Vector<Point>>>
		updateClusters =
			new RichMapFunction<Tuple4<Integer, Point, Boolean, Vector<Point>>, Tuple4<Integer, Point, Boolean, Vector<Point>>> (){

				private Collection<Tuple3<Integer, Point, Vector<Point>>> clusters;

				@Override
				public void open(Configuration parameters) {
					this.clusters = getRuntimeContext().getBroadcastVariable("clusters");
				}

				@Override
				public Tuple4<Integer, Point, Boolean, Vector<Point>> map
						(Tuple4<Integer, Point, Boolean, Vector<Point>> clusterToUpdate) {

					for(Tuple3<Integer, Point, Vector<Point>> cluster : clusters){
						if(clusterToUpdate.f0.equals(cluster.f0)) {
							if (clusterToUpdate.f1.getCoordinates().equals(cluster.f1.getCoordinates())
									&& clusterToUpdate.f3.size() == cluster.f2.size()) {
								return new Tuple4<>(cluster.f0, cluster.f1, false, cluster.f2);
							} else {
								return new Tuple4<>(cluster.f0, cluster.f1, true, cluster.f2);
							}
						}
					}
					return new Tuple4<>(clusterToUpdate.f0, clusterToUpdate.f1, false, clusterToUpdate.f3);
				}
			};

	private static final MapFunction<Cluster, Tuple4<Integer, Point, Boolean, Vector<Point>>> computeTupleFromCluster =
			(MapFunction<Cluster, Tuple4<Integer, Point, Boolean, Vector<Point>>>) cluster -> {
				int id = cluster.getId();
				Point centroid = new Point(cluster.getCentroidCoordinates());
				Vector<Point> points = cluster.getPoints();
				return new Tuple4<>(id, centroid, true, points);
			};

	private static final FilterFunction<Tuple4<Integer, Point, Boolean, Vector<Point>>> onlyChanged =
			(FilterFunction<Tuple4<Integer, Point, Boolean, Vector<Point>>>) value -> value.f2;
}

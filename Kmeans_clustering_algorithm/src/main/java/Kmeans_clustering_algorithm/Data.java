package Kmeans_clustering_algorithm;

import java.util.*;

import static java.lang.Math.round;

class Data {

    private long dimension;
    private long numberOfPoints;
    private long numberOfCentroids;

    private Set<Point> points;
    private Set<Cluster> clusters;

    Data() {
        this.points = new HashSet<>();
        this.clusters = new HashSet<>();
    }

    long getDimension() {
        return dimension;
    }

    void setDimension(long dimension) {
        this.dimension = dimension;
    }

    long getNumberOfPoints() {
        return numberOfPoints;
    }

    void setNumberOfPoints(long numberOfPoints) {
        this.numberOfPoints = numberOfPoints;
    }

    void setNumberOfCentroids(long numberOfCentroids) {
        this.numberOfCentroids = numberOfCentroids;
    }

    Set<Point> getPoints() {
        return points;
    }

    Set<Cluster> getClusters(){return  clusters; }

    void addPoint(Point point){
        this.points.add(point);
    }

    void createCentroidsAndClusters(){

        Vector<Double> minCoordinates = new Vector<>();
        Vector<Double> maxCoordinates = new Vector<>();

        //Steps to create reasonable random centroid between the maximum and minimum values of the point
        for(int dim = 0; dim < dimension; dim++){
            minCoordinates.add(Double.MAX_VALUE);
            maxCoordinates.add(Double.MIN_VALUE);
        }
        for (Point point : this.points) {
            for (int dim = 0; dim < dimension; dim ++){
                if(point.getCoordinates().get(dim) < minCoordinates.get(dim)){
                    minCoordinates.set(dim, point.getCoordinates().get(dim));
                }
                if(point.getCoordinates().get(dim) > maxCoordinates.get(dim)){
                    maxCoordinates.set(dim, point.getCoordinates().get(dim));
                }
            }
        }

        //Creations of the random initial centroids and relatives clusters
        Vector<Double> coordinates = new Vector<>();
        Random r = new Random();
        for(int i = 0; i < numberOfCentroids; i++){
            for(int dim = 0; dim < dimension; dim++){
                int min = (int) round(minCoordinates.get(dim));
                int max = (int) round(maxCoordinates.get(dim));
                double coordinate = r.nextInt((max - min )+ 1) + min;
                coordinates.add(coordinate);
            }
            this.clusters.add(new Cluster(i, coordinates));
            coordinates.clear();
        }

    }

}

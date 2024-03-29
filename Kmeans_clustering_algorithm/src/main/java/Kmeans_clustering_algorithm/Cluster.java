package Kmeans_clustering_algorithm;

import java.util.Vector;

class Cluster {

    private int id;
    private Vector<Double> centroidCoordinates;
    private Vector<Point> points;

    Cluster(int id, Vector<Double> centroidCoordinates) {
        this.id = id;
        this.centroidCoordinates = new Vector<>(centroidCoordinates);
        this.points = new Vector<>();
    }

    int getId() {
        return id;
    }

    Vector<Double> getCentroidCoordinates() {
        return centroidCoordinates;
    }

    Vector<Point> getPoints() {
        return points;
    }

    void setPoints(Vector<Point> points) {
        this.points.clear();
        this.points.addAll(points);
    }
}

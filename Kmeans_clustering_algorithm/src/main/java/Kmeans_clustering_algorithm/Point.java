package Kmeans_clustering_algorithm;

import java.util.Vector;
import java.util.zip.DataFormatException;

public class Point{

    private Vector<Double> coordinates;

    Point(Vector<Double> coordinates) {
        this.coordinates = new Vector<>(coordinates);
    }

    Vector<Double> getCoordinates() {
        return coordinates;
    }

    private void setCoordinates(Vector<Double> coordinates) {
        this.coordinates.clear();
        this.coordinates.addAll(coordinates);
    }

    public Point sumPoint (Point otherPoint) throws DataFormatException{

        if (this.coordinates.size() == otherPoint.getCoordinates().size()) {

            Vector<Double> coordinates = new Vector<>(this.coordinates);
            Vector<Double> otherCoordinates = new Vector<>(otherPoint.getCoordinates());
            Vector<Double> newCoordinates = new Vector<>();

            int size = this.coordinates.size();

            for(int i=0; i<size; i++){
                newCoordinates.add(coordinates.get(i) + otherCoordinates.get(i));
            }

            this.setCoordinates(newCoordinates);

        }
        else{
            throw new DataFormatException("Addition of two points: the dimension of the two points is not the same");
        }

        return  this;

    }

    public void dividePoint (Long value){

        Vector<Double> coordinates = new Vector<>(this.coordinates);
        Vector<Double> newCoordinates = new Vector<>();

        for (Double coordinate : coordinates) {
            coordinate /= value;
            newCoordinates.add(coordinate);
        }

        this.setCoordinates(newCoordinates);
    }

    double distanceFromCentroid(Vector<Double> centroidCoordinates) throws DataFormatException{

        double coordinatesSquareSum = 0;
        double coordinate;
        double otherCoordinate;

        if (this.coordinates.size() == centroidCoordinates.size()) {

            for(int i=0; i<this.coordinates.size(); i++){
                coordinate = this.coordinates.get(i);
                otherCoordinate = centroidCoordinates.get(i);
                coordinatesSquareSum += (coordinate - otherCoordinate) * (coordinate - otherCoordinate);
            }
        }
        else{
            throw new DataFormatException("Distance of two points: the dimension of the two points is not the same");
        }

        return Math.sqrt(coordinatesSquareSum);

    }

}
# OpenMP/MPI
This project implements a simple [k-mean clustering](http://home.deib.polimi.it/matteucc/Clustering/tutorial_html/kmeans.html) algorithm using MPI for splitting the points set all over the processes and OMP for maximizing parallelism among the single process.

## Usage
The program requires an input file as parameter, if not given the default filename will be *points-generated.txt*.
The file must contains 3 rows each describing in order:
* the number of centroids *k*
* the number of points *n*
* the number of coordinates for each point *m*
Then *n* lines each with *m* floats follows, each representing a point.
The output will be written on the file *out.txt*. The output contains a set of *k* arrays of size *m* representing the centroid's coordinates, each followed by a list of points coordinates representing the cluster associated with the centroid.
The time spent for the computation is written down at the end of the output file.

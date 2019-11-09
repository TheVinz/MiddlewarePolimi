k-mean_clustering_MPI_OpenMP: 
	mpic++ -fopenmp k-mean_clustering_MPI_OpenMP.cpp -o k-mean_clustering_MPI_OpenMP
clean:
	rm k-mean_clustering_MPI_OpenMP
run:
	mpirun ./k-mean_clustering_MPI_OpenMP "points-generated.txt" "out.txt" -hostfile hostfile
testfile: points-generated.txt
	./test/points_generator.o

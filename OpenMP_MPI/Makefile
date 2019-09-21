k-mean_clustering_MPI_OpenMP: 
	mpic++ -fopenmp k-mean_clustering_MPI_OpenMP.cpp -o k-mean_clustering_MPI_OpenMP
clean:
	rm k-mean_clustering_MPI_OpenMP
run:
	cat points-generated.txt | mpirun ./k-mean_clustering_MPI_OpenMP -hostfile hostfile
testfile: points-generated.txt
	./test/points_generator.o

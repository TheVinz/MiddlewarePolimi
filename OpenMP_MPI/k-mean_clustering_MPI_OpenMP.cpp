#include <iostream>
#include <vector>
#include <math.h>
#include <stdlib.h>
#include <limits>
#include <set>
#include <sys/time.h>
#include <mpi.h>
#include <omp.h>
#include <fstream> 

#define MAX_K 500
#define MAX_N 10000
#define MAX_M 100


using namespace std;

typedef vector<double> point;

double cpuSecond(){
    struct timeval tp;
    gettimeofday(&tp, NULL);
    return ((double)tp.tv_sec+(double)tp.tv_usec*1.e-6);
};

void print_point(point x){
    cout << "[";
    for(int i=0; i<x.size(); i++){
        if(i<x.size()-1) cout<< x.at(i) << ", ";
        else cout << x.at(i) << "]\n";
    }
}

bool equal_points(point a, point b){
    for(int i=0; i< a.size(); i++){
        if(a.at(i) != b.at(i)) return false;
    }
    return true;
}

double point_dist(point a, point b){
    double result=0;
    for(int i=0; i<a.size(); i++){
        result+=pow(a.at(i)-b.at(i), 2);
    }
    return sqrt(result);
}

int main(int argc, char* argv[]){
    int k, n, m, rank, size, root=0;
    double time_start, time_end;
    set<point> clusters[MAX_K];
    point all_points[MAX_N];
    point points[MAX_N], centroids[MAX_K];



    MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    ifstream file;

    if(argc > 0)
        file.open(argv[1]);
    else 
        file.open("points-generated.txt"); 

    if(rank==root){
        file >> k;
        file >> n;
        file >> m;
    }

    MPI_Bcast(&k, 1, MPI_INT, root, MPI_COMM_WORLD);
    MPI_Bcast(&n,1 ,MPI_INT, root,  MPI_COMM_WORLD);
    MPI_Bcast(&m, 1, MPI_INT, root,  MPI_COMM_WORLD);

    for(int i=0; i<k; i++){
        centroids[i]=point(m);
    }

    int num_points=n/size, remainder;
    remainder = n-num_points*size;
    if(rank<remainder) num_points++;

    //La radice inizializza tutti i punti e i centroidi, dopodichè li distribuisce a tutti i nodi
    if(rank==root){
        vector<double> max(m, numeric_limits<double>::min()), min(m, numeric_limits<double>::max());

        int dest, cont=0;

        //Inizializzazione punti
        for(int i=0; i<n; i++){
            point p(m);
            for(int j=0; j<m; j++){
                double in;
                file >> in;
                p.at(j) = in;
                if(in > max.at(j))
                    max.at(j)=in;
                if(in < min.at(j))
                    min.at(j)=in;
            }
            dest=i%size;
            all_points[i] = p;
            if(dest==0){
                points[cont]=p;
                cont++;
            } else{
                MPI_Send(p.data(), m, MPI_DOUBLE, dest, 0, MPI_COMM_WORLD);
            }
        }

        for(int i=0; i<k; i++){
            point centroid(m);
            for(int j=0; j<m; j++){
                double coord_min=min.at(j), coord_max=max.at(j);
                centroid.at(j)=(drand48()*(coord_max-coord_min))+coord_min;
            }
            centroids[i]=centroid;
        }

        file.close();

    } else{
        MPI_Status status;
        for(int i=0; i<num_points; i++){
            points[i]= point(m);
            MPI_Recv(points[i].data(), m, MPI_DOUBLE, root, 0, MPI_COMM_WORLD, &status);
        }
    }

    bool same_centroids=false;

    time_start = cpuSecond();
    int local_clusters_size[MAX_K], global_clusters_size[MAX_K];
    point local_new_centroids[MAX_K], global_new_centroids[MAX_K];


    while(!same_centroids){
        same_centroids=true;

        for(int cenIt=0; cenIt<k; cenIt++){
            MPI_Bcast(centroids[cenIt].data(), m, MPI_DOUBLE, root, MPI_COMM_WORLD);
        }

        for(int i=0; i<k; i++){
            clusters[i].clear();
        }

        set<point> threadClusters[omp_get_max_threads()][k];
        int chunkSize = num_points / omp_get_max_threads();
        #pragma omp parallel for schedule(dynamic, chunkSize)
        for(int pointIt=0; pointIt<num_points; pointIt++){
            //Riazzero distanza di confronto
            double min_dist = numeric_limits<double>::max();
            int nearest_centroid;
            //Ciclo sui centroidi
            for(int cenIt=0; cenIt<k; cenIt++){
                double distance = point_dist(points[pointIt], centroids[cenIt]);
                if(distance < min_dist){
                    min_dist=distance;
                    nearest_centroid=cenIt;
                }
            }
            //Inserisco il punto nel cluster identificato dal centroide più vicino nel vettore del thread corrispondente
            threadClusters[omp_get_thread_num()][nearest_centroid].insert(points[pointIt]);
        }

        chunkSize = k / omp_get_max_threads();
        #pragma omp parallel for schedule(dynamic, chunkSize)
        for(int cenIt=0; cenIt<k; cenIt++){
            for(int i=0; i<omp_get_max_threads(); i++){
                for(set<point>::iterator it=threadClusters[i][cenIt].begin(); it!=threadClusters[i][cenIt].end(); ++it){
                        point p=*it;
                        clusters[cenIt].insert(p);
                    }
            }
        }

        for(int i=0; i<k; i++){
            local_clusters_size[i]=clusters[i].size();
        }

        #pragma omp parallel for
        for(int cenIt=0; cenIt<k; cenIt++){
            if(clusters[cenIt].size()!=0){
                point new_centroid= point(m,0);
                for(int dim=0; dim<m; dim++){
                    double new_coord=0;
                    for(set<point>::iterator it=clusters[cenIt].begin(); it!=clusters[cenIt].end(); ++it){
                        point p=*it;
                        new_coord+=p.at(dim);
                    }
                    new_centroid.at(dim)=new_coord;
                }
                local_new_centroids[cenIt]=new_centroid;
            }
            else{
                local_new_centroids[cenIt]=point(m,0);
            }
        }

        MPI_Allreduce(local_clusters_size, global_clusters_size, k, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

        #pragma omp parallel for
        for(int centroidIt = 0; centroidIt<k; centroidIt++){
            for(int coord = 0; coord < m; coord ++){
                local_new_centroids[centroidIt].at(coord) /= global_clusters_size[centroidIt];
            }
        }

        for(int centroid=0; centroid<k; centroid++){
            global_new_centroids[centroid]=point(m,0);
            MPI_Reduce(local_new_centroids[centroid].data(), global_new_centroids[centroid].data(), m, MPI_DOUBLE, MPI_SUM, root, MPI_COMM_WORLD);
        }

        if(rank==root){
            #pragma omp parallel for
            for(int centroid=0; centroid<k; centroid++){
                if(!equal_points(global_new_centroids[centroid], centroids[centroid])){
                    same_centroids=false;
                    if(global_clusters_size[centroid]!=0)
                        centroids[centroid]=global_new_centroids[centroid];
                }
            }
        }

        MPI_Bcast(&same_centroids, 1, MPI_C_BOOL, root, MPI_COMM_WORLD);
    }

    MPI_Finalize();

    if(rank==root){


        time_end=cpuSecond();

        if(argc > 1) {
            ofstream out(argv[2]);

            for(int i=0; i<k; i++){
                clusters[i].clear();
            }

            set<point> threadClusters[omp_get_max_threads()][k];
            int chunkSize = n / omp_get_max_threads();
            #pragma omp parallel for schedule(dynamic, chunkSize)
            for(int pointIt=0; pointIt<n; pointIt++){
                //Riazzero distanza di confronto
                double min_dist = numeric_limits<double>::max();
                int nearest_centroid;
                //Ciclo sui centroidi
                for(int cenIt=0; cenIt<k; cenIt++){
                    double distance = point_dist(all_points[pointIt], centroids[cenIt]);
                    if(distance < min_dist){
                        min_dist=distance;
                        nearest_centroid=cenIt;
                    }
                }
                //Inserisco il punto nel cluster identificato dal centroide più vicino nel vettore del thread corrispondente
                threadClusters[omp_get_thread_num()][nearest_centroid].insert(all_points[pointIt]);
            }

            chunkSize = k / omp_get_max_threads();
            #pragma omp parallel for schedule(dynamic, chunkSize)
            for(int cenIt=0; cenIt<k; cenIt++){
                for(int i=0; i<omp_get_max_threads(); i++){
                    for(set<point>::iterator it=threadClusters[i][cenIt].begin(); it!=threadClusters[i][cenIt].end(); ++it){
                            point p=*it;
                            clusters[cenIt].insert(p);
                        }
                }
            }

            for(int i=0; i<k; i++){
                out << endl << i+1 << ".  ";
                out << "[";
                for(int s=0; s<global_new_centroids[i].size(); s++){
                    if(s<global_new_centroids[i].size()-1) out<< global_new_centroids[i].at(s) << ", ";
                    else out << global_new_centroids[i].at(s) << "]\n";
                }
                out << "  Points:  \n";
                for(set<point>::iterator it=clusters[i].begin(); it!=clusters[i].end(); ++it){
                    point p = *it;
                    out << "    [";
                    for(int s=0; s<p.size(); s++){
                        if(s<p.size()-1) out<< p.at(s) << ", ";
                        else out << p.at(s) << "]\n";
                    }
                }
            }

            out << "Time: " << time_end-time_start << endl;
        }
        for (int i=0; i<k ; i++){
            cout << i+1 << ".  ";
            print_point(global_new_centroids[i]);
        }
        cout << "Time: "<<time_end-time_start<<endl;
    }
}

#include <iostream>
#include <vector>
#include <math.h>
#include <stdlib.h>
#include <limits>
#include <set>
#include <sys/time.h>
#include <mpi.h>
#include <omp.h>

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
    int k, n, m, rank, size, root=0, chunkSize;
    double time_start=cpuSecond(), time_end;
    set<point> clusters[MAX_K];
    point points[MAX_N];
    point centroids[MAX_K];

    MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if(rank==root){
        cin >> k;
        cin >> n;
        cin >> m;
    }

    MPI_Bcast(&k, 1, MPI_INT, root, MPI_COMM_WORLD);
    MPI_Bcast(&n,1 ,MPI_INT, root,  MPI_COMM_WORLD);
    MPI_Bcast(&m, 1, MPI_INT, root,  MPI_COMM_WORLD);

    chunkSize = k / omp_get_max_threads();
    #pragma omp parallel for schedule (dynamic, chunkSize)
    for(int i=0; i<k; i++){
        centroids[i]=point(m);
    }

    int scatter_size=n/size, remainder;
    remainder = n-scatter_size*size;
    if(remainder==0)
        remainder=scatter_size;

    //La radice inizializza tutti i punti e i centroidi, dopodichè li distribuisce a tutti i nodi
    if(rank==root){
        /*numeric_limits<int>::min() e max() permettono di inizializzare il vettore min e max rispettivamente
        con il valore minimo e massimo che un int può assumere, i vettori min e max memorizzano il valore minimo e massimo
        assunto da una data coordinata nei vari punti in input, in modo da avere una generazione dei centroidi iniziali meno casuale*/
        vector<double> max(m, numeric_limits<double>::min()), min(m, numeric_limits<double>::max());

        //Inizializzazione punti
        for(int i=0; i<n; i++){
            point p(m);
            for(int j=0; j<m; j++){
                double in;
                cin >> in;
                p.at(j) = in;
                points[i]=p;
                if(in > max.at(j))
                    max.at(j)=in;
                if(in < min.at(j))
                    min.at(j)=in;
            }
        }


        //Inizializzazione della dimensione dei centroidi per permetter il collapse al ciclo dopo
        chunkSize = k / omp_get_max_threads();
        #pragma omp parallel for schedule (dynamic, chunkSize)
        for(int i=0; i<k; i++){
            point centroid(m);
            centroids[i] = centroid;
        }
        /*Inizializzazione centroidi random: le coordinate dei centroid sono numeri casuali
         che variano tra il minimo valore assunto da quella coordinata dai punti e il massimo*/
        chunkSize = k*m / omp_get_max_threads();
        #pragma omp parallel for schedule (dynamic, chunkSize) collapse (2)
        for(int i=0; i<k; i++){
            for(int j=0; j<m; j++){
                double coord_min=min.at(j), coord_max=max.at(j);
                centroids[i].at(j)=(drand48()*(coord_max-coord_min))+coord_min;
            }
        }

        for(int dest=1; dest<size; dest++){
            int start_point=remainder+(dest-1)*scatter_size, end_point;
            end_point=start_point+scatter_size;
            for(int i=start_point; i<end_point; i++){
                MPI_Send(points[i].data(), m, MPI_DOUBLE, dest, 0, MPI_COMM_WORLD);
            }
        }
    } else{
        MPI_Status status;
        for(int i=0; i<scatter_size; i++){
            points[i]= point(m);
            MPI_Recv(points[i].data(), m, MPI_DOUBLE, root, 0, MPI_COMM_WORLD, &status);
        }
    }

    bool same_centroids=false;
    int num_points;
    if(rank==root)
        num_points=remainder;
    else
        num_points=scatter_size;

    while(!same_centroids){
        same_centroids=true;

        for(int cenIt=0; cenIt<k; cenIt++){
            MPI_Bcast(centroids[cenIt].data(), m, MPI_DOUBLE, root, MPI_COMM_WORLD);
        }

        chunkSize = k / omp_get_max_threads();
        #pragma omp parallel for schedule (dynamic, chunkSize)
        for(int i=0; i<k; i++){
            clusters[i].clear();
        }

        chunkSize = num_points / omp_get_max_threads();
        #pragma omp parallel for schedule (dynamic, chunkSize)
        for(int pointIt=0; pointIt<num_points; pointIt++){
            //Riazzero distanza di confronto
            double min_dist=numeric_limits<double>::max();
            int nearest_centroid;
            //Ciclo sui centroidi
            for(int cenIt=0; cenIt<k; cenIt++){
                double distance = point_dist(points[pointIt], centroids[cenIt]);
                if(distance < min_dist){
                    min_dist=distance;
                    nearest_centroid=cenIt;
                }
            }
            #pragma omp critical
            {
                clusters[nearest_centroid].insert(points[pointIt]);
            }
        }

        point local_new_centroids[MAX_K], global_new_centroids[MAX_K];

        chunkSize = k / omp_get_max_threads();
        #pragma omp parallel for schedule (dynamic, chunkSize)
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

        int local_clusters_size[MAX_K], global_clusters_size[MAX_K];

        chunkSize = k / omp_get_max_threads();
        #pragma omp parallel for schedule (dynamic, chunkSize)
        for(int cluster=0; cluster<k; cluster++){
            local_clusters_size[cluster]=clusters[cluster].size();
        }

        MPI_Reduce(local_clusters_size, global_clusters_size, k, MPI_INT, MPI_SUM, root, MPI_COMM_WORLD);

        chunkSize = k / omp_get_max_threads();
        #pragma omp parallel for schedule (dynamic, chunkSize)
        for(int centroid=0; centroid<k; centroid++){
            global_new_centroids[centroid]=point(m,0);
            MPI_Reduce(local_new_centroids[centroid].data(), global_new_centroids[centroid].data(), m, MPI_DOUBLE, MPI_SUM, root, MPI_COMM_WORLD);
            if(rank==root && global_clusters_size[centroid]>0){
                for(int coord=0; coord<m; coord++){
                    global_new_centroids[centroid].at(coord)/=global_clusters_size[centroid];
                }
            }
        }

        if(rank==root){
            chunkSize = k / omp_get_max_threads();
            #pragma omp parallel for schedule (dynamic, chunkSize) shared(same_centroids)
            for(int centroid=0; centroid<k; centroid++){
                if(!equal_points(global_new_centroids[centroid], centroids[centroid])){
                    #pragma critical
                    {
                        same_centroids=false;
                    }
                    if(global_clusters_size[centroid]!=0)
                        centroids[centroid]=global_new_centroids[centroid];
                }
            }
        }

        MPI_Bcast(&same_centroids, 1, MPI_C_BOOL, root, MPI_COMM_WORLD);
    }

    MPI_Finalize();

    if(rank==root){

        for(int i=0; i<k; i++){
            print_point(centroids[i]);
        }

        time_end=cpuSecond();
        cout << "Time: "<<time_end-time_start<<endl;
    }
}

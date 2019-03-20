#include <iostream>
#include <vector>
#include <math.h>
#include <stdlib.h>
#include <limits>
#include <set>
#include <sys/time.h>
#include <fstream>
#include <omp.h>


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
    int k, n, m;
    int chunkSize;
    double time_start, time_end;


    cin >> k;
    cin >> n;
    cin >> m;

    point points[n];
    point centroids[k];
    set<point> clusters[k];




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
            if(in > max.at(j))
                max.at(j)=in;
            if(in < min.at(j))
                min.at(j)=in;
        }
        points[i]=p;
    }

    time_start=cpuSecond();
    /*Inizializzazione centroidi random: le coordinate dei centroid sono numeri casuali
     che variano tra il minimo valore assunto da quella coordinata dai punti e il massimo*/

    //Inizializzazione della dimensione dei centroidi per permetter il collapse al ciclo dopo
    /*for(int i=0; i<k; i++){
        point centroid(m);
        centroids[i] = centroid;
    }*/

    for(int i=0; i<k; i++){
        point centroid(m);
        centroids[i] = centroid;
        for(int j=0; j<m; j++){
            double coord_min=min.at(j), coord_max=max.at(j);
            centroids[i].at(j)=(drand48()*(coord_max-coord_min))+coord_min;
        }
    }

    bool same_centroid = false;
    while(!same_centroid){

        for(int i=0; i<k; i++){
            clusters[i].clear();
        }
        same_centroid = true;


        //Calcolo il centroide più vicino per ogni punto
        set<point> threadClusters[omp_get_max_threads()][k];
        chunkSize = n / omp_get_max_threads();
        #pragma omp parallel for schedule(dynamic, chunkSize)
        for(int pointIt=0; pointIt<n; pointIt++){
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

        //Ricalcolo centroidi
        chunkSize = k / omp_get_max_threads();
        #pragma omp parallel for schedule(dynamic, chunkSize)
        for(int cenIt=0; cenIt<k; cenIt++){

            if(clusters[cenIt].size()!=0){
                point new_centroid= point(m,0);

                for(int dim=0; dim<m; dim++){
                    double new_coord=0;

                    for(set<point>::iterator it=clusters[cenIt].begin(); it!=clusters[cenIt].end(); ++it){
                        point p=*it;
                        new_coord+=p.at(dim);
                    }
                    new_coord/=clusters[cenIt].size();
                    new_centroid.at(dim)=new_coord;
                }
                if(!equal_points(new_centroid, centroids[cenIt])){
                    centroids[cenIt]=new_centroid;
                    same_centroid=false;
                }
            }
        }
    }

    for(int i=0; i<k; i++){
        print_point(centroids[i]);
    }

    time_end=cpuSecond();

    cout << "Time: "<<time_end-time_start<<endl;

}

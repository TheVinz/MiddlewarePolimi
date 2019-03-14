#include <iostream>
#include <vector>
#include <math.h>
#include <stdlib.h>
#include <limits>
#include <set>
#include <sys/time.h>
#include <fstream>
#include <omp.h>


#define MAX_K 500
#define MAX_N 10000
#define MAX_M 10000


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

    ifstream File("points-generated.txt");

    File >> k;
    cout << "Centroids: "  << k << endl;
    File >> n;
    cout << "Points: " << n << endl;
    File >> m;
    cout << "Dimensions: "<< m << endl << endl;

    point points[MAX_N];
    point centroids[MAX_K];
    set<point> clusters[MAX_K];



    /*numeric_limits<int>::min() e max() permettono di inizializzare il vettore min e max rispettivamente
    con il valore minimo e massimo che un int può assumere, i vettori min e max memorizzano il valore minimo e massimo
    assunto da una data coordinata nei vari punti in input, in modo da avere una generazione dei centroidi iniziali meno casuale*/
    vector<double> max(m, numeric_limits<double>::min()), min(m, numeric_limits<double>::max());

    //Inizializzazione punti
    for(int i=0; i<n; i++){
        point p(m);
        for(int j=0; j<m; j++){
            double in;
            File >> in;
            p.at(j) = in;
            //TODO assegnamento sotto può andare fuori dal ciclo interno?
            points[i]=p;
            if(in > max.at(j))
                max.at(j)=in;
            if(in < min.at(j))
                min.at(j)=in;
        }
    }

    cout << "Points list:" << endl;
    for(int i=0; i<n; i++){
        print_point(points[i]);
    }
    cout << endl << endl;

    time_start=cpuSecond();
    /*Inizializzazione centroidi random: le coordinate dei centroid sono numeri casuali
     che variano tra il minimo valore assunto da quella coordinata dai punti e il massimo*/
    long seed=time_start;
    srand48(seed);

    //Inizializzazione della dimensione dei centroidi per permetter il collapse al ciclo dopo
    chunkSize = k / omp_get_max_threads();
    #pragma omp parallel for schedule (dynamic, chunkSize)
    for(int i=0; i<k; i++){
        point centroid(m);
        centroids[i] = centroid;
    }

    chunkSize = k*m / omp_get_max_threads();
    #pragma omp parallel for schedule (dynamic, chunkSize) collapse (2)
    for(int i=0; i<k; i++){
        for(int j=0; j<m; j++){
            double coord_min=min.at(j), coord_max=max.at(j);
            centroids[i].at(j)=(drand48()*(coord_max-coord_min))+coord_min;
        }
    }

    bool same_centroid = false;
    while(!same_centroid){

        chunkSize = k / omp_get_max_threads();
        #pragma omp parallel for schedule (dynamic, chunkSize)
        for(int i=0; i<k; i++){
            clusters[i].clear();
        }
        same_centroid = true;

        //Calcolo il centroide più vicino per ogni punto
        //TODO si blocca quando uso OpenMP qui
        chunkSize = n / omp_get_max_threads();
        #pragma omp parallel for schedule (dynamic, chunkSize)
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
            //Inserisco il punto nel cluster identificato dal centroide più vicino
            #pragma omp critical
            {
            clusters[nearest_centroid].insert(points[pointIt]);
            }
        }

        //Ricalcolo centroidi
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

    cout << "Final centroids:" << endl;
    for(int i=0; i<k; i++){
        print_point(centroids[i]);
    }

    time_end=cpuSecond();
    cout << "Time: "<<time_end-time_start<<endl;

}

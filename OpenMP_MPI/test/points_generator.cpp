#include <iostream>
#include <stdlib.h>
#include <fstream>
#include <sys/time.h>

using namespace std;

int main(int argc, char* argv[]){

    int k, n, m;
    ofstream File("points-generated.txt");

    cout << "Centroids: ";
    cin >> k;
    File << k << endl;


    cout << "Points: ";
    cin >> n;
    File << n << endl;

    cout << "Dimensions: ";
    cin >> m;
    File << m << endl;

    struct timeval tp;
    gettimeofday(&tp, NULL);
    long seed = ((double)tp.tv_sec+(double)tp.tv_usec*1.e-6);

    srand48();

    for(int i=0; i<n; i++){
        for(int j=0; j<m; j++){
            File << drand48()*1000 << "  ";
        }
        File << endl;
    }


}

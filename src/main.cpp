#include <unistd.h> // for getopt
#include <iostream> // for getopt

using namespace std;

int main(int argc, char * argv[]) {
  int c; 

  while ((c = getopt(argc, argv, "")) != -1){
    switch (c) {
    }
  }
  if (optind >= argc) {
    cerr << "Usage: " << argv[0] << " <csvfile>" << endl;
    exit(1);
  }

  const char *filename = argv[optind];
  std::cerr << filename << '\n';
  return 0;
}

/*
 * The dataset is formed by the strings passed as input to the program.  Run as
 * ./example1 <string1> <string2> ...
 */
#include <chrono> // TODO: This has to be included because windflow_gpu.hpp uses
                  // chrono without including the header!  This must be
                  // patched...
#include <cstdlib>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <wf/windflow_gpu.hpp>

using namespace std;
using namespace wf;

int main() {
    cout << "Built GPU example!\n";
    return 0;
}

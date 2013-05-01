#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <cassert>

#include "cli.h"

using namespace std;

CLI *cli;
const int success = 0;

int main()
{
  cli = CLI::Instance();
  string input;
  cout << "************************" << endl;
  cout << "SecSQL CLI started" << endl;
  cout << "Enjoy!" << endl;
  do {
    cout << ">>> ";
    getline (cin, input);
  } while ((cli->process(input)) != -1);
  cout << "Goodbye :(" << endl;
  return 0;
}

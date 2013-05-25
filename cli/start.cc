#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <cassert>

#include "cli.h"

using namespace std;

CLI *cli;

int main()
{
  system("rm -r \"" DATABASE_FOLDER "\" 2> /dev/null");
  
  cli = CLI::Instance();
  cli->start();

  return 0;
}
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <cassert>

#include "cli.h"

using namespace std;

CLI *cli;
const int success = 0;

void Test01()
{
  cout << ">>> create table ekin name=varchar(40), age=int" << endl;
  cli->process("create table ekin name=varchar(40), age=int");
}

int main()
{
  cli = CLI::Instance();
  string input;
  cout << "************************" << endl;
  cout << "SecSQL CLI started" << endl;
  cout << "Enjoy!" << endl;

  Test01();

  do {
    cout << ">>> ";
    getline (cin, input);
  } while ((cli->process(input)) != -1);
  cout << "Goodbye :(" << endl;
  return 0;
}

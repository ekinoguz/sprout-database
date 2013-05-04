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
  // cout << ">>> create table ekin name=varchar(40), age=int" << endl;
  // cli->process("create table ekin name=varchar(40), age=int");
  string command;

  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  cli->process(command);

  command = "load tbl_employee employee_50";
  cout << ">>> " << command << endl;
  cli->process(command);

  command = "print columns cli_columns";
  cout << ">>> " << command << endl;
  cli->process(command);

  command = "print cli_tables";
  cout << ">>> " << command << endl;
  cli->process(command);

  command = "drop table tbl_employee";
  cout << ">>> " << command << endl;
  cli->process(command);

  command = "print cli_tables";
  cout << ">>> " << command << endl;
  cli->process(command);

  // command = "print columns tables";
  // cout << ">>> " << command << endl;
  // cli->process(command);  
}

int main()
{
  system("rm -r \"" DATABASE_FOLDER "\" 2> /dev/null");

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

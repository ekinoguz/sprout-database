#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <cassert>

#include "cli.h"

using namespace std;

#define SUCCESS 0
#define MODE 0  // 0 = TEST MODE
                // 1 = INTERACTIVE MODE

CLI *cli;


void Test01()
{

  // test create table
  // test drop table
  string command;

  command = "create table ekin name=varchar(40), age=int";
  cout << ">>> create table ekin name=varchar(40), age=int" << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print columns cli_columns";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print cli_tables";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "drop table ekin";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print cli_tables";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print cli_columns";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  cout << "We should not see anything related to ekin table" << endl; 
}

void Test02()
{
  string command;

  // test create table
  // test load table
  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  cout << "Before loading file: " << endl;
  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load tbl_employee employee_50";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  cout << "After loading file: " << endl;
  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);
}

void Test03()
{
  string command;

  // test drop attribute
  // test quit
  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load tbl_employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  cout << "Before dropping attibute salary: " << endl;
  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "drop attribute Salary from tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  cout << "After dropping attibute Salary: " << endl;
  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  // cout << "Before dropping attibute EmpName: " << endl;
  // command = "print tbl_employee";
  // cout << ">>> " << command << endl;
  // assert (cli->process(command) == SUCCESS);

  // command = "drop attribute EmpName from tbl_employee";
  // cout << ">>> " << command << endl;
  // assert (cli->process(command) == SUCCESS);

  // cout << "After dropping attibute EmpName: " << endl;
  // command = "print tbl_employee";
  // cout << ">>> " << command << endl;
  // assert (cli->process(command) == SUCCESS);

  command = "quit";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);
}

int main()
{
  system("rm -r \"" DATABASE_FOLDER "\" 2> /dev/null");

  cli = CLI::Instance();

  if (MODE == 0) {
    //Test01();
    //Test02();
    Test03();  
  } else if (MODE == 1) {
    cli->start();
  }
  
  return 0;
}

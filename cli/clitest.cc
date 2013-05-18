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
                // 3 = TEST + INTERACTIVE MODE

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

  command = "print cli_columns";
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

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  cli->process(command);

  // test drop attribute
  // test quit
  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table tbl_employeeReal EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);
  
  command = "load tbl_employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  cout << "Before dropping attibute salary: " << endl;
  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print columns";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "drop attribute Salary from tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);
  
  cout << "After dropping attibute Salary: " << endl;
  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print cli_columns";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  cout << "Before dropping attibute EmpName: " << endl;
  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "drop attribute EmpName from tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  cout << "After dropping attibute EmpName: " << endl;
  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);
}

void Test04()
{
  string command;
  
  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  cli->process(command);
  
  // test add attribute
  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load tbl_employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  cout << "Before adding attibute major=varhar(40) and year=int: " << endl;
  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print columns";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "add attribute Major=varchar(40) to tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "add attribute Year=int to tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  cout << "After adding attibute major=varhar(40) and year=int: " << endl;
  command = "print cli_columns";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print columns";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);
  

  command = "load tbl_employee employee_Year_1";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);
  
  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "drop attribute Major from tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);
}

void Test05()
{
  string command;
  
  // test "neat output"
  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  cli->process(command);

  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print columns";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load tbl_employee employee_50";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);
}

void Test06()
{
  string command;

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  cli->process(command);

  // test forward pointer
  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print attributes columns";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print columns";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load tbl_employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);  

  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS); 

  command = "load tbl_employee employee_200";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);  

  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS); 
}

// create index
void Test07()
{
  string command;

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  cli->process(command) == SUCCESS;

  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print cli_indexes";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print cli_columns";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index Age on tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index Age a tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "create index Agea on tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);
  
  command = "create index Age on atbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "create index Age on atbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);  
}

void Test08()
{
  string command;

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  cli->process(command);

  command = "create table tbl_employee2 EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index EmpName on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  // check index is create for EmpName on tbl_employe2
  command = "print cli_indexes";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);  

  command = ("drop attribute EmpName from tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("print cli_indexes");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("drop attribute EmpName from tbl_employee2");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("print cli_indexes");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = "create index Age on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index Height on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print cli_indexes";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = ("drop table tbl_employee2");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = "print cli_indexes";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);
}

int main()
{
  system("rm -r \"" DATABASE_FOLDER "\" 2> /dev/null");

  cli = CLI::Instance();

  if (MODE == 0 || MODE == 3) {
    Test01();
    Test02();
    Test03();
    Test04();
    Test05();
    Test06();
    Test07();
  } if (MODE == 1 || MODE == 3) {
    cli->start();
  }
  
  return 0;
}

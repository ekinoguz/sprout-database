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

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);
}

// test drop attribute
// test quit
void Test03()
{
  string command;
  
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

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("drop table tbl_employeeReal");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);
}

void Test04()
{
  string command;
  
  // test add attribute
  command = "create table tbl_employee EmpName=varchar(100), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load tbl_employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  cout << "Before adding attibute major=varhar(100) and year=int: " << endl;
  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print columns";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "add attribute Major=varchar(100) to tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "add attribute Year=int to tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  cout << "After adding attibute major=varhar(100) and year=int: " << endl;
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

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);
}

// test "neat output"
void Test05()
{
  string command;

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

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);
}

void Test06()
{
  string command;

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

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);
}

// create index
// drop index
void Test07()
{
  cout << "*********** CLI Test07 begins ******************" << endl;

  string command;

  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table tbl_employee2 EmpName=varchar(30), Age=int, Height=real, Salary=int";
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

  command = "create index EmpName on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index Age on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index Height on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  // check index is created for EmpName on tbl_employe2
  command = "print cli_indexes";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS); 

  command = "drop index Height on tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "drop index Agea on tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "drop index Agea on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "drop index Age on tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "drop index Age on tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "print cli_indexes";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "drop index Age on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print cli_indexes";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print indexes";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  // cleanup tables
  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("drop table tbl_employee2");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = "print indexes";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);
}

// check effects of dropAttribute and dropTable on indexes
void Test08()
{
  cout << "*********** CLI Test08 begins ******************" << endl;

  string command;

  command = "print cli_indexes";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print indexes";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table tbl_employee2 EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index EmpName on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index Age on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print cli_indexes";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create inqdex Height on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index EmpName on tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index Age on tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

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

  command = ("drop table tbl_employee2");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("print cli_indexes");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("print cli_indexes");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("print indexes");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);
}

void Test09()
{
  cout << "*********** CLI Test09 begins ******************" << endl;

  string command;

  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table tbl_employee2 EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index EmpName on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index Age on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index Height on tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index EmpName on tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("drop table tbl_employee2");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);
}

// check insert tuple
void Test10()
{
  cout << "*********** CLI 10 begins ******************" << endl;

  string command;

  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table tbl_employee2 EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "insert inato tbl_employee tuple(EmpName=ekin, Age=22, Height=6.1, Salary=13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "insert into tbl_employee tauple(EmpName=ekin, Age=22, Height=6.1, Salary=13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "insert inato tbl_employee (EmpName=ekin, Age=22, Height=6.1, Salary=13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "insert tbl_employee tuple(EmpName=ekin, Age=22, Height=6.1, Salary=13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "insert into tbl_employee tuple(EmpName=ekin, Age=22, Height=6.1, Salary=13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "insert into tbl_employee2 tuple(EmpName=sky, Age=22, Height=6.1, Salary=13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "insert into tbl_employee2 tuple(EmpName=cesar, Age=22, Height=6.1, Salary=13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "insert into tbl_employee2 tuple(EmpName=naveen, Age=22, Height=6.1, Salary=13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print tbl_employee2";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

}

// check print index
void Test11()
{
  cout << "*********** CLI 11 begins ******************" << endl;

  string command;

  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load tbl_employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index EmpName on tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print index EmpName on tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "insert into tbl_employee tuple(EmpName=sky, Age=22, Height=6.1, Salary=13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "insert into tbl_employee tuple(EmpName=cesar, Age=22, Height=6.1, Salary=13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "insert into tbl_employee tuple(EmpName=naveen, Age=22, Height=6.1, Salary=13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print index EmpName on tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

}

// Verify that load table adds entries to the index
void Test12()
{
  cout << "*********** CLI 12 begins ******************" << endl;

  string command;

  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index EmpName on tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load tbl_employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

}

// first query test
void Test13()
{
  cout << "*********** CLI 10 begins ******************" << endl;

  string command;

  command = "create table tbl_employee EmpName=varchar(30), Age=int, Height=real, Salary=int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load tbl_employee employee_50";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "select * from tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = ("drop table tbl_employee");
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
    Test08();
    Test09();
    Test10();
    Test11();
    Test12();
  } if (MODE == 1 || MODE == 3) {
    cli->start();
  }
  
  return 0;
}

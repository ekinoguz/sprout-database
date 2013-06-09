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

  command = "create table ekin name = varchar(40), age = int";
  cout << command << endl;
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
  command = "create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
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
  
  command = "create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table tbl_employeeReal EmpName = varchar(30), Age = int, Height = real, Salary = int";
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
  command = "create table tbl_employee EmpName = varchar(100), Age = int, Height = real, Salary = int";
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

  command = "add attribute Major = varchar(100) to tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "add attribute Year = int to tbl_employee";
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

  command = "create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
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
  command = "create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
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

  command = "create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table tbl_employee2 EmpName = varchar(30), Age = int, Height = real, Salary = int";
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

  command = "create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table tbl_employee2 EmpName = varchar(30), Age = int, Height = real, Salary = int";
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

  command = "create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table tbl_employee2 EmpName = varchar(30), Age = int, Height = real, Salary = int";
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

  command = "create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table tbl_employee2 EmpName = varchar(30), Age = int, Height = real, Salary = int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "insert inato tbl_employee tuple(EmpName = ekin, Age = 22, Height = 6.1, Salary = 13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "insert into tbl_employee tauple(EmpName = ekin, Age = 22, Height = 6.1, Salary = 13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "insert inato tbl_employee (EmpName = ekin, Age = 22, Height = 6.1, Salary = 13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "insert tbl_employee tuple(EmpName = ekin, Age = 22, Height = 6.1, Salary = 13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) != SUCCESS);

  command = "insert into tbl_employee tuple(EmpName = ekin, Age = 22, Height = 6.1, Salary = 13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "insert into tbl_employee2 tuple(EmpName = sky, Age = 22, Height = 6.1, Salary = 13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "insert into tbl_employee2 tuple(EmpName = cesar, Age = 22, Height = 6.1, Salary = 13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "insert into tbl_employee2 tuple(EmpName = naveen, Age = 22, Height = 6.1, Salary = 13291)";
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

  command = "create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
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

  command = "insert into tbl_employee tuple(EmpName = sky, Age = 22, Height = 6.1, Salary = 13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "insert into tbl_employee tuple(EmpName = cesar, Age = 22, Height = 6.1, Salary = 13291)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "insert into tbl_employee tuple(EmpName = naveen, Age = 22, Height = 6.1, Salary = 13291)";
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

  command = "create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
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

// Projection Test
void Test13()
{
  cout << "*********** CLI 13 begins ******************" << endl;

  string command;

  command = "create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load tbl_employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT PROJECT tbl_employee GET [ * ]";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT PROJECT (PROJECT tbl_employee GET [ * ]) GET [ EmpName ]";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT PROJECT (PROJECT tbl_employee GET [ EmpName, Age ]) GET [ Age ]";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT PROJECT (PROJECT (PROJECT tbl_employee GET [ * ]) GET [ EmpName, Age ]) GET [ Age ]";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT PROJECT tbl_employee GET [ EmpName, Age ]";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

}

// Filter Test
void Test14()
{
  cout << "*********** CLI 14 begins ******************" << endl;

  string command;

  command = "create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load tbl_employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER tbl_employee WHERE Age = 45";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER tbl_employee WHERE Age < 45";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER tbl_employee WHERE Age > 45";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER tbl_employee WHERE Age <= 45";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER tbl_employee WHERE Age >= 45";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER tbl_employee WHERE Age != 45";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER tbl_employee WHERE Height < 6.3";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER tbl_employee WHERE EmpName < L";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER (FILTER tbl_employee WHERE  Age < 67) WHERE EmpName < L";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER (FILTER tbl_employee WHERE  Age <= 67) WHERE Height >= 6.4";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER (FILTER (FILTER tbl_employee WHERE EmpName > Ap) WHERE  Age <= 67) WHERE Height >= 6.4";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);
}

// Projection + Filter Test
void Test15()
{
  cout << "*********** CLI 15 begins ******************" << endl;

  string command;

  command = "create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load tbl_employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print tbl_employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER tbl_employee WHERE Age != 45";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT PROJECT (FILTER tbl_employee WHERE Age != 45) GET [ Age ]";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT PROJECT (FILTER tbl_employee WHERE Age != 45) GET [ EmpName, Age ]";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT PROJECT (FILTER tbl_employee WHERE Age != 45) GET [ EmpName, Height ]";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT PROJECT (FILTER tbl_employee WHERE Age != 45) GET [ * ]";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER (PROJECT tbl_employee GET [ EmpName, Age ]) WHERE Age != 45";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT FILTER (PROJECT tbl_employee GET [ EmpName, Age ]) WHERE Age >= 45";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT PROJECT (FILTER (PROJECT tbl_employee GET [ EmpName, Age ]) WHERE Age >= 45) GET [ EmpName ]";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = ("drop table tbl_employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);
}


// Nested Loop Join
void Test16()
{
  cout << "*********** CLI 16 begins ******************" << endl;

  string command;

  command = "create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table ages Age = int, Explanation = varchar(50)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table salary Salary = int, Explanation = varchar(50)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load ages ages_90";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load salary salary_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT NLJOIN employee, ages WHERE Age = Age PAGES(10)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT NLJOIN (NLJOIN employee, salary WHERE Salary = Salary PAGES(10)), ages WHERE Age = Age PAGES(10)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT NLJOIN (NLJOIN (NLJOIN employee, employee WHERE EmpName = EmpName PAGES(10)), salary) WHERE Salary = Salary PAGES(10)), ages WHERE Age = Age PAGES(10)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = ("drop table employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("drop table ages");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("drop table salary");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);
}


// Aggregate
void Test20()
{
  cout << "*********** CLI 20 begins ******************" << endl;

  string command;

  command = "create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "print employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT AGG employee GET MAX(Height)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT AGG employee GET MIN(Salary)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT AGG (PROJECT employee GET [ * ]) GET MAX(Salary)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT AGG (PROJECT employee GET [ Salary ]) GET SUM(Salary)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT AGG (PROJECT employee GET [ Salary ]) GET COUNT(Salary)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT AGG (PROJECT employee GET [ Salary ]) GET AVG(Salary)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT AGG (PROJECT employee GET [ * ]) GET COUNT(Height)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = ("drop table employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);
}

// Aggregate with Groupby
void Test21()
{
  cout << "*********** CLI 21 begins ******************" << endl;

  string command;

  command = "create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table ages Age = int, Explanation = varchar(50)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table salary Salary = int, Explanation = varchar(50)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load ages ages_90";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load salary salary_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT AGG ages GROUPBY(Explanation) GET AVG(Age)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT AGG ages GROUPBY(Explanation) GET MIN(Age)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT AGG (PROJECT ages GET [ Age, Explanation ]) GROUPBY(Explanation) GET MIN(Age)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT AGG (FILTER ages WHERE Age > 14) GROUPBY(Explanation) GET MIN(Age)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = ("drop table employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("drop table salary");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("drop table ages");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);
}

// INLJoin
void Test22()
{
  cout << "*********** CLI 22 begins ******************" << endl;

  string command;

  command = "create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table ages Age = int, Explanation = varchar(50)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create table salary Salary = int, Explanation = varchar(50)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load employee employee_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load ages ages_90";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "load salary salary_5";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index Age on employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index Age on ages";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index Salary on employee";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "create index Salary on salary";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);  

  command = "print cli_indexes";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT INLJOIN employee, ages WHERE Age = Age PAGES(10)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT INLJOIN (INLJOIN employee, salary WHERE Salary = Salary PAGES(10)), ages WHERE Age = Age PAGES(10)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT INLJOIN (NLJOIN employee, salary WHERE Salary = Salary PAGES(10)), ages WHERE Age = Age PAGES(10)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT INLJOIN (NLJOIN (FILTER employee WHERE Salary > 150000), salary WHERE Salary = Salary PAGES(10)), ages WHERE Age = Age PAGES(10)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT INLJOIN (NLJOIN employee, salary WHERE Salary < Salary PAGES(10)), salary WHERE Salary = Salary PAGES(10)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT INLJOIN (NLJOIN employee, salary WHERE Salary < Salary PAGES(10)), salary WHERE salary.Salary = Salary PAGES(10)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = "SELECT INLJOIN (NLJOIN employee, salary WHERE Salary < Salary PAGES(10)), salary WHERE employee.Salary = Salary PAGES(10)";
  cout << ">>> " << command << endl;
  assert (cli->process(command) == SUCCESS);

  command = ("drop table employee");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);

  command = ("drop table ages");
  cout << ">>> " << command << endl;  
  assert (cli->process(command) == SUCCESS);
}

int main()
{
  system("rm -r \"" DATABASE_FOLDER "\" 2> /dev/null");

  cli = CLI::Instance();

  if (MODE == 0 || MODE == 3) {
    // Test01();
    // Test02();
    // Test03();
    // Test04();
    // Test05();
    // Test06();
    // Test07();
    // Test08();
    // Test09();
    // Test10();
    // Test11();
    // Test12();
    // Test13(); // Projection
    // Test14(); // Filter
    // Test15(); // Projection + Filter
    // Test16(); // NLJoin
    // // TODO Test17(); // NLJoin + Filter
    // // TODO Test18(); // NLJoin + Projection
    // // TODO Test19(); // NLJoin + Filter + Projection
    // Test20(); // Aggregate
    // Test21(); // Aggregate groupby
    Test22(); // INLJoin
  } if (MODE == 1 || MODE == 3) {
    cli->start();
  }
  
  return 0;
}

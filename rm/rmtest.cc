#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include <fstream>
#include <iostream>
#include <cassert>

#include "rm.h"

using namespace std;

RM *rm;
const int success = 0;

// Function to prepare the data in the correct form to be inserted/read/updated
void prepareTuple(const int name_length, const string name, const int age, const float height, const int salary, void *buffer, int *tuple_size)
{
  int offset = 0;
    
  memcpy((char *)buffer + offset, &name_length, sizeof(int));
  offset += sizeof(int);    
  memcpy((char *)buffer + offset, name.c_str(), name_length);
  offset += name_length;
    
  memcpy((char *)buffer + offset, &age, sizeof(int));
  offset += sizeof(int);
    
  memcpy((char *)buffer + offset, &height, sizeof(float));
  offset += sizeof(float);
    
  memcpy((char *)buffer + offset, &salary, sizeof(int));
  offset += sizeof(int);
    
  *tuple_size = offset;
}


// Function to parse the data in buffer and print each field
void printTuple(const void *buffer, const int tuple_size)
{
  int offset = 0;
  cout << "****Printing Buffer: Start****" << endl;
   
  int name_length = 0;     
  memcpy(&name_length, (char *)buffer+offset, sizeof(int));
  offset += sizeof(int);
  cout << "name_length: " << name_length << endl;
   
  char *name = (char *)malloc(100);
  memcpy(name, (char *)buffer+offset, name_length);
  name[name_length] = '\0';
  offset += name_length;
  cout << "name: " << name << endl;
  free(name);
    
  int age = 0; 
  memcpy(&age, (char *)buffer+offset, sizeof(int));
  offset += sizeof(int);
  cout << "age: " << age << endl;
   
  float height = 0.0; 
  memcpy(&height, (char *)buffer+offset, sizeof(float));
  offset += sizeof(float);
  cout << "height: " << height << endl;
       
  int salary = 0; 
  memcpy(&salary, (char *)buffer+offset, sizeof(int));
  offset += sizeof(int);
  cout << "salary: " << salary << endl;

  cout << "****Printing Buffer: End****" << endl << endl;    
}


// Create an employee table
void createTable(const string tablename)
{
  cout << "****Create Table " << tablename << " ****" << endl;
    
  // 1. Create Table ** -- made separate now.
  vector<Attribute> attrs;

  Attribute attr;
  attr.name = "EmpName";
  attr.type = TypeVarChar;
  attr.length = (AttrLength)100;
  attrs.push_back(attr);

  attr.name = "Age";
  attr.type = TypeInt;
  attr.length = (AttrLength)4;
  attrs.push_back(attr);

  attr.name = "Height";
  attr.type = TypeReal;
  attr.length = (AttrLength)4;
  attrs.push_back(attr);

  attr.name = "Salary";
  attr.type = TypeInt;
  attr.length = (AttrLength)4;
  attrs.push_back(attr);

  RC rc = rm->createTable(tablename, attrs);
  assert(rc == success);
  cout << "****Table Created: " << tablename << " ****" << endl << endl;
}


void secA_0(const string tablename)
{
  // Functions Tested
  // 1. Get Attributes
  cout << "****In Test Case 0****" << endl;

  // GetAttributes
  vector<Attribute> attrs;
  RC rc = rm->getAttributes(tablename, attrs);
  assert(rc == success);

  // TODO: Automatically verify this
  for(unsigned i = 0; i < attrs.size(); i++)
    {
      cout << "Attribute Name: " << attrs[i].name << endl;
      cout << "Attribute Type: " << (AttrType)attrs[i].type << endl;
      cout << "Attribute Length: " << attrs[i].length << endl << endl;
    }
  return;
}

void secA_1(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
  // Functions tested
  // 1. Create Table ** -- made separate now.
  // 2. Insert Tuple **
  // 3. Read Tuple **
  // NOTE: "**" signifies the new functions being tested in this test case. 
  cout << "****In Test Case 1****" << endl;
   
  RID rid; 
  int tuple_size = 0;
  void *tuple = malloc(100);
  void *data_returned = malloc(100);

  // Insert a tuple into a table
  prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
  cout << "Insert Data:" << endl;
  printTuple(tuple, tuple_size);
  RC rc = rm->insertTuple(tablename, tuple, rid);
  assert(rc == success);
  
  // Given the rid, read the tuple from table
  rc = rm->readTuple(tablename, rid, data_returned);
  assert(rc == success);

  cout << "Returned Data:" << endl;
  printTuple(data_returned, tuple_size);

  // Compare whether the two memory blocks are the same
  if(memcmp(tuple, data_returned, tuple_size) == 0)
    {
      cout << "****Test case 1 passed****" << endl << endl;
    }
  else
    {
      cout << "****Test case 1 failed****" << endl << endl;
    }

  free(tuple);
  free(data_returned);
  return;
}


void secA_2(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
  // Functions Tested
  // 1. Insert tuple
  // 2. Delete Tuple **
  // 3. Read Tuple
  cout << "****In Test Case 2****" << endl;
   
  RID rid; 
  int tuple_size = 0;
  void *tuple = malloc(100);
  void *data_returned = malloc(100);

  // Test Insert the Tuple    
  prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
  cout << "Insert Data:" << endl;
  printTuple(tuple, tuple_size);
  RC rc = rm->insertTuple(tablename, tuple, rid);
  assert(rc == success);

  // Test Delete Tuple
  rc = rm->deleteTuple(tablename, rid);
  assert(rc == success);

  // Test Read Tuple
  memset(data_returned, 0, 100);
  rc = rm->readTuple(tablename, rid, data_returned);
  assert(rc != success);

  cout << "After Deletion." << endl;
    
  // Compare the two memory blocks to see whether they are different
  if (memcmp(tuple, data_returned, tuple_size) != 0)
    {   
      cout << "****Test case 2 passed****" << endl << endl;
    }
  else
    {
      cout << "****Test case 2 failed****" << endl << endl;
    }
        
  free(tuple);
  free(data_returned);
  return;
}


void secA_3(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
  // Functions Tested
  // 1. Insert Tuple    
  // 2. Update Tuple **
  // 3. Read Tuple
  cout << "****In Test Case 3****" << endl;
   
  RID rid; 
  int tuple_size = 0;
  int tuple_size_updated = 0;
  void *tuple = malloc(100);
  void *tuple_updated = malloc(100);
  void *data_returned = malloc(100);
   
  // Test Insert Tuple 
  prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
  RC rc = rm->insertTuple(tablename, tuple, rid);
  assert(rc == success);
  cout << "Original RID slot = " << rid.slotNum << endl;

  // Test Update Tuple
  prepareTuple(6, "Newman", age, height, 100, tuple_updated, &tuple_size_updated);
  rc = rm->updateTuple(tablename, tuple_updated, rid);
  assert(rc == success);
  cout << "Updated RID slot = " << rid.slotNum << endl;

  // Test Read Tuple 
  rc = rm->readTuple(tablename, rid, data_returned);
  assert(rc == success);
  cout << "Read RID slot = " << rid.slotNum << endl;
   
  // Print the tuples 
  cout << "Insert Data:" << endl; 
  printTuple(tuple, tuple_size);

  cout << "Updated data:" << endl;
  printTuple(tuple_updated, tuple_size_updated);

  cout << "Returned Data:" << endl;
  printTuple(data_returned, tuple_size_updated);
    
  if (memcmp(tuple_updated, data_returned, tuple_size_updated) == 0)
    {
      cout << "****Test case 3 passed****" << endl << endl;
    }
  else
    {
      cout << "****Test case 3 failed****" << endl << endl;
    }

  free(tuple);
  free(tuple_updated);
  free(data_returned);
  return;
}


void secA_4(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
  // Functions Tested
  // 1. Insert tuple
  // 2. Read Attributes **
  cout << "****In Test Case 4****" << endl;
    
  RID rid;    
  int tuple_size = 0;
  void *tuple = malloc(100);
  void *data_returned = malloc(100);
    
  // Test Insert Tuple 
  prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
  RC rc = rm->insertTuple(tablename, tuple, rid);
  assert(rc == success);

  // Test Read Attribute
  rc = rm->readAttribute(tablename, rid, "Salary", data_returned);
  assert(rc == success);
 
  cout << "Salary: " << *(int *)data_returned << endl;
  if (memcmp((char *)data_returned, (char *)tuple+18, 4) != 0)
    {
      cout << "****Test case 4 failed" << endl << endl;
    }
  else
    {
      cout << "****Test case 4 passed" << endl << endl;
    }
    
  free(tuple);
  free(data_returned);
  return;
}


void secA_5(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
  // Functions Tested
  // 0. Insert tuple;
  // 1. Read Tuple
  // 2. Delete Tuples **
  // 3. Read Tuple
  cout << "****In Test Case 5****" << endl;
    
  RID rid;
  int tuple_size = 0;
  void *tuple = malloc(100);
  void *data_returned = malloc(100);
  void *data_returned1 = malloc(100);
   
  // Test Insert Tuple 
  prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
  RC rc = rm->insertTuple(tablename, tuple, rid);
  assert(rc == success);

  // Test Read Tuple
  rc = rm->readTuple(tablename, rid, data_returned);
  assert(rc == success);
  printTuple(data_returned, tuple_size);

  cout << "Now Deleting..." << endl;

  // Test Delete Tuples
  rc = rm->deleteTuples(tablename);
  assert(rc == success);
    
  // Test Read Tuple
  memset((char*)data_returned1, 0, 100);
  rc = rm->readTuple(tablename, rid, data_returned1);
  assert(rc != success);
  printTuple(data_returned1, tuple_size);
    
  if(memcmp(tuple, data_returned1, tuple_size) != 0)
    {
      cout << "****Test case 5 passed****" << endl << endl;
    }
  else
    {
      cout << "****Test case 5 failed****" << endl << endl;
    }
       
  free(tuple);
  free(data_returned);
  free(data_returned1);
  return;
}

void secA_6(const string tablename)
{
  // Functions Tested
  // 1. Simple scan **
  cout << "****In Test Case 6****" << endl;

  RID rid;    
  int tuple_size = 0;
  int num_records = 5;
  void *tuple;
  void *data_returned = malloc(100);

  RID rids[num_records];
  vector<char *> tuples;

  RC rc = 0;
  for(int i = 0; i < num_records; i++)
    {
      tuple = malloc(100);

      // Insert Tuple
      float height = (float)i;
      prepareTuple(6, "Tester", 20+i, height, 123, tuple, &tuple_size);
      rc = rm->insertTuple(tablename, tuple, rid);
      assert(rc == success);

      tuples.push_back((char *)tuple);
      rids[i] = rid;
    }
  cout << "After Insertion!" << endl;

  // Set up the iterator
  RM_ScanIterator rmsi;
  string attr = "Age";
  vector<string> attributes;
  attributes.push_back(attr); 
  rc = rm->scan(tablename, "", NO_OP, NULL, attributes, rmsi);
  assert(rc == success); 

  cout << "Scanned Data:" << endl;
    
  while(rmsi.getNextTuple(rid, data_returned) == 0)
    {
      cout << "Age: " << *(int *)data_returned << endl;
    }
  rmsi.close();
    
  // Deleta Table
  rc = rm->deleteTable(tablename);
  assert(rc == success);

  struct stat stFileInfo;
  string fileName = DATABASE_FOLDER"/" + tablename;
  assert( stat(fileName.c_str(), &stFileInfo) != 0 );

  free(data_returned);
  for(int i = 0; i < num_records; i++)
    {
      free(tuples[i]);
    }
    
  return;
}

void secA_7(const string tablename)
{
  // Functions tested
  // 1. Create Table ** -- made separate now.
  // 2. Insert Tuple **
  // 3. Read Tuple **
  // NOTE: "**" signifies the new functions being tested in this test case. 
  cout << "****In Test Case 7****" << endl;

  string name = "Cesar";
  int name_length = name.size();
  int age = 20;
  int height = 200;
  float salary = 200.0;
   
  RID rid1; 
  int tuple_size = 0;
  void *tuple1 = malloc(100);
  void *data_returned = malloc(100);

  // Insert a tuple into a table
  prepareTuple(name_length, name, age, height, salary, tuple1, &tuple_size);
  cout << "Insert Data:" << endl;
  printTuple(tuple1, tuple_size);
  RC rc = rm->insertTuple(tablename, tuple1, rid1);
  assert(rc == success);

  
  name = "Sky";
  name_length = name.size();
  age = 30;
  height = 300;
  salary = 300.0;

  tuple_size = 0;
  void *tuple2 = malloc(100);
  RID rid2;

  prepareTuple(name_length, name, age, height, salary, tuple2, &tuple_size);
  cout << "Insert Data:" << endl;
  printTuple(tuple2, tuple_size);
  rc = rm->insertTuple(tablename, tuple2, rid2);
  assert(rc == success);



  name = "Ekin";
  name_length = name.size();
  age = 40;
  height = 400;
  salary = 400.0;

  tuple_size = 0;
  void *tuple3 = malloc(100);
  RID rid3;

  prepareTuple(name_length, name, age, height, salary, tuple3, &tuple_size);
  cout << "Insert Data:" << endl;
  printTuple(tuple3, tuple_size);
  rc = rm->insertTuple(tablename, tuple3, rid3);
  assert(rc == success);
  
  // Test Delete Tuple
  rc = rm->deleteTuple(tablename, rid2);
  assert(rc == success);

  // Reorganize page
  rc = rm->reorganizePage(tablename, rid2.pageNum);
  assert(rc == success);



  name = "Mike";
  name_length = name.size();
  age = 50;
  height = 500;
  salary = 500.0;

  tuple_size = 0;
  void *tuple4 = malloc(100);
  RID rid4;

  prepareTuple(name_length, name, age, height, salary, tuple4, &tuple_size);
  cout << "Insert Data:" << endl;
  printTuple(tuple4, tuple_size);
  rc = rm->insertTuple(tablename, tuple4, rid4);
  assert(rc == success);


  // Given the rid, read the tuple from table
  rc = rm->readTuple(tablename, rid3, data_returned);
  assert(rc == success);

  cout << "Returned Data:" << endl;
  printTuple(data_returned, tuple_size);

  // Compare whether the two memory blocks are the same
  if(memcmp(tuple3, data_returned, tuple_size) == 0)
    {
      cout << "****Test case 7 passed****" << endl << endl;
    }
  else
    {
      cout << "****Test case 7 failed****" << endl << endl;
    }

  free(tuple1);
  free(tuple2);
  free(tuple3);
  free(tuple4);
  free(data_returned);
  return;
}

void secA_8()
{
  string tablename = "droptest";
  createTable(tablename);

  string name = "ekin";
  int age = 5;
  int height = 36;
  int salary = 8;
  int name_length = name.size();


  // Functions Tested
  // 1. Insert tuple
  // 2. Read Attributes **
  cout << "****In Test Case 8****" << endl;
    
  RID rid;    
  int tuple_size = 0;
  void *tuple = malloc(100);
  void *data_returned = malloc(100);
    
  // Test Insert Tuple 
  prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
  RC rc = rm->insertTuple(tablename, tuple, rid);
  assert(rc == success);

  // Test Read Attribute
  rc = rm->readAttribute(tablename, rid, "Salary", data_returned);
  assert(rc == success);

  rc = rm->dropAttribute(tablename, "Salary");
  assert(rc == success);
  
  rc = rm->readAttribute(tablename, rid, "Salary", data_returned);
  assert(rc != success);

  memset(data_returned, 0,100);

  rc = rm->readAttribute(tablename, rid, "Age", data_returned);
  assert(rc == success);

  assert( memcmp((char *)data_returned, (char *)&age, 4) == 0 );
  
  rc = rm->readTuple(tablename, rid, data_returned);
  assert(rc == success);
  
  // This will still print salary but it should be 0
  printTuple(data_returned, tuple_size);  

  // Add attribute back in
  Attribute attr;
  attr.name = "Salary";
  attr.type = TypeInt;
  attr.length = 4;
    
  rc = rm->addAttribute(tablename, attr);
  assert(rc == success);

  rc = rm->readAttribute(tablename, rid, "Salary", data_returned);
  assert(rc == success);

  // TODO: Add another attribute and make sure it is initialized to 0
  
  assert(*(int *)data_returned == 8);
  
  printTuple(tuple, tuple_size);
  if (memcmp((char *)data_returned, (char *)tuple+4+4+4+name_length, 4) != 0)
    {
      cout << "****Test case 8 failed" << endl << endl;
    }
  else
    {
      cout << "****Test case 8 passed" << endl << endl;
    }
    
  free(tuple);
  free(data_returned);
  return;
}


void secA_9()
{
  string tablename = "forward";
  createTable(tablename);

  string name = "ekin";
  int age = 5;
  int height = 36;
  int salary = 8;

  // Functions Tested
  // 1. Insert tuple
  // 2. Read Attributes **
  cout << "****In Test Case 9****" << endl;

  RID rid;    
  int tuple_size = 0;
  void *tuple = malloc(100);
  void *data_returned = malloc(100);

  for (int i = 0; i < 500; i++)
    {
      std::stringstream sstm;
      sstm << i;
      string full_name = name + sstm.str();
      prepareTuple(full_name.size(), full_name, age + i, height + i, salary + i, tuple, &tuple_size);
      RC rc = rm->insertTuple(tablename, tuple, rid);
      assert(rc == success);
    }

  RID rid_to_update;
  rid_to_update.pageNum = 3;
  rid_to_update.slotNum = 10;

  // Given the rid, read the tuple from table
  RC rc = rm->readTuple(tablename, rid_to_update, data_returned);
  assert(rc == success);

  printTuple(data_returned, tuple_size);

  void *tuple_updated = malloc(100);
  int tuple_size_updated;
  name = "Testingtestingtestingtestingtestingtestingtesting";
  prepareTuple(name.size(), name, age, height, 100, tuple_updated, &tuple_size_updated);
  rc = rm->updateTuple(tablename, tuple_updated, rid);
  assert(rc == success);
  cout << "Updated PID:S# = " << rid.pageNum << ":" << rid.slotNum << endl;
  
  memset(data_returned, 0, 100);

  // Given the rid, read the tuple from table
  rc = rm->readTuple(tablename, rid_to_update, data_returned);
  assert(rc == success);

  printTuple(data_returned, tuple_size);


  memset(data_returned, 0, 100);

  // Given the rid, read the tuple from table
  rc = rm->readTuple(tablename, rid, data_returned);
  assert(rc == success);

  printTuple(data_returned, tuple_size);

}


void Tests()
{
  // GetAttributes
  //  secA_0("tbl_employee");

  // Insert/Read Tuple
  // secA_1("tbl_employee", 6, "Peters", 24, 170.1, 5000);

  // Delete Tuple
  // secA_2("tbl_employee", 6, "Victor", 22, 180.2, 6000);

  // Update Tuple
  // secA_3("tbl_employee", 6, "Thomas", 28, 187.3, 4000);

  // TODO: need to test update forward pointers

  //  Read Attributes
  // secA_4("tbl_employee", 6, "Veekay", 27, 171.4, 9000);

  // Delete Tuples
  // secA_5("tbl_employee", 6, "Dillon", 29, 172.5, 7000);
  // secA_1("tbl_employee", 6, "Peters", 24, 170.1, 5000); // Make sure delete tuples doesn't kill the db

  // Simple Scan
  // createTable("tbl_employee3");
  // secA_6("tbl_employee3");

  // Reorganize page
  // createTable("tbl_employee4");
  // secA_7("tbl_employee4");
  
  // Test drop and add attributes
  // secA_8();

  // Test forward pointer
  secA_9();
    
  return;
}

RC directoryExists(string name)
{
  struct stat sb;
  return (stat(name.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode));
}

int main()
{
  system("rm -r " DATABASE_FOLDER " 2> /dev/null");
  rm = RM::Instance();
  // Basic Functions
  cout << endl << "Test Basic Functions..." << endl;
    
  // The DB should be created when rm is initialized
  assert( directoryExists(DATABASE_FOLDER) );
    
  // Create Table
  createTable("tbl_employee");

  Tests();

  //  system("rm -r "DATABASE_FOLDER" 2> /dev/null");
  return 0;
}

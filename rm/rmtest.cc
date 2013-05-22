#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include <fstream>
#include <iostream>
#include <cassert>
#include <sys/time.h>
#include <sys/resource.h>
#include "rm.h"

using namespace std;

RM *rm;
const int success = 0;


void memProfile()
{
    int who = RUSAGE_SELF;
    struct rusage usage;
    getrusage(who,&usage);
    cout<<usage.ru_maxrss<<"KB"<<endl;
}
// Function to get the data in the correct form to be inserted/read after adding
// the attribute ssn
void prepareTupleAfterAdd(const int name_length, const string name, const int age, const float height, const int salary, const int ssn, void *buffer, int *tuple_size)
{
    int offset=0;
    
    memcpy((char*)buffer + offset, &(name_length), sizeof(int));
    offset += sizeof(int);    
    memcpy((char*)buffer + offset, name.c_str(), name_length);
    offset += name_length;
    
    memcpy((char*)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);
        
    memcpy((char*)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);
        
    memcpy((char*)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);
    
    memcpy((char*)buffer + offset, &ssn, sizeof(int));
    offset += sizeof(int);

    *tuple_size = offset;
}


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
    free(name);
}


// Function to get the data in the correct form to be inserted/read after adding
// the attribute ssn

void printTupleAfterDrop( const void *buffer, const int tuple_size)
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
       
    cout << "****Printing Buffer: End****" << endl << endl;    
}   


void printTupleAfterAdd(const void *buffer, const int tuple_size)
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
   
    float height = 0; 
    memcpy(&height, (char *)buffer+offset, sizeof(float));
    offset += sizeof(float);
    cout << "height: " << height << endl;
	
	int salary = 0; 
    memcpy(&salary, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "salary: " << salary << endl;
    
    int ssn = 0;   
    memcpy(&ssn, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "SSN: " << ssn << endl;

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


void prepareLargeTuple(const int index, void *buffer, int *size)
{
    int offset = 0;
    
    // compute the count
    int count = index % 50 + 1;

    // compute the letter
    char text = index % 26 + 97;

    for(int i = 0; i < 10; i++)
    {
        memcpy((char *)buffer + offset, &count, sizeof(int));
        offset += sizeof(int);

        for(int j = 0; j < count; j++)
        {
            memcpy((char *)buffer + offset, &text, 1);
            offset += 1;
        }
   
        // compute the integer 
        memcpy((char *)buffer + offset, &index, sizeof(int));
        offset += sizeof(int);
   
        // compute the floating number
        float real = (float)(index + 1); 
        memcpy((char *)buffer + offset, &real, sizeof(float));
        offset += sizeof(float);
    }
    *size = offset; 
}


// Create a large table for pressure test
void createLargeTable(const string tablename)
{
    cout << "****Create Large Table " << tablename << " ****" << endl;
    
    // 1. Create Table ** -- made separate now.
    vector<Attribute> attrs;

    int index = 0;
    char *suffix = (char *)malloc(10);
    for(int i = 0; i < 10; i++)
    {
        Attribute attr;
        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeVarChar;
        attr.length = (AttrLength)50;
        attrs.push_back(attr);
        index++;

        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeInt;
        attr.length = (AttrLength)4;
        attrs.push_back(attr);
        index++;

        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeReal;
        attr.length = (AttrLength)4;
        attrs.push_back(attr);
        index++;
    }

    RC rc = rm->createTable(tablename, attrs);
    assert(rc == success);
    cout << "****Large Table Created: " << tablename << " ****" << endl << endl;

    free(suffix);
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

    for(unsigned i = 0; i < attrs.size(); i++)
    {
        cout << "Attribute Name: " << attrs[i].name << endl;
        cout << "Attribute Type: " << (AttrType)attrs[i].type << endl;
        cout << "Attribute Length: " << attrs[i].length << endl << endl;
    }
    cout<<"** Test Case 0 passed"<<endl;
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
    cout<< "delete data done"<<endl;
    
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


void secA_6(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 0. Insert tuple;
    // 1. Read Tuple
    // 2. Delete Table **
    // 3. Read Tuple
    cout << "****In Test Case 6****" << endl;
   
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

    // Test Delete Table
    rc = rm->deleteTable(tablename);
    assert(rc == success);
    cout << "After deletion!" << endl;
    
    // Test Read Tuple 
    memset((char*)data_returned1, 0, 100);
    rc = rm->readTuple(tablename, rid, data_returned1);
    assert(rc != success);
    
    if(memcmp(data_returned, data_returned1, tuple_size) != 0)
    {
        cout << "****Test case 6 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 6 failed****" << endl << endl;
    }
        
    free(tuple);
    free(data_returned);    
    free(data_returned1);
    return;
}


void secA_7(const string tablename)
{
    // Functions Tested
    // 1. Reorganize Page **
    // Insert records into one page, reorganize that page, 
    // and use the same tids to read data. The results should 
    // be the same as before. Will check code as well.
    cout << "****In Test Case 7****" << endl;
   
    RID rid; 
    int tuple_size = 0;
    int num_records = 5;
    void *tuple;
    void *data_returned = malloc(100);

    int sizes[num_records];
    RID rids[num_records];
    vector<char *> tuples;

    RC rc = 0;
    for(int i = 0; i < num_records; i++)
    {
        tuple = malloc(100);

        // Test Insert Tuple
        float height = (float)i;
        prepareTuple(6, "Tester", 20+i, height, 123, tuple, &tuple_size);
        rc = rm->insertTuple(tablename, tuple, rid);
        assert(rc == success);

        tuples.push_back((char *)tuple);
        sizes[i] = tuple_size;
        rids[i] = rid;
        cout << rid.pageNum << endl;
    }
    cout << "After Insertion!" << endl;
    
    int pageid = 0; // Depends on which page the records are
    rc = rm->reorganizePage(tablename, pageid);
    assert(rc == success);

    // Print out the tuples one by one
    int i = 0;
    for (i = 0; i < num_records; i++)
    {
        rc = rm->readTuple(tablename, rids[i], data_returned);
        assert(rc == success);
        printTuple(data_returned, tuple_size);

        //if any of the tuples are not the same as what we entered them to be ... there is a problem with the reorganization.
        if (memcmp(tuples[i], data_returned, sizes[i]) != 0)
        {      
            cout << "****Test case 7 failed****" << endl << endl;
            break;
        }
    }
    if(i == num_records)
    {
        cout << "****Test case 7 passed****" << endl << endl;
    }
    
    // Delete Table    
    rc = rm->deleteTable(tablename);
    assert(rc == success);

    free(data_returned);
    for(i = 0; i < num_records; i++)
    {
        free(tuples[i]);
    }
    return;
}


void secA_8(const string tablename)
{
    // Functions Tested
    // 1. Simple scan **
    cout << "****In Test Case 8****" << endl;

    RID rid;    
    int tuple_size = 0;
    int num_records = 5;
    void *tuple;
    void *data_returned = malloc(100);

    //    int sizes[num_records];
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
	//    sizes[i] = tuple_size;
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
    
    while(rmsi.getNextTuple(rid, data_returned) != RM_EOF)
    {
        cout << "Age: " << *(int *)data_returned << endl;
    }
    rmsi.close();
    
    // Deleta Table
    rc = rm->deleteTable(tablename);
    assert(rc == success);

    free(data_returned);
    for(int i = 0; i < num_records; i++)
    {
        free(tuples[i]);
    }
     cout << "****Test case 8 passed****" << endl << endl; 
    return;
}


void secA_9(const string tablename, vector<RID> &rids, vector<int> &sizes)
{
    // Functions Tested:
    // 1. create table
    // 2. getAttributes
    // 3. insert tuple
    cout << "****In Test case 9****" << endl;

    RID rid; 
    void *tuple = malloc(1000);
    int num_records = 2000;

    // GetAttributes
    vector<Attribute> attrs;
    RC rc = rm->getAttributes(tablename, attrs);
    assert(rc == success);

    for(unsigned i = 0; i < attrs.size(); i++)
    {
        cout << "Attribute Name: " << attrs[i].name << endl;
        cout << "Attribute Type: " << (AttrType)attrs[i].type << endl;
        cout << "Attribute Length: " << attrs[i].length << endl << endl;
    }

    // Insert 2000 tuples into table
    for(int i = 0; i < num_records; i++)
    {
        // Test insert Tuple
        int size = 0;
        memset(tuple, 0, 1000);
        prepareLargeTuple(i, tuple, &size);

        rc = rm->insertTuple(tablename, tuple, rid);
        assert(rc == success);

        rids.push_back(rid);
        sizes.push_back(size);        
    }
    cout << "****Test case 9 passed****" << endl << endl;

    free(tuple);
}


void secA_10(const string tablename, const vector<RID> &rids, const vector<int> &sizes)
{
    // Functions Tested:
    // 1. read tuple
    cout << "****In Test case 10****" << endl;

    int num_records = 2000;
    void *tuple = malloc(1000);
    void *data_returned = malloc(1000);

    RC rc = 0;
    for(int i = 0; i < num_records; i++)
    {
        memset(tuple, 0, 1000);
        memset(data_returned, 0, 1000);
        rc = rm->readTuple(tablename, rids[i], data_returned);
        assert(rc == success);

        int size = 0;
        prepareLargeTuple(i, tuple, &size);
        if(memcmp(data_returned, tuple, sizes[i]) != 0)
        {
            cout << "****Test case 10 failed****" << endl << endl;
            return;
        }
    }
    cout << "****Test case 10 passed****" << endl << endl;

    free(tuple);
    free(data_returned);
}


void secA_11(const string tablename, vector<RID> &rids, vector<int> &sizes)
{
    // Functions Tested:
    // 1. update tuple
    // 2. read tuple
    cout << "****In Test case 11****" << endl;

    RC rc = 0;
    void *tuple = malloc(1000);
    void *data_returned = malloc(1000);

    // Update the first 1000 records
    int size = 0;
    for(int i = 0; i < 1000; i++)
    {
        memset(tuple, 0, 1000);
        RID rid = rids[i];

        prepareLargeTuple(i+10, tuple, &size);
        rc = rm->updateTuple(tablename, tuple, rid);
        assert(rc == success);

        sizes[i] = size;
        rids[i] = rid;
    }
    cout << "Updated!" << endl;

    // Read the recrods out and check integrity
    for(int i = 0; i < 1000; i++)
    {
        memset(tuple, 0, 1000);
        memset(data_returned, 0, 1000);
        prepareLargeTuple(i+10, tuple, &size);
        rc = rm->readTuple(tablename, rids[i], data_returned);
        assert(rc == success);

        if(memcmp(data_returned, tuple, sizes[i]) != 0)
        {
            cout << "****Test case 11 failed****" << endl << endl;
            return;
        }
    }
    cout << "****Test case 11 passed****" << endl << endl;

    free(tuple);
    free(data_returned);
}


void secA_12(const string tablename, const vector<RID> &rids)
{
    // Functions Tested
    // 1. delete tuple
    // 2. read tuple
    cout << "****In Test case 12****" << endl;

    RC rc = 0;
    void * data_returned = malloc(1000);

    // Delete the first 1000 records
    for(int i = 0; i < 1000; i++)
    {
        rc = rm->deleteTuple(tablename, rids[i]);
        assert(rc == success);

        rc = rm->readTuple(tablename, rids[i], data_returned);
        assert(rc != success);
    }
    cout << "After deletion!" << endl;

    for(int i = 1000; i < 2000; i++)
    {
        rc = rm->readTuple(tablename, rids[i], data_returned);
        assert(rc == success);
    }
    cout << "****Test case 12 passed****" << endl << endl;

    free(data_returned);
}


void secA_13(const string tablename)
{
    // Functions Tested
    // 1. scan
    cout << "****In Test case 13****" << endl;

    RM_ScanIterator rmsi;
    vector<string> attrs;
    attrs.push_back("attr5");
    attrs.push_back("attr12");
    attrs.push_back("attr28");
   
    RC rc = rm->scan(tablename, "", NO_OP, NULL, attrs, rmsi); 
    assert(rc == success);

    RID rid;
    int j = 0;
    void *data_returned = malloc(1000);

    while(rmsi.getNextTuple(rid, data_returned) != RM_EOF)
    {
        if(j % 200 == 0)
        {
            int offset = 0;

            cout << "Real Value: " << *(float *)(data_returned) << endl;
            offset += 4;
        
            int size = *(int *)((char *)data_returned + offset);
            cout << "String size: " << size << endl;
            offset += 4;

            char *buffer = (char *)malloc(size + 1);
            memcpy(buffer, (char *)data_returned + offset, size);
            buffer[size] = 0;
            offset += size;
    
            cout << "Char Value: " << buffer << endl;

            cout << "Integer Value: " << *(int *)((char *)data_returned + offset ) << endl << endl;
            offset += 4;

            free(buffer);
        }
        j++;
        memset(data_returned, 0, 1000);
    }
    rmsi.close();
    cout << "Total number of records: " << j << endl << endl;

    cout << "****Test case 13 passed****" << endl << endl;
    free(data_returned);
}


void secA_14(const string tablename, const vector<RID> &rids)
{
    // Functions Tested
    // 1. reorganize page
    // 2. delete tuples
    // 3. delete table
    cout << "****In Test case 14****" << endl;

    RC rc;
    rc = rm->reorganizePage(tablename, rids[1000].pageNum);
    assert(rc == success);

    rc = rm->deleteTuples(tablename);
    assert(rc == success);

    rc = rm->deleteTable(tablename);
    assert(rc == success);

    cout << "****Test case 14 passed****" << endl << endl;
}


void secA_15(const string tablename) {

    cout << "****In Test case 15****" << endl;
    
    RID rid;    
    int tuple_size = 0;
    int num_records = 500;
    void *tuple;
    void *data_returned = malloc(100);
    int age_ret_val = 25;

    RID rids[num_records];
    vector<char *> tuples;

    RC rc = 0;
    int age;
    for(int i = 0; i < num_records; i++)
    {
        tuple = malloc(100);

        // Insert Tuple
        float height = (float)i;
        
        age = (rand()%20) + 15;
        
        prepareTuple(6, "Tester", age, height, 123, tuple, &tuple_size);
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
    rc = rm->scan(tablename, attr, GT_OP, &age_ret_val, attributes, rmsi);
    assert(rc == success); 

    cout << "Scanned Data:" << endl;
    
    while(rmsi.getNextTuple(rid, data_returned) != RM_EOF)
    {
        cout << "Age: " << *(int *)data_returned << endl;
        assert ( (*(int *) data_returned) > age_ret_val );
    }
    rmsi.close();
    
    // Deleta Table
    rc = rm->deleteTable(tablename);
    assert(rc == success);

    free(data_returned);
    for(int i = 0; i < num_records; i++)
    {
        free(tuples[i]);
    }
    
    cout << "****Test case 15 passed****" << endl << endl;
}

void secO_1(const string tablename)
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

void secO_2()
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


void secO_3()
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
  void *tuple = malloc(255);
  void *data_returned = malloc(255);
  memset(tuple, 0, 255);

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
  rid_to_update.pageNum = 2;
  rid_to_update.slotNum = 3;

  // Given the rid, read the tuple from table
  RC rc = rm->readTuple(tablename, rid_to_update, data_returned);
  assert(rc == success);

  printTuple(data_returned, tuple_size); // This could fail since tuple_size is not the same for all records
  
  void *tuple_updated = malloc(255);
  memset(tuple_updated, 0, 255);

  int tuple_size_updated;
  name = "Testingtestingtestingtestingtestingtestingtesting";
  prepareTuple(name.size(), name, age, height, 100, tuple_updated, &tuple_size_updated);
  //rc = rm->deleteTuple(tablename,  rid_to_update);
  rc = rm->updateTuple(tablename, tuple_updated, rid_to_update);
  assert(rc == success);

  assert(rid_to_update.pageNum == 2);
  assert(rid_to_update.slotNum == 3);
  
  memset(data_returned, 0, 255);

  // Given the rid, read the tuple from table
  rc = rm->readTuple(tablename, rid_to_update, data_returned);
  assert(rc == success);

  //printTuple(data_returned, tuple_size);
  //printTuple(tuple_updated, tuple_size);

  assert(memcmp(data_returned, tuple_updated, tuple_size) == 0);
  
  name = "h";
  prepareTuple(name.size(), name, age, height, salary, tuple, &tuple_size);

  name = "sky";
  for (int i = 0; i < 500; i++)
    {
      std::stringstream sstm;
      sstm << i;
      string full_name = name + sstm.str();
      prepareTuple(full_name.size(), full_name, age + i, height + i, salary + i, tuple, &tuple_size);
      RC rc = rm->insertTuple(tablename, tuple, rid);
      assert(rc == success);
    }
  
  
  name = "short string";
  prepareTuple(name.size(), name, age, height, 100, tuple_updated, &tuple_size_updated);

  //rc = rm->deleteTuple(tablename, rid_to_update);
  rc = rm->updateTuple(tablename, tuple_updated, rid_to_update);
  assert(rc == success);

  assert(rid_to_update.pageNum == 2);
  assert(rid_to_update.slotNum == 3);
  
  memset(data_returned, 0, 255);

  // Given the rid, read the tuple from table
  rc = rm->readTuple(tablename, rid_to_update, data_returned);
  assert(rc == success);

  printTuple(data_returned, tuple_size);
  assert(memcmp(data_returned, tuple_updated, tuple_size) == 0);


  rc = rm->deleteTuple(tablename, rid_to_update);
  assert(rc == success);
  
  rc = rm->readTuple(tablename, rid_to_update, data_returned);
  assert(rc != success);


  vector<string> attributeNames;
  attributeNames.push_back("EmpName");
  RM_ScanIterator rm_ScanIterator;
  assert (rm->scan(tablename, "", NO_OP, NULL, attributeNames, rm_ScanIterator) == 0);
   
  int i = 0;
  int expected_tuples = 999; // Fix this
  void * data = malloc(PF_PAGE_SIZE);
  while (rm_ScanIterator.getNextTuple(rid, data) != RM_EOF) {
    i++;
  }
  cout << "I" << i << endl;
  assert(i == expected_tuples);

  free(data);
  free(tuple);
  free(tuple_updated);
  free(data_returned);

  cout << "Test 9 Passed" << endl;
}

// Advanced Features:
void secB_1(const string tablename, const int name_length, const string name, const int age, const int height, const int salary)
{
    // Functions Tested
    // 1. Insert tuple
    // 2. Read Attributes
    // 3. Drop Attributes **
    cout << "****In Extra Credit Test Case 1****" << endl;

    RID rid;    
    int tuple_size = 0;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);
   
    // Insert Tuple 
    prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
    int rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);

    // Read Attribute
    rc = rm->readAttribute(tablename, rid, "Salary", data_returned);
    assert(rc == success);
    cout << "Salary: " << *(int *)data_returned << endl;
 
    if(memcmp((char *)data_returned, (char *)tuple+18, 4) != 0)
    {
        cout << "Read attribute failed!" << endl; 
    }
    else
    {
        //cout << "Read attribute passed!" << endl; 

        // Drop the attribute
        rc = rm->dropAttribute(tablename, "Salary");
        assert(rc == success);

        // Read Tuple and print the tuple
        rc = rm->readTuple(tablename, rid, data_returned);
        assert(rc == success);
        printTupleAfterDrop(data_returned, tuple_size);
    }
    
    free(tuple);
    free(data_returned);

    cout<<"****Extra Credit Test Case 1 passed****"<<endl;
    return;
}


void secB_2(const string tablename, const int name_length, const string name, const int age, const int height, const int salary, const int ssn)
{
    // Functions Tested
    // 1. Add Attribute **
    // 2. Insert Tuple
    cout << "****In Extra Credit Test Case 2****" << endl;
   
    RID rid; 
    int tuple_size=0;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);
   
    // Test Add Attribute
    Attribute attr;
    attr.name = "SSN";
    attr.type = TypeInt;
    attr.length = 4;
    int rc = rm->addAttribute(tablename, attr);
    assert(rc == success);

    // Test Insert Tuple
    prepareTupleAfterAdd(name_length, name, age, height, salary, ssn, tuple, &tuple_size);
    cout << "First" << tuple_size << endl;
    rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);
    
    // Test Read Tuple
    rc = rm->readTuple(tablename, rid, data_returned);
    assert(rc == success);

    cout << tablename << endl;

    cout << "Insert Data:" << endl;
    printTupleAfterAdd(tuple, tuple_size);

    cout << "Returned Data:" << endl;
    printTupleAfterAdd(data_returned, tuple_size);

    // cout << memcmp(data_returned, tuple, tuple_size - 4) << endl;
    // cout << memcmp(data_returned + tuple_size - 2, tuple + tuple_size -2, 2) << endl;
    // cout << (int)*(char *)(data_returned + tuple_size - 3) << endl;
    // cout << (int)*(char *)(tuple + tuple_size - 3) << endl;
    // cout << tuple_size << endl;

    if (memcmp(data_returned, tuple, tuple_size) != 0)
    {
        cout << "****Extra Credit Test Case 2 failed****" << endl << endl; 
    }
    else
    {
        cout << "****Extra Credit Test Case 2 passed****" << endl << endl; 
    }

    free(tuple);
    free(data_returned);
    return;   
}


// void secB_3(const string tablename)
// {
//     // Functions Tested
//     // 1. Reorganize Table **
//     cout << "****In Extra Credit Test Case 3****" << endl;
    
//     int tuple_size = 0;
//     void *tuple = malloc(100);
//     void *data_returned = malloc(100);
   
//     RID rid; 
//     int num_records = 200;
//     RID rids[num_records];
   
//     int rc = 0; 
//     for(int i=0; i < num_records; i++)
//     {
//         // Insert Tuple
//         prepareTuple(6, "Tester", 100+i, i, 123, tuple, &tuple_size);
//         rc = rm->insertTuple(tablename, tuple, rid);
//         assert(rc == success);

//         rids[i] = rid;
//     }
	
// 	// Delete the first 100 records
//     for(int i = 0; i < 100; i++)
//     {
//         rc = rm->deleteTuple(tablename, rids[i]);
//         assert(rc == success);

//         rc = rm->readTuple(tablename, rids[i], data_returned);
//         assert(rc != success);
//     }
//     cout << "After deletion!" << endl;

//     // Reorganize Table
//     rc = rm->reorganizeTable(tablename);
//     assert(rc == success);

//     for(int i = 0; i < 100; i++)
//     {
//         // Read Tuple
//         rc = rm->readTuple(tablename, rids[i], data_returned);
//         assert(rc == success);

//         // Print the tuple
//         printTuple(data_returned, tuple_size);
//     }

//     // Delete the table
//     rc = rm->deleteTable(tablename);
//     assert(rc == success);

//     free(tuple);
//     free(data_returned);

//     cout<<"****Extra Credit Test Case 3  passed*****"<<endl;
//     return;
// }   

void Tests()
{
    // GetAttributes
    secA_0("tbl_employee");

    // Insert/Read Tuple
    secA_1("tbl_employee", 6, "Peters", 24, 170.1, 5000);

    // Delete Tuple
    secA_2("tbl_employee", 6, "Victor", 22, 180.2, 6000);

    // Update Tuple
    secA_3("tbl_employee", 6, "Thomas", 28, 187.3, 4000);

    // Read Attributes
    secA_4("tbl_employee", 6, "Veekay", 27, 171.4, 9000);

    // Delete Tuples
    secA_5("tbl_employee", 6, "Dillon", 29, 172.5, 7000);

    // Delete Table
    secA_6("tbl_employee", 6, "Martin", 26, 173.6, 8000);
   
    memProfile();
 
    // Reorganize Page
    createTable("tbl_employee2");
    secA_7("tbl_employee2");

    // Simple Scan
    createTable("tbl_employee3");
    secA_8("tbl_employee3");

    memProfile();
	
    // Pressure Test
    createLargeTable("tbl_employee4");

    vector<RID> rids;
    vector<int> sizes;

    // Insert Tuple
    secA_9("tbl_employee4", rids, sizes);
    // Read Tuple
    secA_10("tbl_employee4", rids, sizes);

    // Update Tuple
    secA_11("tbl_employee4", rids, sizes);

    // Delete Tuple
    secA_12("tbl_employee4", rids);

    memProfile();

    // Scan
    secA_13("tbl_employee4");

    // DeleteTuples/Table
    secA_14("tbl_employee4", rids);
    
    // Scan with conditions
    createTable("tbl_b_employee4");  
    secA_15("tbl_b_employee4");

    // Reorganize page
    createTable("tbl_employee5");
    secO_1("tbl_employee5");
  
    // Test drop and add attributes
    secO_2();

    // Test forward pointer
    secO_3();
 
    string name1 = "Peters";
    string name2 = "Victors";

    // Extra Credits
    cout << "Test Extra Credits...." << endl;

    // Drop Attribute
    createTable("tbl_employee");
    secB_1("tbl_employee", 6, name1, 24, 170, 5000);

    // Add Attributes
    createTable("tbl_employee2");
    secB_2("tbl_employee2", 6, name2, 22, 180, 6000, 999);

    // Reorganize Table
    createTable("tbl_employee3");  
    //secB_3("tbl_employee3");

    memProfile();
    return;
}

int main()
{
  system("rm -r " DATABASE_FOLDER " 2> /dev/null");
  rm = RM::Instance();
  // Basic Functions
  cout << endl << "Test Basic Functions..." << endl;

  // Create Table
  createTable("tbl_employee");

  Tests();

  return 0;
}


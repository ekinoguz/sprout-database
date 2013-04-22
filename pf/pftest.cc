#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h> 
#include <string.h>
#include <stdexcept>
#include <stdio.h> 

#include "pf.h"

using namespace std;

const int success = 0;
const int TEST_MODE = 0;    // 0 = Test PF
                            // 1 = Test Cache
                            // 2 = Test Both

// Check if a file exists
bool FileExists(string fileName)
{
    struct stat stFileInfo;

    if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
    else return false;
}


int PFTest_1(PF_Manager *pf)
{
    // Functions Tested:
    // 1. CreateFile
    cout << "****In PF Test Case 1****" << endl;

    RC rc;
    string fileName = "test";

    // Create a file named "test"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl << endl;
        return 0;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        return -1;
    }

    // Create "test" again, should fail
    rc = pf->CreateFile(fileName.c_str());
    assert(rc != success);

    return 0;
}


int PFTest_2(PF_Manager *pf)
{
    // Functions Tested:
    // 1. DestroyFile
    cout << "****In PF Test Case 2****" << endl;

    RC rc;
    string fileName = "test";

    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);

    if(!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        return -1;
    }
}


int PFTest_3(PF_Manager *pf)
{
    // Functions Tested:
    // 1. CreateFile
    // 2. OpenFile
    // 3. GetNumberOfPages
    // 4. CloseFile
    cout << "****In PF Test Case 3****" << endl;

    RC rc;
    string fileName = "test_1";

    // Create a file named "test_1"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
    }

    // Open the file "test_1"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Get the number of pages in the test file
    unsigned count = fileHandle.GetNumberOfPages();
    assert(count == (unsigned)0);

    // Close the file "test_1"
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    cout << "Test Case 3 Passed!" << endl << endl;

    return 0;
}



int PFTest_4(PF_Manager *pf)
{
    // Functions Tested:
    // 1. OpenFile
    // 2. AppendPage
    // 3. GetNumberOfPages
    // 3. CloseFile
    cout << "****In PF Test Case 4****" << endl;

    RC rc;
    string fileName = "test_1";

    // Open the file "test_1"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Append the first page
    void *data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 94 + 32;
    }
    rc = fileHandle.AppendPage(data);
    assert(rc == success);
   
    // Get the number of pages
    unsigned count = fileHandle.GetNumberOfPages();
    //assert(count == (unsigned)1);

    // Close the file "test_1"
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    free(data);

    cout << "Test Case 4 Passed!" << endl << endl;

    return 0;
}


int PFTest_5(PF_Manager *pf)
{
    // Functions Tested:
    // 1. OpenFile
    // 2. ReadPage
    // 3. CloseFile
    cout << "****In PF Test Case 5****" << endl;

    RC rc;
    string fileName = "test_1";

    // Why do these unit tests interact with eachother??
    // Open the file "test_1"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Read the first page
    void *buffer = malloc(PF_PAGE_SIZE);
    cout << "Before read" << endl;
    rc = fileHandle.ReadPage(0, buffer);
    cout<< "After read" << endl;
    assert(rc == success);
  
    
    // Check the integrity of the page    
    void *data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 94 + 32;
    }
    cout << "Did i make it" << endl;
    rc = memcmp(data, buffer, PF_PAGE_SIZE);
    assert(rc == success);
 
    // Close the file "test_1"
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    free(data);
    free(buffer);

    cout << "Test Case 5 Passed!" << endl << endl;

    return 0;
}


int PFTest_6(PF_Manager *pf)
{
    // Functions Tested:
    // 1. OpenFile
    // 2. WritePage
    // 3. ReadPage
    // 3. CloseFile
    cout << "****In PF Test Case 6****" << endl;

    RC rc;
    string fileName = "test_1";

    // Open the file "test_1"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Update the first page
    void *data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 10 + 32;
    }
    rc = fileHandle.WritePage(0, data);
    assert(rc == success);

    // Read the page
    void *buffer = malloc(PF_PAGE_SIZE);
    rc = fileHandle.ReadPage(0, buffer);
    assert(rc == success);

    // Check the integrity
    rc = memcmp(data, buffer, PF_PAGE_SIZE);
    assert(rc == success);
 
    // Close the file "test_1"
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    free(data);
    free(buffer);

    // DestroyFile
    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);
    
    if(!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl;
        cout << "Test Case 6 Passed!" << endl << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        return -1;
    }
}


int PFTest_7(PF_Manager *pf)
{
    // Functions Tested:
    // 1. CreateFile
    // 2. OpenFile
    // 3. AppendPage
    // 4. GetNumberOfPages
    // 5. ReadPage
    // 6. WritePage
    // 7. CloseFile
    // 8. DestroyFile
    cout << "****In PF Test Case 7****" << endl;

    RC rc;
    string fileName = "test_2";

    // Create the file named "test_2"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
    }

    // Open the file "test_2"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Append 50 pages
    void *data = malloc(PF_PAGE_SIZE);
    for(unsigned j = 0; j < 50; j++)
    {
        for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
        {
            *((char *)data+i) = i % (j+1) + 32;
        }
        rc = fileHandle.AppendPage(data);
        assert(rc == success);
    }
    cout << "50 Pages have been successfully appended!" << endl;
   
    // Get the number of pages
    unsigned count = fileHandle.GetNumberOfPages();
    cout << count << endl;
    assert(count == (unsigned)50);

    // Read the 25th page and check integrity
    void *buffer = malloc(PF_PAGE_SIZE);
    rc = fileHandle.ReadPage(24, buffer);
    assert(rc == success);

    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data + i) = i % 25 + 32;
    }
    rc = memcmp(buffer, data, PF_PAGE_SIZE);
    assert(rc == success);
    cout << "The data in 25th page is correct!" << endl;

    // Update the 25th page
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 60 + 32;
    }
    rc = fileHandle.WritePage(24, data);
    assert(rc == success);

    // Read the 25th page and check integrity
    rc = fileHandle.ReadPage(24, buffer);
    assert(rc == success);
    
    rc = memcmp(buffer, data, PF_PAGE_SIZE);
    assert(rc == success);

    // Close the file "test_2"
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    // DestroyFile
    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);

    free(data);
    free(buffer);

    if(!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl;
        cout << "Test Case 7 Passed!" << endl << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        return -1;
    }
}

int ekin_PFTest_2b(PF_Manager *pf)
{
    // Functions Tested:
    // 1. OpenFile
    // 2. AppendPage
    // 3. GetNumberOfPages
    // 4. WritePage
    // 5. ReadPage
    // 6. CloseFile
    // 7. DestroyFile
    cout << "****In PF Test Case 2b****" << endl;

    RC rc;
    string fileName = "test";

    // Open the file "test"
    PF_FileHandle fileHandle;
    pf->CreateFile(fileName.c_str());
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Append the first page
    // Write ASCII characters from 32 to 125 (inclusive)
    void *data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 94 + 32;
    }
    rc = fileHandle.AppendPage(data);
    assert(rc == success);

    // Get the number of pages
    unsigned count = fileHandle.GetNumberOfPages();
    assert(count == (unsigned)1);

    // Update the first page
    // Write ASCII characters from 32 to 41 (inclusive)
    data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 10 + 32;
    }
    rc = fileHandle.WritePage(0, data);
    assert(rc == success);

    // Read the page
    void *buffer = malloc(PF_PAGE_SIZE);
    rc = fileHandle.ReadPage(0, buffer);
    assert(rc == success);

    // Check the integrity
    rc = memcmp(data, buffer, PF_PAGE_SIZE);
    assert(rc == success);

    // Close the file "test"
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    free(data);
    free(buffer);

    cout << "Test Case 2b Passed!" << endl << endl;

    return 0;
}

int ekin_PFTest_3(PF_Manager *pf)
{
    cout << "****In PF Test Case 3****" << endl;

    RC rc;
    string fileName = "test3";

    // Destroy File: file should exist
    rc = pf->DestroyFile((fileName+"a").c_str());
    assert (rc != success);

    if (FileExists(fileName.c_str()))
        rc = pf->DestroyFile(fileName.c_str());
    pf->CreateFile(fileName.c_str());

    // Open the file
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Append the first page
    // Write ASCII characters from 32 to 125 (inclusive)
    void *data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 94 + 32;
    }
    rc = fileHandle.AppendPage(data);
    for (int i = 0; i < 10; i++)
    {
        *((int *)data) = i * i;
        rc = fileHandle.AppendPage(data);
    }

    // Close File: close unopened fileHandle
    PF_FileHandle testFileHandle;
    rc = pf->CloseFile(testFileHandle);
    assert (rc != success);

    // ReadPage: if page does not exist
    rc = fileHandle.ReadPage(-1, data);
    assert (rc != success);

    rc = fileHandle.ReadPage(521, data);
    assert (rc != success);

    rc = fileHandle.ReadPage(3, data);
    assert (*((int *) data) == 4);
    assert(rc == success);

    // Get the number of pages
    unsigned count = fileHandle.GetNumberOfPages();
    assert(count == (unsigned)11);

    rc = fileHandle.WritePage(count+1, data);
    assert (rc != success);
    rc = fileHandle.WritePage(count, data);
    assert (rc == success);

    for (int k = 0; k < 5; k++)
    {
        unsigned j = k * k;
        for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
        {
            j = (j+1) % 21;
            *((char *)data+i) = i % 94 + j;
        }
        rc = fileHandle.AppendPage(data);
    }

    // Get the number of pages
    count = fileHandle.GetNumberOfPages();
    assert(count == (unsigned)17);

    rc = fileHandle.ReadPage(11, data);
    assert (*((int *) data) == 4);

    cout << "****PF Test Case 3 Finishes****" << endl;
    return 0;
}

int ekin_PFTest_4(PF_Manager *pf)
{
    cout << "****In PF Test Case 4****" << endl;

    RC rc;
    string fileName = "test4";


    PF_FileHandle fileHandle;
    if (FileExists(fileName.c_str()))
        rc = pf->DestroyFile(fileName.c_str());
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    // Open the file
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Open file tests
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert (rc != success);
    PF_FileHandle fileHandle2;
    PF_FileHandle fileHandle3;
    rc = pf->OpenFile(fileName.c_str(), fileHandle2);
    assert (rc == success);
    rc = pf->OpenFile(fileName.c_str(), fileHandle3);
    assert (rc == success);

    void *data = malloc(PF_PAGE_SIZE);
    for (int k = 0; k < 5; k++)
    {
        unsigned j = k * k;
        for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
        {
            j = (j+1) % 21;
            *((char *)data+i) = i % 94 + j;
        }
        rc = fileHandle.AppendPage(data);
        assert (rc == success);
        *((char *)data+20) = 20 % 94 + j;
        rc = fileHandle2.AppendPage(data);
        assert (rc == success);
        *((char *)data+20) = 20 % 94 + j;
        rc = fileHandle3.AppendPage(data);
        assert (rc == success);
    }

    for (int k = 2; k >= 0; k--)
    {
        unsigned j = k * k * k;
        for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
        {
            j = (j+1) * 2;
            *((char *)data+i) = i % 94 + j;
        }
        rc = fileHandle.WritePage(k, data);
    }

    rc = fileHandle.ReadPage(5, data);
    assert (((string)((char *)data)).length() == 376);

    rc = fileHandle.ReadPage(14, data);
    assert (rc == success);
    rc = fileHandle.ReadPage(fileHandle.GetNumberOfPages(), data);
    assert (rc != success);

    // destroy all the files
    rc = pf->DestroyFile("test");
    assert(rc == success);
    rc = pf->DestroyFile("test3");
    assert(rc == success);
    rc = pf->DestroyFile("test4");
    assert(rc == success);
    cout << "****PF Test Case 4 Finishes****" << endl;
    return 0;
}

int CacheTest01(PF_Manager *pf)
{
    string fileName = "test_1";
    PF_FileHandle fileHandle;
    pf->CreateFile(fileName);
    RC rc = pf->OpenFile(fileName.c_str(), fileHandle);
    // Append 50 pages
    void *data = malloc(PF_PAGE_SIZE);
    for(unsigned j = 0; j < 1; j++)
    {
        for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
        {
            *((char *)data+i) = i % (j+1) + 32;
        }
        rc = fileHandle.AppendPage(data);
        assert(rc == success);
    }
    cout << fileHandle.GetNumberOfPages() << endl;
    pf->CloseFile(fileHandle);
}

int main()
{
    PF_Manager *pf = PF_Manager::Instance(10);
    if (TEST_MODE == 0 || TEST_MODE == 2) {
        remove("test");
        remove("test_1");
        remove("test_2");
        
        PFTest_1(pf);
        PFTest_2(pf); 
        PFTest_3(pf);
        PFTest_4(pf);
        PFTest_5(pf); 
        PFTest_6(pf);
        PFTest_7(pf);
        
        ekin_PFTest_2b(pf);
        ekin_PFTest_3(pf);
        ekin_PFTest_4(pf);

    } 
    if( TEST_MODE == 1 || TEST_MODE == 2) {
        
        cout << "********************" << endl;
        cout << "BEGINNING CACHE TEST" << endl;
        remove("test_1");
        CacheTest01(pf);

        cout << "ENDING CACHE TEST" << endl;
        cout << "********************" << endl;
    }

    return 0;
}

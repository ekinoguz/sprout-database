#include <iostream>
#include <string>
#include <cassert>
#include <stdio.h>
#include <cstring>
#include <stdlib.h>
#include <sys/stat.h>

#include "pf.h"

using namespace std;

const int success = 0;


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
    // 1. OpenFile
    // 2. AppendPage
    // 3. GetNumberOfPages
    // 4. WritePage
    // 5. ReadPage
    // 6. CloseFile
    // 7. DestroyFile
    cout << "****In PF Test Case 2****" << endl;

    RC rc;
    string fileName = "test";

    // Open the file "test"
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

    // DestroyFile
    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);

    if(!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl;
        cout << "Test Case 2 Passed!" << endl << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        return -1;
    }
}

int PFTest_2b(PF_Manager *pf)
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

int PFTest_3(PF_Manager *pf)
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

int PFTest_4(PF_Manager *pf)
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
    cout << "****PF Test Case 4 Finishes****" << endl;
	return 0;
}

int main()
{
    PF_Manager *pf = PF_Manager::Instance();
    remove("test");
    PFTest_1(pf);
    PFTest_2(pf);
    PFTest_2b(pf);
    PFTest_3(pf);
    PFTest_4(pf);
    return 0;
}

#include "pf.h"

PF_Manager* PF_Manager::_pf_manager = 0;

// Access to the _pf_manager instance
PF_Manager* PF_Manager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PF_Manager();

    return _pf_manager;
}


PF_Manager::PF_Manager()
{

}


PF_Manager::~PF_Manager()
{
}

// Check if a file exists
bool PF_Manager::FileExists(string fileName)
{
	struct stat stFileInfo;
	if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
	else return false;
}

// This method creates a paged file called fileName.
// The file should not already exist.
RC PF_Manager::CreateFile(const char *fileName)
{
	// if fileName already exists, print and return error
	if (FileExists(fileName))
	{
		cout << fileName << " already exists, CreateFile() in pf.cc" << endl;
		return -1;
	}

	// create paged file called fileName
	fstream filestr;
	filestr.open(fileName, ios::out | ios::binary);
	if (filestr.is_open())
	{
		cout << fileName << " successfully created" << endl;
		filestr.close();
		return 0;
	}
    return -1;
}

// This method destroys the paged file whose name is fileName.
// The file should exist.
RC PF_Manager::DestroyFile(const char *fileName)
{
	if (remove(fileName) == 0)
	{
		cout << fileName << " successfully destroyed" << endl;
		return 0;
	}
	// if fileName does not exist, print and return error
	cout << fileName << " error deleting file, DestroyFile()" << endl;
    return -1;
}

// This method opens the paged file whose name is fileName. The file must
// already exist and it must have been created using the CreateFile method.
// If the method is successful, the fileHandle object whose address is passed
// c++as a parameter becomes a "handle" for the open file.
RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
	if (FileExists(fileName) == false)
	{
		cout << fileName << " does not exist, call CreateFile() first" << endl;
		return -1;
	}
	else if (fileHandle.filestr.is_open())
	{
		cout << "fileHandle is already a handle for an another open file" << endl;
		return -1;
	}
	else
	{
		fileHandle.filestr.open(fileName, ios::in | ios::out | ios::binary);
		return 0;
	}
}

// This method closes the open file instance referred to by fileHandle.
// The file must have been opened using the OpenFile method. All of the
// file's pages are flushed to disk when the file is closed.
RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
	if (fileHandle.filestr.is_open())
	{
		fileHandle.filestr.flush();
		fileHandle.filestr.close();
		return 0;
	}
	else
	{
		cout << "fileHandle does not have open file instance to close" << endl;
		return -1;
	}

}


PF_FileHandle::PF_FileHandle()
{
}
 

PF_FileHandle::~PF_FileHandle()
{
	//delete filestr;
}

// This method reads the page into the memory block pointed by data.
// The page should exist. Note the page number starts from 0.
RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
	if (pageNum > GetNumberOfPages())
	{
		cout << pageNum << " page number does not exist, ReadPage()" << endl;
		return -1;
	}
	if (filestr)
	{
		filestr.seekg((int)(pageNum * PF_PAGE_SIZE));
		filestr.read((char *)data, PF_PAGE_SIZE);
		if (!filestr)
		{
			cout << "error in reading" << endl;
			return -1;
		}
		return 0;
	}
    return -1;
}

// This method writes the data into a page specified by the pageNum.
// The page should exist. Note the page number starts from 0.
RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
	if (pageNum > GetNumberOfPages())
	{
		cout << pageNum << " page number does not exist, WritePage()" << endl;
		return -1;
	}
	if (filestr)
	{
		filestr.seekp((int)(pageNum * PF_PAGE_SIZE));
		filestr.write((char *)data, PF_PAGE_SIZE);
		if (!filestr)
		{
			cout << "error in writing" << endl;
			return -1;
		}
		return 0;
	}
	return -1;
}

// This method appends a new page to the file,
// and writes the data into the new allocated page.
RC PF_FileHandle::AppendPage(const void *data)
{
	if (filestr)
	{
		filestr.seekp((int)(GetNumberOfPages() * PF_PAGE_SIZE));
		filestr.write((char *)data, PF_PAGE_SIZE);
		return 0;
	}
	return -1;
}

// This method returns the total number of pages in the file.
unsigned PF_FileHandle::GetNumberOfPages()
{
	if (filestr)
	{
		filestr.seekg(0, filestr.end);
		int length = filestr.tellg();
		return length / PF_PAGE_SIZE;
	}
	else
		return 0;
}



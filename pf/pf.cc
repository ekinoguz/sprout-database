/*
 * ekinoguz
 *
 * Errors:
 *
 * -1: filestr errors
 *  2: file already exists
 * -2: file does not exist
 */

#include "pf.h"

PF_Manager* PF_Manager::_pf_manager = 0;


// Access to the _pf_manager instance
PF_Manager* PF_Manager::Instance(int cacheNumPages)
{
  if(!_pf_manager)
    _pf_manager = new PF_Manager(cacheNumPages);
  
  return _pf_manager;
}

PF_Manager::PF_Manager(int cacheNumPages)
{
  cache = Cache::Instance(cacheNumPages);
}

PF_Manager::~PF_Manager()
{
}

// Check if a file exists
bool PF_Manager::FileExists(string fileName)
{
  struct stat stFileInfo;
  return (stat(fileName.c_str(), &stFileInfo) == 0);
}

RC PF_Manager::CreateDirectory(string directory)
{
  mkdir(directory.c_str(), 0777);
  return 0;
}

// This method creates a paged file called fileName.
// The file should not already exist.
RC PF_Manager::CreateFile(const char *fileName)
{
  // if fileName already exists, print and return error
  if (FileExists(fileName))
      return 2;
  
  // create paged file called fileName
  fstream filestr;
  filestr.open(fileName, ios::out | ios::binary);
  if (filestr.is_open())
    {
      filestr.close();
      return 0;
    }
  return -1;
}

// This method destroys the paged file whose name is fileName.
// The file should exist.
RC PF_Manager::DestroyFile(const char *fileName)
{
  // if fileName does not exist
  if (FileExists(fileName) == false)
      return -2;
  if (remove(fileName) == 0)
      return 0;
  
  return -1;
}

// This method opens the paged file whose name is fileName. The file must
// already exist and it must have been created using the CreateFile method.
// If the method is successful, the fileHandle object whose address is passed
// c++as a parameter becomes a "handle" for the open file.
RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
  if (FileExists(fileName) == false)
      return -2;
  else if (fileHandle.filestr.is_open())
      return -1;
  else
    {
      fileHandle.filestr.open(fileName, ios::in | ios::out | ios::binary);
      if (fileHandle.filestr.is_open() == false)
	{
	  return -1;
	}
      fileHandle.fileName.assign(fileName);
      fileHandle.cache = cache;
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

RC PF_Manager::CreateFile(const string name)
{
  return PF_Manager::CreateFile(name.c_str());
}
RC PF_Manager::DestroyFile(const string name)
{
  return PF_Manager::DestroyFile(name.c_str());
}
RC PF_Manager::OpenFile(const string name)
{
  return PF_Manager::OpenFile(name.c_str());
}
RC PF_Manager::CloseFile(const string name)
{
  return PF_Manager::CloseFile(name.c_str());
}


PF_FileHandle::PF_FileHandle()
{
}

PF_FileHandle::~PF_FileHandle()
{
}

RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
  return cache->ReadPage(this, pageNum, data);
}

RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
  return cache->WritePage(this, pageNum, data);
}

RC PF_FileHandle::AppendPage(const void *data)
{
  return cache->AppendPage(this, data);
}


// This method reads the page into the memory block pointed by data.
// The page should exist. Note the page number starts from 0.
RC PF_FileHandle::ReadPageFromDisk(PageNum pageNum, void *data)
{
  // if page number does not exist
  if (pageNum >= GetNumberOfPages())
    {
      cout << "page number does not exist" << endl;
      return -1;
    }
  if (filestr)
    {
      filestr.seekg((int)(pageNum * PF_PAGE_SIZE));
      filestr.read((char *)data, PF_PAGE_SIZE);
      if (filestr)
	{
	  return 0;
	}
    }
  cout << "filestr error" << endl;
  return -1;
}

// This method writes the data into a page specified by the pageNum.
// The page should exist. Note the page number starts from 0.
RC PF_FileHandle::WritePageToDisk(PageNum pageNum, const void *data)
{
  // if page number does not exist
  if (pageNum > GetNumberOfPages())
    {
      cout << "page number does not exist" << endl;
      return -1;
    }
  if (filestr)
    {
      filestr.seekp((int)(pageNum * PF_PAGE_SIZE));
      filestr.write((char *)data, PF_PAGE_SIZE);
      filestr.flush();
      if (filestr)
	{
	  return 0;
	}
    }
  cout << "filestr error" << endl;
  return -1;
}

// This method appends a new page to the file,
// and writes the data into the new allocated page.
RC PF_FileHandle::AppendPageToDisk(const void *data)
{
  if (filestr)
    {
      filestr.seekp((int)(GetNumberOfPages() * PF_PAGE_SIZE));
      filestr.write((char *)data, PF_PAGE_SIZE);
      if (filestr)
	{
	  return 0;
	}
    }
  cout << "filestr error" << endl;
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
  cout << "filestr error" << endl;
  return -1;
}

#ifndef _pf_h_
#define _pf_h_

#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include "cache.h"

#include "../shared.h"

using namespace std;

/*
 * In this project, you will implement a paged file system. The PF component
 * provides facilities for higher-level client components to perform file I/O
 * in terms of pages. In the PF component, methods are provided to create,
 * destroy, open, and close paged files, to read and write a specific page of
 * a given file, and to add pages to a given file.
 */

// A return code of 0 indicates normal completion. A nonzero return code
// indicates that an exception condition or error has occurred.
typedef int RC;

typedef unsigned PageNum;

#define PF_PAGE_SIZE 4096

class PF_FileHandle;
class Cache;

class PF_Manager
{
 public:
  static PF_Manager* Instance(int cacheNumPages);                     // Access to the _pf_manager instance

  RC CreateFile    (const string);
  RC DestroyFile   (const string);
  RC OpenFile      (const string);
  RC CloseFile     (const string);

  RC CreateFile    (const char *fileName);                            // Create a new file
  RC DestroyFile   (const char *fileName);                            // Destroy a file
  RC OpenFile      (const char *fileName, PF_FileHandle &fileHandle); // Open a file
  RC CloseFile     (PF_FileHandle &fileHandle);                       // Close a file 
  
  RC CreateDirectory(string directory);
  bool	FileExists(string filename);
  
 protected:    
  PF_Manager(int cacheNumPages);                                            // Constructor
  ~PF_Manager();	                                                    // Destructor
  
 private:
  static PF_Manager *_pf_manager;
  Cache *cache;
};


class PF_FileHandle
{
 public:
  PF_FileHandle();                                                    // Default constructor
  ~PF_FileHandle();                                                   // Destructor
  
  RC ReadPage(PageNum pageNum, void *data);                           // Get a specific page
  RC WritePage(PageNum pageNum, const void *data);                    // Write a specific page
  RC AppendPage(const void *data);                                    // Append a specific page
  unsigned GetNumberOfPages();                                        // Get the number of pages in the file

  RC ReadPageFromDisk(PageNum pageNum, void *data);                           // Get a specific page
  RC WritePageToDisk(PageNum pageNum, const void *data);                    // Write a specific page
  RC AppendPageToDisk(const void *data);                                    // Append a specific page
  
  fstream filestr;
  string fileName;
  Cache *cache;
};

#endif

#ifndef _cache_h_
#define _cache_h_

#include <stdint.h>
#include <unordered_map>
#include <string.h>
#include <sstream>
#include <math.h>
#include "pf.h"

//#define CHECK_BIT(variable, position) (variable & (1 << position)) >> position

class PF_FileHandle;

typedef struct FrameInfo
{
  PF_FileHandle* fileHandle;
  unsigned pageNum;
} FrameInfo;

typedef struct FileInfo
{
  unsigned numberOfUsers;
  unsigned numberOfPages;
} FileInfo;

class Cache
{
 public:
  static Cache* Instance(int numCachePages);
  bool Exists(std::string FileName, unsigned pageNum);
  int ReadPage(PF_FileHandle *fileHandle, unsigned pageNum, void *data);
  int WritePage(PF_FileHandle *fileHandle, unsigned pageNum, const void *data);
  int AppendPage(PF_FileHandle *fileHandle, const void *data);
  int getNumCachePages();
  // Page numbers are per file, while frame numbers are for the cache
  uint8_t* getData(unsigned frameNum);
  bool isDirty(unsigned frameNum);
  void SetDirty(unsigned frameNum);
  void UnsetDirty(unsigned frameNum);
  int ClosingFile(PF_FileHandle *fileHandle);
  void AddFileInfo(PF_FileHandle* fileHandle);
  void DeleteFileInfo(PF_FileHandle* fileHandle);
  void DeleteAllFileInfo();
  unsigned GetNumberOfPages(PF_FileHandle* fileHandle);
  unsigned EvictAllPagesToFiles();

 private:
  int GetFrameWithLowestUsage();
  static bool freed;

 protected:
  Cache();
  ~Cache();

 private:
  void init(int numCachePages);
  static Cache _cache;
  bool initialized;
  int numCachePages;
  uint8_t *buffer;
  FrameInfo *framesInfo;
  unsigned short *pinnedFrames;
  bool *dirtyFlag;
  std::unordered_map<std::string, int> existingPages;
  std::unordered_map<std::string, FileInfo*> filesInfo;
  int *frameUsage;  
};

#endif

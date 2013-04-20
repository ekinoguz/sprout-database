#ifndef _cache_h_
#define _cache_h_

#include <stdint.h>
#include <malloc.h>
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

class Cache
{
 public:
  static Cache* Instance(int numCachePages);
  bool Exists(std::string FileName, unsigned pageNum);
  int ReadPage(PF_FileHandle *fileHandle, unsigned pageNum, void *data);
  int WritePage(PF_FileHandle *fileHandle, unsigned pageNum, const void *data);
  int AppendPage(PF_FileHandle *fileHandle, const void *data);

 private:
  int GetFrameWithLowestUsage();

 protected:
  Cache(int numCachePages);
  ~Cache();

 private:
  static Cache* _cache;
  int numCachePages;
  uint8_t *buffer;
  FrameInfo *framesInfo;
  unsigned short* pinnedFrames;
  bool *dirtyFlag;
  std::unordered_map<std::string, int> existingPages;
  int *frameUsage;  
};

#endif

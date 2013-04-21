#include "cache.h"

Cache* Cache::_cache = 0;

Cache* Cache::Instance(int numCachePages)
{
  if (!_cache)
  	_cache = new Cache(numCachePages);

  return _cache;
}

Cache::Cache(int cacheNumPages)
{
  this->numCachePages = cacheNumPages;

  buffer = ((uint8_t*)(malloc(PF_PAGE_SIZE * cacheNumPages)));
  memset(buffer, 0, PF_PAGE_SIZE * cacheNumPages);

  framesInfo = ((FrameInfo*)(malloc(sizeof(FrameInfo) * cacheNumPages)));
  memset(framesInfo, 0, sizeof(FrameInfo) * cacheNumPages);

  pinnedFrames = ((unsigned short*)(malloc(sizeof(short) * cacheNumPages)));
  memset(pinnedFrames, 0, sizeof(short) * cacheNumPages);

  dirtyFlag = ((bool*)(malloc(sizeof(bool) * cacheNumPages)));
  memset(dirtyFlag, 0, sizeof(bool) * cacheNumPages);

  frameUsage = ((int*)(malloc(sizeof(int) * cacheNumPages)));
  memset(frameUsage, 0, sizeof(int) * cacheNumPages);
}

Cache::~Cache()
{
  free(buffer);
  free(pinnedFrames);
  free(dirtyFlag);
}

bool Cache::Exists(std::string FileName, unsigned pageNum)
{
  ostringstream convert;
  convert << pageNum;
  if (existingPages.count(FileName + convert.str()) == 0)
    {
      return false;
    }
  else
    {
      return true;
    }
}

int Cache::ReadPage(PF_FileHandle *fileHandle, unsigned pageNum, void *data)
{
  ostringstream convert;
  convert << pageNum;
  std::unordered_map<std::string, int>::const_iterator element = existingPages.find(fileHandle->fileName + convert.str());
  if (element == existingPages.end())
    {
      // Read the page from disk because it is not in the cache
      RC result = fileHandle->ReadPageFromDisk(pageNum, data);
      if (result == 0)
	     {
    	  // Locate a page to flush
    	  int frameToFlush = GetFrameWithLowestUsage();
    	  // If frame is dirty write it to disk
    	  if (*(dirtyFlag + frameToFlush) == true)
    	    {
    	      (framesInfo + frameToFlush)->fileHandle->WritePageToDisk(pageNum, buffer + (PF_PAGE_SIZE * frameToFlush));
    	      *(dirtyFlag + frameToFlush) = false;
    	    }

    	   // Read the data from disk
    	   fileHandle->ReadPageFromDisk(pageNum, data);
	  
          // Add the data to the cache and set the forward mapping
      	  memcpy(buffer + (PF_PAGE_SIZE * frameToFlush), data, PF_PAGE_SIZE);
      	  (framesInfo + frameToFlush)->fileHandle = fileHandle;
      	  (framesInfo + frameToFlush)->pageNum = pageNum;
      	  // Set usage to 1
      	  *(frameUsage + frameToFlush) = 1;

      	  return 0;
	     }
      else
	     {
	       return result;
	     }
    }
  else
    {
      int frameNum = element->second;
      memcpy(data, buffer + (PF_PAGE_SIZE * frameNum), PF_PAGE_SIZE);
      *(frameUsage + frameNum) = *(frameUsage + frameNum) + 1;
      return 0;
    }
}

int Cache::WritePage(PF_FileHandle *fileHandle, unsigned pageNum, const void *data)
{
  ostringstream convert;
  convert << pageNum;
  std::unordered_map<std::string, int>::const_iterator element = existingPages.find(fileHandle->fileName + convert.str());
  int frameNum;

  if (element == existingPages.end())
    {
      // Locate a page to flush
      int frameToFlush = GetFrameWithLowestUsage();

      // If frame is dirty write it to disk
      if (*(dirtyFlag + frameToFlush) == true)
	{
	  (framesInfo + frameToFlush)->fileHandle->WritePageToDisk(pageNum, buffer + (PF_PAGE_SIZE * frameToFlush));
	  *(dirtyFlag + frameToFlush) = false;
	}
      
      // Add the data to the cache and set the forward mapping
      memcpy(buffer + (PF_PAGE_SIZE * frameToFlush), data, PF_PAGE_SIZE);
      (framesInfo + frameToFlush)->fileHandle = fileHandle;
      (framesInfo + frameToFlush)->pageNum = pageNum;
      // Set usage to 1
      *(frameUsage + frameToFlush) = 1;
      // page will be dirty since it is new
      *(dirtyFlag + pageNum) = true;

      return 0;
    }
  else
    {
      frameNum = element->second;
    }

  memcpy(buffer + (PF_PAGE_SIZE * frameNum), data, PF_PAGE_SIZE);
  *(frameUsage + frameNum) = *(frameUsage + frameNum) + 1;
  *(dirtyFlag + frameNum) = true;
  return 0;
}

int Cache::AppendPage(PF_FileHandle *fileHandle, const void *data)
{
  int pageNum = fileHandle->GetNumberOfPages();
  return WritePage(fileHandle, pageNum, data);
}

int Cache::GetFrameWithLowestUsage()
{
  int minUsage = -1;
  int minUsageIndex = -1;
  
  for (int i = 0; i < numCachePages; i++)
    {
      if ((*(pinnedFrames + i) == 0) && (*(frameUsage + i) < minUsage))
	{
	  minUsage = *(frameUsage + i);
	  minUsageIndex = 0;
	}
    }

  //TODO: Cesar please look over this
  // To me it looks like the for loop will always return 0, but that doesn't sound right to me.
  // I do however, believe 0 shoudl be returned in this conext, do any of the other data structures need to be udpated?
  if(minUsageIndex == -1){
    minUsageIndex = 0;
  }

  return minUsageIndex;
}

int Cache::getNumCachePages()
{
  return numCachePages;
}

void * Cache::getData(unsigned pageNum)
{
  return buffer + (PF_PAGE_SIZE * pageNum);
}

bool Cache::isDirty(unsigned pageNum)
{
  return *(dirtyFlag+pageNum) == true;
}

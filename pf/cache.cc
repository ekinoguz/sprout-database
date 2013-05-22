#include "cache.h"

Cache Cache::_cache;

bool Cache::freed = false;

Cache* Cache::Instance(int numCachePages)
{
  if (!_cache.initialized)
    _cache.init(numCachePages);

  return &_cache;
}
Cache::Cache()
{
  initialized = false;
}

void Cache::init(int cacheNumPages)
{
  initialized = true;
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
  EvictAllPagesToFiles();

  DeleteAllFileInfo();
  
  freed = true;

  free(buffer);
  free(framesInfo);
  free(pinnedFrames);
  free(dirtyFlag);
  free(frameUsage);
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

  std::unordered_map<std::string, int>::const_iterator element = existingPages.find(fileHandle->fileName + convert.str());;
  
  if (element == existingPages.end())
    {
      // Read the page from disk because it is not in the cache
      if(fileHandle->ReadPageFromDisk(pageNum, data)!= 0)
	 return -1;

      // Locate a page to flush
      int frameToFlush = GetFrameWithLowestUsage();

      // If frame is dirty write it to disk
      if (isDirty(frameToFlush))
	{
	  if ((framesInfo + frameToFlush)->fileHandle != 0)
	    {
	      (framesInfo + frameToFlush)->fileHandle->WritePageToDisk((framesInfo + frameToFlush)->pageNum, buffer + (PF_PAGE_SIZE * frameToFlush));
	    }
	  UnsetDirty(frameToFlush);
	}

      // Remove the mapping of evicted page from the existingPage map
      if ((framesInfo + frameToFlush)->fileHandle != NULL)
	{
	  ostringstream flushedPageNum;
	  flushedPageNum << (framesInfo + frameToFlush)->pageNum;
	  existingPages.erase((framesInfo + frameToFlush)->fileHandle->fileName + flushedPageNum.str());
	}
      	  
      // Read the data from disk
      if (fileHandle->ReadPageFromDisk(pageNum, data) != 0)
	{
	  return -1;
	}
      
      // Add the data to the cache and set the forward mapping
      memcpy(buffer + (PF_PAGE_SIZE * frameToFlush), data, PF_PAGE_SIZE);
      (framesInfo + frameToFlush)->fileHandle = fileHandle;
      (framesInfo + frameToFlush)->pageNum = pageNum;

      // Add the appropriate mapping to the existingPages map
      std::pair<std::string, int> newMapping (fileHandle->fileName + convert.str(), frameToFlush);
      existingPages.insert(newMapping);

      // Set usage to 1
      *(frameUsage + frameToFlush) = 1;
      return 0;
    }
  else
    {
      int frameNum = element->second;
      memcpy(data, buffer + (PF_PAGE_SIZE * frameNum), PF_PAGE_SIZE);

      uint16_t temp;
      memcpy(&temp, (char*)data + PF_PAGE_SIZE - 4, 2);
      
      *(frameUsage + frameNum) = *(frameUsage + frameNum) + 1;
      return 0;
    }
}

int Cache::WritePage(PF_FileHandle *fileHandle, unsigned pageNum, const void *data)
{
  if (pageNum > fileHandle->GetNumberOfPages())
    {
      return -1;
    }
  else if (pageNum == fileHandle->GetNumberOfPages())
    {
      return AppendPage(fileHandle, data);
    }
  
  ostringstream convert;
  convert << pageNum;
  std::unordered_map<std::string, int>::const_iterator element = existingPages.find(fileHandle->fileName + convert.str());
  if (element == existingPages.end())
    {
      // Locate a page to flush
      int frameToFlush = GetFrameWithLowestUsage();

      // If frame is dirty write it to disk
      if (isDirty(frameToFlush))
	{
	  int result = (framesInfo + frameToFlush)->fileHandle->WritePageToDisk((framesInfo + frameToFlush)->pageNum, buffer + (PF_PAGE_SIZE * frameToFlush));
	  if (result != 0)
	    {
	      return result;
	    }
	  UnsetDirty(frameToFlush);
	}

      // Remove the mapping of evicted page from the existingPage map
      if ((framesInfo + frameToFlush)->fileHandle != NULL)
	{
	  ostringstream flushedPageNum;
	  flushedPageNum << (framesInfo + frameToFlush)->pageNum;
	  existingPages.erase((framesInfo + frameToFlush)->fileHandle->fileName + flushedPageNum.str());
	}

      // Add the data to the cache and set the forward mapping
      memcpy(buffer + (PF_PAGE_SIZE * frameToFlush), data, PF_PAGE_SIZE);
      (framesInfo + frameToFlush)->fileHandle = fileHandle;
      (framesInfo + frameToFlush)->pageNum = pageNum;

      // Add the appropriate mapping to the existingPages map
      std::pair<std::string, int> newMapping (fileHandle->fileName + convert.str(), frameToFlush);
      existingPages.insert(newMapping);

      // Set usage to 1
      *(frameUsage + frameToFlush) = 1;

      // Page will be dirty since it is new

#ifdef DEBUG
      UnsetDirty(frameToFlush);
#else
      SetDirty(frameToFlush);
#endif
    }
  else
    {
      int frameNum = element->second;

      // Overwrite the cache frame with the new data
      memcpy(buffer + (PF_PAGE_SIZE * frameNum), data, PF_PAGE_SIZE);

      // Increase the frame usage
      *(frameUsage + frameNum) = *(frameUsage + frameNum) + 1;

      // Set the page to be dirty
#ifdef DEBUG
      UnsetDirty(frameNum);
#else
      SetDirty(frameNum);
#endif
    }

  // Write the page to disk to ensure fault tolerance
  #ifdef DEBUG
    fileHandle->WritePageToDisk(pageNum, data);    
  #endif

  return 0;
}

int Cache::AppendPage(PF_FileHandle *fileHandle, const void *data)
{
  std::unordered_map<std::string, FileInfo*>::const_iterator element = filesInfo.find(fileHandle->fileName);
  if (element != filesInfo.end())
    {
      int pageNum = element->second->numberOfPages;
      element->second->numberOfPages++;
      return WritePage(fileHandle, pageNum, data);
    }
  else
    {
      return -3;
    }  
}

int Cache::GetFrameWithLowestUsage()
{
  // Hold the minimum usage value of all frames
  int minUsage = *(frameUsage);
  // Hold the frame number of minUsage
  int minUsageFrameNum = 0;

  // Since pinning is not implemented, we will look for the page with minumum usage
  // However, if pinning is implemented, we need to have a while loop around the next if
  // the reason is because if all the pages are pinned, the while loop will keep trying
  // to locate a page with the minimum usage and not pinned (it will eventually succeeds
  // because upper layer will unpin at some point)
  for (int i = 1; i < numCachePages; i++)
    {
      if (*(frameUsage + i) < minUsage)
	{
	  minUsage = *(frameUsage + i);
	  minUsageFrameNum = i;
	}
    }

  return minUsageFrameNum;
}

int Cache::getNumCachePages()
{
  return numCachePages;
}

uint8_t* Cache::getData(unsigned frameNum)
{
  return buffer + (PF_PAGE_SIZE * frameNum);
}

bool Cache::isDirty(unsigned frameNum)
{
  return *(dirtyFlag + frameNum) == true;
}

void Cache::SetDirty(unsigned frameNum)
{
  *(dirtyFlag + frameNum) = true;
}

void Cache::UnsetDirty(unsigned frameNum)
{
  *(dirtyFlag + frameNum) = false;
}

// This should only be closed for closing fileHandles
int Cache::ClosingFile(PF_FileHandle *fileHandle)
{
  if(freed)
    return 0;

  for (int i = 0; i < numCachePages; i++)
    {
      // If the fileHandle associated with the frame is the same one passed to this function (same one that is been closed)
      // and if the page is dirty, write it to disk

      if (((framesInfo + i)->fileHandle == fileHandle) && isDirty(i))
	{
	  int result = (framesInfo + i)->fileHandle->WritePageToDisk((framesInfo + i)->pageNum, getData(i));
	  UnsetDirty(i);
	  if (result != 0)
	    {
	      return result;
	    }
	}

      if (((framesInfo + i)->fileHandle == fileHandle)){	
	memset(framesInfo+i,0,sizeof(FrameInfo));
      }
    }
  
  DeleteFileInfo(fileHandle);

  auto it = existingPages.begin();
  while (it != existingPages.end())
    {
      string key = it->first;
      it++;
      if (fileHandle->fileName.size() <= key.size())
	{
	  string substr = key.substr(0, fileHandle->fileName.size());
	  if (fileHandle->fileName == substr)
	    {
	      existingPages.erase(key);
	    }
	}
    }

  return 0;
}

void Cache::AddFileInfo(PF_FileHandle* fileHandle)
{
  std::unordered_map<std::string, FileInfo*>::const_iterator element = filesInfo.find(fileHandle->fileName);
  if (element == filesInfo.end())
    {
      FileInfo* fileInfo = ((FileInfo*)(malloc(sizeof(FileInfo))));
      fileInfo->numberOfUsers = 1;
      fileInfo->numberOfPages = fileHandle->GetNumberOfPagesFromDisk();
      
      std::pair<string, FileInfo*> newElement (fileHandle->fileName, fileInfo);
      filesInfo.insert(newElement);
    }
  else
    {
      element->second->numberOfUsers++;
    }
}

void Cache::DeleteAllFileInfo(){
  for ( auto it = filesInfo.begin(); it != filesInfo.end(); ++it )
    free(it->second);
}

void Cache::DeleteFileInfo(PF_FileHandle* fileHandle)
{
  if (filesInfo.size() == 0)
    return;
  std::unordered_map<std::string, FileInfo*>::const_iterator element = filesInfo.find(fileHandle->fileName);
  if (element != filesInfo.end())
    {
      element->second->numberOfUsers--;
      if (element->second->numberOfUsers == 0)
	{
	  free(element->second);
	  filesInfo.erase(fileHandle->fileName);	 
	}
    }
}

unsigned Cache::GetNumberOfPages(PF_FileHandle* fileHandle)
{
  std::unordered_map<std::string, FileInfo*>::const_iterator element = filesInfo.find(fileHandle->fileName);
  if (element != filesInfo.end())
    {
      return element->second->numberOfPages;
    }
  else
    {
      return -1;
    }
}

unsigned Cache::EvictAllPagesToFiles()
{
  for (int i = 0; i < numCachePages; i++)
    {
      if ((framesInfo + i)->fileHandle != NULL)
	{
	  if (PF_Manager::Instance(numCachePages)->CloseFile(*((framesInfo + i)->fileHandle)) != 0)
	    {
	      return -1;
	    }
	}
    }

  return 0;
}

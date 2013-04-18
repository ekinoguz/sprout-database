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
  //buffer = ((uint8_t*)(malloc(PF_PAGE_SIZE * cacheNumPages)));
}
Cache::~Cache()
{
}
#include <cache.h>

Cache* Cache::_cache = 0;

Cache* Cache::Instance(unsigned numCachePages)
{
  if (!_cache)
    {
      _cache = new Cache(numCachePages);
    }

  return _cache;
}

Cache:Cache(int cacheNumPages)
{
  cacheNumPages = cacheNumPages;
  buffer = ((uint8_t*)(malloc(PF_PAGE_SIZE * cacheNumPages)));
}

#ifndef _cache_h_
#define _cache_h_

#include <stdint.h>
#include "pf.h"


class Cache
{
 public:
  static Cache* Instance(numCachePages);

 protected:
  Cache(int numCachePages);
  ~Cache();

 private:
  static Cache* cache;
  int numCachePages;
  uint8_t *buffer;  
};

#endif

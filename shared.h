#ifndef __shared__h
#define __shared__h

#ifdef __APPLE__
#define uint uint32_t
#define NO_HISTORY_LIST
#endif

#include <iostream>
#include <string>

inline int error(std::string err, int rc){
  std::cout << "ERROR!: " << err << std::endl;
  return rc;
}
inline int error(int err, int rc){
  std::cout << "Line: " << err << std::endl;
  return rc;
}

#endif

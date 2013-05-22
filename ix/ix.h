#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pf/pf.h"
#include "rm/rm.h"

# define IX_EOF (-1)  // end of the index scan

using namespace std;

class IX_IndexHandle;

// Node formats:
// - Leaf node: contains sequence of tuples <Key, pageNum, slotNum>
// - IX node: contains <pageNum, key, pageNum, key, pageNum, ..., pageNum>
// Note: all nodes end with 1 bytes for node type and 2 bytes for free space pointer
typedef enum { LEAF_NODE = 0, IX_NODE } nodeType;

class IX_Manager {
 public:
  static IX_Manager* Instance();

  RC CreateIndex(const string tableName,       // create new index
		 const string attributeName);
  RC DestroyIndex(const string tableName,      // destroy an index
		  const string attributeName);
  RC OpenIndex(const string tableName,         // open an index
	       const string attributeName,
	       IX_IndexHandle &indexHandle);
  RC CloseIndex(IX_IndexHandle &indexHandle);  // close index
 
 private:
  RC init();

 protected:
  IX_Manager   ();                             // Constructor
  ~IX_Manager  ();                             // Destructor

 // Private API
 public:
  RC buildIndex(string tableName, string attributeName, IX_IndexHandle & ih);

 private:
  bool initialized;
  static IX_Manager _ix_manager;
  RM* rm;
  PF_Manager* pfm;
  string database_folder;
};


class IX_IndexHandle {
 public:
  IX_IndexHandle  ();                           // Constructor
  ~IX_IndexHandle ();                           // Destructor

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC InsertEntry(const void *key, const RID &rid);  // Insert new index entry
  RC DeleteEntry(void *key, const RID &rid);  // Delete index entry

  // Private API
 public:
  RC FindEntryPage(const void *key, uint16_t &pageNum, const bool doSplit = false);
  RC findOnPage(const void *page, const void *key, int & offset, bool inclusiveSearch = true) const;

  int getKeySize(const void *key, int* shift_offset = NULL) const;
  int split(int pageNum, int prevPageNum, const void * key);
  RC insertKey(void* key, int pointerPage, int toPage);

  int keycmp(const char* key, const char* okey, int key_size = 0, int okey_size = 0, int shift_offset = 0) const;
  int keycmp(const void* key, const void* okey, int key_size = 0, int okey_size = 0, int shift_offset = 0) const{
    return keycmp((char *)key, (char *)okey, key_size, okey_size, shift_offset);
  }
  
 public:
  PF_FileHandle fileHandle;
  int max_key_size;
  bool is_variable_length;
  AttrType type;
  bool is_open;
};


class IX_IndexScan {
 public:
  IX_IndexScan();  								// Constructor
  ~IX_IndexScan(); 								// Destructor

  // for the format of "value", please see IX_IndexHandle::InsertEntry()
  // Opens a scan on the index in the range (lowKey, highKey)
  //
  // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
  // should be included in the scan
  //
  // If lowKey is null, then the range is -infinity to highKey
  // If highKey is null, then the range is lowKey to +infinity
  RC OpenScan(const IX_IndexHandle &indexHandle,
	      void        *lowKey,
        void        *highKey,
        bool        lowKeyInclusive,
        bool        highKeyInclusive);

  RC GetNextEntry(RID &rid);  // Get next matching entry
  RC CloseScan();             // Terminate index scan

  // Private API
 private:
  IX_IndexHandle *indexHandle;
  void * page;
  void *highKey; // Warning: This is not a copy!
  bool highKeyInclusive;
  int offset;
  bool more;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif

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
  RC InsertEntry(void *key, const RID &rid);  // Insert new index entry
  RC DeleteEntry(void *key, const RID &rid);  // Delete index entry

  // Private API
 private:
  RC FindEntryPage(const void *key, uint16_t &pageNum, const bool doSplit = false);

 public:
  PF_FileHandle fileHandle;
  int max_key_size;
  bool is_variable_length;
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
  void * page;
  uint16_t lowPage;
  uint16_t highPage;
  RID current;
  int offset;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif

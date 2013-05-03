#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <ostream>
#include <unordered_map>

#include "../shared.h"
#include "../pf/pf.h"

using namespace std;


// Return code
typedef int RC;


// Record ID
typedef struct
{
  unsigned pageNum;
  unsigned slotNum;
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar, TypeBoolean, TypeShort } AttrType;

typedef unsigned AttrLength;

struct Attribute {
  string   name;     // attribute name
  AttrType type;     // attribute type
  AttrLength length; // attribute length
  int nullable; // TODO: Remove nullable and also TypeBoolean
};

struct Column {
  string column_name;
  string table_name;
  int position;
  AttrType type;
  AttrLength length;
  bool nullable; // TODO: remove this 
  char version;
};


// Comparison Operator
typedef enum { EQ_OP = 0,  // =
           LT_OP,      // <
           GT_OP,      // >
           LE_OP,      // <=
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;


# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through records
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanFormattedIterator {
public:
  RM_ScanFormattedIterator() {current.pageNum = 1; current.slotNum = 0; buffered_page = 0; };
  ~RM_ScanFormattedIterator() {};

  virtual RC getNextTuple(RID &rid, void *data);

  virtual RC close() { if(page != NULL) free(page); page = NULL; return 0; };

  PF_FileHandle * fh;
  vector<Column> columns;
  CompOp compOp;
  const void * value;
  void * page;

protected:
  RID current;
  uint buffered_page;
  

};

class RM_ScanIterator : public RM_ScanFormattedIterator {
public:
  RM_ScanIterator() : RM_ScanFormattedIterator() {};
  ~RM_ScanIterator() {};
  
  vector<Column> projectedColumns;
  
  // "data" follows the same format as RM::insertTuple()
  RC getNextTuple(RID &rid, void *data);
};




// Record Manager
class RM
{
public: 
  static RM* Instance();

  RC createTable(const string tableName, const vector<Attribute> &attrs);

  RC createTable(const string tableName);

  RC deleteTable(const string tableName);

  RC getAttributes(const string tableName, vector<Attribute> &attrs);

  //  Format of the data passed into the function is the following:
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!!The same format is used for updateTuple(), the returned data of readTuple(), and readAttribute()
  RC insertTuple(const string tableName, const void *data, RID &rid, bool useRid = false);
  // Like insertTuple but data is formated appropriately to be stored on disk.
  RC insertFormattedTuple(const string tableName, const void *data, const int length, RID &rid, bool useRid = false);

  RC deleteTuples(const string tableName);

  RC deleteTuple(const string tableName, const RID &rid);

  // Assume the rid does not change after update
  RC updateTuple(const string tableName, const void *data, const RID &rid);

  RC readTuple(const string tableName, const RID &rid, void *data);
  RC readFormattedTuple(const string tabkeName, const RID &rid, void *data);

  RC readAttribute(const string tableName, const RID &rid, const string attributeName, void *data);

  RC reorganizePage(const string tableName, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(const string tableName,
      const string conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  // scanFormatted returns an iterator tp allow the caller to go through the results one by one
  // by calling getNextTupleFormatted of the iterator
  RC scanFormatted(const string tableName,
      const int position, 
      const AttrType type,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      RM_ScanFormattedIterator &rm_ScanIterator);
  RC scanFormatted(const string tableName,
		   const vector<Column> columns,         // All versions of search column
		   const CompOp compOp,                  // comparision type such as "<" and "="
		   const void *value,                    // used in the comparison
		   RM_ScanFormattedIterator &rm_ScanIterator);



// Extra credit
public:
  // Because of the implementaiton choices drop followed by add will *NOT* causee loss of data.
  //   Feature not a bug :)
  RC dropAttribute(const string tableName, const string attributeName);

  RC addAttribute(const string tableName, const Attribute attr);

  RC reorganizeTable(const string tableName);



protected:
  RM();
  ~RM();

private:
  RC addAttributeToCatalog(const string tableName, uint offset, const Attribute &attr, char version = 0);
  RC addTableToCatalog(const string tableName, const string file_url, const string type);
  RC getAttributesFromCatalog(const string tableName, vector<Column> &columns, bool findAll = true, int version = -1 ); // if !findAll then look for only version
  char getLatestVersionFromCatalog(const string tableName);
  
  PF_FileHandle * getFileHandle(const string tableName);
  RC closeFileHandle(const string tableName);

  unordered_map<string,PF_FileHandle *> fileHandles;

  PF_Manager * pfm;
  string database_folder;
  static RM *_rm;
};

#endif

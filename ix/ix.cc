
#include "ix.h"

#define INDEX_TABLE "indexes"
#define INDEX_TABLE_RECORD_MAX_LENGTH 160   // Actual value is 80
#define DIRECTORY_ENTRY_SIZE 2

typedef enum { LEAF_NODE, IX_NODE } nodeType;

IX_Manager IX_Manager::_ix_manager;

IX_Manager* IX_Manager::Instance()
{
  if(!_ix_manager.initialized)
    if(_ix_manager.init()!=0)
      return NULL;

  return &_ix_manager;
}

IX_Manager::IX_Manager()
{
  initialized = false;
}

IX_Manager::~IX_Manager()
{
}

RC IX_Manager::init()
{

  initialized = true;
  rm = RM::Instance();
  pfm = PF_Manager::Instance();

  // Adding the index table attributes to the columns table
  Attribute attr;
  vector<Attribute> index_attr;
  attr.name = "table_name";
  attr.type = TypeVarChar;
  attr.length = 50;
  index_attr.push_back(attr); 

  attr.name = "column_name";
  attr.length = 30;
  attr.type = TypeVarChar;
  index_attr.push_back(attr);
  
  if(rm->createTable(INDEX_TABLE, index_attr) != 0)
    return -1;
  
  return 0;
}


RC IX_Manager::CreateIndex(const string tableName, const string attributeName)
{
  
  string file_url = DATABASE_FOLDER"/" + tableName + "_" + attributeName+".idx"; 
  if(pfm->CreateFile(file_url) != 0)
    return -2;


  PF_FileHandle fh;
  if(pfm->OpenFile(file_url.c_str(),fh)!=0)
     return -2;

  void *data = malloc(PF_PAGE_SIZE);
  memset((char *)data, 0, PF_PAGE_SIZE);
  *((char *)data+PF_PAGE_SIZE-3) = LEAF_NODE;
  // Note: Free pointer starts at 0
  if(fh.AppendPage(data)!=0)
    return -2;
  
  if(pfm->CloseFile(fh) != 0)
    return -2;
     
  IX_IndexHandle ixh;
  if(OpenIndex(tableName,attributeName, ixh)!=0)
    return -3;

  if(buildIndex(tableName, attributeName, ixh)!=0)
    return -3;
  
  CloseIndex(ixh);

  // Add index to INDEX_TABLE
  char * buffer = new char[tableName.size() + attributeName.size()+8];
  int offset = 0;
  int size = tableName.size();
  memcpy(buffer+offset, &size, 4);
  offset += 4;
  memcpy(buffer+offset, tableName.c_str(), tableName.size());
  offset += tableName.size();

  size = attributeName.size();
  memcpy(buffer+offset, &size, 4);
  offset += 4;
  memcpy(buffer+offset, attributeName.c_str(), attributeName.size());
  offset += attributeName.size();
  
  RID rid;
  if(rm->insertTuple(INDEX_TABLE, buffer, rid) != 0)
    return -1;

  return 0;
}

RC IX_Manager::DestroyIndex(const string tableName, const string attributeName)
{
  // TODO: do we have to care about opened file handles to the index file

  // Delete the index file
  string file_url = DATABASE_FOLDER"/" + tableName + "_" + attributeName+".idx"; 
  if(pfm->DestroyFile(file_url) != 0)
    return -2;
  
  // Delete the index from the index table
  vector<string> attributeNames;
  attributeNames.push_back("table_name");
  attributeNames.push_back("column_name");
  RM_ScanIterator rm_ScanIterator;
  rm->scan(INDEX_TABLE, "column_name", EQ_OP, tableName.c_str(), attributeNames, rm_ScanIterator);
  RID rid;
  char *data = (char*)(malloc(INDEX_TABLE_RECORD_MAX_LENGTH));
  bool found = false;
  while (rm_ScanIterator.getNextTuple(rid, data) != RM_EOF)
    {
      uint16_t tableName_size;
      memcpy(&tableName_size, data, 4);

      uint16_t attributeName_size;
      memcpy(&attributeName_size, data + 4 + tableName_size, 4);

      char *attributeName_intable = ((char*)(malloc(attributeName_size + 1)));
      memset(attributeName_intable, 0, attributeName_size + 1);
      memcpy(attributeName_intable, data + 4 + tableName_size + 4, attributeName_size);
      string strAttr (attributeName_intable);
      if (strAttr == attributeName)
	{
	  found = true;
	  break;
	}
    }

  if (found == false)
    {
      return -3;
    }

  if (rm->deleteTuple(INDEX_TABLE, rid) != 0)
    {
      return -1;
    }

  return 0;
}

RC IX_Manager::OpenIndex(const string tableName,
	       const string attributeName,
	       IX_IndexHandle &indexHandle)
{
  string file_url = DATABASE_FOLDER"/" + tableName + "_" + attributeName+".idx"; 
  if(pfm->OpenFile(file_url.c_str(), indexHandle.fileHandle) != 0)
    return -2;

  return 0;
}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle)
{
  if (pfm->CloseFile(indexHandle.fileHandle) != 0)
    {
      return -2;
    }

  return 0;
}

IX_IndexHandle::IX_IndexHandle()
{
}

IX_IndexHandle::~IX_IndexHandle()
{
}

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid){
  // Key is in "their" format
  // Return 1 if (key,rid) already exists or cope with duplicate keys
  // This is gonna be a pain in the ass implementation
  // Search the file and insert the key in the leaf page
  // Split all pages while searching
  // Split the leaf page if needed
  // We have to have a way to locate the root page on our file, I don't think we can have page 0 to be the root all the time, because when we create a new root, we have to put it on page 0, shift all pages and reorganize the whole index pointers


  return -1;
}
RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid){
  // Return 2 if (key,rid) does not exist

  return -1;
}

IX_IndexScan::IX_IndexScan()
{
}

IX_IndexScan::~IX_IndexScan()
{
}

RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle,
			  void *lowKey,
			  void *highKey,
			  bool lowKeyInclusive,
			  bool highKeyInclusive)
{
  return -1;
}

RC IX_IndexScan::GetNextEntry(RID &rid)
{
  // Return current and move to next
  return -1;
}

RC IX_IndexScan::CloseScan()
{
  return -1;
}

void IX_PrintError (RC rc)
{
  // Print error message
  switch (rc)
    {
    case -3:
      cout << "IX error" << endl;
      break;
    case -2:
      cout << "PF error" << endl;
      break;
    case -1:
      cout << "RM error" << endl;
      break;
    case 1:
      cout << "Duplicate found" << endl;
      break;
    case 2:
      cout << "Record not found" << endl;
      break;
    default:
      cout << "Unkown Error" << endl;
      break;
    }
}


// Private API
RC IX_Manager::buildIndex(string tableName, string attributeName, IX_IndexHandle & ih)
{
  // Scann the whole tableName file and project the attributeName only
  // For each record found insert that record in ih
  // Check insert in ih for further details

  return 0;
}

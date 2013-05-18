
#include "ix.h"

#define INDEX_TABLE "indexes"
#define INDEX_TABLE_RECORD_MAX_LENGTH 160   // Actual value is 93
#define DIRECTORY_ENTRY_SIZE 2

// Node formats:
// - Leaf node: contains sequence of tuples <Key, pageNum, slotNum>
// - IX node: contains <pageNum, key, pageNum, key, pageNum, ..., pageNum>
// Note: all nodes end with 1 bytes for node type and 2 bytes for free space pointer
typedef enum { LEAF_NODE = 0, IX_NODE } nodeType;

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

  attr.name = "max_key_length";
  attr.length = 4;
  attr.type = TypeInt;
  index_attr.push_back(attr);

  attr.name = "is_variable_length";
  attr.length = 1;
  attr.type = TypeBoolean;
  index_attr.push_back(attr);
  
  if(rm->createTable(INDEX_TABLE, index_attr) != 0)
    return -1;
  
  return 0;
}


RC IX_Manager::CreateIndex(const string tableName, const string attributeName)
{
  // Collect information from the catalog about the attributeName
  vector<Column> columns;
  if (rm->getAttributesFromCatalog(tableName, columns, false) != 0)
    {
      return -1;
    }

  int max_size = -1;
  bool is_variable = false;
  for (uint i = 0; i < columns.size(); i++)
    {
      if (columns[i].column_name == attributeName)
	{
	  if (columns[i].type == TypeVarChar)
	    {
	      max_size = columns[i].length + 2;
	      is_variable = true;
	    }
	  else
	    {
	      max_size = columns[i].length;
	      is_variable = false;
	    }
	  break;
	}
    }

  if(max_size == -1)
    return -3;

  
  // Create the index file on disk
  string file_url = DATABASE_FOLDER"/" + tableName + "_" + attributeName+".idx"; 
  if(pfm->CreateFile(file_url) != 0)
    return -2;

  // Open the index file
  PF_FileHandle fh;
  if(pfm->OpenFile(file_url.c_str(),fh)!=0)
     return -2;

  void *data = malloc(PF_PAGE_SIZE);
  memset((char *)data, 0, PF_PAGE_SIZE);
  uint8_t type = LEAF_NODE;
  memcpy((char *)data + PF_PAGE_SIZE - 3, &type, 1);
  // *((char *)data+PF_PAGE_SIZE-3) = LEAF_NODE;
  // Note: Free pointer starts at 0
  if(fh.AppendPage(data)!=0)
    {
      free(data);
      return -2;
    }
  free(data);
  
  if(pfm->CloseFile(fh) != 0)
    return -2;

  // Add index to INDEX_TABLE
  char * buffer = (char*)(malloc(tableName.size() + attributeName.size()+8+4+1));
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

  memcpy(buffer + offset, &max_size, sizeof(max_size));
  offset += sizeof(max_size);

  memcpy(buffer + offset, &is_variable, sizeof(is_variable));
  offset += sizeof(is_variable);

  RID rid;
  if(rm->insertTuple(INDEX_TABLE, buffer, rid) != 0)
    {
      free(buffer);
      return -1;
    }

  free(buffer);
  
    

  // Build the index
  IX_IndexHandle ixh;
  if(OpenIndex(tableName,attributeName, ixh)!=0)
    return -3;

  if(buildIndex(tableName, attributeName, ixh)!=0)
    return -3;
  
  CloseIndex(ixh);

  return 0;
}

RC IX_Manager::DestroyIndex(const string tableName, const string attributeName)
{
  // Delete the index file
  string file_url = DATABASE_FOLDER"/" + tableName + "_" + attributeName+".idx"; 
  if(pfm->DestroyFile(file_url) != 0)
    return -2;
  
  // Delete the index from the index table
  vector<string> attributeNames;
  attributeNames.push_back("table_name");
  attributeNames.push_back("column_name");
  RM_ScanIterator rm_ScanIterator;
  rm->scan(INDEX_TABLE, "table_name", EQ_OP, tableName.c_str(), attributeNames, rm_ScanIterator);
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

  // Read the index parameter from the catalog
  vector<string> attributeNames;
  attributeNames.push_back("table_name");
  attributeNames.push_back("column_name");
  attributeNames.push_back("max_key_length");
  attributeNames.push_back("is_variable_length");
  RM_ScanIterator rm_ScanIterator;
  rm->scan(INDEX_TABLE, "table_name", EQ_OP, tableName.c_str(), attributeNames, rm_ScanIterator);
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
      free(attributeName_intable);
      if (strAttr == attributeName)
	{
	  memcpy(&indexHandle.max_key_size, data + 4 + tableName_size + 4 + attributeName_size, 4);
	  memcpy(&indexHandle.is_variable_length, data + 4 + tableName_size + 4 + attributeName_size + 4, 1);
	  found = true;
	}
    }

  free(data);

  if(!found)
    return -3;

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

  uint16_t pageNum;
  if (FindEntryPage(key, pageNum) != 0)
    {
      return -3;
    }

  // Read the page where the key should be inserted
  void *page = malloc(PF_PAGE_SIZE);
  if (fileHandle.ReadPage(pageNum, page) != 0)
    {
      free(page);
      return -2;
    }

  // Read the free pointer
  uint16_t free_pointer = 0;
  memcpy(&free_pointer, (char *)page + PF_PAGE_SIZE - DIRECTORY_ENTRY_SIZE, DIRECTORY_ENTRY_SIZE);

  int key_size = 0;
  if (is_variable_length == false)
    {
      key_size = max_key_size;
    }
  else
    {
      memcpy(&key_size, (char *)key, sizeof(key_size));
      key_size += sizeof(key_size);  // Add the first four bytes that contains the key size
    }

  // 3 in rvalue is the free_pointer + node_type
  if (key_size > PF_PAGE_SIZE - 3 - free_pointer)
    {
      // split
    }
  else
    {
      // Find where to insert the key, all keys on page should be sorted
      int offset = 0;
      while (offset < free_pointer)
	{
	  int key_on_page_size = 0;
	  if (is_variable_length == false)
	    {
	      key_on_page_size = max_key_size;
	    }
	  else
	    {
	      memcpy(&key_on_page_size, (char *)key, sizeof(key_on_page_size));
	      key_on_page_size += sizeof(key_on_page_size);  // Add the first four bytes that contains the key size
	    }

	  // Read key on page
	  void *key_on_page = malloc(key_on_page_size);
	  memcpy(key_on_page, (char *)page + offset, key_on_page_size);

	  // Compare key on page with the current key
	  int cmp = memcmp(key, key_on_page, key_on_page_size);
	  free(key_on_page);

	  if (cmp <= 0)
	    {
	      break;
	    }
	  else
	    {
	      offset += key_on_page_size + 4;
	    }
	}

      // Shift all the subsequent keys to the right
      int subsequent_keys_size = free_pointer - offset;
      void *subsequent_keys = malloc(subsequent_keys_size);
      memcpy(subsequent_keys, (char *)page + offset, subsequent_keys_size);
      memcpy((char *)page + offset + key_size + 4, subsequent_keys, subsequent_keys_size);

      // Inser the new key with the pageNum and slotNum
      uint16_t key_pageNum = rid.pageNum;
      uint16_t key_slotNum = rid.slotNum;

      memcpy((char *)page + offset, key, key_size);
      offset += key_size;
      memcpy((char *)page + offset, &key_pageNum, DIRECTORY_ENTRY_SIZE);
      offset += DIRECTORY_ENTRY_SIZE;
      memcpy((char *)page + offset, &key_slotNum, DIRECTORY_ENTRY_SIZE);
      offset += DIRECTORY_ENTRY_SIZE;

      free_pointer += key_size + 4;

      // Write the free_pointer back to the end of the page
      memcpy((char *)page + PF_PAGE_SIZE - DIRECTORY_ENTRY_SIZE, &free_pointer, DIRECTORY_ENTRY_SIZE);
	      
      // Write the page back to disk
      if (fileHandle.WritePage(pageNum, page) != 0)
	{
	  free(page);
	  return -2;
	}
    }
  
  free(page);
  return 0;
}

RC IX_IndexHandle::FindEntryPage(void *key, uint16_t &pageNum)
{
  void *page = malloc(PF_PAGE_SIZE);
  pageNum = 0;
  if (fileHandle.ReadPage(pageNum, page) != 0)
    {
      free(page);
      return -2;
    }
  // Read node type
  uint8_t type = 0;
  memcpy(&type, (char *)page + PF_PAGE_SIZE - 3, 1);

  while (type == IX_NODE)
    {
      // TODO: Split if needed

      // Parse IX node and set pageNum
      // free_pointer must not be zero
      uint16_t free_pointer = 0;
      memcpy(&free_pointer, (char *)page + PF_PAGE_SIZE - DIRECTORY_ENTRY_SIZE, DIRECTORY_ENTRY_SIZE);

      uint16_t offset = 2;    // Start pointing to the first key on the page, first 2 bytes are pageNum
      while (offset < free_pointer)
	{
	  int key_size = 0;
	  uint16_t shift_offset;
	  if (is_variable_length == false)
	    {
	      key_size = max_key_size;
	      shift_offset = 0;
	    }
	  else
	    {
	      memcpy(&key_size, (char *)page + offset, sizeof(key_size));
	      offset += sizeof(key_size);
	      shift_offset = sizeof(key_size);
	    }

	  void *ix_key = malloc(key_size);
	  memcpy(ix_key, (char *)page + offset, key_size);
	  int cmp = memcmp(key, ix_key, key_size);
	  free(ix_key);
	  if (cmp == 0)
	    {
	      memcpy(&pageNum, (char *)page + offset + key_size, DIRECTORY_ENTRY_SIZE);
	      // SKY: if you think the breaks might cause errors, add a bool found variable and assign true it instead of the breaks and change the inner while to while (!found)
	      break;
	    }
	  else if (cmp < 0)
	    {
	      memcpy(&pageNum, (char *)page + offset - shift_offset - DIRECTORY_ENTRY_SIZE, DIRECTORY_ENTRY_SIZE);
	      break;
	    }
	  else
	    {
	      offset += key_size + DIRECTORY_ENTRY_SIZE; // The plus DIRECTORY_ENTRY_SIZE is to skip the pageNum and point to the next key
	    }
	}

      if (fileHandle.ReadPage(pageNum, page) != 0)
	{
	  free(page);
	  return -2;
	}
      // Read node type
      memcpy(&type, (char *)page + PF_PAGE_SIZE - 3, 1);
    }

  free(page);
  return 0;
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
    case -1:
      cout << "RM error" << endl;
      break;
    case -2:
      cout << "PF error" << endl;
      break;
    case -3:
      cout << "IX error" << endl;
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
RC IX_Manager::buildIndex(string tableName, string attributeName, IX_IndexHandle & indexHandle)
{
  // Scann the whole tableName file and project the attributeName only
  // For each record found insert that record in indexHandle
  // Check insert indexHandle for further details

  vector<string> attributeNames;
  attributeNames.push_back(attributeName);
  RM_ScanIterator rm_ScanIterator;
  if (rm->scan(tableName, "", NO_OP, NULL, attributeNames, rm_ScanIterator) != 0)
    {
      return -1;
    }

  RID rid;
  void* data = malloc(indexHandle.max_key_size+2);
  while (rm_ScanIterator.getNextTuple(rid, data) != RM_EOF)
    {
      if (indexHandle.InsertEntry(data, rid) != 0)
	{
	  free(data);
	  return -3;
	}
    }

  free(data);
  return 0;
}

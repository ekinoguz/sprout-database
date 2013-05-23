#include "ix.h"

#define INDEX_TABLE "indexes"
#define INDEX_TABLE_RECORD_MAX_LENGTH 160   // Actual value is 93
#define DIRECTORY_ENTRY_SIZE 2


RC error(string error, RC rc){
  cout << "ERROR!: " << error << endl;
  return rc;
}
RC error(int error, RC rc){
  cout << "Line: " << error << endl;
  return rc;
}


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
int IX_IndexHandle::keycmp(const char * key, const char * okey, int key_size, int okey_size, int shift_offset) const
{
  if(okey_size == 0 || shift_offset == 0)
    okey_size = getKeySize(okey, &shift_offset);
  if(key_size == 0)
    key_size = getKeySize(key);

  if(is_variable_length){    
    int cmp;
    if(key_size <= okey_size)
      cmp = memcmp(key+shift_offset, okey+shift_offset, key_size-shift_offset);
    else
      cmp = memcmp(key+shift_offset, okey+shift_offset, okey_size-shift_offset);
	  
    if(cmp == 0 && key_size != okey_size)
      cmp = memcmp(&key_size, &okey_size, 4);

    return cmp;
  } else {
    if(memcmp(key, okey, key_size) == 0)
      return 0;

    bool less;
    switch( type ){
    case TypeInt:
      less = *(int *)(key) < *(int *)(okey);
      break;
    case TypeReal:
      less = *(float *)(key) < *(float *)(okey);
      break;
    case TypeShort:
    case TypeBoolean:
      less = *(char *)(key) < *(char *)(okey);
      break;
    default:
      cout << type << endl;
      return error("Unkown type!!!!", -100);
    }

    if(less)
      return -1;
    else
      return 1;
  }

  
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

  attr.name = "type";
  attr.length = 4;
  attr.type = TypeInt;
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
  AttrType type;
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
	  type = columns[i].type;
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
  *((char *)data+PF_PAGE_SIZE-3) = LEAF_NODE;
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
  char * buffer = (char*)(malloc(tableName.size() + attributeName.size()+8+4+1+4));
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

  memcpy(buffer + offset, &type, sizeof(type));
  offset += sizeof(type);

  RID rid;
  if(rm->insertTuple(INDEX_TABLE, buffer, rid) != 0)
    {
      free(buffer);
      //TODO: If this fails we should delete the file
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
      int tableName_size;
      memcpy(&tableName_size, data, 4);

      int attributeName_size;
      memcpy(&attributeName_size, data + 4 + tableName_size, 4);

      char *attributeName_intable = ((char*)(malloc(attributeName_size + 1)));
      memset(attributeName_intable, 0, attributeName_size + 1);
      memcpy(attributeName_intable, data + 4 + tableName_size + 4, attributeName_size);
      string strAttr (attributeName_intable);
      free(attributeName_intable);
      if (strAttr == attributeName)
	{
	  found = true;
	  break;
	}
    }

  free(data);
  if (found == false)
    return -3;

  if (rm->deleteTuple(INDEX_TABLE, rid) != 0)
    return -1;

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
  attributeNames.push_back("type");
  RM_ScanIterator rm_ScanIterator;
  rm->scan(INDEX_TABLE, "table_name", EQ_OP, tableName.c_str(), attributeNames, rm_ScanIterator);
  RID rid;
  char *data = (char*)(malloc(INDEX_TABLE_RECORD_MAX_LENGTH));
  bool found = false;
  while (rm_ScanIterator.getNextTuple(rid, data) != RM_EOF)
    {
      int tableName_size;
      memcpy(&tableName_size, data, 4);

      int attributeName_size;
      memcpy(&attributeName_size, data + 4 + tableName_size, 4);

      char *attributeName_intable = ((char*)(malloc(attributeName_size + 1)));
      memset(attributeName_intable, 0, attributeName_size + 1); // ensure the null terminator
      memcpy(attributeName_intable, data + 4 + tableName_size + 4, attributeName_size);
      string strAttr (attributeName_intable);
      free(attributeName_intable);
      if (strAttr == attributeName)
	{
	  memcpy(&indexHandle.max_key_size, data + 4 + tableName_size + 4 + attributeName_size, 4);
	  memcpy(&indexHandle.is_variable_length, data + 4 + tableName_size + 4 + attributeName_size + 4, 1);
	  memcpy(&indexHandle.type, data + 4 + tableName_size + 4 + attributeName_size + 4 + 1, 4);
	  found = true;
	}
    }
  free(data);

  rm_ScanIterator.close();

  if(!found)
    return -3;

  indexHandle.is_open = true;

  return 0;
}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle)
{
  indexHandle.is_open = false;
  if (pfm->CloseFile(indexHandle.fileHandle) != 0)
    {
      return -2;
    }

  return 0;
}

IX_IndexHandle::IX_IndexHandle()
{
  is_open = false;
}

IX_IndexHandle::~IX_IndexHandle()
{
}

RC IX_IndexHandle::InsertEntry(const void *key, const RID &rid){
  // Key is in "their" format
  // Return 1 if (key,rid) already exists or cope with duplicate keys
  // This is gonna be a pain in the ass implementation
  // Search the file and insert the key in the leaf page
  // Split all pages while searching
  // Split the leaf page if needed
  // We have to have a way to locate the root page on our file, I don't think we can have page 0 to be the root all the time, because when we create a new root, we have to put it on page 0, shift all pages and reorganize the whole index pointers

  if(!is_open)
    return -5;

  uint16_t pageNum;
  // true makes sure that as we search we split
  if (FindEntryPage(key, pageNum, true) != 0)
    return -3;

  // Read the page where the key should be inserted
  void *page = malloc(PF_PAGE_SIZE);
  if (fileHandle.ReadPage(pageNum, page) != 0)
    {
      free(page);
      return error("Read insert entry", -2);
    }

  // Read the free pointer
  uint16_t free_pointer = 0;
  memcpy(&free_pointer, (char *)page + PF_PAGE_SIZE - 2, 2);

  int key_size = getKeySize((char *)key);
  // 3 in rvalue is the free_pointer + node_type
  // 2 in rvalue is the next_pointer (pageNum)
  // 4 in lvalue is the rid pointer
  if (key_size + 4 > PF_PAGE_SIZE - free_pointer - 3 - 2)
    {
      cout << "Fatal ERROR: This shouldn't happen" << endl;
      return -3;
    }
  
  // Find where to insert the key, all keys on page should be sorted
  int offset = 0;
  if(findOnPage(page, key, offset) != 0)
    return -3;
  
  if( offset != free_pointer ) {
    // Shift all the subsequent keys to the right
    int subsequent_keys_size = free_pointer - offset;
    void *subsequent_keys = malloc(subsequent_keys_size);
  
    memcpy(subsequent_keys, (char *)page + offset, subsequent_keys_size);
    memcpy((char *)page + offset + key_size + 4, subsequent_keys, subsequent_keys_size);
    free(subsequent_keys);
  }
  

  // Insert the new key with the pageNum and slotNum
  uint16_t key_pageNum = rid.pageNum;
  uint16_t key_slotNum = rid.slotNum;

  memcpy((char *)page + offset, key, key_size);
  offset += key_size;
  memcpy((char *)page + offset, &key_pageNum, sizeof(key_pageNum));
  offset += sizeof(key_pageNum);
  memcpy((char *)page + offset, &key_slotNum, sizeof(key_slotNum));
  offset += sizeof(key_slotNum);

  free_pointer += key_size + 4;

  // Write the free_pointer back to the end of the page
  memcpy((char *)page + PF_PAGE_SIZE - 2, &free_pointer, 2);
	      
  // Write the page back to disk
  if (fileHandle.WritePage(pageNum, page) != 0)
   {
     free(page);
     return -2;
   }
    
  
  free(page);
  return 0;
}


RC IX_IndexHandle::findOnPage(const void *page, const void *key, int & offset, bool inclusiveSearch) const
{
  //TODO: if inclusiveSearch is false then we need to find the first entry larger than key
  //   This is needed for scan

  uint16_t free_pointer = 0;
  memcpy(&free_pointer, (char *)page + PF_PAGE_SIZE - 2, 2);
  
  while (offset < free_pointer)
    {
      // Read key on page
      void *key_on_page = (void *)((char *)page + offset);
      int key_on_page_size = getKeySize(key_on_page);

      int cmp = keycmp(key, key_on_page);

      if( cmp < 0)
	return 0;
      else if (cmp == 0 && inclusiveSearch)
	return 0;
      else
	offset += key_on_page_size + 4;

    }
  
  if( offset > free_pointer )
    offset = free_pointer;
  
  // TODO: Add a test for looking for a key that isn't there

  return 0;
}

RC IX_IndexHandle::FindEntryPage(const void *key, uint16_t &pageNum, const bool doSplit)
{
  // TODO: If key is NULL then search for the leftmost child.
  //    This is used to implement search
  void *page = malloc(PF_PAGE_SIZE);
  pageNum = 0;
  uint16_t prevPageNum = 0;
  if (fileHandle.ReadPage(pageNum, page) != 0)
    {
      free(page);
      return error(__LINE__, -2);
    }
  // Read node type
  nodeType type = LEAF_NODE;
  memcpy(&type, (char *)page + PF_PAGE_SIZE - 3, 1);

  while (type == IX_NODE)
    {
      prevPageNum = pageNum;
      // Parse IX node and set pageNum
      // free_pointer must not be zero
      uint16_t free_pointer = 0;
      memcpy(&free_pointer, (char *)page + PF_PAGE_SIZE - 2, 2);

      uint16_t offset = 2;    // Start pointing to the first key on the page, first 2 bytes are pageNum
      while (offset < free_pointer)
	{  
	  if(key == NULL) { // Just move to the page to the left, since we want the left most leaf
	    memcpy(&pageNum, (char *)page, 2);
	    break;
	  }

	  int shift_offset;
	  void *ix_key = (char *)page + offset;
	  int key_size = getKeySize(ix_key, &shift_offset);
	  

	  int cmp = keycmp(key, ix_key);
	  if (cmp == 0)
	    {
	      memcpy(&pageNum, (char *)page + offset + key_size, 2);
	      break;
	    }
	  else if (cmp < 0)
	    {
	      memcpy(&pageNum, (char *)page + offset - 2, 2);
	      break;
	    }
	  else
	    {
	      offset += key_size + 2; // The plus 2 is to skip the pageNum and point to the next key
	    }
	}

      // No index <= key follow the last pointer
      if(offset >= free_pointer){
	memcpy(&pageNum, (char *)page + free_pointer - 2, 2);
      }
 
      if (fileHandle.ReadPage(pageNum, page) != 0)
	{
	  free(page);
	  return error(__LINE__, -2);
	}
      // Read node type
      memcpy(&type, (char *)page + PF_PAGE_SIZE - 3, 1);

      if(doSplit){
	if(free_pointer + max_key_size + 4 > PF_PAGE_SIZE - 3 - 2){
	  pageNum = split(pageNum, prevPageNum, key);
	  if(pageNum < 0)
	    return -3;
	} 
      }
    }

  // Check the final leaf and split if necessary
  if(doSplit){
    uint16_t free_pointer = 0;
    memcpy(&free_pointer, (char *)page + PF_PAGE_SIZE - 2, 2);

    // If we knew the key to be inserted here, we could be a bit smarter.
    if(free_pointer + getKeySize(key) + 4 > PF_PAGE_SIZE - 3 - 2){
      pageNum = split(pageNum, prevPageNum, key);

      if(pageNum < 0)
	return -3;
    }
  }

  free(page);
  return 0;
}

int IX_IndexHandle::getKeySize(const void *key, int * shift_offset) const{
  int key_size = max_key_size;
  int shift;
  shift = 0;
  if(is_variable_length){
    memcpy(&key_size, key, 4);
    key_size += 4;
    shift =  4;
  }

  if(shift_offset)
    *shift_offset = shift;

  return key_size;
}

// toPage must be an internal page
RC IX_IndexHandle::insertKey(void * key, int pointerPage, int toPage){
  void * page = malloc(PF_PAGE_SIZE);
  
  if(fileHandle.ReadPage(toPage, page) != 0)
    {
      cout << "Error in insert key" << endl;
      free(page);
      return -2;
    }
    
  // Find on page but for IX_NODE (Copy and Pasted)
  int offset = 2;
  int shift_offset;
  int key_size = getKeySize((char *)key, &shift_offset);

  uint16_t free_pointer = 0;
  memcpy(&free_pointer, (char *)page + PF_PAGE_SIZE - 2, 2);
  
  while (offset < free_pointer)
    {
      // Read key on page
      void *key_on_page = (void *)((char *)page + offset);
      int key_on_page_size = getKeySize(key_on_page);

      int cmp = keycmp(key, key_on_page);

      if (cmp <= 0)
	break;
      else
	offset += key_on_page_size + 2;

    }  


  if( offset >= free_pointer )
    offset = free_pointer;
  else {
    // Insert at this location, first shift to the right (copyied from insertentry)
    // Shift all the subsequent keys to the right
    int subsequent_keys_size = free_pointer - offset;
    void *subsequent_keys = malloc(subsequent_keys_size);
    memcpy(subsequent_keys, (char *)page + offset, subsequent_keys_size);
    memcpy((char *)page + offset + key_size + 2, subsequent_keys, subsequent_keys_size);
    free(subsequent_keys);
  }
  
  // Copy in the new (key,pointer)
  uint16_t tmp = pointerPage;
  memcpy((char *)page + offset, key, key_size);
  memcpy((char *)page + offset + key_size, &tmp, 2);

  // Update the free pointer
  free_pointer += key_size + 2;
  memcpy((char *)page + PF_PAGE_SIZE - 2, &free_pointer, 2);
   
  RC rc = fileHandle.WritePage(toPage, page);
  free(page);

  return rc;
}

int IX_IndexHandle::split(int pageNum, int prevPageNum, const void * key){
  void * page = malloc(PF_PAGE_SIZE);
  void * newPage = malloc(PF_PAGE_SIZE);
  memset(newPage, 0, PF_PAGE_SIZE);

  if(fileHandle.ReadPage(pageNum, page) != 0){
    cout << "Error reading in split" << endl;
    return -2;
  }
  
  // Split the page in two
  nodeType type = LEAF_NODE;
  memcpy(&type, (char *)page + PF_PAGE_SIZE - 3, 1);
  
  // Set the type of the new page
  *((char *)newPage+PF_PAGE_SIZE-3) = type;
  
  int offset = 0;
  uint16_t free_pointer = 0;
  memcpy(&free_pointer, (char *)page + PF_PAGE_SIZE - 2, 2);

  int return_page = -1;
  
  uint16_t new_page_num = fileHandle.GetNumberOfPages(); // This is the page number added at the end with append

  // Switch on type
  if( pageNum == 0 ){ // Root page
    int pointer_size = 4;    
    int key_offset = 0;
    if(type == IX_NODE){
      pointer_size = 2;
      key_offset = 2;
    }
    
    int key_size;
    while(offset <= (free_pointer / 2)){
      key_size = getKeySize((char*)page+offset+key_offset);
      offset += key_size + pointer_size;
    }

    // Copy the other records to the new page
    memcpy(newPage, (char*)page+offset, free_pointer-offset);
    
    
    // Add the free_pointer
    uint16_t tmp = free_pointer-offset;
    memcpy((char*)newPage+PF_PAGE_SIZE-2, &tmp, 2);
    
    if( type == IX_NODE) {
      tmp = offset - key_size;
    }
    else {
      tmp = offset;
    }
    // Update the old free_pointer
    memcpy((char *)page+PF_PAGE_SIZE-2, &tmp, 2);


    uint16_t left_page = fileHandle.GetNumberOfPages(); 
    uint16_t right_page = left_page + 1;
    new_page_num = right_page;
    
    if( type == LEAF_NODE ){
      // Set up the linked pointers
      memset((char *)newPage + PF_PAGE_SIZE - 3 - 2,0, 2);
      memcpy((char *)page + PF_PAGE_SIZE - 3 - 2, &right_page, 2);
    }

    // Copy the old root to the left page
    void * leftPage = malloc(PF_PAGE_SIZE);
    memcpy(leftPage, page,PF_PAGE_SIZE);
    memset((char *)leftPage+tmp, 0, PF_PAGE_SIZE - tmp -3 -2);

    if(fileHandle.AppendPage(leftPage) != 0)
      return -2;

    free(leftPage);

    // To make debugging easier just clear the page
    memset(page, 0, PF_PAGE_SIZE);
        
    // Add in the promoted key
    key_size = getKeySize((char*)newPage + key_offset);
    memcpy((char *)page+2, (char *)newPage+key_offset, key_size);
    // Add in new pointers
    memcpy(page, &left_page, 2);
    memcpy((char *)page+key_size+2, &right_page, 2);
    
    
    // Delete everything else by changing the free pointer
    tmp = 4 + key_size;
    memcpy((char *)page+PF_PAGE_SIZE-2, &tmp, 2);
        
    // Ensure the root is now an IX_NODE
    nodeType new_type = IX_NODE;
    memcpy((char *)page + PF_PAGE_SIZE - 3,&new_type, 1);
    
  } else if( type == LEAF_NODE ) {
    // Set up the linked pointers 
    uint16_t next_page = 0;
    memcpy(&next_page, (char *)page + PF_PAGE_SIZE - 3 - 2, 2);
    memcpy((char *)newPage + PF_PAGE_SIZE - 3 - 2,&next_page, 2);
    memcpy((char *)page + PF_PAGE_SIZE - 3 - 2, &new_page_num, 2);


    while(offset <= (free_pointer / 2)){
      int key_size = getKeySize((char*)page+offset);
      offset += key_size + 4;
    }

    // Copy the other records to the new page
    memcpy(newPage, (char*)page+offset, free_pointer-offset);
    
    // Add the free_pointer
    uint16_t tmp = free_pointer-offset;
    memcpy((char*)newPage+PF_PAGE_SIZE-2, &tmp, 2);

    // Update the old free_pointer
    memcpy((char *)page+PF_PAGE_SIZE-2, &offset, 2);
    
    // Clear the rest
    memset((char *)page+offset, 0,PF_PAGE_SIZE-3-2-offset);
    
    if(insertKey(newPage, new_page_num, prevPageNum) != 0)
      return -3;

  } else if( type == IX_NODE ){
    int key_size;
    while(offset <= (free_pointer / 2)){
      key_size = getKeySize((char*)page+offset+4);
      offset += key_size + 2;
    }

    // Copy the other records to the new page
    memcpy(newPage, (char*)page+offset, free_pointer-offset);
    
    // Add the free_pointer
    uint16_t tmp = free_pointer-offset;
    memcpy((char*)newPage+PF_PAGE_SIZE-2, &tmp, 2);

    // Update the old free_pointer (minus the last key)
    // Note: We don't premote this key, although we could
    tmp = offset - key_size;
    memcpy((char *)page+PF_PAGE_SIZE-2, &tmp, 2);
    
    if(insertKey((char *)newPage+2, new_page_num, prevPageNum) != 0)
      return -3;
    
  } else{
    cout << "The impossible! Unkown Type" << endl;
    return -3;
  }

  if(fileHandle.WritePage(pageNum, page)!=0)
    return -2;

  if(fileHandle.AppendPage(newPage)!=0)
    return -2;

  // Set the return page
  int key_start_offset = 0;  
  if(type == IX_NODE)
    key_start_offset = 4;
  
  int cmp = keycmp(key, ((char *)newPage+key_start_offset));

  if( cmp < 0 ){
    if(pageNum == 0){
      return_page = new_page_num - 1;
    }
    else
      return_page = pageNum;
  }
  else
    return_page = new_page_num;

  free(page);
  free(newPage);
  return return_page;  
}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid){
  if(!is_open)
    return -5;
  
  // Return 2 if (key,rid) does not exist
  uint16_t pageNum;
  if(FindEntryPage(key, pageNum))
    return -3;

  void * page = malloc(PF_PAGE_SIZE);
  if(fileHandle.ReadPage(pageNum, page) != 0)
    return -2;
  
  int offset = 0;
  if(findOnPage(page, key, offset, true) != 0)
    return -3;

  uint16_t free_pointer = 0;
  memcpy(&free_pointer, (char *)page + PF_PAGE_SIZE - 2, 2);

  int key_size;
  bool found = false;
  do  {

    if(offset >= free_pointer){
      // Not found anywhere
      free(page);
      return error("Entry not found on page", 2);
    }

    key_size = getKeySize((char *)page+offset);
  
    // If we want to implement duplicates we will need to continue searching here
    int cmp = keycmp(key, (char *)page + offset);
    if( cmp != 0 ){
      free(page);
      return error("Key not on page", 2);
    }

    offset += key_size;
   
    uint16_t key_pageNum = 0;
    uint16_t key_slotNum = 0;
    memcpy(&key_pageNum, (char *)page + offset, 2);
    offset += sizeof(key_pageNum);
    memcpy(&key_slotNum, (char *)page + offset, 2);
    offset += sizeof(key_slotNum);

    if(rid.pageNum == key_pageNum && rid.slotNum == key_slotNum){
      found = true;
    }
  } while(!found);

  // Actually delete the key by shifting records and the free pointer
  int subsequent_keys_size = free_pointer - offset;
  void *subsequent_keys = malloc(subsequent_keys_size);
  
  memcpy(subsequent_keys, (char *)page + offset, subsequent_keys_size);
  memcpy((char *)page + offset - key_size - 4, subsequent_keys, subsequent_keys_size);
  free(subsequent_keys);

  free_pointer -= (key_size + 4);
  memcpy((char *)page + PF_PAGE_SIZE - 2, &free_pointer,2);

  if(fileHandle.WritePage(pageNum, page) != 0)
    return -2;

  free(page);
  
  return 0;
}

IX_IndexScan::IX_IndexScan()
{
  page = malloc(PF_PAGE_SIZE);
  memset(page, 0, PF_PAGE_SIZE);

  more = false;
  highKeyInclusive = false;
  offset = 0;
}

IX_IndexScan::~IX_IndexScan()
{
  free(page);
}

RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle,
			  void *lowKey,
			  void *highKey,
			  bool lowKeyInclusive,
			  bool highKeyInclusive)
{
  if(!indexHandle.is_open)
    return -5;
  
  this->highKey = highKey;
  this->highKeyInclusive = highKeyInclusive;
  this->indexHandle = const_cast<IX_IndexHandle*>(&indexHandle);
  this->offset = 0;
  
  // Make sure that if either lowKey or highKey is null we search for infiitiy
  // TODO: Add test for this
  uint16_t lowPage = 0;
  if(this->indexHandle->FindEntryPage(lowKey,lowPage,false) != 0)
    return -3;

  // We need to find and set current

  // Read in the lowPage
  if(this->indexHandle->fileHandle.ReadPage(lowPage, page) != 0)
    return -2;
  
  if(lowKey != NULL) {
    // Search for the first key that meets our requirements
    
    if(this->indexHandle->findOnPage(page, lowKey, offset, lowKeyInclusive) != 0)
      return -3;

    uint16_t free_pointer = 0;
    memcpy(&free_pointer, (char *)page + PF_PAGE_SIZE - 2, 2);

    if( offset >= free_pointer )  {
      uint16_t next_page = 0;
      memcpy(&next_page, (char *)page + PF_PAGE_SIZE - 3 - 2, 2);
    
      if(next_page == 0){
	more = false;
	return 0;
      }

      if (this->indexHandle->fileHandle.ReadPage(next_page, page) != 0)
	return -2;

      offset = 0;
    }
  } // Otherwise we use the first record
  
  more = true;
  return 0;
}


// As of now get next entry returns the current and moves to the next element, by saving the offset
//  Depending on how we implement delete this could be an issue, because the offset will change after a delete. However, it may still work since we don't reread the page after each operation. This will work if we employ "merge right" on deletes
RC IX_IndexScan::GetNextEntry(RID &rid)
{
  if(!indexHandle->is_open)
    return -5;
  
  if(!more){
    return -1;
  }
  
  int shift_offset = 0;
  int key_on_page_size = this->indexHandle->getKeySize((char *)page+offset, &shift_offset);
  
  uint16_t tmpPage = 0;
  uint16_t tmpSlot = 0;
  memcpy(&tmpPage, (char *)page+offset+key_on_page_size, 2);
  memcpy(&tmpSlot, (char *)page+offset+key_on_page_size+2, 2);

  rid.pageNum = tmpPage;
  rid.slotNum = tmpSlot;

  uint16_t free_pointer = 0;
  memcpy(&free_pointer, (char *)page + PF_PAGE_SIZE - 2, 2);

  if(offset < free_pointer) {
    offset += key_on_page_size + 4;
  }

  if(offset >= free_pointer){
    // Move to the next page
    uint16_t next_page = 0;
    memcpy(&next_page, (char *)page + PF_PAGE_SIZE - 3 - 2, 2);
    
    if(next_page == 0){
      more = false;
      return 0;
    }
      
    if (this->indexHandle->fileHandle.ReadPage(next_page, page) != 0)
      return -2;
    
    offset = 0;
  }
  
  // Check if we passed the key
  if(highKey != NULL) { 
    // Read the current key
    void *key_on_page = (void *)((char *)page + offset);

    int cmp = this->indexHandle->keycmp(highKey, key_on_page);;

    if(cmp < 0 || (cmp == 0 && !highKeyInclusive) ) {
	more = false;
	return 0;
    }
  }

  return 0;
}

RC IX_IndexScan::CloseScan()
{
  // TODO: Add protection from reading from a closed scan
  highKey = NULL;
  indexHandle = NULL;
  offset = -1;
  memset(page, 0, PF_PAGE_SIZE);
  return 0;
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
  void* data = malloc(PF_PAGE_SIZE);
  int i = 0;
  while (rm_ScanIterator.getNextTuple(rid, data) != RM_EOF)
    {
      i += 1;
      if (indexHandle.InsertEntry(data, rid) != 0)
      	{
      	  cout << "fail " << endl;
      	  free(data);
      	  return -3;
      	}
    }

  free(data);
  return 0;
}

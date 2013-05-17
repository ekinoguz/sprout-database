#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <algorithm>

#include "rm.h"

#define TABLES_TABLE "tables"
#define COLUMNS_TABLE "columns"

#define DIRECTORY_ENTRY_SIZE 2
#define INITIAL_FREE_MEMORY PF_PAGE_SIZE - 4
#define COLUMNS_TABLE_RECORD_MAX_LENGTH 150   // It is actually 112
#define TABLES_TABLE_RECORD_MAX_LENGTH 150   // It is actually 121

RM RM::_rm;

RM* RM::Instance()
{
  if(!_rm.initialized){
    _rm.init();
  }
  return &RM::_rm;
}
RM::RM(){
  initialized = false;
}

void RM::init()
{
  initialized = true;

  pfm = PF_Manager::Instance(10);
  this->database_folder = DATABASE_FOLDER;

  // We may need to add this in if we think that every time the RM is initialized it should be on a 
  //    "clean" database.
  //system("rm -r "DATABASE_FOLDER);

  // Create the database 'somewhere on disk'
  if(pfm->CreateDirectory(database_folder) != 0){
    cout << "CreateDir failed " << endl;
  }
  
  // Create the tables table
  Attribute attr;
  vector<Attribute> table_attrs;
  attr.name = "table_name";
  attr.type = TypeVarChar;
  attr.length = 50;
  table_attrs.push_back(attr);
  
  attr.name = "file_location";
  attr.type = TypeVarChar;
  attr.length = 100;
  table_attrs.push_back(attr);

  attr.name = "type";
  attr.type = TypeVarChar;
  attr.length = 20;
  table_attrs.push_back(attr);
  
  attr.name = "version";
  attr.type = TypeShort;
  attr.length = 1;
  table_attrs.push_back(attr);

  // Ensure the columns table exists
  string file_url = database_folder + '/' + COLUMNS_TABLE;
  if( pfm->CreateFile(file_url) != 0)
    return;

  PF_FileHandle * fh = getFileHandle(COLUMNS_TABLE);
  if(fh == NULL)
    return;
  
  // Add the free space page to the newly created file
  void *data = malloc(PF_PAGE_SIZE);
  memset((char *)data, 0, PF_PAGE_SIZE);
  *(uint16_t *)data = 1;
  ((uint16_t *)data)[1] = INITIAL_FREE_MEMORY;
  fh->AppendPage(data);
  // When the page is first created the free block starts at 0
  memset((char *)data, 0, PF_PAGE_SIZE);
  fh->AppendPage(data);
  free(data);
  
  // Now that the columns table exists create the tables table
  if( this->createTable(TABLES_TABLE, table_attrs) != 0)
    return;

  // Create the columns table
  vector<Attribute> column_attrs;
  attr.name = "column_name";
  attr.type = TypeVarChar;
  attr.length = 30;
  column_attrs.push_back(attr);

  attr.name = "table_name";
  attr.type = TypeVarChar;
  attr.length = 50;
  column_attrs.push_back(attr);

  attr.name = "position";
  attr.type = TypeInt;
  attr.length = 4;
  column_attrs.push_back(attr);
  
  attr.name = "type";
  attr.type = TypeInt;
  attr.length = 4;
  column_attrs.push_back(attr);
  
  attr.name = "length";
  attr.type = TypeInt;
  attr.length = 4;
  column_attrs.push_back(attr);
  
  attr.name = "version";
  attr.type = TypeShort;
  attr.length = 1;
  column_attrs.push_back(attr);

  // Finish creating the columns table, by filling in the columns table
  this->addTableToCatalog(COLUMNS_TABLE, file_url, "heap"); 
  
  for(uint i=0; i < column_attrs.size(); i ++) {
    this->addAttributeToCatalog(COLUMNS_TABLE,i,column_attrs[i]);
  }
}

RM::~RM()
{
  // Close the file handles
  for (unordered_map<string,PF_FileHandle *>::iterator it = fileHandles.begin(); it != fileHandles.end(); ++it) {
    pfm->CloseFile(*it->second);
  }
}

RC RM::addAttributeToCatalog(const string tableName, uint position, const Attribute &attr, char version)
{
  RID rid;

  int num_fields = 6;

  int offset = 0;
  void *buffer = malloc(COLUMNS_TABLE_RECORD_MAX_LENGTH);

  // Forward pointer
  *((bool *)buffer + offset) = false;
  offset += sizeof(bool);

  // Version Info
  *((char *)buffer + offset) = (char)0;
  offset += sizeof(char);
  
  // num_fileds+1 pointers + 1 byte for forward ponter + 1 for version
  uint16_t field_offset = (num_fields+1)*2 + 2; 

  // Pointer to column name
  memcpy((char *)buffer + offset, &field_offset, 2);
  offset += 2;
  field_offset += attr.name.size();

  // Pointer to tablename
  memcpy((char *)buffer + offset, &field_offset, 2);
  offset += 2;
  field_offset += tableName.size();

  // Pointer to position
  memcpy((char *)buffer + offset, &field_offset, 2);
  offset += 2;
  field_offset += sizeof(position);

  // Pointer to column type
  memcpy((char *)buffer + offset, &field_offset, 2);
  offset += 2;
  field_offset += sizeof(attr.type);

  // Pointer to column length
  memcpy((char *)buffer + offset, &field_offset, 2);
  offset += 2;
  field_offset += sizeof(attr.length);

  // Pointer to version
  memcpy((char *)buffer + offset, &field_offset, 2);
  offset += 2;
  field_offset += 1;
  
  // End pointer
  memcpy((char *)buffer + offset, &field_offset, 2);
  offset += 2;
  
  memcpy((char *)buffer + offset, attr.name.c_str(), attr.name.size());
  offset += attr.name.size();

  memcpy((char *)buffer + offset, tableName.c_str(), tableName.size());
  offset += tableName.size();

  memcpy((char *)buffer + offset, &position, sizeof(position));
  offset += sizeof(position);

  memcpy((char *)buffer + offset, &attr.type, sizeof(attr.type));
  offset += sizeof(attr.type);

  memcpy((char *)buffer + offset, &attr.length, sizeof(attr.length));
  offset += sizeof(attr.length);

  memcpy((char *)buffer + offset, &version, 1);
  offset += 1;  
  
  RC ret = insertFormattedTuple(COLUMNS_TABLE, buffer, offset, rid);
  
  free(buffer);
  return ret;
}

RC RM::addTableToCatalog(const string tableName, const string file_url, const string type)
{
  RID rid;

  int num_fields = 4;

  int offset = 0;
  void *buffer = malloc(100);

  // Forward pointer
  *((bool *)buffer + offset) = false;
  offset += sizeof(bool);

  // Version Info
  *((char *)buffer + offset) = (char)0;
  offset += sizeof(char);
  
  // num_fileds+1 pointers + 1 byte for forward ponter + 1 for version
  uint16_t field_offset = (num_fields+1)*2 + 2; 

  // Pointer to table name
  memcpy((char *)buffer + offset, &field_offset, 2);
  offset += 2;
  field_offset += tableName.size();

  // Pointer to fileurl
  memcpy((char *)buffer + offset, &field_offset, 2);
  offset += 2;
  field_offset += file_url.size();

  // Pointer to type
  memcpy((char *)buffer + offset, &field_offset, 2);
  offset += 2;
  field_offset += type.size();

  // Pointer to latest_version
  memcpy((char *)buffer + offset, &field_offset, 2);
  offset += 2;
  field_offset += 1;

  // End pointer
  memcpy((char *)buffer + offset, &field_offset, 2);
  offset += 2;

  memcpy((char *)buffer + offset, tableName.c_str(), tableName.size());
  offset += tableName.size();

  memcpy((char *)buffer + offset, file_url.c_str(), file_url.size());
  offset += file_url.size();

  memcpy((char *)buffer + offset, type.c_str(), type.size());
  offset += type.size();

  // Table starts at version 0;
  memset((char *)buffer + offset, 0, 1);
  offset += 1;


  RC ret = insertFormattedTuple(TABLES_TABLE, buffer, offset, rid);
  
  free(buffer);
  return ret;
}

char RM::getLatestVersionFromCatalog(const string tableName)
{
  int position = 0;    // the position of the table name (zero based)
  AttrType type = TypeVarChar;
  RM_ScanFormattedIterator rm_ScanIterator;
  
  scanFormatted(TABLES_TABLE, position, type, EQ_OP, tableName.c_str(), rm_ScanIterator);

  char latest_version;
  RID rid;
  char *data = (char*)(malloc(256)); // Must be more than the actual maximum
 
  //memset(data, 0, 256);

  // Just return the first one we find
  if(rm_ScanIterator.getNextTuple(rid,data) == RM_EOF)
    return 255; // 255 is an error
  
  // Version is the 4th field
  int offset = 2 + 3*DIRECTORY_ENTRY_SIZE;

  uint16_t field_offset;
  // Copy the version info
  memcpy(&field_offset,data+offset,2);
  memcpy(&latest_version, data+field_offset,1);

  free(data);
  
  return latest_version;
}

RC RM::getAttributesFromCatalog(const string tableName, vector<Column> &columns, bool findAll, int version)
{
  int position = 1;   // the position of the table name (zero based)
  AttrType type = TypeVarChar;
  RM_ScanFormattedIterator rm_ScanIterator;
  scanFormatted(COLUMNS_TABLE, position, type, EQ_OP, tableName.c_str(), rm_ScanIterator);
  RID rid;
  char *data = (char*)(malloc(COLUMNS_TABLE_RECORD_MAX_LENGTH));

  if(!findAll && version == -1){
    version = (int)getLatestVersionFromCatalog(tableName);
    
    if(version == -1){
      cout << "Latest version cannot be found" << endl;
      return -1;
    }
  }

  while (rm_ScanIterator.getNextTuple(rid, data) != RM_EOF)
    {
      // First byte is the forward pointer bit which is 0 when scanning
      // Second byte is the version which is 1 when reading the catalog
      int offset = 2;

      Column column;
      
      uint16_t field_offset;
      uint16_t next_field;
      char * name;
      // Copy the column_name
      memcpy(&field_offset,data+offset,2);
      memcpy(&next_field,data+offset+DIRECTORY_ENTRY_SIZE,2);
      name = (char *)malloc(next_field-field_offset+1);
      name[next_field-field_offset] = '\0';
      memcpy(name, data+field_offset,next_field-field_offset);
      column.column_name = string(name);
      free(name);
      offset += 2;

      // Copy the table_name
      field_offset = next_field;
      memcpy(&next_field,data+offset+DIRECTORY_ENTRY_SIZE,2);
      name = (char *)malloc(next_field-field_offset+1);
      name[next_field-field_offset] = '\0';
      memcpy(name, data+field_offset,next_field-field_offset);
      column.table_name = string(name);
      free(name);
      offset += 2;

      // Copy the position
      field_offset = next_field;
      memcpy(&next_field,data+offset+DIRECTORY_ENTRY_SIZE,2);
      memcpy(&column.position, data+field_offset,next_field-field_offset);
      offset += 2;

      // Copy the type
      field_offset = next_field;
      memcpy(&next_field,data+offset+DIRECTORY_ENTRY_SIZE,2);
      memcpy(&column.type, data+field_offset,next_field-field_offset);
      offset += 2;

      // Copy the length
      field_offset = next_field;
      memcpy(&next_field,data+offset+DIRECTORY_ENTRY_SIZE,2);
      memcpy(&column.length, data+field_offset,next_field-field_offset);
      offset += 2;

      // Copy the version
      field_offset = next_field;
      memcpy(&next_field,data+offset+DIRECTORY_ENTRY_SIZE,2);
      memcpy(&column.version, data+field_offset,next_field-field_offset);
      offset += 2;

      if(!findAll && column.version != version)
	continue;

      // Add the read record to the attributes vector
      columns.push_back(column);
    }

  struct position_comp {
    bool operator() (Column a, Column b) {
      if( a.version < b.version )
	return true;
      else if( a.version == b.version ) 
	return (a.position < b.position);
      else 
	return false;
    }
  } mycomp;
  std::sort(columns.begin(), columns.end(), mycomp);
  
  free(data);

  return 0;
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs)
{
  string file_url = database_folder + '/' + tableName;
  RC ret = pfm->CreateFile(file_url);
  
  if(ret != 0)
    return ret;

  PF_FileHandle * fh = getFileHandle(tableName);
  if(fh == NULL)
    return -1;
  
  // Add the free space page
  void *data = malloc(PF_PAGE_SIZE);
  memset((char *)data, 0, PF_PAGE_SIZE);
  *(uint16_t *)data = 1;
  ((uint16_t *)data)[1] = INITIAL_FREE_MEMORY;
  fh->AppendPage(data);

  // When the page is first created the free block starts at 0
  memset((char *)data, 0, PF_PAGE_SIZE);
  fh->AppendPage(data);
  free(data);

  ret = this->addTableToCatalog(tableName, file_url, "heap"); 
  if(ret!=0)
    return ret;
  
  for(uint i=0; i < attrs.size(); i ++) {
    this->addAttributeToCatalog(tableName,i,attrs[i]);
  }
  
  return 0;
}

// Create an empty table after delete it, the table info is alreay in the catalog
RC RM::createTable(const string tableName)
{
  string file_url = database_folder + '/' + tableName;
  RC ret = pfm->CreateFile(file_url);
  
  if(ret != 0)
    return ret;

  PF_FileHandle * fh = getFileHandle(tableName);
  if(fh == NULL)
    return -1;
  
  // Add the free space page
  void *data = malloc(PF_PAGE_SIZE);
  memset((char *)data, 0, PF_PAGE_SIZE);
  *(uint16_t *)data = 1;
  ((uint16_t *)data)[1] = INITIAL_FREE_MEMORY;
  fh->AppendPage(data);

  // When the page is first created the free block starts at 0
  memset((char *)data, 0, PF_PAGE_SIZE);
  fh->AppendPage(data);
  free(data);

  return 0;
}

RC RM::deleteTable(const string tableName)
{
  // Close file if it's already opened
  if(closeFileHandle(tableName) != 0)
     return -1;

  
  // Delete the table form the tables table
  RM_ScanFormattedIterator tablesScanIterator;
  if(scanFormatted(TABLES_TABLE, 0, TypeVarChar, EQ_OP, tableName.c_str(), tablesScanIterator) != 0)
     return -1;

  RID rid;
  void *data = malloc(TABLES_TABLE_RECORD_MAX_LENGTH);

  if(tablesScanIterator.getNextTuple(rid, data) != 0)
    return -1;

  // TODO: Make this work for *ANY* file name
  // // Delete the table file
  // uint16_t fileLocAttributeOffset;
  // memcpy(&fileLocAttributeOffset, (char*)data + (3 * DIRECTORY_ENTRY_SIZE), DIRECTORY_ENTRY_SIZE);
  
  // uint16_t typeAttributeOffset;
  // memcpy(&typeAttributeOffset, (char*)data + (3 * DIRECTORY_ENTRY_SIZE), DIRECTORY_ENTRY_SIZE);
  
  // char* fileLoc = ((char*)(malloc(typeAttributeOffset - fileLocAttributeOffset + 1)));
  // memset(fileLoc, 0, typeAttributeOffset - fileLocAttributeOffset + 1);
  // memcpy(fileLoc, (char*)data + fileLocAttributeOffset, typeAttributeOffset - fileLocAttributeOffset);
  
  string fileLoc = DATABASE_FOLDER "/" + tableName;
  if(pfm->DestroyFile(fileLoc.c_str()) != 0)
    return -1;

  // Delete the table record
  if(deleteTuple(TABLES_TABLE, rid) != 0)
    return -1;

  free(data);

  // Delete the table info from the columns table
  RM_ScanFormattedIterator columnsScanIterator;
  scanFormatted(COLUMNS_TABLE, 1, TypeVarChar, EQ_OP, tableName.c_str(), columnsScanIterator);

  data = malloc(COLUMNS_TABLE_RECORD_MAX_LENGTH);

  while (columnsScanIterator.getNextTuple(rid, data) != RM_EOF)
    {
      if(deleteTuple(COLUMNS_TABLE, rid) != 0)
	return -1;
    }
  free(data);

  return 0;
}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs)
{
  vector<Column> columns;
  if(getAttributesFromCatalog(tableName, columns, false) != 0)
    return -1;

  Attribute attr;
  for(uint i=0; i < columns.size(); i++){
    attr.name = columns[i].column_name;
    attr.length = columns[i].length;
    attr.type = columns[i].type;
    attrs.push_back(attr);
  }
  
  return 0;
}
RC RM::insertTuple(const string tableName, const void *data, RID &rid, bool useRid)
{  
  // Get information on the latest attributes
  vector<Column> columns;
  if( getAttributesFromCatalog(tableName, columns, false) != 0){
    return -1;
  }
  
  if(columns.size() < 1) {
    cout << "catalog read error" << endl;
    return -1;
  }

  int max_length = 0;
  for(uint i=0; i < columns.size(); i++){
    max_length += columns[i].length;
    max_length += 2; // Account for the size of the directory
  }

  // First two bytes, and last directory
  max_length +=  4;

  void *buffer = malloc(max_length);
  
  // Forward pointer
  *((bool *)buffer) = false;
  
  // Version Info
  *((char *)buffer + 1) = (char)(columns[0].version);

  int data_offset = 0; // offset into data*
  int directory_offset = 2; // offset into the start of the directory 
  uint16_t field_offset = (columns.size()+1)*DIRECTORY_ENTRY_SIZE + 2; // offset to the start of the fields
  
  for(uint i=0; i < columns.size(); i++){
    if(columns[i].type == TypeVarChar){
      // Set the pointer
      memcpy((char *)buffer+directory_offset, &field_offset, 2);
      directory_offset += DIRECTORY_ENTRY_SIZE;

      // Read the length
      uint variable_length;
      memcpy(&variable_length, (char*)data+data_offset,4);
      data_offset+=4;

      if(variable_length > columns[i].length){
	cout << "VarChar is larger then maximum" << endl;
	return -1;
      }

      // Set the data
      memcpy((char *)buffer+field_offset, (char *)data+data_offset,variable_length);
      field_offset += variable_length;

      data_offset+=variable_length;

    } else { // We don't have any other variable length fields
      // Set the pointer
      memcpy((char *)buffer+directory_offset, &field_offset, 2);
      directory_offset += DIRECTORY_ENTRY_SIZE;

      // Set the data
      memcpy((char *)buffer+field_offset, (char *)data+data_offset,columns[i].length);
      field_offset += columns[i].length;

      data_offset+=columns[i].length;
    }
  }
  
  // End pointer
  memcpy((char *)buffer + directory_offset, &field_offset, 2);
  directory_offset += 2;

  if(insertFormattedTuple(tableName, buffer, field_offset, rid, useRid)!=0)
    return -1;

  free(buffer);

  return 0;
}

// In order to make use of useRid the rid must have been previously been deleted
//   this should really only be used for update.
RC RM::insertFormattedTuple(const string tableName, const void *data, const int length, RID &rid, bool useRid)
{
  PF_FileHandle * fh = this->getFileHandle(tableName);
  if(fh == NULL)
    return -1;
  
  void *page = malloc(PF_PAGE_SIZE);
  if( fh->ReadPage(0, page) != 0 )
    return -1;
  
  bool found = false;
  uint16_t num_pages; // This will hold the total number of pages. Not counting the first "directory" page
  uint16_t freespace = 0;
  
  uint16_t free_page;

  if(useRid){
    // Check if we have space on that page
    memcpy(&freespace, (char *)page+((rid.pageNum)*2), 2);
  }

  if(length == 0){
    cout << "This should never happen. tuple of length 0" << endl;
    return -1;
  }
    

  // If we have enough room (for an update) don't bother looking for free space
  if(freespace >= ((uint16_t)length)){
    free_page = rid.pageNum;
    found = true;
  }
  else {
    memcpy(&num_pages, page, 2);
    for(uint16_t i = 0; i < num_pages && !found; i++) {
    
      // Check the freespace at page i
      memcpy(&freespace, (char *)page+((i+1)*2), 2);

      if(freespace >= ((uint16_t)length + DIRECTORY_ENTRY_SIZE)){
	found = true;
	free_page = i + 1; //this is stored as the actual page index
      }
    }
  }

  uint16_t slotNum = -1;
  // Even if we need a forward pointer we still continue inserting as normal
  if(!found) {
    // No page with enough space existed, create a new page

    num_pages += 1;
    free_page = num_pages; 
    // Again note free_page needs to be in actual pages. 
    //  (e.g. if we have 2 free pages and want to add a new one free_page 
   //                         should equal 3. page 0 is the directory)

    // Increment the number of pages and set the free length.
    memcpy((char *)page,&num_pages,2);

    ((uint16_t *)page)[free_page] = INITIAL_FREE_MEMORY - length - DIRECTORY_ENTRY_SIZE;
    if( fh->WritePage(0,page) != 0 )
      return -1;

    // Prep page to be written to the next location
    memset((char *)page, 0, PF_PAGE_SIZE);
    *((uint16_t *)((char*)page+PF_PAGE_SIZE-2)) = length; // Update the free list pointer

    // Insert the record
    memcpy((char *)page,data,length);

    // Set the number of records
    *((uint16_t *)((char*)page+PF_PAGE_SIZE- DIRECTORY_ENTRY_SIZE - 2)) = 1;

    // Set the first slot to point to the right place
    *((uint16_t *)((char*)page+PF_PAGE_SIZE- DIRECTORY_ENTRY_SIZE*2 - 2)) = 0;
   
    slotNum = 0; // Slot numbers are zero based, we jsut can't forget about the first two bytes storing length

    if(!useRid){
      rid.pageNum = free_page;
      rid.slotNum = slotNum; 
    }

    if(fh->AppendPage(page)!=0)
      return -1;
  }
  else{ // We have enough space so insert at the correct location
    // First update free_space information on the first page

    // TODO: Check that we won't walk off the end of the first page
    ((uint16_t *)page)[free_page] = ((uint16_t *)page)[free_page] - length - DIRECTORY_ENTRY_SIZE;
    if(fh->WritePage(0, page))
      return -1;
    
    if(fh->ReadPage(free_page,page) != 0)
      return -1;
    
    // Where does the free block begin
    uint16_t offset;
    memcpy(&offset,(char *)page+PF_PAGE_SIZE-2, 2);

    uint16_t number_of_records;
    memcpy(&number_of_records, (char*)page+PF_PAGE_SIZE-4,2);

    // These will help with update
    slotNum = number_of_records;
    int directory_length = DIRECTORY_ENTRY_SIZE;

    if(useRid && free_page == rid.pageNum){
      slotNum = rid.slotNum;
      directory_length = 0;
    }
     
    // Do we have enough room as is
    if(PF_PAGE_SIZE - offset - 4 - number_of_records*DIRECTORY_ENTRY_SIZE< length + directory_length){
      if(reorganizePage(tableName, free_page) != 0){
	return -1;
      }
      
      // Refresh the page after the reorg.
      fh->ReadPage(free_page, page);

      // Where does the new free block begin
      memcpy(&offset,(char *)page+PF_PAGE_SIZE-2, 2);
    }

    // Insert the record
    memcpy((char *)page+offset,data,length);

    // Update the directory

    if(!(useRid && free_page == rid.pageNum)){
      number_of_records++;
      memcpy((char*)page+ PF_PAGE_SIZE - 4, &number_of_records,2);
    }
    
    // Store the offset pointer in slotNum
    memcpy((char*)page+PF_PAGE_SIZE-4-(slotNum+1)*DIRECTORY_ENTRY_SIZE, &offset,2);

    // Update the free pointer
    offset += length;
    memcpy((char *)page+PF_PAGE_SIZE-2,&offset, 2);

    // We don't update rid if this is an update
    if(!useRid){
      rid.pageNum = free_page;
      rid.slotNum = slotNum;
    }

    if(fh->WritePage(free_page,page)!=0)
      return -1;   
  }
  

  // Do we need to write a forward pointer
  if(useRid && free_page != rid.pageNum){ // If we aren't inserting on the appropriate page.
    int forward_pointer_length = 6;
    char * forward_pointer = (char *)malloc(6);
    
    *forward_pointer = 1; // Free pointer
    *(forward_pointer+1) = 0; // Version

    memcpy(forward_pointer+2,&free_page,2);
    memcpy(forward_pointer+4,&slotNum,2);
    
    // read in the free page again and update the free_space info
    if( fh->ReadPage(0,page) != 0 )
      return -1;

    ((uint16_t *)page)[rid.pageNum] = ((uint16_t *)page)[rid.pageNum] - 6;

    if(fh->WritePage(0, page))
      return -1;
    
    // Read in the old page
    if(fh->ReadPage(rid.pageNum,page) != 0)
      return -1;

     // Where does the free block begin
    uint16_t offset;
    memcpy(&offset,(char *)page+PF_PAGE_SIZE-2, 2);
    
    uint16_t number_of_records;
    memcpy(&number_of_records, (char*)page+PF_PAGE_SIZE-4,2);
  
    // Do we have enough room as is
    if(PF_PAGE_SIZE - offset  - 4 - number_of_records*DIRECTORY_ENTRY_SIZE < forward_pointer_length){
      if(reorganizePage(tableName, rid.pageNum) != 0)
	return -1;

      // Reload the page
      if(fh->ReadPage(rid.pageNum, page) != 0)
	return -1;

      // Where does the new free block begin
      memcpy(&offset,(char *)page+PF_PAGE_SIZE-2, 2);
    }

    // Insert the record
    memcpy((char *)page+offset,forward_pointer,forward_pointer_length);

    // Store the offset pointer in slot rid.slotNum
    memcpy((char*)page+PF_PAGE_SIZE-4-(rid.slotNum+1)*DIRECTORY_ENTRY_SIZE, &offset,2);
    
    // Update the free pointer
    offset += forward_pointer_length;
    memcpy((char *)page+PF_PAGE_SIZE-2,&offset, 2);

    if(fh->WritePage(rid.pageNum,page) != 0)
      return -1;

    free(forward_pointer);
  }
  free(page); // TODO: Free will not be called if we have a failure
  return 0;
}


RC RM::deleteTuples(const string tableName)
{
  // Close file if it's already opened
  if(closeFileHandle(tableName)!=0)
    return -1;

  // Delete the table form the tables table
  RM_ScanFormattedIterator tablesScanIterator;
  scanFormatted(TABLES_TABLE, 0, TypeVarChar, EQ_OP, tableName.c_str(), tablesScanIterator);

  RID rid;
  void *data = malloc(TABLES_TABLE_RECORD_MAX_LENGTH);

  if(tablesScanIterator.getNextTuple(rid, data) == RM_EOF)
    return -1;

  // Delete the table file
  uint16_t fileLocAttributeOffset;
  memcpy(&fileLocAttributeOffset, (char*)data + 2 + (1 * DIRECTORY_ENTRY_SIZE), DIRECTORY_ENTRY_SIZE);
  
  uint16_t typeAttributeOffset;
  memcpy(&typeAttributeOffset, (char*)data + 2 + (2 * DIRECTORY_ENTRY_SIZE), DIRECTORY_ENTRY_SIZE);
  
  char* fileLoc = ((char*)(malloc(typeAttributeOffset - fileLocAttributeOffset + 1)));
  memset(fileLoc, 0, typeAttributeOffset - fileLocAttributeOffset + 1);
  memcpy(fileLoc, (char*)data + fileLocAttributeOffset, typeAttributeOffset - fileLocAttributeOffset);


  if(pfm->DestroyFile(fileLoc)!=0)
    return -1;

  free(data);
  free(fileLoc);

  // Create the table file again
  createTable(tableName);

  return 0;
}
RC RM::deleteTuple(const string tableName, const RID &rid)
{
  PF_FileHandle *fh = getFileHandle(tableName);
  if(fh == NULL)
    return -1;
  void* data = malloc(PF_PAGE_SIZE);

  // Read first page
  void* firstPage = malloc(PF_PAGE_SIZE);
  fh->ReadPage(0, firstPage);

  int pageNum = rid.pageNum;
  int slotNum = rid.slotNum;
  bool done = false;
  while(!done)
    {
      // Read page
      if(fh->ReadPage(pageNum, data) != 0)
	return -1;

      // Read number of records on the page
      uint16_t numOfRecords;

      //Last two bytes contain the offset of the free space on the page
      memcpy(&numOfRecords, (char*)data + PF_PAGE_SIZE - 4, DIRECTORY_ENTRY_SIZE);

      // No such record
      if (slotNum >= numOfRecords)
	{
	  cout << "Not enough records to perofrm delete " << endl;
	  return -1;
	}

      // Read record offset
      uint16_t recordOffset;
      memcpy(&recordOffset, (char*)data + PF_PAGE_SIZE - 4 - ((slotNum+1) * DIRECTORY_ENTRY_SIZE), DIRECTORY_ENTRY_SIZE);

      // Mark record as deleted (0xFFFF is the value that indicate that the field is deleted)
      memset((char *)data + PF_PAGE_SIZE - 4 - ((slotNum+1) * DIRECTORY_ENTRY_SIZE), 0xFFFF, DIRECTORY_ENTRY_SIZE);

      // Write page
      fh->WritePage(pageNum, data);

      // Read records forward pointer bit
      uint8_t forwardPointer;
      memcpy(&forwardPointer, (char*)data+recordOffset, 1);

      uint16_t recordLength;
      if (forwardPointer == 0)
	{
	  // Read record length
	  uint16_t firstAttributeOffset;
	  memcpy(&firstAttributeOffset, (char*)data + recordOffset + 2, 2);
	  memcpy(&recordLength, (char*)data + recordOffset + firstAttributeOffset - 2, 2);

	  done = true;
	}
      else
	{
	  recordLength = 6;
	}

      // Update the free space on the first page
      uint16_t freeSpace;
      memcpy(&freeSpace, (char*)firstPage + (pageNum * 2), 2);      
      ((uint16_t *)firstPage)[pageNum] = freeSpace + recordLength;

      if (forwardPointer != 0)
	{
	  memcpy(&pageNum, (char*)data + recordOffset + 2, 2);
	  memcpy(&slotNum, (char*)data + recordOffset + 4, 2);
	}
    }

  fh->WritePage(0, firstPage);

  free(firstPage);
  free(data);
  return 0;
}
// Assume the rid does not change after update
RC RM::updateTuple(const string tableName, const void *data, const RID &rid)
{
  if (deleteTuple(tableName, rid) != 0)
      return -1;

  // Hackery to get around the way we modify insertTuple to also do updates
  return insertTuple(tableName, data, *(const_cast<RID*>(&rid)), true);
}

// Translate record in page format into fucked up data format
RC RM::translateTuple(void * data, const void *record, const vector<Column> &currentColumns, const vector<Column> &targetColumns){
  // Read record version
  uint8_t version;
  memcpy(&version, (char*)record + 1, 1);

  int offset = 0;

  uint i,j;
  for (i = 0; i < targetColumns.size(); i++)
    {
      for(j = 0; j < currentColumns.size(); j++)
	{
	  if(currentColumns[j].column_name == targetColumns[i].column_name &&
	     currentColumns[j].type == targetColumns[i].type &&
	     currentColumns[j].length == targetColumns[i].length)
	    {
	      int position = currentColumns[j].position;
	      
	      uint16_t field_offset;
	      memcpy(&field_offset,(char*)record+2+position*DIRECTORY_ENTRY_SIZE,DIRECTORY_ENTRY_SIZE);

	      uint16_t field_end;
	      memcpy(&field_end,(char*)record+2+(position+1)*DIRECTORY_ENTRY_SIZE,DIRECTORY_ENTRY_SIZE);
	
	      int length = field_end - field_offset;
	      if(targetColumns[i].type == TypeVarChar){
		memcpy((char*)data+offset, &length, 4);
		offset+=4;		
	      }

	      memcpy((char*)data+offset, (char *)record+field_offset, length);
	      offset += length;
	      
	      break;
	    }
	    
	}

      // The column wasn't present
      if(j == currentColumns.size()){
	if(targetColumns[i].type == TypeVarChar){
	  memset((char *)data+offset, 0,4);
	  offset+= 4;
	}
	else {
	  memset((char *)data+offset, 0,targetColumns[i].length);
	  offset+= targetColumns[i].length;
	}
      }
    }
  
  return 0;
}
RC RM::readTuple(const string tableName, const RID &rid, void *data)
{
  void *record = malloc(PF_PAGE_SIZE);
  if(readFormattedTuple(tableName, rid, record) != 0){
    free(record);
    return -1;
  }

  uint8_t version;
  memcpy(&version, (char*)record + 1, 1);

  vector<Column> currentColumns;
  getAttributesFromCatalog(tableName, currentColumns, false, version);

  vector<Column> latestColumns;
  getAttributesFromCatalog(tableName, latestColumns, false);

  RM::translateTuple(data,record, currentColumns, latestColumns);

  free(record);
  return 0;
}
RC RM::readFormattedTuple(const string tableName, const RID &rid, void *data)
{
  PF_FileHandle *fh = getFileHandle(tableName); 
  if(fh==NULL)
    return -1;

  void *page = malloc(PF_PAGE_SIZE);

  int pageNum = rid.pageNum;
  int slotNum = rid.slotNum;
  bool done = false;
  while (!done)
    {
      // Read page
      if(fh->ReadPage(pageNum, page) != 0){
	free(page);
	return -1;
      }

      uint16_t numOfRecords;
      //Last two bytes contain the offset of the free space on the page
      memcpy(&numOfRecords, (char*)page + PF_PAGE_SIZE - 4, DIRECTORY_ENTRY_SIZE);
      // No such record
      if (slotNum >= numOfRecords)
	{
	  cout << "Not enough records on this page [" << slotNum << ":" << numOfRecords << "]" << endl;
	  free(page);
	  return -1;
	}

      // Read record offest
      uint16_t recordOffset;
      memcpy(&recordOffset, (char*)page + PF_PAGE_SIZE - 4 - ((slotNum+1) * DIRECTORY_ENTRY_SIZE), DIRECTORY_ENTRY_SIZE);

      if(recordOffset == 0xFFFF){
	free(page);
	return -1;
      }

      // Read forward pointer bit
      uint8_t forwardPointer;
      memcpy(&forwardPointer, (char*)page + recordOffset, 1);

      if (forwardPointer == 0)
	{
	  // Read the record length
	  uint16_t firstAttributeOffset;
	  memcpy(&firstAttributeOffset, (char*)page + recordOffset + 2, DIRECTORY_ENTRY_SIZE);

	  uint16_t recordLength;
	  memcpy(&recordLength, (char*)page + recordOffset + firstAttributeOffset - DIRECTORY_ENTRY_SIZE, DIRECTORY_ENTRY_SIZE);

	  memcpy(data, (char*)page + recordOffset, recordLength);

	  done = true;
	}
      else
	{
	  memcpy(&pageNum, (char*)page + recordOffset + 2, 2);
	  memcpy(&slotNum, (char*)page + recordOffset + 4, 2);
	}
    }

  free(page);
  return 0;
}
RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data)
{
  void *record = malloc(PF_PAGE_SIZE);
  if(readFormattedTuple(tableName, rid, record) != 0){
    free(record);
    return -1;
  }

  // Read record version
  uint8_t version;
  memcpy(&version, (char*)record + 1, 1);

  vector<Column> columns;
  if( getAttributesFromCatalog(tableName, columns, false, version) != 0)
    {
      free(record);
      return -1;
    }

  // Read the position and the type of the target attribute
  int position;
  AttrType type;
  
  bool found = false;
  for (uint i = 0; i < columns.size(); i++)
    {
      if (columns.at(i).column_name == attributeName)
	{
	  position = columns.at(i).position;
	  type = columns.at(i).type;
	  
	  found = true;
	}
    }

  if(!found)
    {
      // We need to read the latest to see what to return
      columns.clear();
      if( getAttributesFromCatalog(tableName, columns, false) != 0)
	{
	  free(record);
	  return -1;
	}

      for (uint i = 0; i < columns.size(); i++)
	{
	  if (columns.at(i).column_name == attributeName)
	    {
	      type = columns.at(i).type;
	      
	      if(type == TypeVarChar){
		*((uint *)data) = 0;
	      } else {
		memset(data,0,columns[i].length);
	      }	    
	      
	      free(record);
	      return 0;
	    }
	}
      
      // We didn't find it in latest columns either

      free(record);
      return -1;
    }

  // Make sure it still exists in the latest version
  columns.clear();
  if( getAttributesFromCatalog(tableName, columns, false) != 0){
    free(record);
    return -1;
  }

  bool not_in_latest = true;
  for (uint i = 0; i < columns.size(); i++)
    {
      if (columns.at(i).column_name == attributeName && columns[i].type == type && columns[i].position == position){
	not_in_latest = false;
      }

    }

  if(not_in_latest){
    free(record);
    return -1;
  }
  
  uint16_t attributeOffset;
  memcpy(&attributeOffset, (char*)record + 2 + (position * DIRECTORY_ENTRY_SIZE), DIRECTORY_ENTRY_SIZE);

  uint16_t nextAttributeOffset;
  memcpy(&nextAttributeOffset, (char*)record + 2 + ((position + 1) * DIRECTORY_ENTRY_SIZE), DIRECTORY_ENTRY_SIZE);

  int length = ((int)(nextAttributeOffset - attributeOffset));

  if (type == TypeVarChar)
    {
      memcpy(data, &length, 4);
      memcpy((char*)data + 4, (char*)record + attributeOffset, length);
    }
  else
    {
      memcpy(data, (char*)record + attributeOffset, length);
    }

  free(record);
  return 0;
}
RC RM::reorganizePage(const string tableName, const unsigned pageNumber)
{
  void *reorganized_page = malloc(PF_PAGE_SIZE);
  memset(reorganized_page, 0, PF_PAGE_SIZE);

  PF_FileHandle *fh =  getFileHandle(tableName);
  if(fh == NULL)
    return -1;
  void *page = malloc(PF_PAGE_SIZE);

  if (fh->ReadPage(pageNumber, page) != 0)
    {
      return -1;
    }

  // Read the number of records on page
  uint16_t records_number;
  memcpy(&records_number, (char*)page + PF_PAGE_SIZE - 4, DIRECTORY_ENTRY_SIZE);
  memcpy((char*)reorganized_page + PF_PAGE_SIZE - 4, &records_number, DIRECTORY_ENTRY_SIZE);

  uint16_t record_offset;
  uint16_t offset = 0;
  for (uint16_t i = 0; i < records_number; i++)
    {
      memcpy(&record_offset, (char*)page + PF_PAGE_SIZE - 4 - ((i + 1) * DIRECTORY_ENTRY_SIZE), DIRECTORY_ENTRY_SIZE);

      if (record_offset == 0xFFFF)
	{
	  // Mark the record as deleted on the new page
	  uint16_t deleted = 0xFFFF;
	  memcpy((char*)reorganized_page + PF_PAGE_SIZE - 4 - ((i + 1) * DIRECTORY_ENTRY_SIZE), &deleted, DIRECTORY_ENTRY_SIZE);

	  continue;
	}

      // Read records forward pointer bit
      uint8_t forward_pointer;
      memcpy(&forward_pointer, (char *)page+record_offset, 1);

      uint16_t record_length;
      // It is not a forward_pointer
      if (forward_pointer == 0)
	{
	  // Read record length
	  uint16_t first_attribute_offset;
	  memcpy(&first_attribute_offset, (char*)page + record_offset + 2, 2);
	  memcpy(&record_length, (char*)page + record_offset + first_attribute_offset - 2, 2);
	}
      else
	{
	  record_length = 6;
	}
      
      // Write new record offset in the records directory
      memcpy((char*)reorganized_page + PF_PAGE_SIZE - 4 - ((i + 1) * DIRECTORY_ENTRY_SIZE), &offset, DIRECTORY_ENTRY_SIZE);
      
      // Copy the record to the organized page
      memcpy((char*)reorganized_page + offset, (char*)page + record_offset, record_length);
      offset += record_length;
    }

  // Write the free space pointer
  memcpy((char*)reorganized_page + PF_PAGE_SIZE - 2, &offset, DIRECTORY_ENTRY_SIZE);

  if (fh->WritePage(pageNumber, reorganized_page) != 0)
    {
      return -1;
    }

  free(page);
  free(reorganized_page);

  return 0;
}
RC RM::scanFormatted(const string tableName,
      const int position, 
      const AttrType type,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      RM_ScanFormattedIterator &rm_ScanIterator)
{
  Column column;
  column.position = position;
  column.type = type;
  column.version = 0;

  vector<Column> columns;
  columns.push_back(column);
  
  return scanFormatted(tableName, columns, compOp, value, rm_ScanIterator);
}
RC RM::scanFormatted(const string tableName,
      const vector<Column> columns,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      RM_ScanFormattedIterator &rm_ScanIterator)
{ 
  rm_ScanIterator.columns = columns;
  rm_ScanIterator.fh = getFileHandle(tableName);
  if(rm_ScanIterator.fh == NULL){
    return -1;
  }

  rm_ScanIterator.compOp = compOp;
  rm_ScanIterator.value = value;

  return 0;
}

RC RM::scan(const string tableName,
      const string conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator)
{
  vector<Column> columns;
  if(getAttributesFromCatalog(tableName, columns) != 0)
    return -1;

  vector<Column> projectedColumns;
  vector<Column> conditionColumns;
  
  for(uint i=0;i<columns.size();i++){
    if( columns[i].column_name == conditionAttribute )
      conditionColumns.push_back(columns[i]);
    
    for(uint j=0;j<attributeNames.size();j++)
      if( columns[i].column_name == attributeNames[j] )
	projectedColumns.push_back(columns[i]);
  }

  rm_ScanIterator.projectedColumns = projectedColumns;
  if(conditionColumns.size() == 0 && conditionAttribute != ""){
    cout << "No matching column found" << endl;
    return -1;
  }
  if(projectedColumns.size() == 0){
    cout << "No matching projection found" << endl;
    return -1;
  }
  return scanFormatted(tableName, conditionColumns, compOp, value, rm_ScanIterator);
}

// Get next tuple preemptively loads the next page.
RC RM_ScanFormattedIterator::getNextTuple(RID &rid, void *data){
  bool condition = false;
  while(!condition) {
    if(current.pageNum >= fh->GetNumberOfPages())
      return RM_EOF;

    if(buffered_page != current.pageNum) {
      if(fh->ReadPage(current.pageNum,page) != 0)
	return -2;

      uint16_t number_of_records;
      memcpy(&number_of_records,(char*)page+PF_PAGE_SIZE-4,2);

      if(number_of_records == 0){
	current.pageNum++;
	continue;       // TODO: Verify this continue works (write a test case for it)
      }

    }

    uint16_t offset, first_field, end_offset;
    memcpy(&offset,(char*)page+PF_PAGE_SIZE-4-((current.slotNum+1)*DIRECTORY_ENTRY_SIZE),2);
  
    // If this record wasn't deleted
    if((offset != 0xFFFF)) {
      memcpy(&first_field,(char*)page+offset+2,2);
      memcpy(&end_offset,(char*)page+offset+first_field-DIRECTORY_ENTRY_SIZE,2);
      
      // Copy in the data
      memcpy(data,(char*)page+offset,end_offset);

      // If we are not a forward pointer and this is a NO_OP      
      if(compOp == NO_OP && *(char *)data == 0)
	condition = true;
      
      // If we are not a forward pointer
      else if(*(char *)data == 0) {
	
	int version = *((char *)data+1);
	int position;
	AttrType type;

	for(uint i=0;i<columns.size();i++){
	  if(columns[i].version == version){
	    position = columns[i].position;
	    type = columns[i].type;
	  }
	}
	
	// We should check that position/type are set to see if a version was found...
       
	// Grab the field offset at position +1 and then subtract the offset at position
	uint16_t length = *((uint16_t *)data+1+position+1) - *((uint16_t *)data+1+position);
	
	void *lvalue = malloc(length+1);
	memset(lvalue,0,length+1); // Make sure strings have a null terminator
	
	memcpy(lvalue,(char*)data + *((uint16_t*)data+1+position),length);

	switch(compOp){
	case EQ_OP:
	case NE_OP:
	  switch(type){
	  case TypeInt:
	    condition = ( *(int*)lvalue == *(int*)value );
	    break;
	  case TypeReal:
	    condition = (*(float*)lvalue == *(float*)value); 
	    break;
	  case TypeVarChar:
	    if( strcmp((char *)lvalue,(char *)value ) == 0 )
	      condition = true;

	    break;
	  case TypeShort:
	    condition = (*(char*)lvalue == *(char*)value);
	    break;  
	  case TypeBoolean:
	    condition = (*(bool*)lvalue == *(bool*)value);
	    break;  
	  }    
	  condition = condition ^ (compOp == NE_OP);

	  break;
	case LT_OP:
	case GE_OP:
	  switch(type){
	  case TypeInt:
	    condition = ( *(int*)lvalue < *(int*)value );
	    break;
	  case TypeReal:
	    condition = (*(float*)lvalue < *(float*)value); 
	    break;
	  case TypeVarChar:
	    if( strcmp((char *)lvalue,(char *)value ) < 0 )
	      condition = true;
	    break;
	  case TypeShort:
	    condition = (*(char*)lvalue < *(char*)value);
	    break;  
	  case TypeBoolean:
	    condition = (*(bool*)lvalue != *(bool*)value);
	    break;  
	  }    
	  condition = condition ^ (compOp == GE_OP);
	  break;
	case GT_OP:
	case LE_OP:
	  switch(type){
	  case TypeInt:
	    condition = ( *(int*)lvalue > *(int*)value );
	    break;
	  case TypeReal:
	    condition = (*(float*)lvalue > *(float*)value); 
	    break;
	  case TypeVarChar:
	    if( strcmp((char *)lvalue,(char *)value ) > 0 )
	      condition = true;
	    break;
	  case TypeShort:
	    condition = (*(char*)lvalue > *(char*)value);
	    break;  
	  case TypeBoolean:
	    condition = (*(bool*)lvalue != *(bool*)value);
	    break;  
	  }    
	  condition = condition ^ (compOp == LE_OP);
	  break;
	case NO_OP: // We should never actually reach here
	  condition = true;
	  break;
	default:
	  cout << "Op not supported" << endl;
	  free(lvalue);
	  return -3;
	}
	free(lvalue);
      }
    } 

    // Update rid
    rid.pageNum = current.pageNum;
    rid.slotNum = current.slotNum;
  
    // increment current
    uint16_t number_of_records;
    memcpy(&number_of_records,(char*)page+PF_PAGE_SIZE-4,2);
    current.slotNum++;

    if(number_of_records <= current.slotNum){
      current.slotNum = 0;
      current.pageNum++;
    }
    
  }// end while

  return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
  
  void * buffer = malloc(PF_PAGE_SIZE);
  switch(RM_ScanFormattedIterator::getNextTuple(rid,buffer)){
  case 0:
    break;
  case RM_EOF:
    free(buffer);
    return RM_EOF;
  default:
    free(buffer);
    return -2;
  }
  char version = *((char *)buffer+1);
  int latest_version = projectedColumns[projectedColumns.size()-1].version;
  vector<Column> currentColumns;
  vector<Column> latestColumns;
  for(uint i=0; i < projectedColumns.size(); i++){
    if(projectedColumns[i].version == (int)version){
      currentColumns.push_back(projectedColumns[i]);
    }

    if(projectedColumns[i].version == (int)latest_version)
      latestColumns.push_back(projectedColumns[i]);
  }

  
  RC rc = RM::translateTuple(data, buffer, currentColumns, latestColumns);
  free(buffer);

  return rc;
}

PF_FileHandle * RM::getFileHandle(const string tableName) 
{
  unordered_map<string,PF_FileHandle *>::const_iterator got = fileHandles.find (tableName);

  if ( got == fileHandles.end() )
    {
      PF_FileHandle * fh = new PF_FileHandle(); 
      if(pfm->OpenFile((database_folder+'/'+tableName).c_str(), *fh) != 0)
	return NULL;

      fileHandles[tableName] = fh;
    }

  return fileHandles[tableName];
}

RC RM::closeFileHandle(const string tableName) 
{
  unordered_map<string,PF_FileHandle *>::const_iterator got = fileHandles.find (tableName);

  if ( got != fileHandles.end() )
    {
      if (pfm->CloseFile(*(fileHandles[tableName])) != 0)
	{
	  return -1;
	}
      
      delete fileHandles[tableName];
      fileHandles.erase(tableName);
    }

  return 0;
}

// Extra stuff
RC RM::dropAttribute(const string tableName, const string attributeName)
{
  // Read the latest version of the attributes from the catalog
  vector<Column> latest_columns;
  if (getAttributesFromCatalog(tableName, latest_columns, false) != 0)
    {
      return -1;
    }

  // Calculate the new version
  uint8_t new_version = latest_columns[0].version + 1;
  int position = latest_columns[0].position;

  // Insert the new version attributes in the catalog
  for (uint i = 0; i < latest_columns.size(); i++)
    {
      if (latest_columns[i].column_name == attributeName)
	{
	  continue;
	}

      Attribute attr;
      attr.name = latest_columns[i].column_name;
      attr.type = latest_columns[i].type;
      attr.length = latest_columns[i].length;
      
      addAttributeToCatalog(tableName, position, attr, new_version);
      
      // Advance the position
      position++;
    }



  // Write the new version number to the tables table
  if (updateTablesTableLatestVersion(tableName, new_version) != 0)
    {
      return -1;
    }

  return 0;
}

RC RM::addAttribute(const string tableName, const Attribute attr)
{
  // Read the latest version of the attributes from the catalog
  vector<Column> latest_columns;
  if (getAttributesFromCatalog(tableName, latest_columns, false) != 0)
    {
      return -1;
    }

  // Calculate the new version
  uint8_t new_version = latest_columns[0].version + 1;

  // Insert the new version attributes in the catalog
  for (uint i = 0; i < latest_columns.size(); i++)
    {
      Attribute attr;
      attr.name = latest_columns[i].column_name;
      attr.type = latest_columns[i].type;
      attr.length = latest_columns[i].length;
      
      addAttributeToCatalog(tableName, latest_columns[i].position, attr, new_version);
    }


  // Add the new attribute
  addAttributeToCatalog(tableName, latest_columns[latest_columns.size()-1].position+1, attr, new_version);

  // Write the new version number to the tables table
  if (updateTablesTableLatestVersion(tableName, new_version) != 0)
    {
      return -1;
    }

  return 0;
}

RC RM::updateTablesTableLatestVersion(const string tableName, uint8_t new_version)
{
  // Write the new version number to the tables table
  RM_ScanFormattedIterator scanFormattedIterator;
  scanFormatted(TABLES_TABLE, 0, TypeVarChar, EQ_OP, tableName.c_str(), scanFormattedIterator);

  void *table_tuple = malloc(TABLES_TABLE_RECORD_MAX_LENGTH);
  memset(table_tuple, 0, TABLES_TABLE_RECORD_MAX_LENGTH);

  RID tables_rid;
  if(scanFormattedIterator.getNextTuple(tables_rid, table_tuple)!=0)
    return -1;

  void * page = (void *) malloc(PF_PAGE_SIZE);
  
  PF_FileHandle * fh = getFileHandle(TABLES_TABLE);
  if(fh == NULL)
    return -1;
  if(fh->ReadPage(tables_rid.pageNum, page)!=0){
    return -1;
  }
  
  uint16_t tupleOffset;
  memcpy(&tupleOffset, (char *)page+PF_PAGE_SIZE-(tables_rid.slotNum+1)*DIRECTORY_ENTRY_SIZE-4, 2);
  
  // Position of the version
  int position = 3;
  uint16_t field_offset;

  memcpy(&field_offset, (char*)page+tupleOffset+position*DIRECTORY_ENTRY_SIZE+2, DIRECTORY_ENTRY_SIZE);
  memcpy((char*)page + tupleOffset + field_offset, &new_version, 1);

  if(fh->WritePage(tables_rid.pageNum, page)!=0){
    return -1;
  }
  
  free(page);
  free(table_tuple);

  return 0;
}

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstdint>

#include "rm.h"

#define TABLES_TABLE "tables"
#define COLUMNS_TABLE "columns"

#define DIRECTORY_ENTRY_SIZE 2
#define INITIAL_FREE_MEMORY PF_PAGE_SIZE - 4

RM* RM::_rm = 0;

RM* RM::Instance()
{
    if(!_rm)
        _rm = new RM();

    return _rm;
}

RM::RM()
{
  pfm = PF_Manager::Instance(10);
  this->database_folder = DATABASE_FOLDER;

  // We may need to add this in if we think that every time the RM is initialized it should be on a 
  //    "clean" database.
  //system("rm -r "DATABASE_FOLDER);

  // Create the database 'somewhere on disk'
  pfm->CreateDirectory(database_folder);

  
  // Create the tables table
  Attribute attr;
  vector<Attribute> table_attrs;
  attr.name = "table_name";
  attr.type = TypeVarChar;
  attr.length = 50;
  table_attrs.push_back(attr);
  
  attr.name = "file_location";
  attr.type = TypeVarChar;
  attr.length = 50;
  table_attrs.push_back(attr);

  attr.name = "type";
  attr.type = TypeVarChar;
  attr.length = 20;
  table_attrs.push_back(attr);

  // Ensure the columns table exists
  string file_url = database_folder + '/' + COLUMNS_TABLE;
  if( pfm->CreateFile(file_url) != 0)
    return;

  PF_FileHandle * fh = getFileHandle(COLUMNS_TABLE);
  
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

  attr.name = "nullable";
  attr.type = TypeBoolean;
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

  int num_fields = 7;

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

  // Pointer to nullable
  memcpy((char *)buffer + offset, &field_offset, 2);
  offset += 2;
  field_offset += sizeof(attr.nullable);

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

  memcpy((char *)buffer + offset, &attr.nullable, sizeof(attr.nullable));
  offset += sizeof(attr.nullable);  

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

  // Pointer to most_recent_version
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

RC RM::createTable(const string tableName, const vector<Attribute> &attrs)
{
  string file_url = database_folder + '/' + tableName;
  RC ret = pfm->CreateFile(file_url);
  
  if(ret != 0)
    return ret;

  PF_FileHandle * fh = getFileHandle(tableName);
  
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

RC RM::deleteTable(const string tableName)
{
  return 0;
}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs)
{
  return 0;
}
RC RM::insertTuple(const string tableName, const void *data, RID &rid){
  
  // Get the length from the catalog
  int length = 4;
  // Process data to be in directory format

  return insertFormattedTuple(tableName, data, length, rid);
  
}
RC RM::insertFormattedTuple(const string tableName, const void *data, const int length, RID &rid)
{
  PF_FileHandle * fh = this->getFileHandle(tableName);
  
  void *page = malloc(PF_PAGE_SIZE);
  if( fh->ReadPage(0, page) != 0 )
    return -1;
  
  bool found = false;
  uint16_t num_pages; // This will hold the total number of pages. Not counting the first "directory" page
  uint16_t freespace = 0;
  
  uint16_t free_page;

  memcpy(&num_pages, page, 2);
  for(uint16_t i = 0; i < num_pages && !found; i++) {
    
    // Check the freespace at page i
    memcpy(&freespace, (char *)page+((i+1)*2), 2);

    if(freespace >= ((uint16_t)length + DIRECTORY_ENTRY_SIZE)){
      found = true;
      free_page = i + 1; //this is stored as the actual page index
    }
  }

  
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
   
    rid.pageNum = free_page;
    rid.slotNum = 0; // Slot numbers are zero based, we jsut can't forget about the first two bytes storing length

    RC ret = fh->AppendPage(page);
    free(page);
    return ret;
  }
  // We have enough space so insert at the correct location
  else{
    // First update free_space information on the first page
    ((uint16_t *)page)[free_page] = ((uint16_t *)page)[free_page] - length;
    if(fh->WritePage(0, page))
      return -1;
    
    if(fh->ReadPage(free_page,page) != 0)
      return -1;
    
    // Where does the free block begin
    uint16_t offset;
    memcpy(&offset,(char *)page+PF_PAGE_SIZE-2, 2);
    
    uint16_t number_of_records;
    memcpy(&number_of_records, (char*)page+PF_PAGE_SIZE-4,2);
  
    // Do we have enough room as is
    if(PF_PAGE_SIZE - offset < (length + number_of_records*DIRECTORY_ENTRY_SIZE + 4)){
      printf("No Rearrange function available");
      // TODO: Rearrange the page
      return -1;
    }
    
    // Insert the record
    memcpy((char *)page+offset,data,length);

    // Update the directory
    number_of_records++;
    memcpy((char*)page+ PF_PAGE_SIZE - 4, &number_of_records,2);
    
    // Store the offset pointer in slot number_of_records
    memcpy((char*)page+PF_PAGE_SIZE-4-number_of_records*DIRECTORY_ENTRY_SIZE, &offset,2);
    
    // Update the free pointer
    offset += length;
    memcpy((char *)page+PF_PAGE_SIZE-2,&offset, 2);

    rid.pageNum = free_page;
    rid.slotNum = number_of_records;

    RC ret = fh->WritePage(free_page,page);
    free(page);
    return ret;
  }
  return -1;
}


RC RM::deleteTuples(const string tableName)
{
  return 0;
}
RC RM::deleteTuple(const string tableName, const RID &rid)
{
  return 0;
}
// Assume the rid does not change after update
RC RM::updateTuple(const string tableName, const void *data, const RID &rid)
{
  return 0;
}
RC RM::readTuple(const string tableName, const RID &rid, void *data)
{
  return 0;
}
RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data)
{
  return 0;
}
RC RM::reorganizePage(const string tableName, const unsigned pageNumber)
{
  return 0;
}
RC RM::scan(const string tableName,
      const string conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator)
{
  return 0;
}

PF_FileHandle * RM::getFileHandle(const string tableName) 
{
  unordered_map<string,PF_FileHandle *>::const_iterator got = fileHandles.find (tableName);

  if ( got == fileHandles.end() )
    {
      fileHandles[tableName] = new PF_FileHandle();
      pfm->OpenFile((database_folder+'/'+tableName).c_str(), *fileHandles[tableName]);
    }

  return fileHandles[tableName];
}

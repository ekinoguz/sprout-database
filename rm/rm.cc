#include <cstdio>
#include <cstring>
#include <cstdlib>
#include "rm.h"

#define TABLES_TABLE "tables"
#define COLUMNS_TABLE "columns"

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
}

RC RM::addAttributeToCatalog(const string tableName, uint position, const Attribute &attr)
{
  RID rid;

  int offset = 0;
  void *buffer = malloc(100);
  *((char*)buffer + offset) = attr.name.size();
  offset += sizeof(int);
  memcpy((char *)buffer + offset, attr.name.c_str(), attr.name.size());
  offset += attr.name.size();

  *((char*)buffer + offset) = tableName.size();
  offset += sizeof(int);
  memcpy((char *)buffer + offset, tableName.c_str(), tableName.size());
  offset += tableName.size();

  *((char*)buffer + offset) = position;
  offset += sizeof(int);

  *((char*)buffer + offset) = attr.type;
  offset += sizeof(int);

  *((char*)buffer + offset) = attr.length;
  offset += sizeof(int);

  *((char*)buffer + offset) = attr.nullable;
  offset += 1;  
  
  return insertTuple(COLUMNS_TABLE, buffer, rid);
}

RC RM::addTableToCatalog(const string tableName, const string file_url, const string type)
{
  RID rid;

  int offset = 0;
  void *buffer = malloc(100);
  *((char*)buffer + offset) = tableName.size();
  offset += sizeof(int);
  memcpy((char *)buffer + offset, tableName.c_str(), tableName.size());
  offset += tableName.size();
  *((char*)buffer + offset) =file_url.size();
  offset += sizeof(int);
  memcpy((char *)buffer + offset, file_url.c_str(), file_url.size());
  offset += file_url.size();
  *((char*)buffer + offset) =type.size();
  offset += sizeof(int);
  memcpy((char *)buffer + offset, type.c_str(), type.size());
  offset += type.size();

  return insertTuple(TABLES_TABLE, buffer, rid);
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs)
{
  string file_url = database_folder + '/' + tableName;
  RC ret = pfm->CreateFile(file_url);
  
  if(ret != 0)
    return ret;


  this->addTableToCatalog(tableName, file_url, "heap"); 
  
  for(uint i=0; i < attrs.size(); i ++) {
    this->addAttributeToCatalog(tableName,i,attrs[i]);
  }
  
  return -1;
}

RC RM::deleteTable(const string tableName)
{
  return 0;
}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs)
{
  return 0;
}
RC RM::insertTuple(const string tableName, const void *data, RID &rid)
{
  
  return 0;
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

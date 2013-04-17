
#include "rm.h"

RM* RM::_rm = 0;

RM* RM::Instance()
{
    if(!_rm)
        _rm = new RM();

    return _rm;
}

RM::RM()
{
}

RM::~RM()
{
}


RC RM::createTable(const string tableName, const vector<Attribute> &attrs)
{

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

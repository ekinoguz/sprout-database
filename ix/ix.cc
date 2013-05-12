
#include "ix.h"

IX_Manager IX_Manager::_ix_manager;

IX_Manager* IX_Manager::Instance()
{
  return &_ix_manager;
}

IX_Manager::IX_Manager()
{
}

IX_Manager::~IX_Manager()
{
}

RC IX_Manager::init()
{
  return -1;
}


RC IX_Manager::CreateIndex(const string tableName, const string attributeName)
{
  return -1;
}

RC IX_Manager::DestroyIndex(const string tableName, const string attributeName)
{
  return -1;
}

RC IX_Manager::OpenIndex(const string tableName,
	       const string attributeName,
	       IX_IndexHandle &indexHandle)
{
}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle)
{
  return -1;
}

IX_IndexHandle::IX_IndexHandle()
{
}

IX_IndexHandle::~IX_IndexHandle()
{
}

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid){
  // Key is in "their" format
  // Return 1 if (key,rid) already exists
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

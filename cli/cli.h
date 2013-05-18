
#ifndef _cli_h_
#define _cli_h_

#include <string>
#include <cstring>
#include <iomanip>

#include "../shared.h"
#include "../pf/pf.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

using namespace std;


// Return code
typedef int RC;

// Record Manager
class CLI
{
public: 
  static CLI* Instance();

  RC process(const string input);
  RC start();

protected:
  CLI();
  ~CLI();

private:
  // cli parsers
  RC createTable();
  RC createIndex();
  RC dropTable();
  RC dropIndex(const string tableName="", const string columnName="", bool fromCommand=true);
  RC addAttribute();
  RC dropAttribute();
  RC load();
  RC printTable(const string tableName);
  RC printColumns();
  RC help(const string input);
  RC history();

  // cli catalog functions
  RC getAttributesFromCatalog(const string tableName, vector<Attribute> &columns);
  RC addAttributeToCatalog(const Attribute &attr, const string tableName, const int position);
  RC addTableToCatalog(const string tableName, const string file_url, const string type);
  RC addIndexToCatalog(const string tableName, const string indexName);

  // helper functions
  char *  next();
  bool expect(char * tokenizer, const string expected);
  bool checkAttribute(const string tableName, const string columnName, RID &rid, bool searchColumns=true);
  RC error(const string errorMessage);
  RC printTuple(void *data, vector<Attribute> &attrs);
  void printAttributes(vector<Attribute> &attributes);

  RM * rm;
  IX_Manager * ixManager;
  static CLI * _cli;
};

#endif

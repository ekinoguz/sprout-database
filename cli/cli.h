
#ifndef _cli_h_
#define _cli_h_

#include <string>
#include <cstring>
#include <iomanip>
#include <cmath>

#include "../shared.h"
#include "../pf/pf.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

using namespace std;


// Return code
typedef int RC;

struct Table {
  string tableName;
  vector<Attribute> columns;

};

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
  RC insertTuple();
  RC dropAttribute();
  RC load();
  RC printTable(const string tableName);
  RC printAttributes();
  RC printIndex();
  RC help(const string input);
  RC history();

  // query parsers
  RC query();

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
  RC printOutputBuffer(vector<string> &buffer, uint mod, bool firstSpecial=false);
  RC updateOutputBuffer(vector<string> &buffer, void *data, vector<Attribute> &attrs);
  RC insertTupleToDB(const string tableName, const vector<Attribute> attributes, const void *data, unordered_map<int, void *> indexMap);


  RM * rm;
  IX_Manager * ixManager;
  static CLI * _cli;
};

#endif

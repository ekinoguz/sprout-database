
#ifndef _cli_h_
#define _cli_h_

#include <string>
#include <cstring>
#include <iomanip>

#include "../shared.h"
#include "../pf/pf.h"
#include "../rm/rm.h"

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
  RC createTable(const string name, char * tokenizer);
  RC drop(const string type, const string name);
  RC addAttribute(char * tokenizer);
  RC dropAttribute(char * tokenizer);
  RC load(const string tableName, const string fileName);
  RC printTable(const string tableName);
  RC printColumns(char * tokenizer);
  RC printTuple(void *data, vector<Attribute> &attrs);
  RC help(const string input);
  RC history();

  // cli catalog functions
  RC getAttributesFromCatalog(const string tableName, vector<Attribute> &columns);
  RC addAttributeToCatalog(const Attribute &attr, const string tableName, const int position);
  RC addTableToCatalog(const string tableName, const string file_url, const string type);
  
  char *  next();
  bool expect(char * tokenizer, const string expected);
  RC error(const string errorMessage);
  void printAttributes(vector<Attribute> &attributes);

  RM * rm;
  static CLI * _cli;
};

#endif

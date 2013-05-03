
#ifndef _cli_h_
#define _cli_h_

#include <string>
#include <cstring>

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

protected:
  CLI();
  ~CLI();

private:
  RC createTable(const string name, char * tokenizer); // this is how all of them should be
  RC drop(const string type, const string name);
  RC load(const string tableName, const string fileName);
  RC print(const string input);
  RC help(const string input);

  // cli catalog functions
  RC getAttributesFromCatalog(const string tableName, vector<Attribute> &columns);
  RC addAttributeToCatalog(const Attribute &attr, const string tableName, const int position);
  RC addTableToCatalog(const string tableName, const string file_url, const string type);
  
  char *  next();
  RC expect(char * tokenizer, const string expected);
  void error(const string errorMessage);
  
  RM * rm;
  static CLI * _cli;
};

#endif

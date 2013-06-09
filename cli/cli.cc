#include "cli.h"

#include <readline/readline.h>
#include <readline/history.h>

// Command parsing delimiters
// TODO: update delimiters later
#define DELIMITERS " ,()\""

// CVS file read delimiters
#define CVS_DELIMITERS ","
#define CLI_TABLES "cli_tables"
#define CLI_COLUMNS "cli_columns"
#define CLI_INDEXES "cli_indexes"
#define COLUMNS_TABLE_RECORD_MAX_LENGTH 150   // It is actually 112
#define DIVISOR "  |  "
#define DIVISOR_LENGTH 5
#define EXIT_CODE -99

CLI * CLI::_cli = 0;

CLI* CLI::Instance()
{
  if(!_cli)
      _cli = new CLI();

  return _cli;
}

CLI::CLI()
{
  rm = RM::Instance();
  ixManager = IX_Manager::Instance();
  Attribute attr;

  // create cli columns table
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

  rm->createTable(CLI_COLUMNS, column_attrs);

  // add cli catalog attributes to CLI_COLUMNS table
  for(uint i=0; i < column_attrs.size(); i ++) {
    this->addAttributeToCatalog(column_attrs[i], CLI_COLUMNS, i);
  }

  // create CLI_TABLES table
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
  
  if (rm->createTable(CLI_TABLES, table_attrs) != 0)
    return;

  // add cli catalog attributes to cli columns table
  for(uint i=0; i < table_attrs.size(); i ++) {
    this->addAttributeToCatalog(table_attrs[i], CLI_TABLES, i);
  }

  // add cli catalog information to itself
  string file_url = string(DATABASE_FOLDER) + '/' + CLI_COLUMNS;
  if (this->addTableToCatalog(CLI_COLUMNS, file_url, "heap") != 0)
    return;

  file_url = string(DATABASE_FOLDER) + '/' + CLI_TABLES;
  if (this->addTableToCatalog(CLI_TABLES, file_url, "heap") != 0)
    return;

  // Adding the index table attributes to the columns table
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
  
  if(rm->createTable(CLI_INDEXES, index_attr) != 0)
    return;

  // add cli index attributes to cli columns table
  for(uint i=0; i < index_attr.size(); i ++) {
    this->addAttributeToCatalog(index_attr[i], CLI_INDEXES, i);
  }

  // add cli catalog information to itself
  file_url = string(DATABASE_FOLDER) + '/' + CLI_INDEXES;
  if (this->addTableToCatalog(CLI_INDEXES, file_url, "heap") != 0)
    return;
}

CLI::~CLI()
{
}

RC CLI::start()
{

  // what do we want from readline?
  using_history();
  // auto-complete = TAB
  rl_bind_key('\t', rl_complete);
  // 

  char* input, shell_prompt[100];
  cout << "************************" << endl;
  cout << "SecSQL CLI started" << endl;
  cout << "Enjoy!" << endl;
  for(;;) {

    // Create prompt string from user name and current working directory.
    //snprintf(shell_prompt, sizeof(shell_prompt), "%s >>> ", getenv("USER"));
    snprintf(shell_prompt, sizeof(shell_prompt), ">>> ");
    // Display prompt and read input (n.b. input must be freed after use)...
    input = readline(shell_prompt);

    // check for EOF
    if (!input)
      break;
    if ((this->process(string(input))) == EXIT_CODE) {
      free(input);
      break;
    }
    add_history(input);
    // Free Input
    free(input);
  }
  cout << "Goodbye :(" << endl;

  return 0;
}

RC CLI::process(const string input)
{
  // convert input to char *
  RC code = 0;
  char *a=new char[input.size()+1];
  a[input.size()] = 0;
  memcpy(a,input.c_str(),input.size());

  // tokenize input
  char * tokenizer = strtok(a, DELIMITERS);
  if (tokenizer != NULL)
  {

    ////////////////////////////////////////////
    // create table <tableName> (col1=type1, col2=type2, ...)
    // create index <columnName> on <tableName>
    ////////////////////////////////////////////
    if (expect(tokenizer, "create")) {
      tokenizer = next();
      if (tokenizer == NULL) {
        code = error ("I expect <table> or <index>");
      }
      else {
        string type = string(tokenizer);
        
        if (type.compare("table") == 0) // if type equals table, then create table
          code = createTable();
        else if (type.compare("index") == 0) // else if type equals index, then create index
          code = createIndex();
      }
    }
    ////////////////////////////////////////////
    // add attribute <attributeName=type> to <tableName>
    ////////////////////////////////////////////
    else if (expect(tokenizer, "add")) {
      tokenizer = next();
      if (expect(tokenizer, "attribute"))
        code = addAttribute();
      else
        code = error ("I expect <attribute>");
    }

    ////////////////////////////////////////////
    // drop table <tableName>
    // drop index <columnName> on <tableName>
    // drop attribute <attributeName> from <tableName>
    ////////////////////////////////////////////
    else if (expect(tokenizer, "drop")) {
      tokenizer = next();
      if (expect(tokenizer, "table")) {
        code = dropTable();
      }
      else if (expect(tokenizer, "index")) {
        code = dropIndex();
      }
      else if (expect(tokenizer, "attribute")) {
        code = dropAttribute();
      }
      else
        code = error ("I expect <tableName>, <indexName>, <attribute>");
    }

    ////////////////////////////////////////////
    // load <tableName> <fileName>
    // drop index <indexName>
    // drop attribute <attributeName> from <tableName>
    ////////////////////////////////////////////
    else if (expect(tokenizer, "load")) {
      code = load();
    }

    ////////////////////////////////////////////
    // print <tableName>
    // print attributes <tableName>
    ////////////////////////////////////////////
    else if (expect(tokenizer, "print")) {
      tokenizer = next();
      if (expect(tokenizer, "body") || expect(tokenizer, "attributes"))
        code = printAttributes();
      else if (expect(tokenizer, "index"))
        code = printIndex();
      else if (tokenizer != NULL)
        code = printTable(string(tokenizer));
      else
        code = error ("I expect <tableName>");
    }

    ///////////////////////////////////////////////////////////////
    // insert into <tableName> tuple(attr1=val1, attr2=value2, ...)
    ///////////////////////////////////////////////////////////////
    else if (expect(tokenizer, "insert")) {
      code = insertTuple();
    }

    ////////////////////////////////////////////
    // help
    // help <commandName>
    ////////////////////////////////////////////
    else if (expect(tokenizer, "help")) {
      tokenizer = next();
      if (tokenizer != NULL)
        code = help(string(tokenizer));
      else
        code = help("all");
    }
    else if (expect(tokenizer,"quit") || expect(tokenizer,"exit") || 
             expect(tokenizer, "q") || expect(tokenizer, "e")) {
      code = EXIT_CODE;
    }
    else if (expect(tokenizer, "history") || expect(tokenizer, "h")) {
      code = history();
    }

    ////////////////////////////////////////////
    // select...
    ////////////////////////////////////////////
    else if (expect(tokenizer, "SELECT")) {
      Iterator *it = NULL;
      code = run(query(it));
      cout << endl;
    }

    ////////////////////////////////////////////
    // Utopia...
    ////////////////////////////////////////////
    else if (expect(tokenizer, "make")) {
      code = error ("this is for you Sky...");
    }
    else {
      code = error ("i have no idea about this command, sorry");
    }
  }
  delete[] a;
  return code;
}

// query =
// FILTER FROM <query> WHERE <attr> <op> <value>
// PROJECT FROM <query> GET <attrs>
// JOIN <lattr> <op> <rattr> FROM {"INL" <query1>, <query2-Index> | "NL" <query1>, <query2>}
// AGG <OP>(query) OF <attr> [GROUPBY <attr>]
// TABLE tableName
// INDEX indexName

// QUERY = 
// TABLE <query>

// HEREHERE
Iterator * CLI::query(Iterator *previous, int code)
{
  Iterator *it = NULL;
  if (code >= 0 || (code != -2 && isIterator(string(next()), code)) ) {
    switch(code) {
      case FILTER:
        it = filter(previous);
        break;

      case PROJECT:
        it = projection(previous);
        break;

      case AGG:
        it = aggregate(previous);
        break;

      case NL:
        it = nestedloopjoin(previous);
        break;

      case INL:
        it = indexnestedloopjoin(previous);
        break;

      case -1:
        error("dude, be carefuly with what you are writing as a query");
        break;
    }
  }
  return it;
}

// Create INLJoin
Iterator * CLI::indexnestedloopjoin(Iterator *input) {
  char *token = next();
  int code = -2;
  if (isIterator(string(token), code)){
    input = query(input, code);
  }

  if (input == NULL) {
    input = createBaseScanner(string(token));
  }

  // get right table
  token = next();
  string rightTableName = string(token);
  token = next(); // eat WHERE

  // parse the join condition
  Condition cond;
  if (createCondition(getTableName(input), cond, true, rightTableName) != 0)
    error(__LINE__);

  IX_IndexHandle rightHandle;
  ixManager->OpenIndex(rightTableName, getAttribute(cond.rhsAttr), rightHandle);
  IndexScan *right = new IndexScan(*rm, rightHandle, rightTableName);

  token = next(); // eat PAGES
  token = next(); // get page number

  // Create Join 
  INLJoin *join = new INLJoin(input, right, cond, (unsigned) atoi(string(token).c_str()));

  return join;
}

// Create Aggregate
Iterator * CLI::aggregate(Iterator *input) {
  char *token = next();
  int code = -2;
  if (isIterator(string(token), code)){
    input = query(input, code);
  }

  if (input == NULL) {
    input = createBaseScanner(string(token));
  }

  token = next();

  // check GROUPBY
  bool groupby = false;
  Attribute gAttr;
  if (string(token) == "GROUPBY") {
    groupby = true;
    if (createAttribute(getTableName(input), gAttr) != 0)
      error(__LINE__);

    token = next(); // eat GET
  }

  string operation = string(next());

  AggregateOp op;
  if (createAggregateOp(operation, op) != 0)
    error (__LINE__);
  Attribute aggAttr;
  if (createAttribute(getTableName(input), aggAttr) != 0)
    error(__LINE__);

  Aggregate *agg;
  if (groupby) {
    agg = new Aggregate(input, aggAttr, gAttr, op);
  } else {
    agg = new Aggregate(input, aggAttr, op);
  }
  return agg;
}

// Create NLJoin
Iterator * CLI::nestedloopjoin(Iterator *input) {
  char *token = next();
  int code = -2;
  if (isIterator(string(token), code)){
    input = query(input, code);
  }

  if (input == NULL) {
    input = createBaseScanner(string(token));
  }

  // get right table
  token = next();
  string rightTableName = string(token);
  TableScan *right = new TableScan(*rm, rightTableName);

  token = next(); // eat WHERE

  // parse the join condition
  Condition cond;
  if (createCondition(getTableName(input), cond, true, rightTableName) != 0)
    error(__LINE__);

  token = next(); // eat PAGES
  token = next(); // get page number

  // Create Join 
  NLJoin *join = new NLJoin(input, right, cond, (unsigned) atoi(string(token).c_str()));

  return join;
}

// Create Filter
Iterator * CLI::filter(Iterator *input) {
  char *token = next();
  int code = -2;
  if (isIterator(string(token), code)){
    input = query(input, code);
  }

  if (input == NULL) {
    input = createBaseScanner(string(token));
  }

  token = next(); // eat WHERE

  // parse the filter condition
  Condition cond;
  if (createCondition(getTableName(input), cond) != 0)
    error(__LINE__);

  // Create Filter 
  Filter *filter = new Filter(input, cond);

  return filter;
}

// Create Projector
Iterator * CLI::projection(Iterator *input) {
  char *token = next();
  int code = -2;
  if (isIterator(string(token), code)){
    input = query(input, code);
  }
  
  if (input == NULL) {
    input = createBaseScanner(string(token));
  }

  token = next(); // eat GET
  token = next(); // eat [

  // parse the projection attributes
  vector<string> attrNames;
  while (true) {
    token = next();
    if (string(token) == "]")
      break;
    attrNames.push_back(token);
  }

  // if we have "*", convert it to all attributes
  if (attrNames[0].compare("*") == 0) {
    vector<Attribute> attrs;
    input->getAttributes(attrs);
    attrNames.clear();
    for (uint i=0; i < attrs.size(); i++) {
      attrNames.push_back(attrs[i].name);
    }
  }
  else {
    string tableName = getTableName(input);
    addTableNameToAttrs(tableName, attrNames);
  }

  Project *project = new Project(input, attrNames);
  return project;
}

Iterator * CLI::createBaseScanner(const string token) {

  // if token is "IS" (index scanner), create index scanner
  if (token.compare("IS") == 0) {
    string tableName = string(next());
    Condition cond;
    if (createCondition(tableName, cond) != 0)
      error(__LINE__);

    IX_IndexHandle ixHandle;
    ixManager->OpenIndex(tableName, getAttribute(cond.lhsAttr), ixHandle);
    IndexScan *is = new IndexScan(*rm, ixHandle, tableName);

    switch(cond.op) {
      case EQ_OP:
      is->setIterator(cond.rhsValue.data, cond.rhsValue.data, true, true);
      break;
      
      case LT_OP:
      is->setIterator(NULL, cond.rhsValue.data, true, false);
      break;

      case GT_OP:
      is->setIterator(cond.rhsValue.data, NULL, false, true);
      break;

      case LE_OP:
      is->setIterator(NULL, cond.rhsValue.data, true, true);
      break;
      
      case GE_OP:
      is->setIterator(cond.rhsValue.data, NULL, true, true);
      break;
      
      // case NE_OP:
      // is->setIterator(cond.rhsValue.data, NULL, true, true);
      // break;
      
      case NO_OP:
      is->setIterator(NULL, NULL, true, true);
      break;

      default:
      break;
    }
    
    return is;
  }
  // otherwise, create create table scanner
  return new TableScan(*rm, token);
}

bool CLI::isIterator(const string token, int &code) {
  if (token == "FILTER") {
    code = FILTER;
    return true;
  }
  else if (token == "PROJECT") {
    code = PROJECT;
    return true; 
  }
  else if (token == "NLJOIN") {
    code = NL;
    return true;
  }
  else if (token == "INLJOIN") {
    code = INL;
    return true;
  }
  else if (token == "AGG") {
    code = AGG;
    return true;
  }
  return false;
}

RC CLI::run(Iterator *it) {
  void *data = malloc(PF_PAGE_SIZE);
  vector<Attribute> attrs;
  vector<string> outputBuffer;
  it->getAttributes(attrs);

  for (uint i=0; i < attrs.size(); i++)
    outputBuffer.push_back(attrs[i].name);

  while (it->getNextTuple(data) != QE_EOF) {
    if ( updateOutputBuffer(outputBuffer, data, attrs) != 0)
      return error(__LINE__);
  }

  if (printOutputBuffer(outputBuffer, attrs.size()) != 0)
    return error(__LINE__);
  return 0;
}

RC CLI::createProjectAttributes(const string tableName, vector<Attribute> &attrs) {
  char *token = next();
  Attribute attr;
  vector<Attribute> inputAttrs;
  getAttributesFromCatalog(tableName, inputAttrs);
  while (expect(token, "FROM")) {
    // get the attribute
    if (getAttribute(string(token), inputAttrs, attr) != 0)
      return error("given " + string(token) + " is not found in attributes");
    attrs.push_back(attr);
    token = next();
  }
  return 0;
}

RC CLI::createCondition(const string tableName, Condition &condition, const bool join, const string joinTable) {
  // get left attribute
  char *token = next();

  string attribute = string(token);
  // concatenate left attribute with tableName
  condition.lhsAttr = fullyQualify(attribute, tableName);

  // get operation
  token = next();
  if (string(token) == "=")
    condition.op = EQ_OP;
  else if (string(token) == "<")
    condition.op = LT_OP;
  else if (string(token) == ">")
    condition.op = GT_OP;
  else if (string(token) == "<=")
    condition.op = LE_OP;
  else if (string(token) == ">=")
    condition.op = GE_OP;
  else if (string(token) == "!=")
    condition.op = NE_OP;
  else if (string(token) == "NOOP") 
  {
    condition.op = NO_OP;
    return 0;
  }

  if (join) {
    condition.bRhsIsAttr = true;
    token = next();
    condition.rhsAttr = fullyQualify(string(token), joinTable);
    return 0;
  }

  condition.bRhsIsAttr = false;

  // get attribute from catalog
  Attribute attr;
  if (this->getAttribute(tableName, attribute, attr) != 0)
    return error(__LINE__);
  
  Value value;
  value.type = attr.type;
  value.data = malloc(PF_PAGE_SIZE);
  token = next();
  attribute = string(token);

  int num;
  float floatNum;
  switch(attr.type){
    case TypeVarChar:
      num = attribute.size();
      memcpy((char *)value.data, &num, sizeof(int));
      memcpy((char *)value.data+sizeof(int), attribute.c_str(), num);
      break;
    case TypeInt:
      num = atoi(string(token).c_str());
      memcpy((char *)value.data, &num, sizeof(int));
      break;
    case TypeReal:
      floatNum = atof(string(token).c_str());
      memcpy((char *)value.data, &floatNum, sizeof(float));
      break;
    default:
      return error("cli: " + __LINE__);
  }
  condition.rhsValue = value;
  return 0;
}

RC CLI::createAttribute(const string tableName, Attribute &attr) {
  string attribute = string(next());
  // get attribute from catalog
  if (this->getAttribute(tableName, attribute, attr) != 0)
    return error(__LINE__);
  attr.name = tableName + "." + attr.name;
  return 0;
}

RC CLI::createAggregateOp(const string operation, AggregateOp &op) {
  if (operation == "MAX")
    op = MAX;
  else if (operation == "MIN")
    op = MIN;
  else if (operation == "SUM")
    op = SUM;
  else if (operation == "AVG")
    op = AVG;
  else if (operation == "COUNT")
    op = COUNT;
  else {
    return error("create aggregate op: " + __LINE__);
  }
  return 0;
}

string CLI::getTableName(Iterator *it) {
  vector<Attribute> attrs;
  it->getAttributes(attrs);
  unsigned loc = attrs[0].name.find(".", 0);
  return attrs[0].name.substr(0, loc);
}

// input is like tableName.attributeName
// returns attributeName
string CLI::getAttribute(const string input) {
  unsigned loc = input.find(".", 0);
  return input.substr(loc+1, input.size()-loc);
}

string CLI::fullyQualify(const string attribute, const string tableName) {
  unsigned loc = attribute.find(".", 0);
  if (loc >= 0 && loc < attribute.size())
    return attribute;
  else
    return tableName + "." + attribute;
}

///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
// END OF QUERY ENGINE ////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
//===============================//

RC CLI::createTable()
{
  char * tokenizer = next();
  if (tokenizer == NULL) {
    return error ("I expect <name> to be created");
  }
  string name = string(tokenizer);

  // parse columnNames and types
  vector<Attribute> table_attrs;
  Attribute attr;
  while (tokenizer != NULL)
  {
    // get name if there is
    tokenizer = next();
    if (tokenizer == NULL) {
      break;
    }
    attr.name = string(tokenizer);

    tokenizer = next(); // eat =

    // get type
    tokenizer = next();
    if (tokenizer == NULL) {
      return error("expecting type");
    }
    if (expect(tokenizer, "int")) {
      attr.type = TypeInt;
      attr.length = 4;
    }
    else if (expect(tokenizer, "real")) {
      attr.type = TypeReal;
      attr.length = 4;
    }
    else if (expect(tokenizer, "varchar")) {
      attr.type = TypeVarChar;
      // read length
      tokenizer = next();
      attr.length = atoi(tokenizer);
    }
    else if (expect(tokenizer, "boolean")) {
      attr.type = TypeBoolean;
      attr.length = 1;
    }
    else if (expect(tokenizer, "short")) {
      attr.type = TypeShort;
      attr.length = 1;
    }
    else {
      return error ("problem in attribute type in create table: " + string(tokenizer));
    }
    table_attrs.push_back(attr);
  }
  // cout << "create table for <" << name << "> and attributes:" << endl;
  // for (std::vector<Attribute>::iterator it = table_attrs.begin() ; it != table_attrs.end(); ++it)
 //    std::cout << ' ' << it->length;
 //  cout << endl;

  RC ret = rm->createTable(name, table_attrs);
  if (ret != 0)
    return ret;

  // add table to cli catalogs
  string file_url = string(DATABASE_FOLDER) + '/' + name;
  ret = this->addTableToCatalog(name, file_url, "heap");
  if (ret != 0)
    return ret;

  // add attributes to cli columns table
  for(uint i=0; i < table_attrs.size(); i ++) {
    this->addAttributeToCatalog(table_attrs[i], name, i);
  }

  return 0;
}

// create index <columnName> on <tableName>
RC CLI::createIndex()
{
  char * tokenizer = next();
  string columnName = string(tokenizer);

  tokenizer = next();
  if (!expect(tokenizer, "on")) {
    return error ("syntax error: expecting \"on\"");
  }

  tokenizer = next();
  string tableName = string(tokenizer);
  
  // check if columnName, tableName is valid
  RID rid;
  if (this->checkAttribute(tableName, columnName, rid) == false)
    return error("Given tableName-columnName does not exist");

  // check if index is already there
  IX_IndexHandle ixHandle;
  if (ixManager->OpenIndex(tableName, columnName, ixHandle) == 0)
    return error("index is already there");

  // create index
  if (ixManager->CreateIndex(tableName, columnName) != 0)
    return error("cannot create index, ixManager error");

  if (ixManager->OpenIndex(tableName, columnName, ixHandle) != 0)
    return error("cannot open the index");

  // add index to cli_indexes table
  if (this->addIndexToCatalog(tableName, columnName) != 0)
    return error("error in adding index to cli_indexes table");

  // Scann the whole tableName file and project the attributeName only
  // For each record found insert that record in indexHandle
  // Check insert indexHandle for further details
  vector<string> attributeNames;
  attributeNames.push_back(columnName);
  RM_ScanIterator rm_ScanIterator;
  if (rm->scan(tableName, "", NO_OP, NULL, attributeNames, rm_ScanIterator) != 0)
    return error(__LINE__);

  void* data = malloc(PF_PAGE_SIZE);
  while (rm_ScanIterator.getNextTuple(rid, data) != RM_EOF) {
    if (ixHandle.InsertEntry(data, rid) != 0) {
        free(data);
        return error(__LINE__);
    }
  }
  free(data);

  return 0;
}

RC CLI::addAttribute()
{
  Attribute attr;
  char * tokenizer = next(); // attributeName
  attr.name = string(tokenizer);
  if (tokenizer == NULL)
    return error ("I expect type for attribute");

  tokenizer = next(); // eat =

  tokenizer = next(); // type

  if (expect(tokenizer, "int")) {
    attr.type = TypeInt;
    attr.length = 4;
  }
  else if (expect(tokenizer, "real")) {
    attr.type = TypeReal;
    attr.length = 4;
  }
  else if (expect(tokenizer, "varchar")) {
    attr.type = TypeVarChar;
    // read length
    if (tokenizer == NULL)
      return error ("I expect length for varchar");
    tokenizer = next();
    attr.length = atoi(tokenizer);
  }
  else if (expect(tokenizer, "boolean")) {
    attr.type = TypeBoolean;
    attr.length = 1;
  }
  else if (expect(tokenizer, "short")) {
    attr.type = TypeShort;
    attr.length = 1;
  }

  tokenizer = next();
  if (expect(tokenizer, "to") == false) {
    return error ("expect to");
  }
  tokenizer = next(); //tableName
  string tableName = string(tokenizer);

  // add entry to CLI_COLUMNS
  vector<Attribute> attributes;
  this->getAttributesFromCatalog(CLI_COLUMNS, attributes);

  // Set up the iterator
  RM_ScanIterator rmsi;
  RID rid;
  void *data_returned = malloc(PF_PAGE_SIZE);

  // convert attributes to vector<string>
  vector<string> stringAttributes;
  stringAttributes.push_back("position");
  
  // Delete columns    
  if( rm->scan(CLI_COLUMNS, "table_name", EQ_OP, tableName.c_str(), stringAttributes, rmsi) != 0)
    return -1;
  
  int biggestPosition = 0, position = 0;
  while(rmsi.getNextTuple(rid, data_returned) != RM_EOF){
    memcpy(&position, (char *)data_returned, sizeof(int));
    if (biggestPosition < (int)position)
      biggestPosition = position;
  }
  if (this->addAttributeToCatalog(attr, tableName, biggestPosition+1) != 0)
    return -1;
  free(data_returned);
  return rm->addAttribute(tableName, attr);
}

RC CLI::dropTable()
{
  char * tokenizer = next();
  if (tokenizer == NULL)
    return error ("I expect <tableName> to be dropped");

  string tableName = string(tokenizer);


  // delete indexes if there are
  RID rid;
  vector<Attribute> attributes;
  this->getAttributesFromCatalog(tableName, attributes);
  for (uint i = 0; i < attributes.size(); i++) {
    if(this->checkAttribute(tableName, attributes[i].name, rid, false))
      if(this->dropIndex(tableName, attributes[i].name, false) != 0)
        return error("error while dropping an index in dropTable");
  }

  // Set up the iterator
  Attribute attr;
  RM_ScanIterator rmsi;
  void *data_returned = malloc(PF_PAGE_SIZE);

  // convert attributes to vector<string>
  vector<string> stringAttributes;
  stringAttributes.push_back("table_name");
  if( rm->scan(CLI_TABLES, "table_name", EQ_OP, tableName.c_str(), stringAttributes, rmsi) != 0)
    return -1;
  
  // delete tableName from CLI_TABLES
  while(rmsi.getNextTuple(rid, data_returned) != RM_EOF){
    if(rm->deleteTuple(CLI_TABLES, rid) != 0)
      return -1;
  }
  rmsi.close();

  // Delete columns from CLI_COLUMNS  
  if( rm->scan(CLI_COLUMNS, "table_name", EQ_OP, tableName.c_str(), stringAttributes, rmsi) != 0)
    return -1;

  // We rely on the fact that RM_EOF is not 0. 
  // we want to return -1 when getNext tuple errors
  RC ret = -10;
  while((ret = rmsi.getNextTuple(rid, data_returned)) == 0){
    if(rm->deleteTuple(CLI_COLUMNS, rid) != 0)
      return -1;
  }
  if(ret!=RM_EOF)
    return -1;

  free(data_returned);

  // and finally DeleteTable
  ret = rm->deleteTable(tableName);
  if (ret != 0)
    return error("error in deleting table in recordManager");

  return 0;
}

// drop index <columnName> on <tableName>
RC CLI::dropIndex(const string tableName, const string columnName, bool fromCommand)
{
  string realTable;
  string realColumn;
  if (fromCommand == false) {
    realTable = tableName;
    realColumn = columnName;
  }
  else {
    // parse willDelete from command line
    char * tokenizer = next();
    realColumn = string(tokenizer);

    tokenizer = next();
    if (!expect(tokenizer, "on")) {
      return error ("syntax error: expecting \"on\"");
    }

    tokenizer = next();
    realTable = string(tokenizer);
  }
  RC rc;
  // check if index is there or not
  RID rid;
  if (!this->checkAttribute(realTable, realColumn, rid, false)) {
    if (fromCommand)
      return error("given " + realTable + ":" + realColumn + " index does not exist in cli_indexes");
    else // return error but print nothing
      return -1;
  }

  // drop the index
  rc = ixManager->DestroyIndex(realTable, realColumn);
  if (rc != 0)
    return error("error while destroying index in ixManager");

  // delete the index from cli_indexes table
  rc = rm->deleteTuple(CLI_INDEXES, rid) != 0;

  return rc;
}

RC CLI::dropAttribute()
{
  char * tokenizer = next(); // attributeName
  string attrName = string(tokenizer);
  tokenizer = next();
  if (expect(tokenizer, "from") == false) {
    return error ("expect from");
  }
  tokenizer = next(); //tableName
  string tableName = string(tokenizer);

  RID rid;
  if (!this->checkAttribute(tableName, attrName, rid)) {
    return error("given tableName-attrName does not exist");
  }
  // delete entry from CLI_COLUMNS
  RC rc = rm->deleteTuple(CLI_COLUMNS, rid) != 0;
  if (rc != 0)
    return rc;

  // drop attribute
  rc = rm->dropAttribute(tableName, attrName);
  if (rc != 0)
    return rc;

  // if there is an index on dropped attribute
  //    delete the index from cli_indexes table
  bool hasIndex = this->checkAttribute(tableName, attrName, rid, false);

  if (hasIndex) {
    // drop index if there is one
    rc = this->dropIndex(tableName, attrName, false);
    if (rc != 0)
      return rc;
  }

  return 0;
}

// CSV reader without escaping commas
// should be fixed
// reads files in data folder
RC CLI::load()
{
  char * commandTokenizer = next();
  if (commandTokenizer == NULL)
    return error ("I expect <tableName>");

  string tableName = string(commandTokenizer);
  commandTokenizer = next();
  if (commandTokenizer == NULL) {
    return error ("I expect <fileName> to be loaded");
  }
  string fileName = string(commandTokenizer);

  // get attributes from catalog
  Attribute attr;
  vector<Attribute> attributes;
  this->getAttributesFromCatalog(tableName, attributes);
  uint offset = 0, index = 0, keyIndex = 0;
  uint length;
  void *buffer = malloc(PF_PAGE_SIZE);
  void *key = malloc(PF_PAGE_SIZE);
  RID rid;

  // find out if there is any index for tableName
  unordered_map<int, void *> indexMap;
  for (uint i = 0; i < attributes.size(); i++) {
    if (this->checkAttribute(tableName, attributes[i].name, rid, false))
      // add index to index-map
      indexMap[i] = malloc(PF_PAGE_SIZE);
  }

  // read file
  ifstream ifs;
  string file_url = DATABASE_FOLDER"/../data/" + fileName;
  ifs.open (file_url, ifstream::in);

  if (!ifs.is_open())
    return error("could not open file: " + file_url);

  string line, token;
  char * tokenizer;
  while (ifs.good()) {
    getline(ifs, line);
    if (line.compare("") == 0)
      continue;
    char *a=new char[line.size()+1];
    a[line.size()] = 0;
    memcpy(a,line.c_str(),line.size());
    index = 0, offset = 0;
    
    // tokenize input
    tokenizer = strtok(a, CVS_DELIMITERS);
    while (tokenizer != NULL) {
      attr = attributes.at(index++);
      token = string(tokenizer);
      if (attr.type == TypeVarChar) {
        length = token.size();
        memcpy((char *)buffer + offset, &length, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, token.c_str(), length);
        offset += length;

        auto got = indexMap.find(keyIndex);
        if (got != indexMap.end())
          memcpy((char *) indexMap[keyIndex], (char *)buffer+offset-sizeof(int)-length, sizeof(int)+length);
      } 
      else if (attr.type == TypeInt) {
        int num = atoi(tokenizer);
        memcpy((char *)buffer + offset, &num, sizeof(num));
        offset += sizeof(num);

        auto got = indexMap.find(keyIndex);
        if (got != indexMap.end())
          memcpy((char *) indexMap[keyIndex], (char *)buffer+offset-sizeof(int), sizeof(int));
      }
      else if (attr.type == TypeReal) {
        float num = atof(tokenizer);
        memcpy((char *)buffer + offset, &num, sizeof(num));
        offset += sizeof(num);

        auto got = indexMap.find(keyIndex);
        if (got != indexMap.end())
          memcpy((char *) indexMap[keyIndex], (char *)buffer+offset-sizeof(float), sizeof(int));
      }
      else if (attr.type == TypeBoolean || attr.type == TypeShort) {
        // TODO: this should be fixed, not sure about size
        int num = atoi(tokenizer);
        memcpy((char *)buffer + offset, &num, sizeof(num));
        offset += sizeof(num);
        return -1; 
      }
      tokenizer = strtok(NULL, CVS_DELIMITERS);
      keyIndex += 1;
      if (keyIndex == attributes.size())
        keyIndex = 0;
    }
    if (this->insertTupleToDB(tableName, attributes, buffer, indexMap) != 0) {
      return error("error while inserting tuple");
    }
    
    delete [] a;
    // prepare tuple for addition
    // for (std::vector<Attribute>::iterator it = attrs.begin() ; it != attrs.end(); ++it)
    // totalLength += it->length;
  }
  // clear up indexMap
  for (auto it=indexMap.begin(); it != indexMap.end(); ++it) {
    free (it->second);
  }

  free(buffer);
  free(key);
  ifs.close();
  return 0;
}

RC CLI::insertTuple() {
  char * token = next();
  if (!expect(token, "into"))
    return error("expecting into" + __LINE__);

  string tableName = next();

  token = next(); // tuple
  if (!expect(token, "tuple"))
    return error("expectin tuple" + __LINE__);

  // get attributes from catalog
  Attribute attr;
  vector<Attribute> attributes;
  this->getAttributesFromCatalog(tableName, attributes);
  int offset = 0, index = 0;
  int length;
  void *buffer = malloc(PF_PAGE_SIZE);
  memset(buffer, 0, PF_PAGE_SIZE);
  void *key = malloc(PF_PAGE_SIZE);
  RID rid;

  // find out if there is any index for tableName
  unordered_map<int, void *> indexMap;
  for (uint i = 0; i < attributes.size(); i++) {
    if (this->checkAttribute(tableName, attributes[i].name, rid, false))
      // add index to index-map
      indexMap[i] = malloc(PF_PAGE_SIZE);
  }

  // tokenize input
  token = next();
  while (token != NULL) {
    attr = attributes[index];
    if (!expect(token, attributes[index].name))
      return error("this table does not have this attribute!");
    token = next(); // eat =
    token = next();
    if (attr.type == TypeVarChar) {
      string varChar = string(token);
      length = varChar.size();
      memcpy((char *)buffer + offset, &length, sizeof(int));
      offset += sizeof(int);
      memcpy((char *)buffer + offset, varChar.c_str(), length);
      offset += length;

      auto got = indexMap.find(index);
      if (got != indexMap.end()) {
        memcpy((char *) indexMap[index], (char *)buffer+offset-sizeof(int)-length, sizeof(int)+length);
      }
    } 
    else if (attr.type == TypeInt) {
      int num = atoi(token);
      memcpy((char *)buffer + offset, &num, sizeof(num));
      offset += sizeof(num);

      auto got = indexMap.find(index);
      if (got != indexMap.end())
        memcpy((char *) indexMap[index], (char *)buffer+offset-sizeof(int), sizeof(int));
    }
    else if (attr.type == TypeReal) {
      float num = atof(token);
      memcpy((char *)buffer + offset, &num, sizeof(num));
      offset += sizeof(num);

      auto got = indexMap.find(index);
      if (got != indexMap.end())
        memcpy((char *) indexMap[index], (char *)buffer+offset-sizeof(float), sizeof(int));
    }
    else if (attr.type == TypeBoolean || attr.type == TypeShort) {
      return error("I do not wanna add this type of variable"); 
    }
    token = next();
    index += 1;
  }
  if (this->insertTupleToDB(tableName, attributes, buffer, indexMap) != 0) {
    return error("error while inserting tuple");
  }

  // clear up indexMap
  for (auto it=indexMap.begin(); it != indexMap.end(); ++it) {
    free (it->second);
  }

  free(buffer);
  free(key);
  return 0;
}

RC CLI::insertTupleToDB(const string tableName, const vector<Attribute> attributes, const void *data, unordered_map<int, void *> indexMap) {
  RID rid;

  // insert data to given table
  if (rm->insertTuple(tableName, data, rid) != 0)
    return error("error CLI::insertTuple in rm->insertTuple");

  // if there is any index in table, insert index as well
  for (auto it=indexMap.begin(); it != indexMap.end(); ++it) {
    IX_IndexHandle ixHandle;
    if (ixManager->OpenIndex(tableName, attributes[it->first].name, ixHandle) != 0)
      return error("error in opening index in insertTuple");

    if (ixHandle.InsertEntry(it->second, rid) != 0)
      return error("error in inserting entry to insert");
  }
  return 0;
}

RC CLI::printAttributes()
{  
  char * tokenizer = next();
  if (tokenizer == NULL) {
    error ("I expect tableName to print its attributes/columns");
    return -1;
  }

  string tableName = string(tokenizer);

  // get attributes of tableName
  Attribute attr;
  vector<Attribute> attributes;
  this->getAttributesFromCatalog(tableName, attributes);

  // update attributes
  vector<string> outputBuffer;
  outputBuffer.push_back("name");
  outputBuffer.push_back("type");
  outputBuffer.push_back("length");

  for (std::vector<Attribute>::iterator it = attributes.begin() ; it != attributes.end(); ++it) {
    outputBuffer.push_back(it->name);
    outputBuffer.push_back(to_string(it->type));
    outputBuffer.push_back(to_string(it->length));
  }

  return this->printOutputBuffer(outputBuffer, 3);
}

RC CLI::printIndex() {
  char * tokenizer = next();
  string columnName = string(tokenizer);

  tokenizer = next();
  if (!expect(tokenizer, "on")) {
    return error ("syntax error: expecting \"on\"");
  }

  tokenizer = next();
  string tableName = string(tokenizer);

  IX_IndexHandle ixHandle;
  if (ixManager->OpenIndex(tableName, columnName, ixHandle) != 0)
    return error ("error in OpenIndex::printIndex");

  IX_IndexScan ixScan;
  if (ixScan.OpenScan(ixHandle, NULL, NULL, false, false) != 0)
    return error("error in OpenScan::printIndex");

  vector<string> outputBuffer;
  RID rid;

  outputBuffer.push_back("PageNum");
  outputBuffer.push_back("SlotNum");
  while (ixScan.GetNextEntry(rid) == 0) {
    cout << "HERE" << endl;
    outputBuffer.push_back(to_string(rid.pageNum));
    outputBuffer.push_back(to_string(rid.slotNum));
  }

  return this->printOutputBuffer(outputBuffer, 2);
}

// print every tuples in given tableName
RC CLI::printTable(const string tableName)
{
  vector<Attribute> attributes;
  RC rc = this->getAttributesFromCatalog(tableName, attributes);
  if (rc != 0)
    return error ("table: " + tableName + " does not exist");

  // Set up the iterator
  RM_ScanIterator rmsi;
  RID rid;
  void *data_returned = malloc(4096);
  

  // convert attributes to vector<string>
  vector<string> stringAttributes;
  for (std::vector<Attribute>::iterator it = attributes.begin() ; it != attributes.end(); ++it)
    stringAttributes.push_back(it->name);

  rc = rm->scan(tableName, "", NO_OP, NULL, stringAttributes, rmsi);
  if (rc != 0)
    return rc;

  // print
  vector<string> outputBuffer;
  for (std::vector<Attribute>::iterator it = attributes.begin() ; it != attributes.end(); ++it) {
    outputBuffer.push_back(it->name);
  }

  while( (rc = rmsi.getNextTuple(rid, data_returned)) != RM_EOF) {
    if ( rc != 0) {
      cout << "fata" << endl;
      exit(1);
    }
      
    if (this->updateOutputBuffer(outputBuffer, data_returned, attributes) != 0) {
      free(data_returned);
      return error("problem in updateOutputBuffer");
    }
  }
  rmsi.close();
  free(data_returned);

  return this->printOutputBuffer(outputBuffer, attributes.size());
}

RC CLI::help(const string input)
{
  if (input.compare("create") == 0) {
    cout << "\tcreate table <tableName> (col1=type1, col2=type2, ...): creates table with given properties" << endl;
    cout << "\tcreate index <columnName> on <tableName>: creates index for <columnName> in table <tableName>" << endl;
  }
  else if (input.compare("add") == 0) {
    cout << "\tadd attribute \"attributeName=type\" to \"tableName\": drops given table" << endl;
  }
  else if (input.compare("drop") == 0) {
    cout << "\tdrop table <tableName>: drops given table" << endl;
    cout << "\tdrop index \"indexName\": drops given index" << endl;
    cout << "\tdrop attribute \"attributeName\" from \"tableName\": drops attributeName from tableName" << endl;
  }
  else if (input.compare("load") == 0) {
    cout << "\tload <tableName> \"fileName\"";
    cout << ": loads given filName to given table" << endl;
  }
  else if (input.compare("insert") == 0) {
    cout << "\tinsert into <tableName> tuple(attr1=val1, attr2=value2, ...)";
    cout << ": inserts given tuple to given tableName" << endl;
  }
  else if (input.compare("print") == 0) {
    cout << "\tprint <tableName>: print every record in tableName" << endl;
    cout << "\tprint attributes <tableName>: print columns of given tableName" << endl;
  }
  else if (input.compare("help") == 0) {
    cout << "\thelp <commandName>: print help for given command" << endl;
    cout << "\thelp: show help for all commands" << endl;
  }
  else if (input.compare("quit") == 0) {
    cout << "\tquit or exit: quit SecSQL. But remember, love never ends!" << endl;
  }
  else if (input.compare("all") == 0) {
    help("create");
    help("drop");
    help("load");
    help("print");
    help("insert");
    help("help");
    help("quit");
  }
  else {
    cout << "I dont know how to help you with <" << input << ">" << endl;
    return -1;
  }
  return 0;
}

RC CLI::getAttributesFromCatalog(const string tableName, vector<Attribute> &columns)
{
  return rm->getAttributes(tableName, columns);
}

// Add given table to CLI_TABLES
RC CLI::addTableToCatalog(const string tableName, const string file_url, const string type)
{
  int offset = 0;
  int length;
  void *buffer = malloc(COLUMNS_TABLE_RECORD_MAX_LENGTH);

  length = tableName.size();
  memcpy((char *)buffer + offset, &length, sizeof(int));
  offset += sizeof(int);
  memcpy((char *)buffer + offset, tableName.c_str(), tableName.size());
  offset += tableName.size();

  length = file_url.size();
  memcpy((char *)buffer + offset, &length, sizeof(int));
  offset += sizeof(int);
  memcpy((char *)buffer + offset, file_url.c_str(), file_url.size());
  offset += file_url.size();

  length = type.size();
  memcpy((char *)buffer + offset, &length, sizeof(int));
  offset += sizeof(int);
  memcpy((char *)buffer + offset, type.c_str(), type.size());
  offset += type.size();

  RID rid;
  RC ret = rm->insertTuple(CLI_TABLES, buffer, rid);

  free(buffer);
  return ret;
}

// adds given attribute to CLI_COLUMNS
RC CLI::addAttributeToCatalog(const Attribute &attr, const string tableName, const int position)
{
  int offset = 0;
  int length;
  void *buffer = malloc(COLUMNS_TABLE_RECORD_MAX_LENGTH);

  length = attr.name.size();
  memcpy((char *)buffer + offset, &length, sizeof(int));
  offset += sizeof(int);
  memcpy((char *)buffer + offset, attr.name.c_str(), attr.name.size());
  offset += attr.name.size();

  length = tableName.size();
  memcpy((char *)buffer + offset, &length, sizeof(int));
  offset += sizeof(int);
  memcpy((char *)buffer + offset, tableName.c_str(), tableName.size());
  offset += tableName.size();

  memcpy((char *)buffer + offset, &position, sizeof(position));
  offset += sizeof(position);

  memcpy((char *)buffer + offset, &attr.type, sizeof(attr.type));
  offset += sizeof(attr.type);

  memcpy((char *)buffer + offset, &attr.length, sizeof(attr.length));
  offset += sizeof(attr.length);

  RID rid;
  RC ret = rm->insertTuple(CLI_COLUMNS, buffer, rid);

  free(buffer);
  return ret;
}

// Add given index to CLI_INDEXES
RC CLI::addIndexToCatalog(const string tableName, const string columnName)
{
  // Collect information from the catalog for the columnName
  vector<Attribute> columns;
  if (this->getAttributesFromCatalog(tableName, columns) != 0)
    return -1;

  int max_size = -1;
  bool is_variable = false;
  for (uint i = 0; i < columns.size(); i++) {
    if (columns[i].name == columnName) {
      if (columns[i].type == TypeVarChar) {
        max_size = columns[i].length + 2;
        is_variable = true;
      }
      else {
        max_size = columns[i].length;
      }
      break;
    }
  }

  if(max_size == -1)
    return error("max-size returns -1");

  int offset = 0;
  int length;
  void *buffer = malloc(tableName.size() + columnName.size()+8+4+1);
 
  length = tableName.size();
  memcpy((char *)buffer + offset, &length, sizeof(int));
  offset += sizeof(int);
  memcpy((char *)buffer + offset, tableName.c_str(), tableName.size());
  offset += tableName.size();

  length = columnName.size();
  memcpy((char *)buffer + offset, &length, sizeof(int));
  offset += sizeof(int);
  memcpy((char *)buffer + offset, columnName.c_str(), columnName.size());
  offset += length;

  memcpy((char *)buffer + offset, &max_size, sizeof(max_size));
  offset += sizeof(max_size);

  memcpy((char *)buffer + offset, &is_variable, sizeof(is_variable));
  offset += sizeof(is_variable);

  RID rid;
  RC rc = rm->insertTuple(CLI_INDEXES, buffer, rid);
  
  free(buffer);
  
  return rc;
}

RC CLI::history()
{
#ifndef NO_HISTORY_LIST
  HIST_ENTRY **the_list;
  int ii;
  the_list = history_list();
  if (the_list)
  for (ii = 0; the_list[ii]; ii++)
     printf ("%d: %s\n", ii + history_base, the_list[ii]->line);
#else
  HIST_ENTRY *the_list;
  the_list = current_history();
  vector<string> list;
  while (the_list) {
    list.push_back(the_list->line);
    the_list = next_history();
  }
  int tot = list.size();
  for (int i = tot-1; i >= 0; i--) {
    cout << (tot-i) << ": " << list[i] << endl;
  } 
#endif
  return 0;
}

// checks whether given tableName-columnName exists or not in cli_columns or cli_indexes
bool CLI::checkAttribute(const string tableName, const string columnName, RID &rid, bool searchColumns)
{
  string searchTable = CLI_COLUMNS;
  if (searchColumns == false)
    searchTable = CLI_INDEXES;

  vector<Attribute> attributes;
  this->getAttributesFromCatalog(CLI_COLUMNS, attributes);

  // Set up the iterator
  RM_ScanIterator rmsi;
  void *data_returned = malloc(PF_PAGE_SIZE);

  // convert attributes to vector<string>
  vector<string> stringAttributes;
  //stringAttributes.push_back("column_name");
  stringAttributes.push_back("table_name");
  
  // Find records whose column is columnName
  if( rm->scan(searchTable, "column_name", EQ_OP, columnName.c_str(), stringAttributes, rmsi) != 0)
    return -1;
  
  // check if tableName is what we want
  while(rmsi.getNextTuple(rid, data_returned) != RM_EOF){
    int length = 0, offset = 0;
  
    memcpy(&length, (char *)data_returned+offset, sizeof(int));
    offset += sizeof(int);

    char *str=(char *)malloc(length+1);
    str[length] = '\0';
    
    memcpy(str, (char *)data_returned+offset, length);
    offset += length;

    if(tableName.compare(string(str)) == 0) {
      free(data_returned);
      free(str);
      return true;
    }
    free(str);
  }
  free(data_returned);
  return false;
}

RC CLI::updateOutputBuffer(vector<string> &buffer, void *data, vector<Attribute> &attrs)
{
  int length, offset = 0, number;
  float fNumber;
  char *str;
  string record = "";
  for (std::vector<Attribute>::iterator it = attrs.begin() ; it != attrs.end(); ++it) {
    switch(it->type) {
      case TypeInt:
        number = 0;
        memcpy(&number, (char *)data+offset, sizeof(int));
        offset += sizeof(int);
        buffer.push_back(to_string(number));
        break;
      case TypeReal:
        fNumber = 0;
        memcpy(&fNumber, (char *)data+offset, sizeof(float));
        offset += sizeof(float);
        buffer.push_back(to_string(fNumber));
        break;
      case TypeVarChar:
        length = 0;
        memcpy(&length, (char *)data+offset, sizeof(int));
        
        if(length == 0){
          buffer.push_back("--");
          break;
        }

        offset += sizeof(int);

        str = (char *)malloc(length+1);
        memcpy(str, (char *)data+offset, length);
        str[length] = '\0';
        offset += length;

        buffer.push_back(str);
        free(str);
        break;
      case TypeShort:       
      case TypeBoolean:
        buffer.push_back(to_string((int)(*((char*)data+offset))));
        offset += 1;
        break;
    }
  }
  return 0;
}

// 2-pass output function
RC CLI::printOutputBuffer(vector<string> &buffer, uint mod)
{
  // find max for each column
  uint *maxLengths = new uint[mod];
  for(uint i=0; i < mod; i++)
    maxLengths[i] = 0;

  int index;
  for (uint i=0; i < buffer.size(); i++) {
    index = i%mod;
    maxLengths[index] = fmax(maxLengths[index], buffer[i].size());
  }

  uint startIndex = 0;
  int totalLength = 0;

  for(uint i=0; i < mod; i++) {
    cout << setw(maxLengths[i]) << left << buffer[i] << DIVISOR;
    totalLength += maxLengths[i] + DIVISOR_LENGTH;
  }
  cout << endl;

  // totalLength - 2 because I do not want to count for extra spaces after last column
  for (int i=0; i < totalLength-2; i++)
    cout << "=";
  startIndex = mod;

  // output columns
  for (uint i=startIndex; i < buffer.size(); i++) {
    if (i % mod == 0)
      cout << endl;
    cout << setw(maxLengths[i%mod]) << left << buffer[i] << DIVISOR;
  }
  cout << endl;
  delete[] maxLengths;
  return 0;
}

// advance tokenizer to next token
char * CLI::next()
{
  return strtok (NULL, DELIMITERS);
}

// return 0 if tokenizer is equal to expected string
bool CLI::expect(char * token, const string expected)
{
  if (token == NULL) {
    error ("tokenizer is null, expecting: " + expected);
    return -1;
  }
  return expected.compare(string(token)) == 0;
}

RC CLI::error(const string errorMessage)
{
  cout << errorMessage << endl;
  return -2;
}

RC CLI::error(uint errorCode)
{
  cout << errorCode << endl;
  return -3;
}

RC CLI::getAttribute(const string name, const vector<Attribute> pool, Attribute &attr) {
  for (uint i=0; i < pool.size(); i++) {
    if (0 == pool[i].name.compare(name)) {
      attr = pool[i];
      return 0;
    }
  }
  return error("attribute cannot be found");
}

RC CLI::getAttribute(const string tableName, const string attrName, Attribute &attr) {
  vector<Attribute> attrs;
  if (this->getAttributesFromCatalog(tableName, attrs) != 0)
    return error(__LINE__);
  return getAttribute(attrName, attrs, attr);
}

void CLI::addTableNameToAttrs(const string tableName, vector<string> &attrs) {
  for (uint i=0; i < attrs.size(); i++)
    attrs[i] = tableName + "." + attrs[i];
}
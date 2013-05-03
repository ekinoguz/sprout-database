#include "cli.h"

// Command parsing delimiters
// TODO: update delimiters later
#define DELIMITERS " =,()\""

// CVS file read delimiters
#define CVS_DELIMITERS ","
#define CLI_TABLES "cli_tables"
#define CLI_COLUMNS "cli_columns"
#define COLUMNS_TABLE_RECORD_MAX_LENGTH 150   // It is actually 112

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

  attr.name = "nullable";
  attr.type = TypeBoolean;
  attr.length = 1;
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
  attr.length = 50;
  table_attrs.push_back(attr);

  attr.name = "type";
  attr.type = TypeVarChar;
  attr.length = 20;
  table_attrs.push_back(attr);
  
	rm->createTable(CLI_TABLES, table_attrs);

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
}

CLI::~CLI()
{
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
		if (expect(tokenizer, "create") == 0) {
			tokenizer = next();
			if (tokenizer == NULL) {
				error ("I expect <table>");
				return 0;
			}
			string type = string(tokenizer);
			tokenizer = next();
			if (tokenizer == NULL) {
				error ("I expect <name> to be created");
				return 0;
			}
			string name = string(tokenizer);
			// if type equals table, then create table
			return createTable(name, tokenizer);
			// TODO: create index
		}
		else if (expect(tokenizer, "drop") == 0) {
			tokenizer = next();
			if (tokenizer == NULL) {
				error ("I expect <table> or <index>");
				return 0;
			}
			string type = string(tokenizer);
			tokenizer = next();
			if (tokenizer == NULL) {
				error ("I expect <name> to be dropped");
				return 0;
			}
			string name = string(tokenizer);
			drop(type, name);
		}
		else if (expect(tokenizer, "load") == 0) {
			tokenizer = next();
			
			if (tokenizer == NULL) {
				error ("I expect <tableName>");
				return 0;
			}
			string name = string(tokenizer);
			tokenizer = next();
			if (tokenizer == NULL) {
				error ("I expect <fileName> to be loaded");
				return 0;
			}
			string fileName = string(tokenizer);
			load(name, fileName);
		}
		else if (expect(tokenizer, "print") == 0) {
			tokenizer = next();
			if (tokenizer != NULL)
				print(string(tokenizer));
			else
				error ("I expect \"tableName\"");
		}
		else if (expect(tokenizer, "help") == 0) {
			tokenizer = next();
			if (tokenizer != NULL)
				help(string(tokenizer));
			else
				help("all");
		}
		else if (expect(tokenizer,"quit") == 0) {
			code = -1;
		}
		else {
			error ("i have no idea about this command, sorry");
		}
	}
	delete[] a;
	return code;
}

RC CLI::createTable(const string name, char * tokenizer)
{
	// parse col and types
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

		// get type
		tokenizer = next();
		if (tokenizer == NULL) {
			cout << "expecting type" << endl;
			break;
		}
		if (expect(tokenizer, "int") == 0) {
			attr.type = TypeInt;
			attr.length = 4;
		}
		else if (expect(tokenizer, "real") == 0) {
			attr.type = TypeReal;
			attr.length = 4;
		}
		else if (expect(tokenizer, "varchar") == 0) {
			attr.type = TypeVarChar;
			// read length
			tokenizer = next();
			attr.length = atoi(tokenizer);
		}
		else if (expect(tokenizer, "boolean") == 0) {
			attr.type = TypeBoolean;
			attr.length = 1;
		}
		else if (expect(tokenizer, "short") == 0) {
			attr.type = TypeShort;
			attr.length = 1;
		}
		else {
			// TODO this is actually error
			error ("problem in attribute type in create table");
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

RC CLI::drop(const string type, const string name)
{
	if (type.compare("table") == 0) {
		return rm->deleteTable(name);
	}
	else if (type.compare("index") == 0) {
		// TODO: drop index here
		return 0;
	}
	else {
		error ("I can either drop table or index");
		return -1;
	}
}

// CSV reader without escaping commas
// should be fixed
// reads files in data folder
RC CLI::load(const string tableName, const string fileName)
{
	cout << "we will load file <" << fileName << "> to table <" << tableName << ">" << endl;
	ifstream ifs;
	string file_url = string("../data/") + fileName;
	ifs.open (file_url, ifstream::in);

	if (!ifs.is_open()) {
		cout << "could not open file: " << file_url << endl;
		return -1;
	}

	string line;
	char * tokenizer;
	while (ifs.good()) {
		getline(ifs, line);

		char *a=new char[line.size()+1];
		a[line.size()] = 0;
		memcpy(a,line.c_str(),line.size());
		
		// tokenize input
		tokenizer = strtok(a, CVS_DELIMITERS);
		while (tokenizer != NULL) {
			cout << tokenizer << endl;
			tokenizer = strtok(NULL, CVS_DELIMITERS);
		}
		
	}

	ifs.close();
	return 0;
}

RC CLI::print(const string input)
{
	cout << "we will print table <" << input << ">" << endl;
	return 0;
}

RC CLI::help(const string input)
{
	if (input.compare("create") == 0) {
		cout << "\tcreate table \"tableName\" (col1=type1, col2=type2, ...): creates table with given properties" << endl;
	}
	else if (input.compare("drop") == 0) {
		cout << "\tdrop table \"tableName\": drops given table" << endl;
		cout << "\tdrop index \"indexName\": drops given index" << endl;
	}
	else if (input.compare("load") == 0) {
		cout << "\tload \"tableName\" \"fileName\"";
		cout << ": loads given filName to given table" << endl;
	}
	else if (input.compare("print") == 0) {
		cout << "\tprint \"tableName\"" << endl;
	}
	else if (input.compare("help") == 0) {
		cout << "\thelp <commandName>: print help for given command" << endl;
		cout << "\thelp: show help for all commands" << endl;
	}
	else if (input.compare("quit") == 0) {
		cout << "\tquit: quit SecSQL. But remember, love never ends!" << endl;
	}
	else if (input.compare("all") == 0) {
		help("create");
		help("drop");
		help("load");
		help("print");
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

  memcpy((char *)buffer + offset, &attr.nullable, sizeof(attr.nullable));
  offset += sizeof(attr.nullable);  

  RID rid;
  RC ret = rm->insertTuple(CLI_COLUMNS, buffer, rid);

  free(buffer);
  return ret;
}

// advance tokenizer to next token
char * CLI::next()
{
	return strtok (NULL, DELIMITERS);
}

// return 0 if tokenizer is equal to expected string
RC CLI::expect(char * tokenizer, const string expected)
{
	if (tokenizer == NULL) {
		error ("tokenizer is null, expecting: " + expected);
		return -1;
	}
	return expected.compare(string(tokenizer));
}

void CLI::error(const string errorMessage)
{
	cout << errorMessage << endl;
}
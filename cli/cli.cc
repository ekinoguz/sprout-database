#include "cli.h"

#define DELIMITERS " =,()\"" // TODO: update delimiters later

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
			tokenizer = next(tokenizer);
			if (tokenizer == NULL) {
				error ("I expect <table>");
				return 0;
			}
			string type = string(tokenizer);
			tokenizer = next(tokenizer);
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
			tokenizer = next(tokenizer);
			if (tokenizer == NULL) {
				error ("I expect <table> or <index>");
				return 0;
			}
			string type = string(tokenizer);
			tokenizer = next(tokenizer);
			if (tokenizer == NULL) {
				error ("I expect <name> to be dropped");
				return 0;
			}
			string name = string(tokenizer);
			drop(type, name);
		}
		else if (expect(tokenizer, "load") == 0) {
			tokenizer = next(tokenizer);
			
			if (tokenizer == NULL) {
				error ("I expect <tableName>");
				return 0;
			}
			string name = string(tokenizer);
			tokenizer = next(tokenizer);
			if (tokenizer == NULL) {
				error ("I expect <fileName> to be loaded");
				return 0;
			}
			string fileName = string(tokenizer);
			load(name, fileName);
		}
		else if (expect(tokenizer, "print") == 0) {
			tokenizer = next(tokenizer);
			if (tokenizer != NULL)
				print(string(tokenizer));
			else
				error ("I expect \"tableName\"");
		}
		else if (expect(tokenizer, "help") == 0) {
			tokenizer = next(tokenizer);
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
	vector<string> names;
	vector<string> types;
	while (tokenizer != NULL)
	{
		// get name if there is
		tokenizer = next(tokenizer);
		if (tokenizer == NULL) {
			break;
		}
		names.push_back(string(tokenizer));

		// get type
		tokenizer = next(tokenizer);
		if (tokenizer == NULL) {
			cout << "expecting type" << endl;
			break;
		}
		types.push_back(string(tokenizer));
	}
	cout << "create table for <" << name << "> and attributes:" << endl;
	for (std::vector<string>::iterator it = names.begin() ; it != names.end(); ++it)
    std::cout << ' ' << *it;
  cout << endl;
  for (std::vector<string>::iterator it = types.begin() ; it != types.end(); ++it)
    std::cout << ' ' << *it;
  cout << endl;
	return 0;
}

// check type, it should be either table or index
RC CLI::drop(const string type, const string name)
{
	cout << "we will drop <" << type << "> <" << name << ">" << endl;
	return 0;
}

RC CLI::load(const string tableName, const string fileName)
{
	cout << "we will load file <" << fileName << "> to table <" << tableName << ">" << endl;
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
		cout << "\tquit: quit SecSQL. But remember, you have to come back!" << endl;
	}
	else if (input.compare("all") == 0) {
		help("create");
		help("drop");
		help("load");
		help("print");
		help("help");
	}
	else {
		cout << "I dont know how to help you with <" << input << ">" << endl;
	}
	return 0;
}

// advance tokenizer to next token
char * CLI::next(char * tokenizer)
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
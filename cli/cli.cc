#include "cli.h"

#define DELIMITERS " =,()\""

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
	char *a=new char[input.size()+1];
	a[input.size()] = 0;
	memcpy(a,input.c_str(),input.size());

	// tokenize input
	char * tokenizer = strtok(a, DELIMITERS);
	if (tokenizer != NULL)
	{
		if (string(tokenizer).compare("create") == 0) {
			tokenizer = strtok (NULL, DELIMITERS);
			if (tokenizer == 0) {
				cout << "I expect <table>" << endl;
				return 0;
			}
			string type = string(tokenizer);
			tokenizer = strtok (NULL, DELIMITERS);
			if (tokenizer == 0) {
				cout << "I expect <name> to be created" << endl;
				return 0;
			}
			string name = string(tokenizer);
			// if type equals table, then create table
			return createTable(name, tokenizer);
			// TODO: index
		}
		else if (string(tokenizer).compare("drop") == 0) {
			tokenizer = strtok (NULL, DELIMITERS);
			if (tokenizer == 0) {
				cout << "I expect <table> or <index>" << endl;
				return 0;
			}
			string type = string(tokenizer);
			tokenizer = strtok (NULL, DELIMITERS);
			if (tokenizer == 0) {
				cout << "I expect <name> to be dropped" << endl;
				return 0;
			}
			string name = string(tokenizer);
			return drop(type, name);
		}
		else if (string(tokenizer).compare("load") == 0) {
			tokenizer = strtok (NULL, DELIMITERS);
			
			if (tokenizer == NULL) {
				cout << "I expect <tableName>" << endl;
				return 0;
			}
			string name = string(tokenizer);
			tokenizer = strtok (NULL, DELIMITERS);
			if (tokenizer == NULL) {
				cout << "I expect <fileName> to be loaded" << endl;
				return 0;
			}
			string fileName = string(tokenizer);
			return load(name, fileName);
		}
		else if (string(tokenizer).compare("print") == 0) {
			tokenizer = strtok (NULL, DELIMITERS);
			if (tokenizer != NULL)
				return print(string(tokenizer));
			else
				cout << "I expect \"tableName\"" << endl;
		}
		else if (string(tokenizer).compare("help") == 0) {
			tokenizer = strtok (NULL, DELIMITERS);
			if (tokenizer != NULL)
				return help(string(tokenizer));
			else
				return help("all");
		}
		else if (string(tokenizer).compare("quit") == 0) {
			delete[] a;
			return -1;
		}
		else {
			cout << "i have no idea about this command, sorry" << endl;
		}
	}
	delete[] a;
	return 0;
}

RC CLI::createTable(const string name, char * tokenizer)
{
	// parse col and types
	vector<string> names;
	vector<string> types;
	while (tokenizer != NULL)
	{
		// get name if there is
		tokenizer = strtok (NULL, DELIMITERS);
		if (tokenizer == NULL) {
			break;
		}
		names.push_back(string(tokenizer));

		// get type
		tokenizer = strtok (NULL, DELIMITERS);
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
		cout << "\tdrop \"tableName\": drops given table" << endl;
		cout << "\tdrop \"indexName\": drops given index" << endl;
	}
	else if (input.compare("load") == 0) {
		cout << "\tload \"tableName\" \"fileName\"";
		cout << ": loads given filName to given table" << endl;
	}
	else if (input.compare("print") == 0) {
		cout << "\tprint \"tableName\"" << endl;
	}
	else if (input.compare("help") == 0) {
		cout << "\thelp \"commandName\": print help for given command" << endl;
		cout << "\thelp: show help for all commands" << endl;
	}
	else if (input.compare("quit") == 0) {
		cout << "\tquit: quit SecSQL. But remember, you have to come back!" << endl;
	}
	else if (input.compare("all") == 0) {
		help("create");
		help("drop");
		help("drop");
		help("print");
		help("help");
	}
	else {
		cout << "I dont know how to help you with <" << input << ">" << endl;
	}
	return 0;
}
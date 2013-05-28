#include "qe.h"

RC Iterator::getNextTuple(void *data) {
	return -1;
}

void Iterator::getAttributes(vector<Attribute> &attrs) const{
}

Iterator::~Iterator() {

}

Filter::Filter(Iterator* input, const Condition &condition) {
}

Filter::~Filter() {

}

RC Filter::getNextTuple(void *data) {
	return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const {

}


// input: Iterator of input R
// vector containing attribute names
Project::Project(Iterator *input, const vector<string> &attrNames) {
	rm = RM::Instance();
	this->tablename = input->tablename;

	// get attributes from catalog
	vector<Attribute> fromCatalog;
	RC rc = rm->getAttributes(this->tablename, fromCatalog);
	if (rc != 0)
		error(__LINE__, rc);

	rc = rm->scan(this->tablename, "", NO_OP, NULL, attrNames, rmsi);
	if (rc != 0)
		error(__LINE__, rc);

	for (unsigned i=0; i < attrNames.size(); i++) {
		// find the given attrName[i] in fromCatalog and add it to this->attrs
		for (unsigned index=0; index < fromCatalog.size(); index++) {
			if (fromCatalog[index].name.compare(attrNames[i]) == 0) {
				this->attrs.push_back(fromCatalog[index]);
				break;
			}
		}
	}
}

Project::~Project() {
	rmsi.close();
}

RC Project::getNextTuple(void *data) {
	RID rid;
	RC rc = rmsi.getNextTuple(rid, data);
	return rc != RM_EOF ? 0 : rc;
}

// For attribute in vector<Attribute>, name it as rel.attr
void Project::getAttributes(vector<Attribute> &attrs) const {
  attrs.clear();
  attrs = this->attrs;
  
  string tmp;
  // For attribute in vector<Attribute>, name it as rel.attr
  for(unsigned i = 0; i < attrs.size(); ++i)
  {
      tmp = tablename;
      tmp += ".";
      tmp += attrs[i].name;
      attrs[i].name = tmp;
  }
}

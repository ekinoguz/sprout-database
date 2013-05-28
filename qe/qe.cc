#include "qe.h"


void Iterator::getAttributes(vector<Attribute> &attrs) const {

}

Iterator::~Iterator() {

}

RC Iterator::getNextTuple(void *data) {
	return -1;
}


///////////////////////////////////////////////
///////////////////////////////////////////////
// Nested Loop Join Interface //
///////////////////////////////////////////////
///////////////////////////////////////////////


// Iterator of input R
// TableScan Iterator of input S
// Join condition
// Number of pages can be used to do join (decided by the optimizer)
NLJoin::NLJoin(Iterator *leftIn,
               TableScan *rightIn,
               const Condition &condition,
               const unsigned numPages
        			) {

}

NLJoin::~NLJoin() {

}

RC NLJoin::getNextTuple(void *data) {
	return QE_EOF;
}
// For attribute in vector<Attribute>, name it as rel.attr
void NLJoin::getAttributes(vector<Attribute> &attrs) const{

}

///////////////////////////////////////////////
///////////////////////////////////////////////
// Index Nested Loop Join Interface //
///////////////////////////////////////////////
///////////////////////////////////////////////

// Iterator of input R
// IndexScan Iterator of input S
// Join condition
// Number of pages can be used to do join (decided by the optimizer)
INLJoin::INLJoin(	Iterator *leftIn,
					        IndexScan *rightIn,
					        const Condition &condition,
					        const unsigned numPages
								) {

}

INLJoin::~INLJoin() {

}

RC INLJoin::getNextTuple(void *data) {
	return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const {

}

///////////////////////////////////////////////
///////////////////////////////////////////////
// Aggregate Interface //
///////////////////////////////////////////////
///////////////////////////////////////////////

// Iterator of input R
// The attribute over which we are computing an aggregate
// Aggregate operation
Aggregate::Aggregate(	Iterator *input,
						          Attribute aggAttr,
						          AggregateOp op
										) {

}

// Extra Credit
// Iterator of input R
// The attribute over which we are computing an aggregate
// The attribute over which we are grouping the tuples
// Aggregate operation
Aggregate::Aggregate(Iterator *input,
					          Attribute aggAttr,
					          Attribute gAttr,
					          AggregateOp op
							) {

}

Aggregate::~Aggregate(){

}

RC Aggregate::Aggregate::getNextTuple(void *data) {
	return QE_EOF;
}

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const {

}


///////////////////////////////////////////////
///////////////////////////////////////////////
// Filter Interface //
///////////////////////////////////////////////
///////////////////////////////////////////////

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


///////////////////////////////////////////////
///////////////////////////////////////////////
// Project Interface //
///////////////////////////////////////////////
///////////////////////////////////////////////

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

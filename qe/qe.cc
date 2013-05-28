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
INLJoin::INLJoin(  Iterator *leftIn,
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
Aggregate::Aggregate(  Iterator *input,
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
  input->getAttributes(this->attrs);
  this->nextIndex = 0;
  RC rc;
  void *data = malloc(PF_PAGE_SIZE);
  int dataOffset, projectedDataOffset;

  while (0 == (rc = input->getNextTuple(data)))
  {
  	void *projectedData = malloc(PF_PAGE_SIZE);
    projectedDataOffset = 0;
    
    // project data to desired attributes
    for (unsigned i=0; i < attrNames.size(); i++) {
      dataOffset = 0;
      unsigned index;

      // find the desired attribute in data
      for (index = 0; index < this->attrs.size(); index++) {
        if (0 == this->attrs[index].name.compare(attrNames[i]))
          break;

        // calculate the offset to reach desired attribute
        switch(this->attrs[index].type) {
          case TypeInt:
            case TypeReal:
            case TypeShort:
            case TypeBoolean:
              dataOffset += this->attrs[index].length;
              break;
            case TypeVarChar:
              dataOffset += sizeof(int) + this->attrs[index].length;
              break;
          }
      }

      // copy desired attribute to projectedData
      switch(this->attrs[index].type){
        case TypeInt:
        case TypeReal:
        case TypeShort:
        case TypeBoolean:
            memcpy((char *)projectedData+projectedDataOffset, (char *) data+dataOffset, this->attrs[index].length);
            projectedDataOffset += this->attrs[index].length;
            break;
        case TypeVarChar:
            memcpy((char *)projectedData+projectedDataOffset, (char *)data+dataOffset, sizeof(int)+this->attrs[index].length);
            projectedDataOffset += sizeof(int)+this->attrs[index].length;
            break;
      }
    }
    results.push_back(projectedData);
    sizes.push_back(projectedDataOffset);
  }
  free(data);
  if (rc != 0)
    error(__LINE__, rc);
}

Project::~Project() {
  // free everything in results vector
  for (auto it=results.begin(); it != results.end(); ++it) {
    free(*it);
  }
}

RC Project::getNextTuple(void *data) {
  if (nextIndex < results.size()) {
  	memcpy(data, results[nextIndex], sizes[nextIndex]);
    nextIndex += 1;
    return 0;
  }
  return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void Project::getAttributes(vector<Attribute> &attrs) const {
  attrs = this->attrs;
}

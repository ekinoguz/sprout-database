#include "qe.h"

RC getAttributeOffsetAndIndex(const vector<Attribute> attrs, const string targetName, int &dataOffset, int &index) {
  // find the desired attribute in data
  for (int i = 0; i < attrs.size(); i++) {
    if (0 == attrs[i].name.compare(targetName)) {
      index = i;
      break;
    }
    // calculate the offset to reach desired attribute
    switch(attrs[i].type) {
      case TypeInt:
      case TypeReal:
      case TypeShort:
      case TypeBoolean:
        dataOffset += attrs[i].length;
        break;
      case TypeVarChar:
        dataOffset += sizeof(int) + attrs[i].length;
        break;
    }
  }
  cout << dataOffset << endl;
  return (index != -1) ? 0 : error(__LINE__, -1);
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
  input->getAttributes(this->attrs);
  this->nextIndex = 0;
  RC rc;
  void *data = malloc(PF_PAGE_SIZE);
  int dataOffset, filteredDataOffset;

  while (0 == (rc = input->getNextTuple(data)))
  {
    void *filteredData = malloc(PF_PAGE_SIZE);

    // check if data satisfies the given condition or not

    // get the left hand side attribute
    Attribute leftAttribute;
    rc = getAttribute(condition.lhsAttr, leftAttribute);
    if (rc != 0) {
      error(__LINE__, rc);
      return;
    }

    // bool condition = false;
    // switch(compOp){
    //   case EQ_OP:
    //   case NE_OP:
    //     switch(type){
    //     case TypeInt:
    //       condition = ( *(int*)lvalue == *(int*)value );
    //       break;
    //     case TypeReal:
    //       condition = (*(float*)lvalue == *(float*)value); 
    //       break;
    //     case TypeVarChar:
    //       if( strcmp((char *)lvalue,(char *)value ) == 0 )
    //         condition = true;

    //       break;
    //     case TypeShort:
    //       condition = (*(char*)lvalue == *(char*)value);
    //       break;  
    //     case TypeBoolean:
    //       condition = (*(bool*)lvalue == *(bool*)value);
    //       break;  
    //     }
    //   }    
    // condition = condition ^ (compOp == NE_OP);

    // compare left hand side attribute with right hand side value
    

    results.push_back(filteredData);
    sizes.push_back(filteredDataOffset);
  }
  free(data);
  if (rc != 0)
    error(__LINE__, rc);
}

Filter::~Filter() {
  // free everything in results vector
  for (auto it=results.begin(); it != results.end(); ++it) {
    free(*it);
  }
}

RC Filter::getNextTuple(void *data) {
  if (nextIndex < results.size()) {
    memcpy(data, results[nextIndex], sizes[nextIndex]);
    nextIndex += 1;
    return 0;
  }
  return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const {
  attrs = this->attrs;
}

RC Filter::getAttribute(const string name, Attribute &attr) {
  for (unsigned i=0; i < this->attrs.size(); i++) {
    if (0 == this->attrs[i].name.compare(name)) {
      attr = this->attrs[i];
      return 0;
    }
  }
  return error(__LINE__, -1);
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
  int projectedDataOffset;

  while (0 == (rc = input->getNextTuple(data)))
  {
  	void *projectedData = malloc(PF_PAGE_SIZE);
    projectedDataOffset = 0;
    
    // project data to desired attributes
    for (unsigned i=0; i < attrNames.size(); i++) {
      int dataOffset = 0;
      int index = -1;

      // find the desired attribute in data
      rc = getAttributeOffsetAndIndex(this->attrs, attrNames[i], dataOffset, index);
      if (rc != 0)
        error(__LINE__, rc);

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


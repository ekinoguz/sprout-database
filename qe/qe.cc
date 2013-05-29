#include "qe.h"

RC getAttributeOffsetAndIndex(const vector<Attribute> attrs, const string targetName, int &dataOffset, int &index) {
  index = -1;
  dataOffset = 0;
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
  return (index != -1) ? 0 : error(__LINE__, -1);
}

int getAttributeSize(const AttrType type, const void *data) {
  int size=0;
  switch(type){
    case TypeInt:
    case TypeReal:
    case TypeShort:
    case TypeBoolean:
      return sizeof(int);
    case TypeVarChar:
      memcpy(&size, (char *)data, sizeof(int));
      return size + sizeof(int);
    default:
      return error(__LINE__, -1);
    }
}

int getDataSize(const vector<Attribute> attr, const void *data) {
  int size=0, length=0;
  for (unsigned i=0; i < attr.size(); i++) {
    switch(attr[i].type){
      case TypeInt:
      case TypeReal:
      case TypeShort:
      case TypeBoolean:
        size += sizeof(int);
        break;
      case TypeVarChar:
        memcpy(&length, (char *)data+size, sizeof(int));
        size += length + sizeof(int);
        break;
      default:
        error(__LINE__, -1);
        break;
      }
  }
  return size;
}

RC getAttribute(const string name, const vector<Attribute> pool, Attribute &attr) {
  for (unsigned i=0; i < pool.size(); i++) {
    if (0 == pool[i].name.compare(name)) {
      attr = pool[i];
      return 0;
    }
  }
  return error(__LINE__, -1);
}

string getAttributeName(const void *data, const vector<Attribute> attrs, const int dataOffset, const int index) {
  int intVal;
  float floatVal;
  string output;
  std::ostringstream ss;
  char *str;
  
  switch(attrs[index].type) {
  case TypeInt:
    memcpy(&intVal, (char *)data+dataOffset, sizeof(int));
    ss << intVal;
    return ss.str();
  case TypeReal:
    memcpy(&floatVal, (char *)data+dataOffset, sizeof(float));
    ss << floatVal;
    return ss.str();
  case TypeVarChar:
    str = (char *)malloc(attrs[index].length);
    memset(str, 0, attrs[index].length);
    memcpy(&intVal, (char *)data+dataOffset, sizeof(int));
    memcpy(str, (char *)data+dataOffset+sizeof(int), intVal);
    output = str;
    free(str);
    return output;
  default:
    cout << "should not see this in getAttributeName" << endl;
    return "";
  }
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
Aggregate::Aggregate( Iterator *input,
                      Attribute aggAttr,
                      AggregateOp op
                    ) 
{
  init();
  input->getAttributes(this->attrs);
  RC rc = getAttributeOffsetAndIndex(attrs, aggAttr.name, aggOffset, aggIndex);
  if (rc != 0)
    error(__LINE__, rc);

  rc = this->doOp(input, op);
  if (rc != 0)
    error(__LINE__, rc);
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
                    ) 
{
  init();
  this->isGroupBy = true;
  this->resultAttribute = gAttr;
  input->getAttributes(this->attrs);
  RC rc = getAttributeOffsetAndIndex(attrs, aggAttr.name, aggOffset, aggIndex);
  if (rc != 0)
    error(__LINE__, rc);

  rc = getAttributeOffsetAndIndex(this->attrs, resultAttribute.name, groupByOffset, groupByIndex);
  if (0 != rc)
    error(__LINE__, -1);

  rc = this->doOp(input, op);
  if (rc != 0)
    error(__LINE__, rc);
}

Aggregate::~Aggregate(){
  free(result);
}

void Aggregate::init() {
  this->aggOffset = 0;
  this->aggIndex = 0;
  this->isGroupBy = false;
  this->outputInt = false;
  this->result = malloc(PF_PAGE_SIZE);
}

RC Aggregate::doOp(Iterator *input, AggregateOp op) {
  RC rc;
  switch(op) {
  case 0:
    if (this->attrs[aggIndex].type == TypeInt)
      this->outputInt = true;
    return rc = MIN(input);
  case 1:
  if (this->attrs[aggIndex].type == TypeInt)
      this->outputInt = true;
    return rc = MAX(input);
  case 2:
  if (this->attrs[aggIndex].type == TypeInt)
      this->outputInt = true;
    return rc = SUM(input);
  case 3:
    return rc = AVG(input);
  case 4:
    this->outputInt = true;
    return rc = COUNT(input);
  default:
    cout << "do not support this aggreate operation" << endl;
    return error(__LINE__, -1);
  }
}

RC Aggregate::MIN(Iterator *input) {
  void *data = malloc(PF_PAGE_SIZE);
  void *val = malloc(sizeof(int));
  int intMin = INT_MAX, intTmp=0;
  float floatMin = FLT_MAX, floatTmp=0.0;

  while (QE_EOF != input->getNextTuple(data))
  {
    memcpy((char *)val, (char *)data+aggOffset, sizeof(int));
    switch(attrs[aggIndex].type) {
    case TypeInt:
      intTmp = *((int *) val);
      if (isGroupBy) {
        string str = getAttributeName(data, this->attrs, groupByOffset, groupByIndex);
        auto got = results.find(str);
        if (got == results.end())
          results.emplace(str, intTmp);
        else
          results[str] = fmin(got->second, intTmp);
      } else {
        intMin = fmin(intMin, intTmp);
        this->updateResultMap("no-group-by", intMin, false);  
      }
      break;
    case TypeReal:
      floatTmp = *((float *) val);
      if (isGroupBy) {
        string str = getAttributeName(data, this->attrs, groupByOffset, groupByIndex);
        auto got = results.find(str);
        if (got == results.end())
          results.emplace(str, floatTmp);
        else
          results[str] = fmin(got->second, floatTmp);
      } else {
        floatMin = fmin(floatMin, floatTmp);
        this->updateResultMap("no-group-by", floatMin, false);  
      }
      break;
    default:
      break;
    }
  }
  free(data);
  free(val);
  return 0;
}

RC Aggregate::MAX(Iterator *input) {
  void *data = malloc(PF_PAGE_SIZE);
  void *val = malloc(sizeof(int));
  int intMax = INT_MIN, intTmp=0;
  float floatMax = FLT_MIN, floatTmp=0.0;
  while (QE_EOF != input->getNextTuple(data))
  {
    memcpy((char *)val, (char *)data+aggOffset, sizeof(int));
    switch(attrs[aggIndex].type) {
    case TypeInt:
      intTmp = *((int *) val);
      
      if (isGroupBy) {

      } else {
        intMax = fmax(intMax, intTmp);
        this->updateResultMap("no-group-by", intMax, false);  
      }
      break;
    case TypeReal:
      floatTmp = *((float *) val);
      
      if (isGroupBy) {

      } else {
        floatMax = fmax(floatMax, floatTmp);
        this->updateResultMap("no-group-by", floatMax, false);  
      }
      break;
    default:
      break;
    }
  }
  free(data);
  free(val);
  return 0;
}

RC Aggregate::SUM(Iterator *input) {
  void *data = malloc(PF_PAGE_SIZE);
  void *val = malloc(sizeof(int));
  int intSum = 0, intTmp=0;
  float floatSum = 0.0, floatTmp=0.0;
  while (QE_EOF != input->getNextTuple(data))
  {
    memcpy((char *)val, (char *)data+aggOffset, sizeof(int));
    switch(attrs[aggIndex].type) {
    case TypeInt:
      intTmp = *((int *) val);
      
      if (isGroupBy) {

      } else {
        this->updateResultMap("no-group-by", intTmp, true);  
      }
      break;
    case TypeReal:
      floatTmp = *((float *) val);
      if (isGroupBy) {

      } else {
        this->updateResultMap("no-group-by", floatTmp, true);  
      }
      break;
    default:
      break;
    }
  }
  free(data);
  free(val);
  return 0;
}

RC Aggregate::AVG(Iterator *input) {
  void *data = malloc(PF_PAGE_SIZE);
  void *val = malloc(sizeof(int));
  int intSum = 0, intTmp=0;
  float floatSum = 0.0, floatTmp=0.0;
  int count = 0;
  while (QE_EOF != input->getNextTuple(data))
  {
    count++;
    memcpy((char *)val, (char *)data+aggOffset, sizeof(int));
    switch(attrs[aggIndex].type) {
    case TypeInt:
      intTmp = *((int *) val);
      if (isGroupBy) {

      } else {
        this->updateResultMap("no-group-by", intTmp, true);
        this->updateCounterMap("no-group-by");
      }
      break;
    case TypeReal:
      floatTmp = *((float *) val);
      if (isGroupBy) {

      } else {
        this->updateResultMap("no-group-by", floatTmp, true);
        this->updateCounterMap("no-group-by");
      }
      break;
    default:
      break;
    }
  }
  double sum;
  // calculate the average
  for ( auto it = counters.begin(); it != counters.end(); ++it ) {
    sum = results[it->first];
    sum = sum / it->second;
    this->updateResultMap(it->first, sum, false);
  }
  
  free(data);
  free(val);
  return 0;
}

RC Aggregate::COUNT(Iterator *input) {
  void *data = malloc(PF_PAGE_SIZE);
  while (QE_EOF != input->getNextTuple(data))
  {
    if (isGroupBy) {

    } else {
      this->updateResultMap("no-group-by", 1, true);  
    }
  }
  free(data);
  return 0;
}

RC Aggregate::Aggregate::getNextTuple(void *data) {
  auto it = results.begin();
  if (it == results.end()) {
    return QE_EOF;
  }

  int offset = 0, length = 0, intVal;
  float floatVal;
  // if we have a group-by attribute, add the attribute name to result
  if (this->isGroupBy) {
    length = (it->first).size();
    switch(resultAttribute.type) {
    case TypeInt:
      intVal = atoi((it->first).c_str());
      memcpy((char *)data+offset, &intVal, sizeof(int));
      offset += sizeof(int);
      break;
    case TypeReal:
      floatVal = ::atof((it->first).c_str());
      memcpy((char *)data+offset, &floatVal, sizeof(float));
      offset += sizeof(float);
      break;
    case TypeVarChar:
      memcpy((char *)data+offset, &length, sizeof(int));
      offset += sizeof(int);
      memcpy((char *)data+offset, (it->first).c_str(), length);
      offset += length;
      break;
    default:
      cout << "should not see this in getAttributeName" << endl;
      break;
    }
  }
  // now add the result to output
  if (this->outputInt){
    intVal = (int)it->second;
    memcpy((char *)data+offset, &intVal, sizeof(int));
    offset += sizeof(int);
  } else {
    floatVal = it->second;
    memcpy((char *)data+offset, &(floatVal), sizeof(float));
    offset += sizeof(int);
  }
  results.erase(it->first);
    
  return 0;
}

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const {
  attrs = this->attrs;
}

// if cumulative = true, add the result to previous result
// otherwise overwrite the result
void Aggregate::updateResultMap(const string name, const double value, const bool cumulative) {
  auto got = results.find(name);
  if ( got == results.end() )
    results.emplace(name, value);
  else {
    int val = results.find(name)->second;
    results.erase(name);
    if (cumulative)
      results.emplace(name, val+value);
    else
      results.emplace(name, value);
  }
}

// increment counter by 1 for the given name
void Aggregate::updateCounterMap(const string name) {
  auto got = counters.find(name);
  if ( got == counters.end() ) {
    counters.emplace(name, 1);
  }
  else {
    int val = counters.find(name)->second;
    counters.erase(name);
    counters.emplace(name, val+1);
  }
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
  void *lvalue = malloc(PF_PAGE_SIZE);
  void *value = malloc(PF_PAGE_SIZE);
  int dataOffset, filteredDataOffset;

  while (QE_EOF != input->getNextTuple(data))
  {
    // get the left hand side attribute
    int dataOffset = 0, index = -1;
    
    // find the desired attribute in data
    rc = getAttributeOffsetAndIndex(this->attrs, condition.lhsAttr, dataOffset, index);
    if (rc != 0)
      error(__LINE__, rc);

    // copy attribute to lvalue
    int size = getAttributeSize(this->attrs[index].type, data);
    memcpy((char *)lvalue, (char *)data+dataOffset, size);

    // copy right value to value
    int rightSize = getAttributeSize(condition.rhsValue.type, condition.rhsValue.data);
    memcpy((void *)value, (void *)condition.rhsValue.data, rightSize);
    
    // check if data satisfies the given condition or not
    bool satisfy = false;
    switch(condition.op){
    case EQ_OP:
    case NE_OP:
      switch(this->attrs[index].type){
      case TypeInt:
        satisfy = ( *(int*)lvalue == *(int*)value );
        break;
      case TypeReal:
        satisfy = (*(float*)lvalue == *(float*)value); 
        break;
      case TypeVarChar:
        if( strcmp((char *)lvalue,(char *)value ) == 0 )
          satisfy = true;
        break;
      case TypeShort:
        satisfy = (*(char*)lvalue == *(char*)value);
        break;  
      case TypeBoolean:
        satisfy = (*(bool*)lvalue == *(bool*)value);
        break;  
      }    
      satisfy = satisfy ^ (condition.op == NE_OP);
      break;
    case LT_OP:
    case GE_OP:
      switch(this->attrs[index].type){
      case TypeInt:
        satisfy = ( *(int*)lvalue < *(int*)value );
        break;
      case TypeReal:
        satisfy = (*(float*)lvalue < *(float*)value); 
        break;
      case TypeVarChar:
        if( strcmp((char *)lvalue,(char *)value ) < 0 )
          satisfy = true;
        break;
      case TypeShort:
        satisfy = (*(char*)lvalue < *(char*)value);
        break;  
      case TypeBoolean:
        satisfy = (*(bool*)lvalue != *(bool*)value);
        break;  
      }    
      satisfy = satisfy ^ (condition.op == GE_OP);
      break;
    case GT_OP:
    case LE_OP:
      switch(this->attrs[index].type){
      case TypeInt:
        satisfy = ( *(int*)lvalue > *(int*)value );
        break;
      case TypeReal:
        satisfy = (*(float*)lvalue > *(float*)value); 
        break;
      case TypeVarChar:
        if( strcmp((char *)lvalue,(char *)value ) > 0 )
          satisfy = true;
        break;
      case TypeShort:
        satisfy = (*(char*)lvalue > *(char*)value);
        break;  
      case TypeBoolean:
        satisfy = (*(bool*)lvalue != *(bool*)value);
        break;  
      }    
      satisfy = satisfy ^ (condition.op == LE_OP);
      break;
    case NO_OP: // We should never actually reach here
      satisfy = true;
      break;
    default:
      cout << "Operation not supported" << endl;
      error(__LINE__, -3);
    }
    if (satisfy) {
      void *filteredData = malloc(PF_PAGE_SIZE);
      filteredDataOffset = getDataSize(this->attrs, data);
      memcpy(filteredData, data, filteredDataOffset);
      results.push_back(filteredData);
      sizes.push_back(filteredDataOffset);
    }
    // break;
  }
  free(value);
  free(lvalue);
  free(data);
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

  while (QE_EOF != input->getNextTuple(data))
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
      int length;
      switch(this->attrs[index].type){
        case TypeInt:
        case TypeReal:
        case TypeShort:
        case TypeBoolean:
          memcpy((char *)projectedData+projectedDataOffset, (char *) data+dataOffset, this->attrs[index].length);
          projectedDataOffset += this->attrs[index].length;
          break;
        case TypeVarChar:
          memcpy(&length, (char *)data+dataOffset, sizeof(int));
          memcpy((char *)projectedData+projectedDataOffset, (char *)data+dataOffset, sizeof(int)+length);
          projectedDataOffset += sizeof(int)+length;
          break;
      }
    }
    results.push_back(projectedData);
    sizes.push_back(projectedDataOffset);
  }
  free(data);
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
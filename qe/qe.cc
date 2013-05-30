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
              )
{
  this->leftIn = leftIn;
  this->rightIn = rightIn;
  this->condition = condition;
  this->left_has_more = true;

  vector<Attribute> attrs;
  leftIn->getAttributes(attrs);
  this->max_left_record_size = 0;
  for (uint i = 0; i < attrs.size(); i++)
    {
      this->max_left_record_size += attrs[i].length;
    }

  attrs.clear();
  rightIn->getAttributes(attrs);
  this->max_right_record_size = 0;
  for (uint i = 0; i < attrs.size(); i++)
    {
      this->max_right_record_size += attrs[i].length;
    }
  this->right_tuple = malloc(this->max_right_record_size);

  this->num_of_block_records = floor(((double)numPages * PF_PAGE_SIZE) / this->max_left_record_size);

  this->readBlockLeftIn();
}

NLJoin::~NLJoin()
{
  auto it = this->tuples_map.begin();
  while (it != this->tuples_map.end())
    {
      for (uint i = 0; it->second.size(); i++)
	{
	  free(it->second[i].tuple);
	}
    }

  this->tuples_map.clear();

  free(this->right_tuple);
}

RC NLJoin::readBlockLeftIn()
{
  void *tuple = malloc(this->max_left_record_size);

  auto it = this->tuples_map.begin();
  while (it != this->tuples_map.end())
    {
      for (uint i = 0; it->second.size(); i++)
	{
	  free(it->second[i].tuple);
	}
    }

  this->tuples_map.clear();
  unsigned tuples_read = 0;
  while (tuples_read <= this->num_of_block_records)
    {
      memset(tuple, 0, this->max_left_record_size);
      if (this->leftIn->getNextTuple(tuple) != 0)
	{
	  this->left_has_more = false;
	  break;
	}

      string key = getKey(this->leftIn, this->condition.lhsAttr, tuple);
      unsigned tuple_size = getTupleSize(this->leftIn, tuple);
      TupleInfo info;
      info.tuple = malloc(tuple_size);
      memcpy(info.tuple, tuple, tuple_size);
      info.size = tuple_size;

      it = this->tuples_map.find(key);
      if (it == this->tuples_map.end())
	{
	  vector<TupleInfo> tuples_info;
	  pair<string, vector<TupleInfo> > new_element (key, tuples_info);
	  this->tuples_map.insert(new_element);
	}

      it = this->tuples_map.find(key);
      it->second.push_back(info);

      free(info.tuple);

      tuples_read++;
    }

  free(tuple);
  return 0;
}

string NLJoin::getKey(Iterator *iter, string attribute, void *tuple)
{
  char *key;

  vector<Attribute> attrs;
  iter->getAttributes(attrs);

  bool found = false;
  unsigned offset = 0;
  for (uint i = 0; i < attrs.size(); i++)
    {
      if (attrs[i].name == attribute)
	{
	  if (attrs[i].type == TypeVarChar)
	    {
	      int attribute_size = 0;
	      memcpy(&attribute_size, (char *)tuple + offset, sizeof(attribute_size));
	      offset += sizeof(attribute_size);

	      key = (char*)malloc(attribute_size + 1);
	      memset(key, 0, attribute_size + 1);
	      memcpy(key, (char *)tuple + offset, attribute_size);

	      offset += attribute_size;
	    }
	  else
	    {
	      key = (char*)malloc(attrs[i].length + 1);
	      memset(key, 0, attrs[i].length + 1);
	      memcpy(key, (char *)tuple + offset, attrs[i].length);

	      offset += attrs[i].length;
	    }

	  found = true;
	}
      else
	{
	  if (attrs[i].type == TypeVarChar)
	    {
	      int attribute_size = 0;
	      memcpy(&attribute_size, (char *)tuple + offset, sizeof(attribute_size));
	      offset += sizeof(attribute_size) + attribute_size;
	    }
	  else
	    {
	      offset += attrs[i].length;
	    }	  
	}
    }

  if (found)
    {
      return string(key);
    }
  else
    {
      return "";
    }
}

unsigned NLJoin::getTupleSize(Iterator *iter, void *tuple)
{
  vector<Attribute> attrs;
  iter->getAttributes(attrs);

  unsigned offset = 0;
  for (uint i = 0; i < attrs.size(); i++)
    {
      if (attrs[i].type == TypeVarChar)
	{
	  int attribute_size = 0;
	  memcpy(&attribute_size, (char *)tuple + offset, sizeof(attribute_size));
	  offset += sizeof(attribute_size) + attribute_size;
	}
      else
	{
	  offset += attrs[i].length;
	}
    }

  return offset;
}


RC NLJoin::getNextTuple(void *data)
{
  if (this->tuples_info_more == true)
    {
      if (this->tuples_info_index < this->tuples_info.size())
	{
	  memcpy(data, this->tuples_info[this->tuples_info_index].tuple, this->tuples_info[this->tuples_info_index].size);
	  memcpy((char *)data + this->tuples_info[this->tuples_info_index].size, this->right_tuple, this->max_right_record_size);

	  this->tuples_info_index++;

	  return 0;
	}
      else
	{
	  this->tuples_info_more == false;
	}
    }

  memset(this->right_tuple, 0, this->max_right_record_size);
  while (true)
    {
      if (this->rightIn->getNextTuple(this->right_tuple) != 0)
	{
	  if (this->left_has_more == false)
	    {
	      return QE_EOF;
	    }
	  else
	    {
	      readBlockLeftIn();
	      this->rightIn->setIterator();
	      return getNextTuple(data);
	    }
	}
      
      string key = getKey(this->rightIn, this->condition.rhsAttr, this->right_tuple);
      auto it = this->tuples_map.find(key);
      if (it != this->tuples_map.end())
	{
	  memcpy(data, it->second[0].tuple, it->second[0].size);
	  memcpy((char *)data + it->second[0].size, this->right_tuple, this->max_right_record_size);

	  if (it->second.size() > 1)
	    {
	      this->tuples_info = it->second;
	      this->tuples_info_index = 1;
	      this->tuples_info_more = true;
	    }

	  return 0;
	}
    }
}
// For attribute in vector<Attribute>, name it as rel.attr
void NLJoin::getAttributes(vector<Attribute> &attrs) const
{

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
  RC rc = getAttributeOffsetAndIndex(attrs, aggAttr.name, dataOffset, index);
  if (rc != 0)
    error(__LINE__, rc);

  switch(op) {
  case 0:
    rc = MIN(input);
    break;
  case 1:
    rc = MAX(input);
    break;
  case 2:
    rc = SUM(input);
    break;
  case 3:
    rc = AVG(input);
    break;
  case 4:
    rc = COUNT(input);
    break;
  default:
    cout << "do not support this aggreate operation" << endl;
    break; 
  }
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
}

Aggregate::~Aggregate(){
  free(result);
}

void Aggregate::init() {
  this->done = false;
  this->dataOffset = 0;
  this->index = 0;
  this->result = malloc(PF_PAGE_SIZE);
}

RC Aggregate::MIN(Iterator *input) {
  void *data = malloc(PF_PAGE_SIZE);
  void *val = malloc(sizeof(int));
  int intMin = INT_MAX, intTmp=0;
  float floatMin = FLT_MAX, floatTmp=0.0;
  while (QE_EOF != input->getNextTuple(data))
  {
    memcpy((char *)val, (char *)data+dataOffset, sizeof(int));
    switch(attrs[index].type) {
    case TypeInt:
      intTmp = *((int *) val);
      intMin = fmin(intMin, intTmp); 
      break;
    case TypeReal:
      floatTmp = *((float *) val);
      floatMin = fmin(floatMin, floatTmp);
      break;
    default:
      break;
    }
  }
  if (intMin != INT_MAX)
    memcpy(result, &intMin, sizeof(int));
  else if (floatMin != FLT_MAX)
    memcpy(result, &floatMin, sizeof(float));
  else
    return error(__LINE__, -87);
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
    memcpy((char *)val, (char *)data+dataOffset, sizeof(int));
    switch(attrs[index].type) {
    case TypeInt:
      intTmp = *((int *) val);
      intMax = fmax(intMax, intTmp); 
      break;
    case TypeReal:
      floatTmp = *((float *) val);
      floatMax = fmax(floatMax, floatTmp);
      break;
    default:
      break;
    }
  }
  if (intMax != INT_MIN)
    memcpy(result, &intMax, sizeof(int));
  else if (floatMax != FLT_MIN)
    memcpy(result, &floatMax, sizeof(float));
  else
    return error(__LINE__, -87);
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
    memcpy((char *)val, (char *)data+dataOffset, sizeof(int));
    switch(attrs[index].type) {
    case TypeInt:
      intTmp = *((int *) val);
      intSum += intTmp;
      break;
    case TypeReal:
      floatTmp = *((float *) val);
      floatSum += floatTmp;
      break;
    default:
      break;
    }
  }
  if (intSum != 0)
    memcpy(result, &intSum, sizeof(int));
  else if (floatSum != 0.0)
    memcpy(result, &floatSum, sizeof(float));
  else
    return error(__LINE__, -87);
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
    memcpy((char *)val, (char *)data+dataOffset, sizeof(int));
    switch(attrs[index].type) {
    case TypeInt:
      intTmp = *((int *) val);
      intSum += intTmp;
      break;
    case TypeReal:
      floatTmp = *((float *) val);
      floatSum += floatTmp;
      break;
    default:
      break;
    }
  }
  float out;
  if (intSum != 0) {
    out = intSum/(float)count;
    memcpy(result, &out, sizeof(int));
  }
  else if (floatSum != 0.0) {
    out = floatSum/(float)count;
    memcpy(result, &out, sizeof(float));
  }
  else
    return error(__LINE__, -87);
  free(data);
  free(val);
  return 0;
}

RC Aggregate::COUNT(Iterator *input) {
  int count = 0;
  void *data = malloc(PF_PAGE_SIZE);
  while (QE_EOF != input->getNextTuple(data))
  {
    count += 1;
  }
  memcpy(result, &count, sizeof(int));
  free(data);
  return 0;
}

RC Aggregate::Aggregate::getNextTuple(void *data) {
  if (!done) {
    memcpy(data, result, sizeof(int));
    done = true;
    return 0;
  }
  return QE_EOF;
}

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const {
  attrs = this->attrs;
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

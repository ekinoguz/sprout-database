#include "qe.h"



RC getAttributeOffsetAndIndex(const vector<Attribute> attrs, const string targetName, const void *data, int &dataOffset, int &index) {
  index = -1;
  dataOffset = 0;
  int length=0;
  // find the desired attribute in data
  for (uint i = 0; i < attrs.size(); i++) {
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
        memcpy(&length, (char *)data+dataOffset, sizeof(int));
        dataOffset += sizeof(int) + length;
        break;
    }
  }
  return (index != -1) ? 0 : error(__LINE__, -1);
}

int getAttributeFromData(const void *buffer, const vector<Attribute> attrs, const string target, void *data) {
  int length=0;
  int offset = 0;
  int i;
  // find the desired attribute in data
  if (getAttributeOffsetAndIndex(attrs, target, buffer, offset, i) != 0)
    return -1;
  
  switch(attrs[i].type) {
    case TypeInt:
    case TypeReal:
    case TypeShort:
    case TypeBoolean:;
      memcpy((char *)data, (char *)buffer+offset, sizeof(int));
      offset += sizeof(int);
      break;
    case TypeVarChar:
      memcpy(&length, (char *)buffer+offset, sizeof(int));
      memcpy((char *)data, (char *)buffer+offset, sizeof(int)+length);
      offset += sizeof(int)+length;
      break;
    default:
      return -1;
  }
  return offset;
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

  this->tuples_info_more = false;
  this->left_block_has_more = false;
}

NLJoin::~NLJoin()
{
  auto it = this->tuples_map.begin();
  while (it != this->tuples_map.end())
    {
      for (uint i = 0; i < it->second.size(); i++)
	{
	  free(it->second[i].tuple);
	}

      it++;
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
      for (uint i = 0; i < it->second.size(); i++)
	{
	  free(it->second[i].tuple);
	}

      it++;
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

      tuples_read++;
    }

  free(tuple);
  return 0;
}

// Return the key as a string of hex digits
string NLJoin::getKey(Iterator *iter, string attribute, void *tuple)
{
  char *key;

  vector<Attribute> attrs;
  iter->getAttributes(attrs);

  bool found = false;
  unsigned offset = 0;
  unsigned key_size;
  for (uint i = 0; i < attrs.size(); i++)
    {
      if (attrs[i].name == attribute)
	{
	  if (attrs[i].type == TypeVarChar)
	    {
	      int attribute_size = 0;
	      memcpy(&attribute_size, (char *)tuple + offset, sizeof(attribute_size));
	      offset += sizeof(attribute_size);
	      
	      key_size = attribute_size;

	      key = (char*)malloc(attribute_size);
	      memset(key, 0, attribute_size);
	      memcpy(key, (char *)tuple + offset, attribute_size);

	      offset += attribute_size;
	    }
	  else
	    {
	      key_size = attrs[i].length;

	      key = (char*)malloc(attrs[i].length);
	      memset(key, 0, attrs[i].length);
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
      const char* const lut = "0123456789ABCDEF";

      string input;
      input.reserve(key_size);
      if(!is_big_endian())
	for (int i = key_size - 1; i >= 0; i--)
	  {
	    input.push_back(key[i]);
	  }
      else
	for (uint i = 0; i < key_size; i++)
	  {
	    input.push_back(key[i]);
	  }
      
      string strKey;
      unsigned char c;
      strKey.reserve(2 * key_size);
      for (uint i = 0; i < key_size; ++i)
	{
	  c = input[i];
	  strKey.push_back(lut[c >> 4]);
	  strKey.push_back(lut[c & 15]);
	}

      free(key);
      return strKey;
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
	  this->tuples_info_more = false;
	}
    }

  bool skip_right_read = false;
  if (this->left_block_has_more == true && this->left_block_it != this->tuples_map.end())
    {
      skip_right_read = true;
    }
  else
    {
      this->left_block_it = tuples_map.begin();
      this->left_block_has_more = false;
    }

  while (true)
    {
      if(skip_right_read)
	skip_right_read = false;
      else {
	memset(this->right_tuple, 0, this->max_right_record_size);
	int rc;
	if ((rc = this->rightIn->getNextTuple(this->right_tuple)) != 0)
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

	left_block_it = tuples_map.begin();
      }
	
      string key = getKey(this->rightIn, this->condition.rhsAttr, this->right_tuple);
      if (this->condition.op == EQ_OP)
	{
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
      else
	{
	  bool found = false;
	  auto it = left_block_it;
	  while ((!found) && (it != this->tuples_map.end())) {
	    switch (this->condition.op){
	    case LT_OP:
	      if (it->first < key){
		// cout << "    MATCHED: ";
		// int x, y;   
		// std::stringstream ss, sd;
		// ss << std::hex << it->first;
		// ss >> x;
		// cout << x << ":";
		
		
		// sd << std::hex <<  key;
		// sd >> y;		  
		// cout << y << endl;
		found = true;
	      }
	      break;
	    case GT_OP:
	      if (it->first > key)
		  found = true;
	      break;
		  
	    case LE_OP:
	      if (it->first <= key)
		  found = true;
	      break;
		  
	    case GE_OP:
	      if (it->first >= key)
		  found = true;
	      break;
	      
	    case NE_OP:
	      if (it->first != key)
		  found = true;
	      break;
		  
	    case NO_OP:
	      // TODO: implement this
	      cout << "Bad operator" << endl;
	      return QE_EOF;
	      break;
	    } // end switch
	    

	    if (found == true)
	      {
		memcpy(data, it->second[0].tuple, it->second[0].size);
		memcpy((char *)data + it->second[0].size, this->right_tuple, this->max_right_record_size);
	      
		if (it->second.size() > 1)
		  {
		    this->tuples_info = it->second;
		    this->tuples_info_index = 1;
		    this->tuples_info_more = true;
		  }

		it++;
		this->left_block_it = it;
		this->left_block_has_more = true;
	      
		return 0;
	      }
	    else
	      {
		it++;
	      }
	  }
	} // END WHILE (it not end)
    } // end while (progress right tuple)
}
// For attribute in vector<Attribute>, name it as rel.attr
void NLJoin::getAttributes(vector<Attribute> &attrs) const
{
  attrs.clear();
  this->leftIn->getAttributes(attrs);
  vector<Attribute> rightAttrs;
  rightAttrs.clear();
  this->rightIn->getAttributes(rightAttrs);
  for (uint i = 0; i < rightAttrs.size(); i++)
    {
      attrs.push_back(rightAttrs[i]);
    }
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

  this->num_of_block_records = floor(((double)numPages * PF_PAGE_SIZE) / this->max_left_record_size);

  this->readBlockLeftIn();
}

INLJoin::~INLJoin()
{
  for (uint i = 0; i < this->tuples_vector.size(); i++)
    {
      free(this->tuples_vector[i].tuple);
    }

  this->tuples_vector.clear();
}

RC INLJoin::readBlockLeftIn()
{
  void *tuple = malloc(this->max_left_record_size);

  for (uint i = 0; i < this->tuples_vector.size(); i++)
    {
      free(this->tuples_vector[i].tuple);
    }

  this->tuples_vector.clear();
  unsigned tuples_read = 0;
  while (tuples_read <= this->num_of_block_records)
    {
      memset(tuple, 0, this->max_left_record_size);
      if (this->leftIn->getNextTuple(tuple) != 0)
	{
	  this->left_has_more = false;
	  break;
	}

      unsigned tuple_size = getTupleSize(this->leftIn, tuple);
      TupleInfo info;
      info.tuple = malloc(tuple_size);
      memcpy(info.tuple, tuple, tuple_size);
      info.size = tuple_size;
      this->tuples_vector.push_back(info);

      tuples_read++;
    }

  this->tuples_vector_index = -1;
  this->right_has_more = false;

  free(tuple);
  return 0;
}

unsigned INLJoin::getTupleSize(Iterator *iter, void *tuple)
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

RC INLJoin::getNextTuple(void *data)
{
  void *key = malloc(PF_PAGE_SIZE);

  if (!left_has_more && tuples_vector_index >= (int)tuples_vector.size()){
    return QE_EOF;
  }

  if (this->right_has_more == true)
    {
      void *left_key;
      void *right_key;

      if (this->condition.op == NE_OP)
	{
	  // Get the key of the left tuple
	  vector<Attribute> left_attrs;
	  this->leftIn->getAttributes(left_attrs);
	  string attribute = this->condition.lhsAttr;
	  
	  bool left_found = false;
	  unsigned left_offset = 0;
	  unsigned left_key_size;
	  TupleInfo left_tuple_info = this->tuples_vector[this->tuples_vector_index];
	  for (uint i = 0; i < left_attrs.size(); i++)
	    {
	      if (left_attrs[i].name == attribute)
		{
		  if (left_attrs[i].type == TypeVarChar)
		    {
		      int attribute_size = 0;
		      memcpy(&attribute_size, (char *)left_tuple_info.tuple + left_offset, sizeof(attribute_size));
		      left_offset += sizeof(attribute_size);
		      
		      left_key_size = attribute_size;
		      
		      left_key = malloc(attribute_size);
		      memset(left_key, 0, attribute_size);
		      memcpy(left_key, (char *)left_tuple_info.tuple + left_offset, attribute_size);
		      
		      left_offset += attribute_size;
		    }
		  else
		    {
		      left_key_size = left_attrs[i].length;
		      
		      left_key = malloc(left_attrs[i].length);
		      memset(left_key, 0, left_attrs[i].length);
		      memcpy(left_key, (char *)left_tuple_info.tuple + left_offset, left_attrs[i].length);
		      
		      left_offset += left_attrs[i].length;
		    }
		  
		  left_found = true;
		}
	      else
		{
		  if (left_attrs[i].type == TypeVarChar)
		    {
		      int attribute_size = 0;
		      memcpy(&attribute_size, (char *)left_tuple_info.tuple + left_offset, sizeof(attribute_size));
		      left_offset += sizeof(attribute_size) + attribute_size;
		    }
		  else
		    {
		      left_offset += left_attrs[i].length;
		    }	  
		}
	    }
	  
	  void *right_tuple = malloc(this->max_right_record_size);
	  memset(right_tuple, 0, this->max_right_record_size);
	  while(true)
	    {
	      if (this->rightIn->getNextTuple(right_tuple) == 0)
		{
		  unsigned right_tuple_size = this->getTupleSize(this->rightIn, right_tuple);

		  // Get the key of the right tuple
		  vector<Attribute> right_attrs;
		  this->rightIn->getAttributes(right_attrs);
		  string attribute = this->condition.rhsAttr;
		  
		  bool right_found = false;
		  unsigned right_offset = 0;
		  unsigned right_key_size;
		  TupleInfo right_tuple_info;
		  right_tuple_info.tuple = right_tuple;
		  right_tuple_info.size = right_tuple_size;
		  for (uint i = 0; i < right_attrs.size(); i++)
		    {
		      if (right_attrs[i].name == attribute)
			{
			  if (right_attrs[i].type == TypeVarChar)
			    {
			      int attribute_size = 0;
			      memcpy(&attribute_size, (char *)right_tuple_info.tuple + right_offset, sizeof(attribute_size));
			      right_offset += sizeof(attribute_size);
			      
			      right_key_size = attribute_size;
			      
			      right_key = malloc(attribute_size);
			      memset(right_key, 0, attribute_size);
			      memcpy(right_key, (char *)right_tuple_info.tuple + right_offset, attribute_size);
			      
			      right_offset += attribute_size;
			    }
			  else
			    {
			      right_key_size = right_attrs[i].length;
			      
			      right_key = malloc(right_attrs[i].length);
			      memset(right_key, 0, right_attrs[i].length);
			      memcpy(right_key, (char *)right_tuple_info.tuple + right_offset, right_attrs[i].length);
			      
			      right_offset += right_attrs[i].length;
			    }
			  
			  right_found = true;
			}
		      else
			{
			  if (right_attrs[i].type == TypeVarChar)
			    {
			      int attribute_size = 0;
			      memcpy(&attribute_size, (char *)right_tuple_info.tuple + right_offset, sizeof(attribute_size));
			      right_offset += sizeof(attribute_size) + attribute_size;
			    }
			  else
			    {
			      right_offset += right_attrs[i].length;
			    }	  
			}
		    }
		  
		  if (left_tuple_info.size == right_tuple_info.size)
		    {
		      if (memcmp(left_tuple_info.tuple, right_tuple_info.tuple, right_tuple_info.size) == 0)
			{
			  memcpy(data, left_tuple_info.tuple, left_tuple_info.size);
			  memcpy((char *)data + left_tuple_info.size, right_tuple, right_tuple_size);
			  free(right_tuple);
			  free(key);
			  return 0;
			}
		    }
		}
	      else
		{
		  free(right_tuple);

		  if (this->left_has_more == true)
		    {
		      this->right_has_more = false;
		      free(key);
		      return this->getNextTuple(data);
		    }
		}
	    }
	}
      else
	{
	  void *right_tuple = malloc(this->max_right_record_size);
	  memset(right_tuple, 0, this->max_right_record_size);
	  if (this->rightIn->getNextTuple(right_tuple) == 0)
	    {
	      unsigned right_tuple_size = this->getTupleSize(this->rightIn, right_tuple);
	      TupleInfo tuple_info = this->tuples_vector[this->tuples_vector_index];
	      memcpy(data, tuple_info.tuple, tuple_info.size);
	      memcpy((char *)data + tuple_info.size, right_tuple, right_tuple_size);
	      free(right_tuple);
	      free(key);
	      return 0;
	    }
	  else
	    {
	      this->tuples_vector_index++;
	      free(right_tuple);
	      
	      if (this->left_has_more == true)
		{
		  free(key);
		  this->right_has_more = false;
		  return this->getNextTuple(data);
		}
	      // else
	      // 	{
	      // 	  cout << "We probably shouldn't be seeing this" << endl;
	      // 	}
	    }
	}
    }
  else
    {
      this->tuples_vector_index++;
    }


  while(true)
    {
      if (this->tuples_vector_index == this->tuples_vector.size())
	{
	  if (this->left_has_more == true)
	    {
	      free(key);
	      this->readBlockLeftIn();
	      return this->getNextTuple(data);
	    }
	  else
	    {
	      free(key);
	      return QE_EOF;
	    }
	}
      
      vector<Attribute> attrs;
      this->leftIn->getAttributes(attrs);
      string attribute = this->condition.lhsAttr;

      bool found = false;
      unsigned offset = 0;
      unsigned key_size;
      TupleInfo tuple_info = this->tuples_vector[this->tuples_vector_index];
      for (uint i = 0; i < attrs.size(); i++)
	{
	  if (attrs[i].name == attribute)
	    {
	      if (attrs[i].type == TypeVarChar)
		{
		  int attribute_size = 0;
		  memcpy(&attribute_size, (char *)tuple_info.tuple + offset, sizeof(attribute_size));
		  offset += sizeof(attribute_size);
		  
		  key_size = attribute_size;
		  
		  // key = malloc(attribute_size);
		  memset(key, 0, attribute_size);
		  memcpy(key, (char *)tuple_info.tuple + offset, attribute_size);
		  
		  offset += attribute_size;
		}
	      else
		{
		  key_size = attrs[i].length;
		  
		  // key = malloc(attrs[i].length);
		  memset(key, 0, attrs[i].length);
		  memcpy(key, (char *)tuple_info.tuple + offset, attrs[i].length);
		  
		  offset += attrs[i].length;
		}
	      
	      found = true;
	    }
	  else
	    {
	      if (attrs[i].type == TypeVarChar)
		{
		  int attribute_size = 0;
		  memcpy(&attribute_size, (char *)tuple_info.tuple + offset, sizeof(attribute_size));
		  offset += sizeof(attribute_size) + attribute_size;
		}
	      else
		{
		  offset += attrs[i].length;
		}	  
	    }
	}
      
      // TODO: if found == false
      
      switch (this->condition.op)
	{
	case EQ_OP:
	  this->rightIn->setIterator(key, key, true, true);
	  this->right_has_more = true;
	  break;
	  
	case GT_OP:
	  this->rightIn->setIterator(NULL, key, false, false);
	  this->right_has_more = true;
	  
	  break;
	case LT_OP:
	  // Find everyything larger than key (those are the things that match)
	  this->rightIn->setIterator(key, NULL, false, false);
	  this->right_has_more = true;

	  break;
	case GE_OP:
	  this->rightIn->setIterator(NULL, key, false, true);
	  this->right_has_more = true;

	  break;
	case LE_OP:
	  this->rightIn->setIterator(key, NULL, true, false);
	  this->right_has_more = true;

	  break;
	case NE_OP:
	  this->rightIn->setIterator(NULL, NULL, false, false);
	  this->right_has_more = true;
	  
	  break;
	case NO_OP:
	  // TODO: implement this
	  cout << "Bad operator" << endl;
	  return QE_EOF;
	}
      
      void *right_tuple = malloc(this->max_right_record_size);
      memset(right_tuple, 0, this->max_right_record_size);
      if (this->rightIn->getNextTuple(right_tuple) == 0)
	{
	  unsigned right_tuple_size = this->getTupleSize(this->rightIn, right_tuple);
	  memcpy(data, tuple_info.tuple, tuple_info.size);
	  memcpy((char *)data + tuple_info.size, right_tuple, right_tuple_size);
	  free(right_tuple);
	 
	  break;
	}
      else
	{
	  //	  this->tuples_vector_index++;
	  free(right_tuple);
	  free(key);

	  return getNextTuple(data);
	}
    }

  free(key);
  return 0;
}

// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
  attrs.clear();
  this->leftIn->getAttributes(attrs);
  vector<Attribute> rightAttrs;
  rightAttrs.clear();
  this->rightIn->getAttributes(rightAttrs);
  for (uint i = 0; i < rightAttrs.size(); i++)
    {
      attrs.push_back(rightAttrs[i]);
    }
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
  this->aggAttr = aggAttr;
  if (this->doOp(input, op) != 0)
    error(__LINE__, -1);
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
  this->groupByAttr = gAttr;
  this->aggAttr = aggAttr;
  input->getAttributes(this->attrs);
  RC rc = this->doOp(input, op);
  if (rc != 0)
    error(__LINE__, rc);
}

Aggregate::~Aggregate(){
}

void Aggregate::init() {
  this->isGroupBy = false;
  this->groupByOffset = 0;
  this->groupByIndex = -1;
}

RC Aggregate::doOp(Iterator *input, AggregateOp op) {
  RC rc;
  switch(op) {
  case 0:
    return rc = MIN(input);
  case 1:
    return rc = MAX(input);
  case 2:
    return rc = SUM(input);
  case 3:
    return rc = AVG(input);
  case 4:
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
    if (getAttributeFromData(data, attrs, aggAttr.name, val) == -1)
      return error(__LINE__, -1); 
    switch(aggAttr.type) {
    case TypeInt:
      intTmp = *((int *) val);
      if (isGroupBy) {
        if (0 != getAttributeOffsetAndIndex(this->attrs, groupByAttr.name, data, groupByOffset, groupByIndex))
          return error(__LINE__, -1);  
        string str = getAttributeName(data, this->attrs, groupByOffset, groupByIndex);
        auto got = results.find(str);
        if (got == results.end())
          results[str] = intTmp;
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
        if (0 != getAttributeOffsetAndIndex(this->attrs, groupByAttr.name, data, groupByOffset, groupByIndex))
          return error(__LINE__, -1);         
        string str = getAttributeName(data, this->attrs, groupByOffset, groupByIndex);
        auto got = results.find(str);
        if (got == results.end())
          results[str] = floatTmp;
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
    if (getAttributeFromData(data, attrs, aggAttr.name, val) == -1)
      return error(__LINE__, -1);
    switch(aggAttr.type) {
    case TypeInt:
      intTmp = *((int *) val);
      if (isGroupBy) {
        if (0 != getAttributeOffsetAndIndex(this->attrs, groupByAttr.name, data, groupByOffset, groupByIndex))
          return error(__LINE__, -1);         
        string str = getAttributeName(data, this->attrs, groupByOffset, groupByIndex);
        auto got = results.find(str);
        if (got == results.end())
          results[str] = intTmp;
        else
          results[str] = fmax(got->second, intTmp);
      } else {
        intMax = fmax(intMax, intTmp);
        this->updateResultMap("no-group-by", intMax, false);  
      }
      break;
    case TypeReal:
      floatTmp = *((float *) val);
      if (isGroupBy) {
        if (0 != getAttributeOffsetAndIndex(this->attrs, groupByAttr.name, data, groupByOffset, groupByIndex))
          return error(__LINE__, -1);        
        string str = getAttributeName(data, this->attrs, groupByOffset, groupByIndex);
        auto got = results.find(str);
        if (got == results.end())
          results[str] = floatTmp;
        else
          results[str] = fmax(got->second, floatTmp);
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
  int intTmp=0;
  float floatTmp=0.0;
  string str;
  while (QE_EOF != input->getNextTuple(data))
  {
    str = "no-group-by";
    if (getAttributeFromData(data, attrs, aggAttr.name, val) == -1)
      return error(__LINE__, -1);
    switch(aggAttr.type) {
    case TypeInt:
      intTmp = *((int *) val);
      if (isGroupBy) {
        if (0 != getAttributeOffsetAndIndex(this->attrs, groupByAttr.name, data, groupByOffset, groupByIndex))
          return error(__LINE__, -1);         
        str = getAttributeName(data, this->attrs, groupByOffset, groupByIndex);
      }
      this->updateResultMap(str, intTmp, true);
      break;
    case TypeReal:
      floatTmp = *((float *) val);
      if (isGroupBy) {
        if (0 != getAttributeOffsetAndIndex(this->attrs, groupByAttr.name, data, groupByOffset, groupByIndex))
          return error(__LINE__, -1);        
        string str = getAttributeName(data, this->attrs, groupByOffset, groupByIndex);
      }
      this->updateResultMap(str, floatTmp, true);
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
  int intTmp=0;
  float floatTmp=0.0;
  string str;
  while (QE_EOF != input->getNextTuple(data))
  {
    str = "no-group-by";
    if (getAttributeFromData(data, attrs, aggAttr.name, val) == -1)
      return error(__LINE__, -1);
    switch(aggAttr.type) {
    case TypeInt:
      intTmp = *((int *) val);
      if (isGroupBy) {
        if (0 != getAttributeOffsetAndIndex(this->attrs, groupByAttr.name, data, groupByOffset, groupByIndex))
          return error(__LINE__, -1);          
        str = getAttributeName(data, this->attrs, groupByOffset, groupByIndex);
      }
      this->updateResultMap(str, intTmp, true);
      this->updateCounterMap(str);
      break;
    case TypeReal:
      floatTmp = *((float *) val);
      if (isGroupBy) {
        if (0 != getAttributeOffsetAndIndex(this->attrs, groupByAttr.name, data, groupByOffset, groupByIndex))
          return error(__LINE__, -1);       
        str = getAttributeName(data, this->attrs, groupByOffset, groupByIndex);
      }
      this->updateResultMap(str, floatTmp, true);
      this->updateCounterMap(str);
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
  string str;
  while (QE_EOF != input->getNextTuple(data))
  {
    str = "no-group-by";
    if (isGroupBy) {
      if (0 != getAttributeOffsetAndIndex(this->attrs, groupByAttr.name, data, groupByOffset, groupByIndex))
        return error(__LINE__, -1);      
      str = getAttributeName(data, this->attrs, groupByOffset, groupByIndex);
    }
    this->updateResultMap(str, 1, true);
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
    switch(groupByAttr.type) {
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
  floatVal = it->second;
  memcpy((char *)data+offset, &(floatVal), sizeof(float));
  offset += sizeof(int);
  results.erase(it->first);
  
  return 0;
}

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const {
  attrs.clear();
  if (isGroupBy) {
    attrs.push_back(groupByAttr);
  }
  // aggAttr always returns as real
  Attribute attr = aggAttr;
  attr.type = TypeReal;
  attrs.push_back(attr);
}

// if cumulative = true, add the result to previous result
// otherwise overwrite the result
void Aggregate::updateResultMap(const string name, const double value, const bool cumulative) {
  auto got = results.find(name);
  if ( got == results.end() )
    results[name] = value;
  else {
    if (cumulative)
      results[name] = results[name] + value;
    else
      results[name] = value;
  }
}

// increment counter by 1 for the given name
void Aggregate::updateCounterMap(const string name) {
  auto got = counters.find(name);
  if ( got == counters.end() ) {
    counters[name] = 1;
  }
  else {
    counters[name] = counters[name] + 1;
  }
}

///////////////////////////////////////////////
///////////////////////////////////////////////
// Filter Interface //
///////////////////////////////////////////////
///////////////////////////////////////////////

Filter::Filter(Iterator* input, const Condition &condition) {
  input->getAttributes(this->attrs);
  this->input = input;
  this->condition = condition;
}

Filter::~Filter() {
}

RC Filter::getNextTuple(void *filteredData) {
  RC rc;
  void *data = malloc(PF_PAGE_SIZE);
  void *lvalue = malloc(PF_PAGE_SIZE);
  void *value = malloc(PF_PAGE_SIZE);
  int filteredDataOffset;

  bool satisfy = false; 
  while(!satisfy) {
    if(input->getNextTuple(data) == QE_EOF){
      free(value);
      free(lvalue);
      free(data);
      return QE_EOF;
    }

    // get the left hand side attribute
    int dataOffset = 0, index = -1;
  
    // find the desired attribute in data
    if (getAttributeOffsetAndIndex(this->attrs, condition.lhsAttr, data, dataOffset, index) != 0)
      return error(__LINE__, -1);

    // copy attribute to lvalue
    if (getAttributeFromData(data, attrs, condition.lhsAttr, lvalue) == -1)
      return error(__LINE__, -1); 

    // copy right value to value
    int rightSize = getAttributeSize(condition.rhsValue.type, condition.rhsValue.data);
    memcpy((void *)value, (void *)condition.rhsValue.data, rightSize);
  
    char *slvalue, *svalue;
    if (this->attrs[index].type == TypeVarChar) {
      int ll = 0;
      memcpy(&ll, (char *)data+dataOffset, sizeof(int));
      slvalue = (char *)malloc(ll+1);
      memcpy((char *)slvalue, (char *)data+dataOffset+sizeof(int), ll);
      slvalue[ll] = '\0';
          
      memcpy(&ll, (char *)condition.rhsValue.data, sizeof(int));
      svalue = (char *)malloc(ll+1);
      memcpy((char *)svalue, (char *)condition.rhsValue.data+sizeof(int), ll);
      svalue[ll] = '\0';
    }
  
    // check if data satisfies the given condition or not
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
        if( strcmp(slvalue,svalue ) == 0 )
          satisfy = true;
        free(slvalue);
        free(svalue);
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
        if( strcmp(slvalue,svalue ) < 0 )
          satisfy = true;
        free(slvalue);
        free(svalue);
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
        if( strcmp(slvalue,svalue ) > 0 )
          satisfy = true;
        free(slvalue);
        free(svalue);
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
      return error(__LINE__, -3);
    }
    if (satisfy) {
      filteredDataOffset = getDataSize(this->attrs, data);
      memcpy(filteredData, data, filteredDataOffset);
    }
  }
  free(value);
  free(lvalue);
  free(data);
  return 0;
}

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const {
  attrs.clear();
  attrs = this->attrs;
}

int Filter::getAttributeSize(const AttrType type, const void *data) {
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

int Filter::getDataSize(const vector<Attribute> attr, const void *data) {
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
// Project Interface //
///////////////////////////////////////////////
///////////////////////////////////////////////

// input: Iterator of input R
// vector containing attribute names
Project::Project(Iterator *input, const vector<string> &attrNames) {
  this->input = input;
  input->getAttributes(this->input_attrs);
  this->output_attrs = attrNames;
}

Project::~Project() {
}

RC Project::getNextTuple(void *projectedData) {
  int projectedDataOffset = 0;

  void *data = malloc(PF_PAGE_SIZE);
  if(input->getNextTuple(data) == QE_EOF){
    free(data);
    return QE_EOF;
  }
    
  // project data to desired attributes
  for (unsigned i=0; i < output_attrs.size(); i++) {
    int dataOffset = 0;
    int index = -1;

    // find the desired attribute in data
    if (getAttributeOffsetAndIndex(this->input_attrs, output_attrs[i], data, dataOffset, index) != 0)
      return error(__LINE__, -1);

    // copy desired attribute to projectedData
    int length;
    switch(this->input_attrs[index].type){
    case TypeInt:
    case TypeReal:
    case TypeShort:
    case TypeBoolean:
      memcpy((char *)projectedData+projectedDataOffset, (char *) data+dataOffset, this->input_attrs[index].length);
      projectedDataOffset += this->input_attrs[index].length;
      break;
    case TypeVarChar:
      memcpy(&length, (char *)data+dataOffset, sizeof(int));
      memcpy((char *)projectedData+projectedDataOffset, (char *)data+dataOffset, sizeof(int)+length);
      projectedDataOffset += sizeof(int)+length;
      break;
    }
  }
  free(data);
  return 0;
}

// For attribute in vector<Attribute>, name it as rel.attr
void Project::getAttributes(vector<Attribute> &attrs) const {
  attrs.clear();
  for( uint i = 0; i < output_attrs.size(); i++ ){
    for (uint search=0; search < input_attrs.size(); search++) {
      if (0 == input_attrs[search].name.compare(output_attrs[i])) {
        attrs.push_back(input_attrs[search]);
        break;
      }
    }
  }
}

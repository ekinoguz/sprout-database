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

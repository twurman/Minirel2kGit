#include "catalog.h"
#include "query.h"
#include "index.h"

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
Status Operators::Select(const string & result,      // name of the output relation
						 const int projCnt,          // number of attributes in the projection
						 const attrInfo projNames[], // the list of projection attributes
						 const attrInfo *attr,       // attribute used inthe selection predicate
						 const Operator op,         // predicate operation
						 const void *attrValue)     // literal value in the predicate
{
	
	
	AttrDesc predicate;
	if(attr != NULL){
		//turn attrInfo into AttrDesc predicate
		attrCat->getInfo(attr->relName, attr->attrName, predicate);
	}
	int recLen = 0;
	AttrDesc temp[projCnt];
	for(int i = 0; i < projCnt; i++){
		//get length of record
		attrCat->getInfo(projNames[i].relName, projNames[i].attrName, temp[i]);
		recLen += temp[i].attrLen;
	}
	
	if(op == EQ && attr != NULL && predicate.indexed){
		//call index select
		IndexSelect(result, projCnt, temp, &predicate, op, attrValue, recLen);
	} else {
		//call scan select
		ScanSelect(result, projCnt, temp, &predicate, op, attrValue, recLen);
	}
	
	
	return OK;
}


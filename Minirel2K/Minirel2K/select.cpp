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
		attrCat->getInfo(attr->relName, attr->attrName, predicate);
	}
	int recLen = 0;
	AttrDesc temp[projCnt];
	for(int i = 0; i < projCnt; i++){
		attrCat->getInfo(projNames[i].relName, projNames[i].attrName, temp[i]);
		recLen += temp[i].attrLen;
	}
	
	if(op == EQ && attr != NULL && predicate.indexed){
		IndexSelect(result, projCnt, temp, &predicate, op, attrValue, recLen);
	} else {
		ScanSelect(result, projCnt, temp, &predicate, op, attrValue, recLen);
	}
	
	/*
	cerr << "Result File: " << result << endl;
	for(int i = 0; i < projCnt; i++){
		cerr << "Projection: " << projNames[i].relName << ", " << projNames[i].attrName << ", " << (Datatype)projNames[i].attrType << ", " << projNames[i].attrLen << ", " << endl;
		if((Datatype)projNames[i].attrType == INTEGER){
			cout << projNames[i].attrValue << endl;
		} else if((Datatype)projNames[i].attrType == DOUBLE){
			cout << projNames[i].attrValue << endl;
		} else {
			cout << projNames[i].attrValue << endl;
		}
	}
	cerr << attr->relName << ", " << attr->attrName << ", " << attr->attrLen << ", " << attr->attrType << ", " << attr->attrValue << endl;
	
	cerr << op << endl;
	cerr << attrValue << endl;*/
	
	//if index and op is equality, call indexSelect
	/*if(attr == NULL){
		int recLen = 0;
		AttrDesc *aList;
		for(int i = 0; i < projCnt; i++){
			recLen += projNames[i].attrLen;
		}
		ScanSelect(result, projCnt, projNames, <#const AttrDesc *attrDesc#>, op, attrValue, <#const int reclen#>)
	}*/
	//else call scanSelect
	return OK;
}


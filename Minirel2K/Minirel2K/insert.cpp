#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>

/*
 * Inserts a record into the specified relation
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{
    /* Your solution goes here */
	AttrDesc *aList;
	int aCount;
	bool attrInRelation = false;
	if(attrCat->getRelInfo(relation, aCount, aList)){
		return RELNOTFOUND;
	} if(aCount != attrCnt){
		return RELNOTFOUND;
	}
	
	Record newRecord;
	RID newRecRID;
	for(int i = 0; i < aCount; i++){
		attrInRelation = false;
		//for loop again - check if attribute is in relation
		for(int j = 0; j < attrCnt; j++){
			cout << attrList[j].attrName << " : " << (aList+i)->attrName << endl;
			if(attrList[j].attrName == (aList+i)->attrName){
				attrInRelation = true;
				//check if attribute is same type
				if(attrList[j].attrType != (aList+i)->attrType){
					cout << 1;
					return ATTRTYPEMISMATCH;
				}
				//check that aList->attrLen >= attrList->attrLen
				if(attrList[j].attrLen > (aList+i)->attrLen){
					cout << 2;
					return ATTRTOOLONG;
				}
				//check that value is not null -- nullptr C++11?
				if(attrList[j].attrValue == NULL){
					cout << 3;
					return ATTRNOTFOUND;
				}
				//all is good, create record
				
				memcpy((char *)newRecord.data + newRecord.length, attrList[j].attrValue, attrList[j].attrLen);
				
				Status checkIndex;
				Index i = Index(relation,
								newRecord.length,
								attrList[j].attrLen,
								(Datatype)attrList[j].attrType,
								NONUNIQUE,
								checkIndex);
				
				if(checkIndex != OK)
					return checkIndex;
				
				newRecord.length += attrList[j].attrLen;
			}
		}
		if(!attrInRelation){
			cout << 4;
			return ATTRNOTFOUND;
		}
		
	}
	
	//insert record into db & into catalog
	attrCat->insertRecord(newRecord, newRecRID);
	
	

    return OK;
}

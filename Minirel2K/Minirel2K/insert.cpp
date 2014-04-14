#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>
#include <stdlib.h>

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
	if(attrCat->getRelInfo(relation, aCount, aList) != OK){
		return RELNOTFOUND;
	} if(aCount != attrCnt){
		return RELNOTFOUND;
	}
	
	//get number of bytes to allocate
	int recSize = 0;
	for(int i = 0; i < aCount; i++){
		recSize += aList[i].attrLen;
	}
	void * rec = malloc(recSize);
	
	Record newRecord;
	RID newRecRID;
	for(int i = 0; i < aCount; i++){
		attrInRelation = false;
		//for loop again - check if attribute is in relation
		for(int j = 0; j < attrCnt; j++){
			if(strcmp(attrList[j].attrName, (aList+i)->attrName) == 0){
				attrInRelation = true;
				//check if attribute is same type
				if(attrList[j].attrType != (aList+i)->attrType){
					free(rec);
					return ATTRTYPEMISMATCH;
				}
				//check that aList->attrLen >= attrList->attrLen
				if(attrList[j].attrLen > (aList+i)->attrLen){
					free(rec);
					return ATTRTOOLONG;
				}
				//check that value is not null -- nullptr C++11?
				if(attrList[j].attrValue == NULL){
					free(rec);
					return ATTRNOTFOUND;
				}
				//all is good, create record
				memcpy(rec, attrList[j].attrValue, attrList[j].attrLen);
				
				/*Status checkIndex;
				Index i = Index(relation,
								newRecord.length,
								attrList[j].attrLen,
								(Datatype)attrList[j].attrType,
								NONUNIQUE,
								checkIndex);
				
				
				if(checkIndex != OK)
					return checkIndex;
				*/
				newRecord.length += attrList[j].attrLen;
			}
		}
		if(!attrInRelation){
			free(rec);
			return ATTRNOTFOUND;
		}
		
	}
	
	newRecord.data = rec;
	
	//insert record into db & into catalog
	Status heapInsert;
	HeapFile page = HeapFile(relation, heapInsert);
	page.insertRecord(newRecord, newRecRID);
	
	
	
	
	

    return OK;
}

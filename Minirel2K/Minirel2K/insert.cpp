#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>
#include <stdlib.h>
#include "utility.h"

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
	AttrDesc *aList;
	int aCount;
	bool attrInRelation = false;
	if(attrCat->getRelInfo(relation, aCount, aList) != OK){
		return RELNOTFOUND;
	} if(aCount != attrCnt){
		return RELNOTFOUND;
	}
	
	Record newRecord;
	RID newRecRID;
	int numKeys = 0;
	Index* indices[aCount];
	int keyNums[aCount];
	
	//get number of bytes to allocate
	int recSize = 0;
	for(int i = 0; i < aCount; i++){
		recSize += (aList+i)->attrLen;
	}
	newRecord.data = malloc(recSize);
	newRecord.length = recSize;
	
	for(int i = 0; i < aCount; i++){
		attrInRelation = false;
		AttrDesc at;
		Status inRelation = attrCat->getInfo(relation, attrList[i].attrName, at);
		if(inRelation != OK){
			free(newRecord.data);
			newRecord.data = NULL;
			return inRelation;
		}
		if(attrList[i].attrType != at.attrType){
			free(newRecord.data);
			newRecord.data = NULL;
			return ATTRTYPEMISMATCH;
		}
		if(attrList[i].attrValue == NULL){
			free(newRecord.data);
			newRecord.data = NULL;
			return ATTRNOTFOUND;
		}
		memcpy((char*)newRecord.data + at.attrOffset, attrList[i].attrValue, at.attrLen);
		if(at.indexed){
			Status checkIndex = OK;
			indices[numKeys] = new Index(relation,
										 at.attrOffset,
										 at.attrLen,
										 (Datatype)at.attrType,
										 NONUNIQUE,
										 checkIndex);
			keyNums[numKeys++] = i;
			
			if(checkIndex != OK){
				return checkIndex;
			}

		}
		
		
		/*
		
		//for loop again - check if attribute is in relation
		for(int j = 0; j < attrCnt; j++){
			if(strcmp(attrList[j].attrName, (aList+i)->attrName) == 0){
				attrInRelation = true;
				//check if attribute is same type
				if(attrList[j].attrType != (aList+i)->attrType){
					free(newRecord.data);
					newRecord.data = NULL;
					return ATTRTYPEMISMATCH;
				}
				//check that aList->attrLen >= attrList->attrLen
				if(attrList[j].attrLen > (aList+i)->attrLen){
					free(newRecord.data);
					newRecord.data = NULL;
					return ATTRTOOLONG;
				}
				//check that value is not null -- nullptr C++11?
				if(attrList[j].attrValue == NULL){
					free(newRecord.data);
					newRecord.data = NULL;
					return ATTRNOTFOUND;
				}
				//all is good, add to record
				memcpy((char*)newRecord.data + (aList+i)->attrOffset, attrList[j].attrValue, (aList+i)->attrLen);
				
				
				if((aList+i)->indexed){
					Status checkIndex = OK;
					indices[numKeys] = new Index(relation,
												 (aList+i)->attrOffset,
												 (aList+i)->attrLen,
												 (Datatype)(aList+i)->attrType,
												 NONUNIQUE,
												 checkIndex);
					keyNums[numKeys++] = j;
					
					if(checkIndex != OK){
						return checkIndex;
					}
				}
				
				
			}
		}
		if(!attrInRelation){
			free(newRecord.data);
			newRecord.data = NULL;
			return ATTRNOTFOUND;
		}*/
		
	}
	
	//insert record into db & into catalog
	Status heapInsert;
	HeapFile page = HeapFile(relation, heapInsert);
	if(heapInsert != OK){
		free(newRecord.data);
		return heapInsert;
	}
	heapInsert = page.insertRecord(newRecord, newRecRID);
	if(heapInsert != OK){
		free(newRecord.data);
		return heapInsert;
	}
	//add index for all indexed elements
	for(int i = 0; i < numKeys; i++){
		indices[i]->insertEntry(attrList[keyNums[i]].attrValue, newRecRID);
		delete indices[i];
	}
	
	free(newRecord.data);
	return OK;
}

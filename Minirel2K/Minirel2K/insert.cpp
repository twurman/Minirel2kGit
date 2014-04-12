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
			if(attrList[j].attrName == (aList+i)->attrName){
				attrInRelation = true;
				//check if attribute is same type
				if(attrList[j].attrType != (aList+i)->attrType){
					return ATTRTYPEMISMATCH;
				}
				//check that aList->attrLen >= attrList->attrLen
				if(attrList[j].attrLen > (aList+i)->attrLen){
					return ATTRTOOLONG;
				}
			}
		}
		if(!attrInRelation){
			return ATTRNOTFOUND;
		}
		//all is good, create record
		memcpy((char *)newRecord.data + newRecord.length, attrList->attrValue, attrList->attrLen);
		newRecord.length += attrList->attrLen;
		
		/*(char *)newRecord.data + newRecord.length = attrList->attrValue;
		newRecord.data = (char*)newRecord.data + aList->attrLen;*/
	}
	
	
	

    return OK;
}

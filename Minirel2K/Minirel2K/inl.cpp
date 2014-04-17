#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <string.h>
#include <stdlib.h>

/* 
 * Indexed nested loop evaluates joins with an index on the 
 * inner/right relation (attrDesc2)
 */

Status Operators::INL(const string& result,           // Name of the output relation
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // The projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // Length of a tuple in the output relation
{
  cout << "Algorithm: Indexed NL Join" << endl;

	Status getRes = OK;
	HeapFile res = HeapFile(result, getRes);
	if(getRes != OK){
		return getRes;
	}
	
	HeapFileScan *outer, *inner;
	Index *innerInd;
	if(attrDesc1.indexed){
		outer = new HeapFileScan(attrDesc2.relName, getRes);
		if (getRes != OK){
			delete outer;
			return getRes;
		}
		inner = new HeapFileScan(attrDesc1.relName, getRes);
		if (getRes != OK){
			delete outer;
			delete inner;
			return getRes;
		}
		innerInd = new Index(attrDesc1.relName, attrDesc1.attrOffset, attrDesc1.attrLen, (Datatype)attrDesc1.attrType, NONUNIQUE, getRes);
		if (getRes != OK){
			delete outer;
			delete inner;
			delete innerInd;
			return getRes;
		}
	} else {
		outer = new HeapFileScan(attrDesc1.relName, getRes);
		if (getRes != OK){
			delete outer;
			return getRes;
		}
		inner = new HeapFileScan(attrDesc2.relName, getRes);
		if (getRes != OK){
			delete outer;
			delete inner;
			return getRes;
		}
		innerInd = new Index(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen, (Datatype)attrDesc2.attrType, NONUNIQUE, getRes);
		if (getRes != OK){
			delete outer;
			delete inner;
			delete innerInd;
			return getRes;
		}
	}
	
	
	RID outerRid, innerRid, newRecRID;
	Record outerRec, innerRec, newRec;
	newRec.data = malloc(reclen);
	newRec.length = 0;
	
	Status endScan = OK; //used below
	Status beginScan = outer->startScan(-1, -1, INTEGER, NULL, NOTSET);
	if (beginScan != OK){
		free(newRec.data);
		delete outer;
		delete inner;
		delete innerInd;
		return beginScan;
	}
	
	//begin outer loop
	while(outer->scanNext(outerRid, outerRec) == OK){
		
		//restart scan every iteration
		if(attrDesc1.indexed){
			beginScan = innerInd->startScan((char*)outerRec.data + attrDesc1.attrOffset);
		} else {
			beginScan = innerInd->startScan((char*)outerRec.data + attrDesc2.attrOffset);
		}
		
		if (beginScan != OK){
			free(newRec.data);
			delete outer;
			delete inner;
			delete innerInd;
			return beginScan;
		}
		
		//inner loop for inner table
		while(innerInd->scanNext(innerRid) == OK){
			//compare records with match rec
			newRec.length = 0;
			//get the record from the index scan RID
			inner->getRandomRecord(innerRid, innerRec);
			int compare = matchRec(outerRec, innerRec, attrDesc1, attrDesc2);
			
			if(compare == 0){
				for (int i = 0; i < projCnt; i++){
					if (strcmp(attrDescArray[i].relName, attrDesc1.relName) == 0){
						memcpy((char*)newRec.data + newRec.length, (char*)outerRec.data+attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
					}
					else {
						memcpy((char*)newRec.data + newRec.length, (char*)innerRec.data+attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
					}
					newRec.length += attrDescArray[i].attrLen;
				}
				//insert projected record into result
				res.insertRecord(newRec, newRecRID);
			}
		}
		
		
		
		
		endScan = innerInd->endScan();
		if (endScan != OK){
			free(newRec.data);
			delete outer;
			delete inner;
			delete innerInd;
			return endScan;
		}
		
	}
	
	endScan = outer->endScan();
	if (endScan != OK){
		free(newRec.data);
		delete outer;
		delete inner;
		delete innerInd;
		return endScan;
	}
	
	
	
	free(newRec.data);
	delete outer;
	delete inner;
	delete innerInd;
	return OK;

}


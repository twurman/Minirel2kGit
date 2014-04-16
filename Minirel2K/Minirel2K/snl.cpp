#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"

Status Operators::SNL(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
	cout << "Algorithm: Simple NL Join" << endl;

	Status getRes = OK;
	HeapFile res = HeapFile(result, getRes);
	if(getRes != OK){
		return getRes;
	}
	
	HeapFileScan outer = HeapFileScan(attrDesc1.relName, getRes);
	if (getRes != OK){
		return getRes;
	}
	HeapFileScan inner = HeapFileScan(attrDesc2.relName, getRes);
	if (getRes != OK){
		return getRes;
	}
	
	RID outerRid, innerRid;
	Record outerRec, innerRec, newRec;
	newRec.data = malloc(reclen);
	newRec.length = 0;
	
	Status beginScan = outer.startScan(-1, -1, INTEGER, NULL, NOTSET);
	if (beginScan != OK){
		free(newRec.data);
		return beginScan;
	}
	
	//begin outer loop
	while(outer.scanNext(outerRid, outerRec) == OK){
		
		//restart scan ever iteration
		beginScan = inner.startScan(-1, -1, INTEGER, NULL, NOTSET);
		if (beginScan != OK){
			free(newRec.data);
			return beginScan;
		}
		
		//inner loop for inner table
		while(inner.scanNext(innerRid, innerRec) == OK){
			//compare records with match rec
			newRec.length = 0;
			int compare = matchRec(outerRec, innerRec, attrDesc1, attrDesc2);
			
			//identify comparator
			if(op == LT && compare < 0){
				//projection
				for (int i = 0; i < projCnt; i++){
					if(attrDescArray[i].relName == attrDesc1.relName){
						memcpy((char*)newRec.data + newRec.length,(char*)outerRec.data+attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
					} else {
						memcpy((char*)newRec.data + newRec.length,(char*)innerRec.data+attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
					}
					newRec.length += attrDescArray[i].attrLen;
					
				}
				
			} else if(op == LTE && compare <= 0){
				
			} else if(op == GT && compare > 0){
				
			} else if(op == GTE && compare >= 0){
				
			} else if(op == NE && compare != 0){
				
			}
			
		}
		
		beginScan = inner.endScan();
		if (beginScan != OK){
			free(newRec.data);
			return beginScan;
		}
		
	}
	
	
	
	
	free(newRec.data);
	return OK;
}


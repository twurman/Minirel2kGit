#include "catalog.h"
#include "query.h"
#include "index.h"
#include "heapfile.h"
#include <string.h>
#include <stdlib.h>

Status Operators::IndexSelect(const string& result,       // Name of the output relation
                              const int projCnt,          // Number of attributes in the projection
                              const AttrDesc projNames[], // Projection list (as AttrDesc)
                              const AttrDesc* attrDesc,   // Attribute in the selection predicate
                              const Operator op,          // Predicate operator
                              const void* attrValue,      // Pointer to the literal value in the predicate
                              const int reclen)           // Length of a tuple in the output relation
{
	cout << "Algorithm: Index Select" << endl;

	Status getRes = OK;
	HeapFile res = HeapFile(result, getRes);
	if(getRes != OK){
		return getRes;
	}
	
	//get index for relation
	Index ind = Index(attrDesc->relName, attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, NONUNIQUE, getRes);
	if(getRes != OK){
		return getRes;
	}
	
	//start index scan
	getRes = ind.startScan(attrValue);
	if(getRes != OK){
		return getRes;
	}
	
	RID lookup;
	Record rec, newRec;
	newRec.data = malloc(reclen);
	newRec.length = 0;
	HeapFileScan db = HeapFileScan(attrDesc->relName, getRes);
	if(getRes != OK){
		return getRes;
	}
	
	//scan all the records that match the index value
	while(ind.scanNext(lookup) == OK){
		//lookup the matching record using the heapfilescan
		db.getRandomRecord(lookup, rec);
		//build the projected Record
		for(int i = 0; i < projCnt; i++){
			memcpy((char*)newRec.data + newRec.length, (char*)rec.data + projNames[i].attrOffset, projNames[i].attrLen);
			newRec.length += projNames[i].attrLen;
		}
		//insert the projected Record into result
		res.insertRecord(newRec, lookup);
	}
	
	ind.endScan();
	free(newRec.data);
	
	
	return OK;
}


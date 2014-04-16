#include "catalog.h"
#include "query.h"
#include "index.h"
#include <stdlib.h>
#include <string.h>

/*
 * A simple scan select using a heap file scan
 */

Status Operators::ScanSelect(const string& result,       // Name of the output relation
							 const int projCnt,          // Number of attributes in the projection
							 const AttrDesc projNames[], // Projection list (as AttrDesc)
							 const AttrDesc* attrDesc,   // Attribute in the selection predicate
							 const Operator op,          // Predicate operator
							 const void* attrValue,      // Pointer to the literal value in the predicate
							 const int reclen){          // Length of a tuple in the result relation
	
	cout << "Algorithm: File Scan" << endl;
	
	/* Your solution goes here */
	Status getResStatus = OK;
	HeapFile res = HeapFile(result, getResStatus);
	if (getResStatus != OK){
		return getResStatus;
	}
	
	cout << "--TESTING-- record count is: " << res.getRecCnt() << endl;
	//projNames[0].relName;
	
	Record newRecord,scannedRec;
	RID newRecRID,scannedRID; //bc we never create an index, we actually don't care what
	newRecord.data = malloc(reclen);
	newRecord.length = 0;
	//this is. we just need it to pass on to insertRecord
	//We will insert this record into HeapFile res
	
	
	
	
	//if (i) attrDesc == NULL, (ii) op == NOTSET or (iii) attrValue == NULL
	//then we scan the entire relation table. no filters
	if (attrDesc == NULL || op == NOTSET || attrValue == NULL){
		cerr << "inside" << endl;
		HeapFileScan scanner = HeapFileScan(projNames[0].relName, getResStatus);
		if (getResStatus != OK){
			return getResStatus;
		}
		cerr << "tginb" << endl;
		//now proceed to scan through shtuff and modify record
		Status beginScan = scanner.startScan(-1, -1, INTEGER, NULL, NOTSET);
		if (beginScan != OK){
			return beginScan;
		}
		
		cerr << "not there" << endl;
		while(scanner.scanNext(scannedRID, scannedRec) == OK)
		{
			cerr << "fuck with me" << endl;
			//do the projection stuff
			for(int i = 0; i < projCnt; i++){
				memcpy((char*)newRecord.data + newRecord.length, (char*)scannedRec.data + projNames[i].attrOffset, projNames[i].attrLen);
				newRecord.length += projNames[i].attrLen;
			}
			//insert the projected Record into result
			res.insertRecord(newRecord, newRecRID);
			//scanner.getRecord(newRecRID, newRecord);
			//newRecord.length = reclen; //from above
			//newRecord.data = malloc(newRecord.length); //where do I use this?
		}
		scanner.endScan();
	}
	else {
		//do HeapFileScan with other constructor
		cout << "IMPLEMENT ME!!" << endl;
	}
	
	
	
	
	
	
	return OK;
}
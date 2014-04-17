#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <stdlib.h>
#include <string.h>

/* Consider using Operators::matchRec() defined in join.cpp
 * to compare records when joining the relations */
  
Status Operators::SMJ(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
	cout << "Algorithm: SM Join" << endl;

	Status getRes = OK;
	HeapFile res = HeapFile(result, getRes);
	if(getRes != OK){
		return getRes;
	}
	
	//sort the left relation
	//calculate the number of unpinned pages to use
	int k = .8 * (bufMgr->numUnpinnedPages());
	
	//get the description of the relation & add up the size of all the attributes
	AttrDesc *relationStats;
	int numAttr, recSize = 0;
	attrCat->getRelInfo(attrDesc1.relName, numAttr, relationStats);
	for(int i = 0; i < numAttr; i++){
		recSize += relationStats[i].attrLen;
	}
	
	//calculation the number of tuples (aka max items) with k pages available
	int numTuples = k * PAGESIZE / recSize;
	SortedFile left = SortedFile(attrDesc1.relName, attrDesc1.attrOffset, attrDesc1.attrLen, (Datatype)attrDesc1.attrType, numTuples, getRes);
	if (getRes != OK){
		return getRes;
	}
	
	//now do same calculation to sort the right relation
	k = .8 * (bufMgr->numUnpinnedPages());
	recSize = 0;
	attrCat->getRelInfo(attrDesc2.relName, numAttr, relationStats);
	for(int i = 0; i < numAttr; i++){
		recSize += relationStats[i].attrLen;
	}
	numTuples = k * PAGESIZE / recSize;
	SortedFile right = SortedFile(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen, (Datatype)attrDesc2.attrType, numTuples, getRes);
	if (getRes != OK){
		return getRes;
	}
	

	Record leftRec, rightRec, newRec;
	newRec.data = malloc(reclen);
	newRec.length = 0;
	RID leftRid, rightRid, newRecRID;
	Status leftIsGood = left.next(leftRec);
	if(leftIsGood != OK){
		free(newRec.data);
		return leftIsGood;
	}
	Status rightIsGood = right.next(rightRec);
	if(rightIsGood != OK){
		free(newRec.data);
		return rightIsGood;
	}
	
	
	
	while(rightIsGood == OK && leftIsGood == OK){
		
		int compare = matchRec(leftRec, rightRec, attrDesc1, attrDesc2);
		if(compare == 0){
			Status setmark = right.setMark();
			if(setmark != OK){
				free(newRec.data);
				return setmark;
			}
		}
		while(compare == 0){
			newRec.length = 0;
			//project the joined tuple
			for (int i = 0; i < projCnt; i++){
				if (strcmp(attrDescArray[i].relName, attrDesc1.relName) == 0){
					memcpy((char*)newRec.data + newRec.length, (char*)leftRec.data+attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
				}
				else {
					memcpy((char*)newRec.data + newRec.length, (char*)rightRec.data+attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
				}
				newRec.length += attrDescArray[i].attrLen;
			}
			//insert projected record into result
			res.insertRecord(newRec, newRecRID);
			
			rightIsGood = right.next(rightRec);
			if (rightIsGood == OK){
				compare = matchRec(leftRec, rightRec, attrDesc1, attrDesc2);
			}
			else {
				compare = -1; //break out of while loop
			}
			
			
		}
		
		if (compare < 0){
			//scan the next on the left
			Record nextRec;
			leftIsGood = left.next(nextRec);
			if(leftIsGood == OK){	//continue, otherwise ENDOFPAGE and done
				if(matchRec(leftRec, nextRec, attrDesc1, attrDesc1) == 0){
					rightIsGood = right.next(rightRec);
					rightIsGood = right.gotoMark();
					if(rightIsGood != OK){
						free(newRec.data);
						return rightIsGood;
					}
				}
				leftRec.data = nextRec.data;
				leftRec.length = nextRec.length;
			}
	
		} else {
			//right is smaller, scan the next right tuple and set the mark
			rightIsGood = right.next(rightRec);
			Status setmark = right.setMark();
			if(setmark != OK){
				free(newRec.data);
				return setmark;
			}
		}
		
		
	}
	
	
	
	
	
	free(newRec.data);
	return OK;
}


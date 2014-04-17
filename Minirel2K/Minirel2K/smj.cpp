#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"

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
	
	

  return OK;
}


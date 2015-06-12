
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include "../ix/ix.h"

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {

  };
  ~RM_ScanIterator() {

  };


  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data)
  {
	  return RBFM_iter.getNextRecord(rid,data);
	  //return RM_EOF;
  };
  RBFM_ScanIterator RBFM_iter;
  FileHandle fileHandle;
  RC close() { return 0; };
};




class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator(){};  	// Constructor
  ~RM_IndexScanIterator(){}; 	// Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key)
  {
	  return index_iter.getNextEntry(rid,key);
			  // Get next matching entry
  }
  IXFileHandle file;
  IX_ScanIterator index_iter;
  RC close(){return 0;}            			// Terminate index scan
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // mainly for debugging
  // Print a tuple that is passed to this utility method.
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  // mainly for debugging
  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // scan returns an iterator to allow the caller to go through the results one by one.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  void* prepareTabRecord(int tID,string TableName,string fileName,vector<Attribute> &recordDescriptor);	//Function to prepare record for storing in Tables File
  void* prepareColRecord(int tID,string ColName,int colType,int colLength,int colPosition,vector<Attribute> &recordDescriptor);	//Function to prepare record from raw data
  void* prepareIndexRecord(string TableName,string fileName,string attributeName,vector<Attribute> &recordDescriptor);
  void formTableAttributeVector(vector<Attribute> &recordDescriptor);	//Used for forming the table attribute vector
  void formColumnAttributeVector(vector<Attribute> &recordDescriptor);	//Used for forming the column attribute vector
  void formIndexAttributeVector(vector<Attribute> &recordDescriptor);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  RC indexScan(const string &tableName, const string &attributeName, const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive, RM_IndexScanIterator &rm_IndexScanIterator);

  RC getAttributesIndex(const string &tableName, vector<string> &names);

// Extra credit work (10 points)
public:
  RC dropAttribute(const string &tableName, const string &attributeName);

  RC addAttribute(const string &tableName, const Attribute &attr);

  int tableID;	//Used for storing the table ID, starts from 1


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
};

#endif

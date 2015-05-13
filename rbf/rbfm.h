#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>

#include "../rbf/pfm.h"
#define FREE_REC 10000
#define RELOC 15000

using namespace std;

// Record ID
typedef struct
{
  unsigned pageNum;	// page number
  unsigned slotNum; // slot number in the page
} RID;
typedef struct
{
  unsigned short length;	// page number
  unsigned short offset; // slot number in the page
} slot;

// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { NO_OP = 0,  // no condition
		   EQ_OP,      // =
           LT_OP,      // <
           GT_OP,      // >
           LE_OP,      // <=
           GE_OP,      // >=
           NE_OP,      // !=
} CompOp;



/****************************************************************************
The scan iterator is NOT required to be implemented for part 1 of the project 
*****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();


class RBFM_ScanIterator {


private:
	  FileHandle* fileHandle;
	  vector<Attribute> recordDescriptor;
	  unsigned currentPageNo;
	  unsigned TotalPages;
	  short currentPageTotalSlots;
	  short attrSize;
	  RID currentrid;
	  string conditionAttribute;
	  CompOp compOp;
	  unsigned char *value;
	  vector<string> attributeNames;

	  unsigned char pagedata[PAGE_SIZE];
public:
	  RC loadscandata(FileHandle&,
			  const vector<Attribute>&,
			  const string&,
			  const CompOp,
			  const void*,
			  const vector<string>&);
	  RBFM_ScanIterator(){
		  fileHandle=NULL;
		  value=NULL;
	  };
	  ~RBFM_ScanIterator();

  // "data" follows the same format as RecordBasedFileManager::insertRecord()
	  RC LoadNewPage();
	  short AttributeSize(string,const void*);
	 short AttrNumber(string);
  RC getNextRecord(RID &rid, void *data);
  RC close() { return -1; };
};



class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  const void* recordFormat_forwardmask(const void* data,const vector<Attribute> &recordDescriptor,short &length);
  const void* recordFormat_backwardmask(const void* data,const vector<Attribute> &recordDescriptor,short &length);


  short recordSize(const vector<Attribute> &recordDescriptor,const void* data);
  short getfreepageNo(FileHandle &fileHandle,short datalength);  //-1 if no free page, else free page no.
  void updatePagedirectory(unsigned char *pagedata);
  void copyrecord(unsigned char *pagedata,const void* data,short datalength);
  void updateslotdirectory(unsigned char *pagedata,short datalength,bool newpage,unsigned& slotNo);
  void updatepageheader(PageNum pageNum,unsigned char* pagedata,FileHandle &fileHandle);
  void getfreespacepointer(unsigned char *pagedata,short *freespace);
  void gettotalslots(unsigned char *pagedata,short *slots);
  void setfreespacepointer(unsigned char *pagedata,short *freespace);
  void settotalslots(unsigned char *pagedata,short *slots);
  void getslotentry(unsigned char *pagedata,slot *slotEntry,short slotNo);
  void setslotentry(unsigned char *pagedata,slot *slotEntry,short slotNo);
  RC CompactPage(unsigned char *destPage,unsigned char *sourcePage,const RID &rid);

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

   // Assume the rid does not change after update
   RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

   RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

   // scan returns an iterator to allow the caller to go through the results one by one.
   RC scan(FileHandle &fileHandle,
       const vector<Attribute> &recordDescriptor,
       const string &conditionAttribute,
       const CompOp compOp,                  // comparision type such as "<" and "="
       const void *value,                    // used in the comparison
       const vector<string> &attributeNames, // a list of projected attributes
       RBFM_ScanIterator &rbfm_ScanIterator);

    // This method will be mainly used for debugging/testing
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field contains null value or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If thre are more than 8 fields, then you need to find the corresponding byte, then a bit inside that byte.
  //  2) actual data is a concatenation of values of the attributes
  //  3) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
  // For example, refer to the Q8 of Project 1 wiki page.
  

/**************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
***************************************************************************************************************************************************************/

public:

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;
};

#endif

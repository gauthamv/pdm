#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
#define ROOT_PAGE 0
#define NON_LEAF 20000
#define LEAF -1

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {


    public:
	//int currentPageNo;
        static IndexManager* instance();

        // Create an index file
        RC createFile(const string &fileName);

        // Delete an index file
        RC destroyFile(const string &fileName);

        // Open an index and return a file handle
        RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

        // Close a file handle for an index. 
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given fileHandle
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to supports a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree JSON record in pre-order
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

        RC formNonLeafHeader(unsigned char *Page);
        RC formLeafHeader(unsigned char *Page);
        RC readFreeSpacePtr(unsigned char *Page,int &freeSpace);
        RC setFreeSpacePtr(unsigned char *Page,int offset);
        RC checkSpaceLeafPage(unsigned char *leafPage,const Attribute &attribute, const void *key, const RID &rid);
        RC insertLeafEntry(unsigned char *leafPage,const Attribute &attribute, const void *key, const RID &rid);
        RC getnoofEntries(unsigned char *Page,int &entries) const;
        RC getrecEntries(unsigned char *Page,int &entries,int offset);
        RC setrecEntries(unsigned char *Page,int value,int offset);
        RC setnoofEntries(unsigned char *Page,int entries);
        RC getNonLeafFlag(unsigned char *Page) const;
        RC copyEntryToPage(unsigned char *leafPage,int offset,int recEntries,const Attribute &attribute, const void *key, const RID &rid);
        RC splitLeafPage(unsigned char *leafPage,unsigned char *newPage,IXFileHandle &ixfileHandle, int pageNo,const Attribute &attribute);
        RC splitNonLeafPage(unsigned char *nonLeafPage,unsigned char *newPage,const Attribute &attribute);
        RC entryLength(unsigned char *leafPage,int offset,int &length,const Attribute &attribute);
        RC entryLengthNonLeaf(unsigned char *nonLeafPage,int offset,int &length,const Attribute &attribute);
        RC setDLLpointers(IXFileHandle &ixfileHandle,unsigned char *newPage,unsigned char *oldPage,int oldPageNo);
        RC fillRootPage(unsigned char *rootPage,int leftPageNo,int rightPageNo,unsigned char *firstEntry,int length,const Attribute &attribute);
        RC getnoofEntriesNonLeaf(unsigned char *Page,int &entries) const;
        RC setnoofEntriesNonLeaf(unsigned char *Page,int entries);
        RC findPath(unsigned char *rootPage,int &childPageNo,const void *key,const Attribute &attribute);
        RC removeEntry(unsigned char *Page,const Attribute &attribute, const void *key, const RID &rid);
       // RC getFlag(unsigned char *page,bool &flag);
    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};

class IX_ScanIterator {
	FileHandle* fileHandle;
	Attribute attribute;
	int currentPageNo;
	int currentOffset;
	int noOfEntries;
	int currentEntry;
	 bool lowKeyInclusive;
	 bool highKeyInclusive;
	 bool newentry;
	 int noofrids;
	 int currentrid;
	 unsigned char *lowkey;
	 unsigned char *highkey;
	 unsigned char pagedata[PAGE_SIZE];
    public:
		RC loadscandata(FileHandle&,Attribute,const void*,const void*,bool,bool);
        IX_ScanIterator();  							// Constructor
        ~IX_ScanIterator(); 							// Destructor
        short AttributeSize(Attribute,const void*);
        RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
        RC close();             						// Terminate index scan

};


class IXFileHandle {
    public:
        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        IXFileHandle();  							// Constructor
        ~IXFileHandle(); 							// Destructor
        //PagedFileManager pfManager;
        FileHandle fileHandle;

};

// print out the error message for a given return code
void IX_PrintError (RC rc);

#endif

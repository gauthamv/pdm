#ifndef _pfm_h_
#define _pfm_h_

typedef int RC;
typedef char byte;
typedef unsigned PageNum;

#define PAGE_SIZE 4096
#include <string>
#include <climits>
#include<iostream>
#include<fstream>
using namespace std;

class FileHandle;


class PagedFileManager
{
public:
    static PagedFileManager* instance();                     // Access to the _pf_manager instance

    RC createFile    (const string &fileName);                         // Create a new file
    RC destroyFile   (const string &fileName);                         // Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                         // Close a file

protected:
    PagedFileManager();                                   // Constructor
    ~PagedFileManager();                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{
public:
    // variables to keep counter for each operation
	unsigned readPageCounter;
	unsigned writePageCounter;
	unsigned appendPageCounter;
	fstream file;
	
    FileHandle();                                                    // Default constructor
    ~FileHandle();                                                   // Destructor
    unsigned transformPageNum(PageNum pageNum);//abstract the page number and generate the original based on the directory pages.
    unsigned originalPageNum(PageNum pageNum);
    bool filehandlefree();											 //check whether the file handle is free
    RC readHeaderPage(PageNum HeaderPageNum,void* data);
    RC updateHeaderPage(PageNum HeaderPageNum,void* data);
    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // put the current counter values into variables
}; 

#endif


#include "ix.h"
#include<cstring>
#include<map>
#include<cmath>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	int sFlag = 0; //success flag
	IXFileHandle ixfileHandle;
	if(!PagedFileManager::instance()->createFile(fileName))
	{
		unsigned char *rootPage = new unsigned char[PAGE_SIZE];
		unsigned char *leafPage = new unsigned char[PAGE_SIZE];
		if(!formNonLeafHeader(rootPage))
		{
			if(!PagedFileManager::instance()->openFile(fileName,ixfileHandle.fileHandle))
			{
				if(!ixfileHandle.fileHandle.appendPage(rootPage))
				{
					sFlag = 1;
				}
			}
		}
		if(!formLeafHeader(leafPage))
		{
			if(!ixfileHandle.fileHandle.appendPage(leafPage))
			{
				if(sFlag == 1)
				{
					return 0;
				}
			}
		}
	}
	return -1;
}

RC IndexManager::destroyFile(const string &fileName)
{
	return PagedFileManager::instance()->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
	if(!PagedFileManager::instance()->openFile(fileName,ixFileHandle.fileHandle))
	{
		return 0;
	}
	return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	if(!PagedFileManager::instance()->closeFile(ixfileHandle.fileHandle))
	{
		return 0;
	}
	return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	//First check if Page0(root) has any entry
	int childPageNo = 0;	//gets which child node to go to
	int parentPage = 0;	//initial parent page is root page
	unsigned char *rootPage = new unsigned char[PAGE_SIZE];
	if(!ixfileHandle.fileHandle.readPage(ROOT_PAGE,rootPage))	//Read root page
	{
		int freeSpace;
		readFreeSpacePtr(rootPage,freeSpace);
		if(freeSpace == sizeof(int))
		{
			//means root page is free, that is there is only one leaf page
			unsigned char *leafPage = new unsigned char[PAGE_SIZE];
			if(!ixfileHandle.fileHandle.readPage(1,leafPage))	//1st leaf page
			{
				if(checkSpaceLeafPage(leafPage,attribute,key,rid))	//if space is present in the 1st leaf page
				{
					if(!insertLeafEntry(leafPage,attribute,key,rid))
					{
						return 0;
					}
				}
				else	//no space in leaf page, therefore leaf page needs to be split and a root entry needs to be added
				{
					unsigned char *newPage = new unsigned char[PAGE_SIZE];
					formLeafHeader(newPage);	//formatting the new page
					splitLeafPage(leafPage,newPage,ixfileHandle,1,attribute);
					int length=0;	//used for getting the length of 1st node in the new page
					if(attribute.type == TypeVarChar)
					{
						memcpy(&length,newPage+sizeof(int),sizeof(int)); //copying the length of key field
						unsigned char *firstEntry = new unsigned char[length];
						memcpy(firstEntry,newPage+sizeof(int),length+sizeof(int));
						fillRootPage(rootPage,1,2,firstEntry,length+sizeof(int),attribute);
					}
					else
					{
						ixfileHandle.fileHandle.appendPage(newPage); //appending the new page
						unsigned char *firstEntry = new unsigned char[sizeof(int)];
						memcpy(firstEntry,newPage+sizeof(int),sizeof(int));
						fillRootPage(rootPage,1,2,firstEntry,sizeof(int),attribute);
					}
					ixfileHandle.fileHandle.writePage(1,leafPage);  //writing the old page back
					ixfileHandle.fileHandle.appendPage(newPage); //appending the new page
					ixfileHandle.fileHandle.writePage(0,rootPage);  //writing the old root page back
					if(!insertEntry(ixfileHandle,attribute,key,rid))
					{
						return 0;
					}
				}
			}
		}
		else
		{
			while(true && (getNonLeafFlag(rootPage) == 20000)) //check if page is nonLeaf
			{
				//checking if root is unstable or not
				int freeSpace;
				readFreeSpacePtr(rootPage,freeSpace);
				int keyLength;
				if(attribute.type == TypeVarChar)
				{
					memcpy(&keyLength,key,sizeof(int));
					keyLength+=sizeof(int);
				}
				else
				{
					keyLength = sizeof(int);
				}
				keyLength+=sizeof(int); //takes care of the right pointer to every key
				//Checking is space is der in the root node
				if(PAGE_SIZE-(3*sizeof(int))-freeSpace < keyLength)
				{
					//means free space is not there in the nonLeaf(potential unstable node)
					unsigned char *newPage = new unsigned char[PAGE_SIZE];
					unsigned char *newRootPage = new unsigned char[PAGE_SIZE];
					formNonLeafHeader(newPage);	//formatting the new page
					formNonLeafHeader(newRootPage);	//formatting the new page
					splitNonLeafPage(rootPage,newPage,attribute);
					ixfileHandle.fileHandle.appendPage(newPage); //appending the new page
					ixfileHandle.fileHandle.appendPage(rootPage); //appending the new page2
					int noOfPages = ixfileHandle.fileHandle.getNumberOfPages();
					int length=0;	//used for getting the length of 1st node in the new page
					if(attribute.type == TypeVarChar)
					{
						memcpy(&length,newPage+sizeof(int),sizeof(int)); //copying the length of key field
						unsigned char *firstEntry = new unsigned char[length];
						memcpy(firstEntry,newPage+sizeof(int),length+sizeof(int));
						fillRootPage(newRootPage,noOfPages-1,noOfPages,firstEntry,length+sizeof(int),attribute);
					}
					else
					{
						unsigned char *firstEntry = new unsigned char[sizeof(int)];
						memcpy(firstEntry,newPage+sizeof(int),sizeof(int));
						fillRootPage(newRootPage,noOfPages-1,noOfPages,firstEntry,sizeof(int),attribute);
					}
					ixfileHandle.fileHandle.writePage(parentPage,newRootPage);  //writing the old page back
					if(ixfileHandle.fileHandle.readPage(childPageNo,rootPage) == -1)
					{
						return -1;
					}
				}
				else
				{
					parentPage = childPageNo;
					findPath(rootPage,childPageNo,key,attribute);
					if(ixfileHandle.fileHandle.readPage(childPageNo,rootPage) == -1)
					{
						return -1;
					}
				}

			}

			//this point means leaf is reached
			if(checkSpaceLeafPage(rootPage,attribute,key,rid))	//if space is present in the 1st leaf page
			{
				if(!insertLeafEntry(rootPage,attribute,key,rid))
				{
					return 0;
				}
			}
			else	//no space in leaf page, therefore leaf page needs to be split and a root entry needs to be added
			{
				unsigned char *newLeafPage = new unsigned char[PAGE_SIZE];
				unsigned char *parentPageTemp = new unsigned char[PAGE_SIZE];
				if(ixfileHandle.fileHandle.readPage(parentPage,parentPageTemp) == -1)
				{
					return -1;
				}
				formLeafHeader(newLeafPage);	//formatting the new page
				splitLeafPage(rootPage,newLeafPage,ixfileHandle,childPageNo,attribute);
				ixfileHandle.fileHandle.appendPage(newLeafPage); //appending the new page
				int tempnoOfPage = ixfileHandle.fileHandle.getNumberOfPages();
				int length=0;	//used for getting the length of 1st node in the new page
				if(attribute.type == TypeVarChar)
				{
					memcpy(&length,newLeafPage+sizeof(int),sizeof(int)); //copying the length of key field
					unsigned char *firstEntry = new unsigned char[length];
					memcpy(firstEntry,newLeafPage+sizeof(int),length+sizeof(int));
					fillRootPage(parentPageTemp,childPageNo,tempnoOfPage,firstEntry,length+sizeof(int),attribute);
				}
				else
				{
					unsigned char *firstEntry = new unsigned char[sizeof(int)];
					memcpy(firstEntry,newLeafPage+sizeof(int),sizeof(int));
					fillRootPage(parentPageTemp,childPageNo,tempnoOfPage,firstEntry,sizeof(int),attribute);
				}
				ixfileHandle.fileHandle.writePage(parentPage,parentPageTemp);  //writing the old page back
				ixfileHandle.fileHandle.writePage(childPageNo,rootPage);  //writing the old page back
				if(!insertEntry(ixfileHandle,attribute,key,rid))
				{
					return 0;
				}
			}
		}
	}
    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

RC IndexManager::formNonLeafHeader(unsigned char *Page)
{
	int freePtr = sizeof(int);
	int firstPointerLocation = -1;
	int noOfEntries = 0;
	int nonLeaf = 20000;  //value of nonLeaf(flag)
	memcpy(Page+PAGE_SIZE-sizeof(int),&freePtr,sizeof(int));	//Copying the free space ptr location of that page
	memcpy(Page+PAGE_SIZE-(2*sizeof(int)),&noOfEntries,sizeof(int));	//storing the number of entries slot
	memcpy(Page+PAGE_SIZE-(3*sizeof(int)),&nonLeaf,sizeof(int));	//flag to identify nonLeaf Node
	memcpy(Page+0,&firstPointerLocation,sizeof(int));	//Copying the left most ptr location as -1 as a default value
	return 0;
}

RC IndexManager::formLeafHeader(unsigned char *Page)
{
	int freePtr = 0;
	int rightPtr = -1;
	int leftPtr = -1;
	int noOfEntries = 0;
	int leaf = -1;
	memcpy(Page+PAGE_SIZE-sizeof(int),&freePtr,sizeof(int));	//Copying the free space ptr location of that page
	memcpy(Page+PAGE_SIZE-(2*sizeof(int)),&rightPtr,sizeof(int));
	memcpy(Page+PAGE_SIZE-(3*sizeof(int)),&leftPtr,sizeof(int));
	memcpy(Page+PAGE_SIZE-(4*sizeof(int)),&noOfEntries,sizeof(int));	//storing the number of entries slot
	memcpy(Page+PAGE_SIZE-(5*sizeof(int)),&leaf,sizeof(int));	//flag to identify leaf
	return 0;
}

RC IndexManager::readFreeSpacePtr(unsigned char *Page,int &freeSpace)
{
	memcpy(&freeSpace,Page+PAGE_SIZE-sizeof(int),sizeof(int));
	return 0;
}

RC IndexManager::checkSpaceLeafPage(unsigned char *leafPage,const Attribute &attribute, const void *key, const RID &rid)
{
	int freeSpacePtr;
	int freeSpaceValue = 0;	//Stores the amount of freespace in page
	readFreeSpacePtr(leafPage,freeSpacePtr);	//reads the value of freespace ptr
	if(attribute.type == TypeVarChar)
	{
		int length;
		memcpy(&length,(char*)key,sizeof(int));
		freeSpaceValue = PAGE_SIZE - freeSpacePtr -(5*sizeof(int)) -(2*sizeof(int)) -length -sizeof(RID);
	}
	else
	{
		freeSpaceValue = PAGE_SIZE - freeSpacePtr -(5*sizeof(int)) -(2*sizeof(int)) -sizeof(RID);
	}
	if(freeSpaceValue > 0)	//means space is present
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

RC IndexManager::insertLeafEntry(unsigned char *leafPage,const Attribute &attribute, const void *key, const RID &rid)
{
	int noOfEntries;
	int freeSpacePtr;
	int offset=0;
	readFreeSpacePtr(leafPage,freeSpacePtr);
	getnoofEntries(leafPage,noOfEntries);	//gets the number of key rid pairs in the page

	if(noOfEntries == 0)	//no entry in the leaf page yet
	{
		copyEntryToPage(leafPage,freeSpacePtr,1,attribute,key,rid); //This is the 1st entry
		setnoofEntries(leafPage,noOfEntries+1);
		return 0;
	}
	int oldLength,newLength;
	if(attribute.type == TypeVarChar)
	{
		int i;
		for(i=0;i<noOfEntries;i++)
		{
			memcpy(&oldLength,leafPage+offset+sizeof(int),sizeof(int));
			memcpy(&newLength,(char*)key,sizeof(int));
			char *newC = new char[newLength+1];
			char *oldC = new char[oldLength+1];
			memset(newC,'\0',newLength+1);
			memset(oldC,'\0',oldLength+1);
			memcpy(oldC,leafPage+offset+(2*sizeof(int)),oldLength);
			memcpy(newC,(char*)key+sizeof(int),newLength);
			string newString(newC);
			string oldString(oldC);
			if(newString.compare(oldString) == 0)
			{
				int tempEntries;
				memcpy(&tempEntries,leafPage+offset,sizeof(int)); //copying the number of recEntries
				setrecEntries(leafPage,tempEntries+1,offset);	//increasing the recEntries by 1
				offset+=(2*sizeof(int))+oldLength+tempEntries*sizeof(RID);
				int tempSize =  freeSpacePtr-offset;
				unsigned char *tempData = new unsigned char[tempSize];
				memcpy(tempData,leafPage+offset,tempSize);
				memcpy(leafPage+offset,&rid,sizeof(RID));
				offset+=sizeof(RID);
				memcpy(leafPage+offset,tempData,tempSize);
				offset+=tempSize;
				setFreeSpacePtr(leafPage,offset);	//Setting the new freeSpaceptr
				break;
			}
			else if(newString.compare(oldString) < 0)
			{
				int tempSize =  freeSpacePtr-offset;
				unsigned char *tempData = new unsigned char[tempSize];
				memcpy(tempData,leafPage+offset,tempSize);
				copyEntryToPage(leafPage,offset,1,attribute,key,rid);
				offset+=(2*sizeof(int))+newLength+sizeof(RID);
				memcpy(leafPage+offset,tempData,tempSize);
				setnoofEntries(leafPage,noOfEntries+1);
				offset+=tempSize;
				setFreeSpacePtr(leafPage,offset);	//Setting the new freeSpaceptr
				break;
			}
			else
			{
				int tempEntries;
				memcpy(&tempEntries,leafPage+offset,sizeof(int)); //copying the number of recEntries
				offset+=(2*sizeof(int))+oldLength+tempEntries*sizeof(RID);
			}
		}
		if(i == noOfEntries)
		{
			copyEntryToPage(leafPage,offset,1,attribute,key,rid);
			setnoofEntries(leafPage,noOfEntries+1);
			offset+=(2*sizeof(int))+oldLength+sizeof(RID);
			setFreeSpacePtr(leafPage,offset);	//Setting the new freeSpaceptr
		}
	}
	else
	{
		int i;
		for(i=0;i<noOfEntries;i++)
		{
			int oldValue,newValue;
			memcpy(&oldValue,leafPage+offset+sizeof(int),sizeof(int));
			memcpy(&newValue,(char*)key,sizeof(int));
			if(newValue == oldValue)
			{
				int tempEntries;
				memcpy(&tempEntries,leafPage+offset,sizeof(int)); //copying the number of recEntries
				setrecEntries(leafPage,tempEntries+1,offset);	//increasing the recEntries by 1
				offset+=(2*sizeof(int))+tempEntries*sizeof(RID);
				int tempSize =  freeSpacePtr-offset;
				unsigned char *tempData = new unsigned char[tempSize];
				memcpy(tempData,leafPage+offset,tempSize);
				memcpy(leafPage+offset,&rid,sizeof(RID));
				offset+=sizeof(RID);
				memcpy(leafPage+offset,tempData,tempSize);
				offset+=tempSize;
				setFreeSpacePtr(leafPage,offset);	//Setting the new freeSpaceptr
				break;
			}
			else if(newValue < oldValue)
			{
				int tempSize =  freeSpacePtr-offset;
				unsigned char *tempData = new unsigned char[tempSize];
				memcpy(tempData,leafPage+offset,tempSize);
				copyEntryToPage(leafPage,offset,1,attribute,key,rid);
				offset+=(2*sizeof(int))+sizeof(RID);
				memcpy(leafPage+offset,tempData,tempSize);
				setnoofEntries(leafPage,noOfEntries+1);
				offset+=tempSize;
				setFreeSpacePtr(leafPage,offset);	//Setting the new freeSpaceptr
				break;
			}
			else
			{
				int tempEntries;
				memcpy(&tempEntries,leafPage+offset,sizeof(int)); //copying the number of recEntries
				offset+=(2*sizeof(int))+tempEntries*sizeof(RID);
			}
		}
		if(i == noOfEntries)
		{
			copyEntryToPage(leafPage,offset,1,attribute,key,rid);
			setnoofEntries(leafPage,noOfEntries+1);
			offset+=(2*sizeof(int))+sizeof(RID);
			setFreeSpacePtr(leafPage,offset);	//Setting the new freeSpaceptr
		}
	}
	return 0;
}

RC IndexManager::getnoofEntries(unsigned char *Page,int &entries)
{
	memcpy(&entries,Page+PAGE_SIZE-(4*sizeof(int)),sizeof(int));
	return 0;
}

RC IndexManager::setnoofEntries(unsigned char *Page,int entries)
{
	memcpy(Page+PAGE_SIZE-(4*sizeof(int)),&entries,sizeof(int));
	return 0;
}

RC IndexManager::setrecEntries(unsigned char *Page,int value,int offset)
{
	memcpy(Page+offset,&value,sizeof(int));
	return 0;
}

RC IndexManager::copyEntryToPage(unsigned char *leafPage,int offset,int recEntries,const Attribute &attribute, const void *key, const RID &rid)
{
	memcpy(leafPage+offset,&recEntries,sizeof(int));  //copying the number of recEntries
	offset+=sizeof(int);
	int length;
	if(attribute.type == TypeVarChar)
	{
		memcpy(&length,(char*)key,sizeof(int));
		length = length + sizeof(int); //including the int field which stores the length
	}
	else
	{
		length = sizeof(int);
	}
	memcpy(leafPage+offset,(char*)key,length); //copying the key
	offset+=length;
	memcpy(leafPage+offset,&rid,sizeof(RID));	//copying the rid
	return 0;
}

RC IndexManager::getrecEntries(unsigned char *Page,int &entries,int offset)
{
	memcpy(&entries,Page+offset,sizeof(int));
	return 0;
}

RC IndexManager::setFreeSpacePtr(unsigned char *Page,int offset)
{
	memcpy(Page+PAGE_SIZE-sizeof(int),&offset,sizeof(int));
	return 0;
}

RC IndexManager::splitLeafPage(unsigned char *leafPage,unsigned char *newPage,IXFileHandle &ixfileHandle, int pageNo,const Attribute &attribute)
{
	int freeSpacePtr,offset=0;
	int recEntries;
	int length=0;
	int entries = 0;
	int countEntries = 0; //to count the entries from start of page to start of split
	readFreeSpacePtr(leafPage,freeSpacePtr);
	getnoofEntries(leafPage,entries);	//gets the number of entries as we are going to split
	while(offset < (freeSpacePtr/2))
	{
		entryLength(leafPage,offset,length,attribute); // gets the length of an entry
		offset+=length; //incrementing offset
		countEntries++;
	}
	memcpy(newPage,leafPage+offset,freeSpacePtr-offset); //copying data to new page
	setnoofEntries(leafPage,countEntries); //updating the entries for old page
	setnoofEntries(newPage,entries-countEntries); //setting the entries for new page
	setFreeSpacePtr(leafPage,offset);	//Updating the freeSpaceptr of oldpage
	setFreeSpacePtr(newPage,freeSpacePtr-offset);	//setting the freeSpaceptr of newpage
	setDLLpointers(ixfileHandle,newPage,leafPage,pageNo); //setting the dll pointers to page no's
	return 0;
}

RC IndexManager::splitNonLeafPage(unsigned char *nonLeafPage,unsigned char *newPage,const Attribute &attribute)
{
	int freeSpacePtr,offset=sizeof(int);
	int recEntries;
	int length=0;
	int entries = 0;
	int countEntries = 0; //to count the entries from start of page to start of split
	readFreeSpacePtr(nonLeafPage,freeSpacePtr);
	getnoofEntriesNonLeaf(nonLeafPage,entries);	//gets the number of entries as we are going to split
	while(offset < (freeSpacePtr/2))
	{
		entryLengthNonLeaf(nonLeafPage,offset,length,attribute);
		offset+=length; //incrementing offset
		countEntries++;
	}
	memcpy(newPage+sizeof(int),nonLeafPage+offset,freeSpacePtr-offset); //copying data to new page
	setnoofEntriesNonLeaf(nonLeafPage,countEntries); //updating the entries for old page
	setnoofEntries(newPage,entries-countEntries); //setting the entries for new page
	setFreeSpacePtr(nonLeafPage,offset);	//Updating the freeSpaceptr of oldpage
	setFreeSpacePtr(newPage,freeSpacePtr-offset);	//setting the freeSpaceptr of newpage
	return 0;
}


RC IndexManager::entryLength(unsigned char *leafPage,int offset,int &length,const Attribute &attribute)
{
	int recEntries;
	getrecEntries(leafPage,recEntries,offset);
	offset+=sizeof(int);
	if(attribute.type == TypeVarChar)
	{
		memcpy(&length,leafPage+offset,sizeof(int));
		length+=sizeof(int)+sizeof(int)+(recEntries*sizeof(RID)); //total length = no of entires + length field + data(length) + RID's
	}
	else
	{
		length = sizeof(int) + sizeof(int) + (recEntries*sizeof(RID));
	}
}

RC IndexManager::entryLengthNonLeaf(unsigned char *nonLeafPage,int offset,int &length,const Attribute &attribute)
{
	//offset+=sizeof(int);
	int tempLength=0;
	if(attribute.type == TypeVarChar)
	{
		memcpy(&tempLength,nonLeafPage+offset,sizeof(int));
		length = (2*sizeof(int))+tempLength; //total length = no of entires + length field + data(length) + RID's
	}
	else
	{
		length = sizeof(int) + sizeof(int);
	}
}

RC IndexManager::setDLLpointers(IXFileHandle &ixfileHandle,unsigned char *newPage,unsigned char *oldPage,int oldPageNo)
{
	int noOfPages = ixfileHandle.fileHandle.getNumberOfPages() + 1;
	memcpy(newPage+PAGE_SIZE-(3*sizeof(int)),&oldPageNo,sizeof(int));
	memcpy(oldPage+PAGE_SIZE-(2*sizeof(int)),&noOfPages,sizeof(int));
	return 0;
}

RC IndexManager::fillRootPage(unsigned char *rootPage,int leftPageNo,int rightPageNo,unsigned char *firstEntry,int length,const Attribute &attribute)
{
	int noOfEntries,offset=sizeof(int);
	getnoofEntriesNonLeaf(rootPage,noOfEntries);
	int freeSpacePtr;
	readFreeSpacePtr(rootPage,freeSpacePtr);
	if(noOfEntries == 0)
	{
		memcpy(rootPage,&leftPageNo,sizeof(int));
		memcpy(rootPage+sizeof(int),firstEntry,length);
		memcpy(rootPage+sizeof(int)+length,&rightPageNo,sizeof(int));
		setnoofEntriesNonLeaf(rootPage,noOfEntries+1);
	}
	else
	{
		if(attribute.type == TypeVarChar)
		{
			int i,oldLength,newLength;
			for(i=0;i<noOfEntries;i++)
			{
				memcpy(&oldLength,rootPage+offset,sizeof(int));
				memcpy(&newLength,firstEntry,sizeof(int));
				char *newC = new char[newLength+1];
				char *oldC = new char[oldLength+1];
				memset(newC,'\0',newLength+1);
				memset(oldC,'\0',oldLength+1);
				memcpy(oldC,rootPage+offset+sizeof(int),oldLength);
				memcpy(newC,firstEntry+sizeof(int),newLength);
				string newString(newC);
				string oldString(oldC);
				if(newString.compare(oldString) < 0)
				{
					int tempSize =  freeSpacePtr-offset;
					unsigned char *tempData = new unsigned char[tempSize];
					memcpy(tempData,rootPage+offset,tempSize);
					memcpy(rootPage+offset,firstEntry,length);
					offset+=length;
					memcpy(rootPage+offset,&rightPageNo,sizeof(int));
					offset+=sizeof(int);
					memcpy(rootPage+offset,tempData,tempSize);
					setnoofEntriesNonLeaf(rootPage,noOfEntries+1);
					offset+=tempSize;
					setFreeSpacePtr(rootPage,offset);	//Setting the new freeSpaceptr
					break;
				}
				else if(newString.compare(oldString) > 0)
				{
					int tempLength;
					memcpy(&tempLength,rootPage+offset,sizeof(int)); //copying the number of recEntries
					offset+=sizeof(int)+tempLength;
				}
			}
			if(i == noOfEntries)
			{
				setnoofEntriesNonLeaf(rootPage,noOfEntries+1);
				memcpy(rootPage+offset,firstEntry,length);
				offset+=length;
				memcpy(rootPage+offset,&rightPageNo,sizeof(int));
				offset+=sizeof(int);
				setFreeSpacePtr(rootPage,offset);	//Setting the new freeSpaceptr
			}
		}
		else
		{
			int i;
			for(i=0;i<noOfEntries;i++)
			{
				int oldValue,newValue;
				memcpy(&oldValue,rootPage+offset,sizeof(int));
				memcpy(&newValue,firstEntry,sizeof(int));
				if(newValue < oldValue)
				{
					int tempSize =  freeSpacePtr-offset;
					unsigned char *tempData = new unsigned char[tempSize];
					memcpy(tempData,rootPage+offset,tempSize);
					memcpy(rootPage+offset,firstEntry,length);
					offset+=length;
					memcpy(rootPage+offset,&rightPageNo,sizeof(int));
					offset+=sizeof(int);
					memcpy(rootPage+offset,tempData,tempSize);
					setnoofEntriesNonLeaf(rootPage,noOfEntries+1);
					offset+=tempSize;
					setFreeSpacePtr(rootPage,offset);	//Setting the new freeSpaceptr
					break;
				}
				else if(newValue > oldValue)
				{
					int tempValue;
					offset+=(2*sizeof(int));
				}
			}
			if(i == noOfEntries)
			{
				setnoofEntriesNonLeaf(rootPage,noOfEntries+1);
				memcpy(rootPage+offset,firstEntry,length);
				offset+=length;
				memcpy(rootPage+offset,&rightPageNo,sizeof(int));
				offset+=sizeof(int);
				setFreeSpacePtr(rootPage,offset);	//Setting the new freeSpaceptr
			}
		}

	}
	return 0;
}

RC IndexManager::getnoofEntriesNonLeaf(unsigned char *Page,int &entries)
{
	memcpy(&entries,Page+PAGE_SIZE-(2*sizeof(int)),sizeof(int));
	return 0;
}
RC IndexManager::setnoofEntriesNonLeaf(unsigned char *Page,int entries)
{
	memcpy(Page+PAGE_SIZE-(2*sizeof(int)),&entries,sizeof(int));
	return 0;
}

RC IndexManager::findPath(unsigned char *rootPage,int &childPageNo,const void *key,const Attribute &attribute)
{
	int offset = sizeof(int);
	int noOfEntries;
	getnoofEntriesNonLeaf(rootPage,noOfEntries);
	if(attribute.type == TypeVarChar)
	{
		int i,oldLength,newLength;
		for(i=0;i<noOfEntries;i++)
		{
			memcpy(&oldLength,rootPage+offset,sizeof(int));
			memcpy(&newLength,(char*)key,sizeof(int));
			char *newC = new char[newLength+1];
			char *oldC = new char[oldLength+1];
			memset(newC,'\0',newLength+1);
			memset(oldC,'\0',oldLength+1);
			memcpy(oldC,rootPage+offset+sizeof(int),oldLength);
			memcpy(newC,(char*)key+sizeof(int),newLength);
			string newString(newC);
			string oldString(oldC);
			if(newString.compare(oldString) < 0)
			{
				memcpy(&childPageNo,rootPage+offset-sizeof(int),sizeof(int)); //copying the left child's page no
				break;
			}
			else
			{
				int tempLength;
				memcpy(&tempLength,rootPage+offset,sizeof(int)); //copying the length
				offset+=(2*sizeof(int))+tempLength;
			}
		}
		if(i == noOfEntries)
		{
			memcpy(&childPageNo,rootPage+offset-sizeof(int),sizeof(int)); //rightmost child
		}
	}
	else
	{
		int i;
		for(i=0;i<noOfEntries;i++)
		{
			int oldValue,newValue;
			memcpy(&oldValue,rootPage+offset,sizeof(int));
			memcpy(&newValue,(char*)key,sizeof(int));
			if(newValue < oldValue)
			{
				memcpy(&childPageNo,rootPage+offset-sizeof(int),sizeof(int)); //copying the left child's page no
				break;
			}
			else
			{
				offset+=(2*sizeof(int));
			}
		}
		if(i == noOfEntries)
		{
			memcpy(&childPageNo,rootPage+offset-sizeof(int),sizeof(int)); //rightmost child
		}
	}
	return 0;
}

RC IndexManager::getNonLeafFlag(unsigned char *Page)
{
	int flag;
	memcpy(&flag,Page+PAGE_SIZE-(3*sizeof(int)),sizeof(int));
	return flag;
}


IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
	//fileHandle = NULL;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	if(!fileHandle.collectCounterValues(readPageCount,writePageCount,appendPageCount))
	{
		return 0;
	}
    return -1;
}

void IX_PrintError (RC rc)
{
}

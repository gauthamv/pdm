
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
					delete[] rootPage;
					delete[] leafPage;
					return 0;
				}
			}
		}
		delete[] rootPage;
		delete[] leafPage;
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
						if(!ixfileHandle.fileHandle.writePage(1,leafPage))
						{
							delete[] leafPage;
							delete[] rootPage;
							return 0;
						}
					}
				}
				else	//no space in leaf page, therefore leaf page needs to be split and a root entry needs to be added
				{
					cout<<"No of Pages: "<<ixfileHandle.fileHandle.getNumberOfPages()<<endl;
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
						delete[] firstEntry;
					}
					else
					{
						unsigned char *firstEntry = new unsigned char[sizeof(int)];
						memcpy(firstEntry,newPage+sizeof(int),sizeof(int));
						fillRootPage(rootPage,1,2,firstEntry,sizeof(int),attribute);
						delete[] firstEntry;
					}
					ixfileHandle.fileHandle.writePage(1,leafPage);  //writing the old page back
					ixfileHandle.fileHandle.appendPage(newPage); //appending the new page
					ixfileHandle.fileHandle.writePage(0,rootPage);  //writing the old root page back
					if(!insertEntry(ixfileHandle,attribute,key,rid))
					{
						delete[] rootPage;
						delete[] newPage;
						delete[] leafPage;
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
					//if(parentPage == 0)
					if(childPageNo == 0)
					{
						//means free space is not there in the nonLeaf(potential unstable node)
						unsigned char *newPage = new unsigned char[PAGE_SIZE];
						unsigned char *newRootPage = new unsigned char[PAGE_SIZE];
						formNonLeafHeader(newPage);	//formatting the new page
						formNonLeafHeader(newRootPage);	//formatting the new page
						int noOfPages = ixfileHandle.fileHandle.getNumberOfPages();
						splitNonLeafPage(rootPage,newPage,attribute);
						ixfileHandle.fileHandle.appendPage(rootPage); //appending the new page2
						ixfileHandle.fileHandle.appendPage(newPage); //appending the new page
						int length=0;	//used for getting the length of 1st node in the new page
						if(attribute.type == TypeVarChar)
						{
							memcpy(&length,newPage+sizeof(int),sizeof(int)); //copying the length of key field
							unsigned char *firstEntry = new unsigned char[length];
							memcpy(firstEntry,newPage+sizeof(int),length+sizeof(int));
							fillRootPage(newRootPage,noOfPages,noOfPages+1,firstEntry,length+sizeof(int),attribute);
							delete[] firstEntry;
						}
						else
						{
							unsigned char *firstEntry = new unsigned char[sizeof(int)];
							memcpy(firstEntry,newPage+sizeof(int),sizeof(int));
							fillRootPage(newRootPage,noOfPages,noOfPages+1,firstEntry,sizeof(int),attribute);
							delete[] firstEntry;
						}
						ixfileHandle.fileHandle.writePage(parentPage,newRootPage);  //writing the old page back
						//if(ixfileHandle.fileHandle.readPage(childPageNo,rootPage) == -1)
						//{
							//return -1;
						//}
						delete[] newPage;
						delete[] newRootPage;
					}
					else
					{
						//opening parent page
						unsigned char *parentPageTemp = new unsigned char[PAGE_SIZE];
						if(ixfileHandle.fileHandle.readPage(parentPage,parentPageTemp) == -1)
						{
							return -1;
						}
						unsigned char *newPage = new unsigned char[PAGE_SIZE];
						formNonLeafHeader(newPage);	//formatting the new page
						splitNonLeafPage(rootPage,newPage,attribute);
						int noOfPages = ixfileHandle.fileHandle.getNumberOfPages();
						ixfileHandle.fileHandle.appendPage(newPage); //appending the new page
						int length=0;	//used for getting the length
						if(attribute.type == TypeVarChar)
						{
							memcpy(&length,newPage+sizeof(int),sizeof(int)); //copying the length of key field
							unsigned char *firstEntry = new unsigned char[length];
							memcpy(firstEntry,newPage+sizeof(int),length+sizeof(int));
							fillRootPage(parentPageTemp,childPageNo,noOfPages,firstEntry,length+sizeof(int),attribute);
							delete[] firstEntry;
						}
						else
						{
							unsigned char *firstEntry = new unsigned char[sizeof(int)];
							memcpy(firstEntry,newPage+sizeof(int),sizeof(int));
							fillRootPage(parentPageTemp,childPageNo,noOfPages,firstEntry,sizeof(int),attribute);
							delete[] firstEntry;
						}
						ixfileHandle.fileHandle.writePage(parentPage,parentPageTemp);
						ixfileHandle.fileHandle.writePage(childPageNo,rootPage);
						delete[] parentPageTemp;
						delete[] newPage;
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
					if(!ixfileHandle.fileHandle.writePage(childPageNo,rootPage))
					{
						delete[] rootPage;
						return 0;
					}
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
				int tempnoOfPage = ixfileHandle.fileHandle.getNumberOfPages();
				//ixfileHandle.fileHandle.appendPage(newLeafPage); //appending the new page
				int length=0;	//used for getting the length of 1st node in the new page
				if(attribute.type == TypeVarChar)
				{
					memcpy(&length,newLeafPage+sizeof(int),sizeof(int)); //copying the length of key field
					unsigned char *firstEntry = new unsigned char[length];
					memcpy(firstEntry,newLeafPage+sizeof(int),length+sizeof(int));
					fillRootPage(parentPageTemp,childPageNo,tempnoOfPage,firstEntry,length+sizeof(int),attribute);
					delete[] firstEntry;
				}
				else
				{
					unsigned char *firstEntry = new unsigned char[sizeof(int)];
					memcpy(firstEntry,newLeafPage+sizeof(int),sizeof(int));
					fillRootPage(parentPageTemp,childPageNo,tempnoOfPage,firstEntry,sizeof(int),attribute);
					delete[] firstEntry;
				}
				ixfileHandle.fileHandle.writePage(parentPage,parentPageTemp);  //writing the old page back
				ixfileHandle.fileHandle.writePage(childPageNo,rootPage);  //writing the old page back
				ixfileHandle.fileHandle.appendPage(newLeafPage); //appending the new page
				ixfileHandle.fileHandle.writePage(tempnoOfPage,newLeafPage);  //PLZ CHECK WHY!!!
				if(!insertEntry(ixfileHandle,attribute,key,rid))
				{
					delete[] rootPage;
					delete[] newLeafPage;
					delete[] parentPageTemp;
					return 0;
				}
			}
		}
	}
    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	//opening the root page
	unsigned char *rootPage = new unsigned char[PAGE_SIZE];
	if(!ixfileHandle.fileHandle.readPage(ROOT_PAGE,rootPage))	//Read root page
	{
		int freeSpace;
		readFreeSpacePtr(rootPage,freeSpace);
		if(freeSpace == sizeof(int))
		{
			//root is empty, only leaf pages present
			unsigned char *leafPage = new unsigned char[PAGE_SIZE];
			if(!ixfileHandle.fileHandle.readPage(1,leafPage))	//1st leaf page
			{
				delete[] rootPage;
				if(removeEntry(leafPage,attribute,key,rid) == -1)
				{
					delete[] leafPage;
					return -1;
				}
				if(!ixfileHandle.fileHandle.writePage(1,leafPage))
				{
					delete[] leafPage;
					return 0;
				}
			}
		}
		else
		{
			int childPageNo; //gets the child page no
			while(true && (getNonLeafFlag(rootPage) == 20000)) //check if page is nonLeaf
			{
				findPath(rootPage,childPageNo,key,attribute);
				if(ixfileHandle.fileHandle.readPage(childPageNo,rootPage) == -1)
				{
					return -1;
				}
			}
			if(removeEntry(rootPage,attribute,key,rid) == -1)
			{
				delete[] rootPage;
				return -1;
			}
			else
			{
				if(!ixfileHandle.fileHandle.writePage(childPageNo,rootPage))
				{
					delete[] rootPage;
					return 0;
				}
				delete[] rootPage;
				return 0;
			}
		}
	}
	delete[] rootPage;
    return -1;
}

RC IndexManager::removeEntry(unsigned char *Page,const Attribute &attribute, const void *key, const RID &rid)
{
	int offset=0;
	int freeSpace;
	readFreeSpacePtr(Page,freeSpace);
	int recEntries;
	int found = 0;	//checks whether entry is found or not

	while(offset != freeSpace)
	{
		memcpy(&recEntries,Page+offset,sizeof(int));
		offset+=sizeof(int);
		if(attribute.type == TypeVarChar)
		{
			int length;
			int length2;
			memcpy(&length,Page+offset,sizeof(int));
			memcpy(&length2,(char*)key,sizeof(int));
			char *pageEntry = new char[length+1];
			char *keyEntry = new char[length2+1];
			memset(pageEntry,'\0',length+1);
			memset(keyEntry,'\0',length2+1);
			memcpy(pageEntry,Page+offset+sizeof(int),length);
			memcpy(keyEntry,(char*)key+sizeof(int),length2);
			string pageString(pageEntry);
			string keyString(keyEntry);
			delete[] pageEntry;
			delete[] keyEntry;
			if(keyString.compare(pageString) == 0)
			{
				if(recEntries == 1)
				{
					int diff = offset+sizeof(int)+length+sizeof(RID);
					unsigned char *temp = new unsigned char[freeSpace-diff];
					memcpy(temp,Page+diff,freeSpace-diff);
					memcpy(Page+offset-sizeof(int),temp,freeSpace-diff);
					found =1;
					setFreeSpacePtr(Page,freeSpace-(2*sizeof(int) + length +sizeof(RID)));
					int entries;
					getnoofEntries(Page,entries);
					setnoofEntries(Page,entries-1);	//reducing the entries by 1
					//setrecEntries(Page,recEntries,offset-sizeof(int));
					break;
				}
				else
				{
					offset+=sizeof(int)+length;
					for(int i=1;i<=recEntries;i++)
					{
						RID tempRID;
						memcpy(&tempRID,Page+offset,sizeof(RID));
						if((tempRID.pageNum == rid.pageNum) && (tempRID.slotNum == rid.slotNum))
						{
							unsigned char *temp = new unsigned char[freeSpace-offset-sizeof(RID)];
							memcpy(temp,Page+offset+sizeof(RID),freeSpace-offset-sizeof(RID));
							memcpy(Page+offset,temp,freeSpace-offset-sizeof(RID));
							setFreeSpacePtr(Page,freeSpace-sizeof(RID));
							setrecEntries(Page,recEntries-1,offset-length-sizeof(int));
							found = 1;
							break;
						}
						else
						{
							offset+=sizeof(RID);
						}
					}
					break;
				}
			}
			offset+=sizeof(int)+length+(recEntries*sizeof(RID));
		}
		else
		{
			int keyValue,pageValue;
			memcpy(&keyValue,(char*)key,sizeof(int));
			memcpy(&pageValue,Page+offset,sizeof(int));
			if(keyValue == pageValue)
			{
				if(recEntries == 1)
				{
					int diff = offset+sizeof(int)+sizeof(RID);
					unsigned char *temp = new unsigned char[freeSpace-diff];
					memcpy(temp,Page+diff,freeSpace-diff);
					memcpy(Page+offset-sizeof(int),temp,freeSpace-diff);
					found = 1;
					setFreeSpacePtr(Page,freeSpace-(2*sizeof(int) + sizeof(RID)));
					int entries;
					getnoofEntries(Page,entries);
					setnoofEntries(Page,entries-1);	//reducing the entries by 1
					//setrecEntries(Page,recEntries-1,offset-sizeof(int));
					break;
				}
				else
				{
					offset+=sizeof(int);
					for(int i=1;i<=recEntries;i++)
					{
						RID tempRID;
						memcpy(&tempRID,Page+offset,sizeof(RID));
						if((tempRID.pageNum == rid.pageNum) && (tempRID.slotNum == rid.slotNum))
						{
							unsigned char *temp = new unsigned char[freeSpace-offset-sizeof(RID)];
							memcpy(temp,Page+offset+sizeof(RID),freeSpace-offset-sizeof(RID));
							memcpy(Page+offset,temp,freeSpace-offset-sizeof(RID));
							found = 1;
							setFreeSpacePtr(Page,freeSpace-sizeof(RID));
							setrecEntries(Page,recEntries-1,offset-(2*sizeof(int)));
							//delete[] temp; //check over here
							break;
						}
						else
						{
							offset+=sizeof(RID);
						}
					}
					break;
				}
			}
			offset+=sizeof(int)+(recEntries*sizeof(RID));
		}
	}
	if(found == 1)
	{
		return 0;
	}
	else
	{
		return -1;
	}
}

int crrPageNo=0;
void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
	int flag;
	unsigned char pagedt[PAGE_SIZE];
	ixfileHandle.fileHandle.readPage(::crrPageNo,pagedt);
	flag=getNonLeafFlag(pagedt);
	int noofEntries;
	if(flag==NON_LEAF)
	{

		getnoofEntriesNonLeaf(pagedt,noofEntries);
		if(noofEntries==0){
			::crrPageNo=1;
			ixfileHandle.fileHandle.readPage(::crrPageNo,pagedt);
			flag=getNonLeafFlag(pagedt);
		}
	}
	if(flag==NON_LEAF)
	{

		cout<<"{ \"keys\":[";
		int offset=0;
		for(int i=0;i<noofEntries;i++)
		{
			int k;
			float fk;
			int vkeysize;
			char *vkey;
			switch(attribute.type)
			{
			case TypeInt:

				memcpy(&k,pagedt+offset+sizeof(int),sizeof(int));
				cout<<"\""<<k<<"\"";
				offset=offset+sizeof(int)*2;
				break;
			case TypeReal:

				memcpy(&fk,pagedt+offset+sizeof(int),sizeof(float));
				cout<<"\""<<fk<<"\"";
				offset=offset+sizeof(int)+sizeof(float);
				break;

			case TypeVarChar:

				memcpy(&vkeysize,pagedt+offset+sizeof(int),sizeof(int));
				vkey=new char[vkeysize+1];
				memset(vkey,'\0',vkeysize+1);
				memcpy(vkey,pagedt+sizeof(int)*2,vkeysize);
				offset=offset+sizeof(int)*2+vkeysize;
				cout<<"\""<<vkey<<"\""<<endl;

				break;

			}
			if(i!=(noofEntries-1))
			{
				cout<<",";
			}
		}
		cout<<"],"<<endl;
		cout<<"\"children\":[";
		offset=0;
		for(int i=0;i<noofEntries;i++)
		{
			int k,temp,keysize;

			switch(attribute.type)
			{
			case TypeInt:

				memcpy(&k,pagedt+offset+sizeof(int),sizeof(int));
				temp=::crrPageNo;
				::crrPageNo=k;
				printBtree(ixfileHandle,attribute);
				::crrPageNo=temp;
				offset=offset+sizeof(int)*2;
				break;
			case TypeReal:

				memcpy(&k,pagedt+offset+sizeof(float),sizeof(int));
				temp=::crrPageNo;
				::crrPageNo=k;
				printBtree(ixfileHandle,attribute);
				::crrPageNo=temp;
				offset=offset+sizeof(int)+sizeof(float);
				break;

			case TypeVarChar:

				memcpy(&keysize,pagedt+offset+sizeof(int),sizeof(int));

				memcpy(&k,pagedt+offset+sizeof(int)*2+keysize,sizeof(int));
				temp=::crrPageNo;
				::crrPageNo=k;
				printBtree(ixfileHandle,attribute);
				::crrPageNo=temp;
				offset=offset+sizeof(int)+sizeof(int)+keysize;


				break;

			}
			if(i!=(noofEntries-1))
			{
				cout<<",";
			}
		}
		cout<<"]}"<<endl;

	}

	else
	{

		int noofEntries;
		int offset=0;
		getnoofEntries(pagedt,noofEntries);

		for(int i=0;i<noofEntries;i++){
			cout<<"{\"keys\":";
			cout<<"[";
			int noofrids,offset2=0;
			memcpy(&noofrids,pagedt+offset,sizeof(int));
			switch(attribute.type)
			{
			case TypeInt:
				int k;
				memcpy(&k,pagedt+offset+sizeof(int),sizeof(int));
				cout<<"\""<<k<<"\":[";
				offset2=sizeof(int)*2;
				break;
			case TypeReal:
				float kf;
				memcpy(&kf,pagedt+offset+sizeof(int),sizeof(float));
				cout<<"\""<<kf<<":[";
				offset2=sizeof(int)+sizeof(float);
				break;
			case TypeVarChar:
				int keysize;
				memcpy(&keysize,pagedt+offset+sizeof(int),sizeof(int));
				char *key=new char[keysize+1];
				memset(key,'\0',keysize+1);
				memcpy(key,pagedt+offset+sizeof(int)*2,keysize);
				cout<<"\""<<key<<":[";
				offset2=sizeof(int)*2+keysize;

				break;
			}

			for(int j=0;j<noofrids;j++)
			{
				cout<<"(";
				RID temprid;
				memcpy(&temprid,pagedt+offset+offset2,sizeof(RID));
				cout<<temprid.pageNum<<","<<temprid.slotNum;
				cout<<")";
				if(j!=(noofrids-1))
				{
					cout<<",";
				}
				offset2+=sizeof(RID);

			}

			switch(attribute.type)
			{
			case TypeInt:
				offset=offset+sizeof(int)*2+noofrids*sizeof(RID);
				break;
			case TypeReal:
				offset=offset+sizeof(int)+sizeof(float)+noofrids*sizeof(RID);
				break;
			case TypeVarChar:
				int keysize;
				memcpy(&keysize,pagedt+offset+sizeof(int),sizeof(int));
				offset=offset+keysize+sizeof(int)*2+noofrids*sizeof(RID);
				break;
			}
			cout<<"]\"]}"<<endl;
			if(i!=(noofEntries-1))
					{
						cout<<",";
					}
		}
		//cout<<"}";



	}


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
	memcpy(Page+PAGE_SIZE-(2*sizeof(int)),&noOfEntries,sizeof(int));	//storing the number of entries slot
	memcpy(Page+PAGE_SIZE-(3*sizeof(int)),&leaf,sizeof(int));	//flag to identify leaf
	memcpy(Page+PAGE_SIZE-(4*sizeof(int)),&rightPtr,sizeof(int));
	memcpy(Page+PAGE_SIZE-(5*sizeof(int)),&leftPtr,sizeof(int));
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
			delete[] newC;
			delete[] oldC;
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
				delete[] tempData;
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
				delete[] tempData;
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
			offset+=(2*sizeof(int))+newLength+sizeof(RID);
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
				delete[] tempData;
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
				delete[] tempData;
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

RC IndexManager::getnoofEntries(unsigned char *Page,int &entries) const
{
	memcpy(&entries,Page+PAGE_SIZE-(2*sizeof(int)),sizeof(int));
	return 0;
}

RC IndexManager::setnoofEntries(unsigned char *Page,int entries)
{
	memcpy(Page+PAGE_SIZE-(2*sizeof(int)),&entries,sizeof(int));
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
	offset+=sizeof(RID);
	setFreeSpacePtr(leafPage,offset);
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
	int noOfPages = ixfileHandle.fileHandle.getNumberOfPages();
	memcpy(newPage+PAGE_SIZE-(5*sizeof(int)),&oldPageNo,sizeof(int));
	memcpy(oldPage+PAGE_SIZE-(4*sizeof(int)),&noOfPages,sizeof(int));
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
		offset+=length+sizeof(int);
		setFreeSpacePtr(rootPage,offset);	//Setting the new freeSpaceptr
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
				delete[] newC;
				delete[] oldC;
				if(newString.compare(oldString) < 0)
				{
					int tempSize =  freeSpacePtr-offset;
					unsigned char *tempData;
					if(tempSize>0)
					{
						tempData = new unsigned char[tempSize];
						memcpy(tempData,rootPage+offset,tempSize);
					}
					memcpy(rootPage+offset,firstEntry,length);
					offset+=length;
					memcpy(rootPage+offset,&rightPageNo,sizeof(int));
					offset+=sizeof(int);
					if(tempSize > 0)
					{
						memcpy(rootPage+offset,tempData,tempSize);
						delete[] tempData;
					}
					setnoofEntriesNonLeaf(rootPage,noOfEntries+1);
					offset+=tempSize;
					setFreeSpacePtr(rootPage,offset);	//Setting the new freeSpaceptr
					break;
				}
				else if(newString.compare(oldString) > 0)
				{
					int tempLength;
					memcpy(&tempLength,rootPage+offset,sizeof(int)); //copying the number of recEntries
					offset+=(2*sizeof(int))+tempLength;
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
					delete[] tempData;
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

RC IndexManager::getnoofEntriesNonLeaf(unsigned char *Page,int &entries) const
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
			delete[] newC;
			delete[] oldC;
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

RC IndexManager::getNonLeafFlag(unsigned char *Page) const
{
	int flag;
	memcpy(&flag,Page+PAGE_SIZE-(3*sizeof(int)),sizeof(int));
	return flag;
}
short IX_ScanIterator::AttributeSize(Attribute attr,const void* value)
{

			switch(attr.type)
					{
					case TypeInt:
						return 4;
						break;

					case TypeReal:
						return 4;
						break;
					case TypeVarChar:

							int varcharSize;
							memcpy(&varcharSize,(char*)value,4);
							return (varcharSize+sizeof(int));
							break;

					}


	return -1;

}
RC IndexManager::scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator)
{
	int success=ix_ScanIterator.loadscandata(ixfileHandle.fileHandle,
			attribute,
			lowKey,
			highKey,
			lowKeyInclusive,
			highKeyInclusive);
	return success;
}
RC IX_ScanIterator::loadscandata(FileHandle &fileH,Attribute attr,const void* lowk,const void* highk,bool lowkeyincl,bool highkeyincl)
{
	fileHandle= &fileH;
	if(!fileHandle->file.is_open())
		return -1;
	if((attr.type!=TypeInt)&&(attr.type!=TypeReal)&&(attr.type!=TypeVarChar))
		return -1;
	attribute=attr;
	if(lowk!=NULL){
	short attrslk=AttributeSize(attribute,lowk);
	lowkey=new unsigned char[attrslk];
	memcpy(lowkey,lowk,attrslk);
	}
	else
	{
		lowkey=NULL;
	}
	if(highk!=NULL)
	{

	short attrshk=AttributeSize(attribute,highk);
	highkey=new unsigned char[attrshk];


	memcpy(highkey,highk,attrshk);
	}
	else
	{
		highkey=NULL;
	}
	lowKeyInclusive=lowkeyincl;
	highKeyInclusive=highkeyincl;
	currentPageNo=ROOT_PAGE;
	currentOffset=0;
	currentEntry=1;
	fileHandle->readPage(currentPageNo,pagedata);
	newentry=true;
	noofrids=0;
	currentrid=1;
	IndexManager::instance()->getnoofEntriesNonLeaf(pagedata,noOfEntries);
	if(noOfEntries==0){
		currentPageNo=1;
		fileHandle->readPage(currentPageNo,pagedata);
		IndexManager::instance()->getnoofEntries(pagedata,noOfEntries);

	}


	return 0;


}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if(currentPageNo==-1)
		return IX_EOF;

	int flag=IndexManager::instance()->getNonLeafFlag(pagedata);

	if(flag!=NON_LEAF)
	{
		int right;
			memcpy(&right,pagedata+PAGE_SIZE-sizeof(int)*4,sizeof(int));
	if((currentEntry>noOfEntries)&&(right==-1))
	{
		return IX_EOF;
	}
	/*else
	{
		if((currentEntry>noOfEntries))
			{
				return IX_EOF;
			}
	}*/
	}

	if(flag==NON_LEAF)
	{
		int lowpointer;
		if(currentEntry==(noOfEntries+1))
			{
				memcpy(&currentPageNo,pagedata+currentOffset,sizeof(int));
				currentOffset=0;
				currentEntry=1;
				fileHandle->readPage(currentPageNo,pagedata);
				IndexManager::instance()->getnoofEntriesNonLeaf(pagedata,noOfEntries);
				return getNextEntry(rid,key);

			}
		bool nullCheck=false;
		switch(attribute.type)
		{
			case TypeInt:
				int k;

				int lowk;

				memcpy(&lowpointer,pagedata+currentOffset,sizeof(int));
				memcpy(&k,pagedata+currentOffset+sizeof(int),sizeof(int));
				if(lowkey!=NULL){
				memcpy(&lowk,lowkey,sizeof(int));
				}
				else
				{
					nullCheck=true;
				}


				if((lowk<k)|| nullCheck)
				{

					currentPageNo=lowpointer;
					currentOffset=0;
					fileHandle->readPage(currentPageNo,pagedata);
					int f=IndexManager::instance()->getNonLeafFlag(pagedata);
					if(f==NON_LEAF)
						IndexManager::instance()->getnoofEntriesNonLeaf(pagedata,noOfEntries);
					else
					{
						IndexManager::instance()->getnoofEntries(pagedata,noOfEntries);
						newentry=true;
					}
				}
				else
				{
					currentOffset=currentOffset+sizeof(int)*2;
					currentEntry=currentEntry+1;


				}
				break;
			case TypeReal:
				float kf;

				float flowk;

				memcpy(&lowpointer,pagedata+currentOffset,sizeof(int));
				memcpy(&kf,pagedata+currentOffset+sizeof(int),sizeof(float));
				if(lowkey!=NULL)
				{
				memcpy(&flowk,lowkey,sizeof(float));
				}
				else
				{
					nullCheck=true;

				}


				if((flowk<kf)||nullCheck)
				{
					currentPageNo=lowpointer;
					currentOffset=0;
					fileHandle->readPage(currentPageNo,pagedata);
					int f=IndexManager::instance()->getNonLeafFlag(pagedata);
					if(f==NON_LEAF)
						IndexManager::instance()->getnoofEntriesNonLeaf(pagedata,noOfEntries);
					else
					{
						IndexManager::instance()->getnoofEntries(pagedata,noOfEntries);
					newentry=true;
					}
					}
				else
				{
					currentOffset=currentOffset+sizeof(int)+sizeof(float);
					currentEntry=currentEntry+1;


				}

				break;
			case TypeVarChar:
				int keysize;
				char* lowkeyvarchar;
				char* keyvarchar;
				int compare;
				if(lowkey!=NULL){
				//int keysize;
				memcpy(&keysize,lowkey,sizeof(int));

				lowkeyvarchar=new char[keysize+1];
				memset(lowkeyvarchar,'\0',keysize+1);
				memcpy(lowkeyvarchar,lowkey+sizeof(int),keysize);
				string varlowk(lowkeyvarchar);
				memcpy(&lowpointer,pagedata+currentOffset,sizeof(int));
				memcpy(&keysize,pagedata+currentOffset+sizeof(int),sizeof(int));
				keyvarchar=new char[keysize+1];
				memset(keyvarchar,'\0',keysize+1);
				memcpy(keyvarchar,pagedata+currentOffset+sizeof(int)*2,keysize);
				string vark(keyvarchar);
				compare=varlowk.compare(vark);
				}
				else{
					nullCheck=true;
				}
				if((compare<0)||nullCheck)
				{

					currentPageNo=lowpointer;
					currentOffset=0;
					fileHandle->readPage(currentPageNo,pagedata);
					int f=IndexManager::instance()->getNonLeafFlag(pagedata);
					if(f==NON_LEAF)
						IndexManager::instance()->getnoofEntriesNonLeaf(pagedata,noOfEntries);
					else
					{
						IndexManager::instance()->getnoofEntries(pagedata,noOfEntries);
						newentry=true;
					}

				}
				else
				{
					currentOffset=currentOffset+sizeof(int)*2+keysize;
					currentEntry=currentEntry+1;
				}

				delete lowkeyvarchar;
				break;


		}
		return getNextEntry(rid,key);
	}
	else
	{
		if(currentEntry>noOfEntries)
		{
			currentEntry=1;
			currentOffset=0;
			int right;
			memcpy(&right,pagedata+PAGE_SIZE-sizeof(int)*4,sizeof(int));
			if(right==-1)
				return IX_EOF;
			currentPageNo=right;
			cout<<"currentPage"<<currentPageNo<<endl;
			fileHandle->readPage(currentPageNo,pagedata);
			IndexManager::instance()->getnoofEntries(pagedata,noOfEntries);
			currentrid=1;
			newentry=true;
		}
		bool nullCheck=false;
		switch(attribute.type)
		{
			case TypeInt:
				//int noOfrids;


				if(newentry)
				{
					memcpy(&noofrids,pagedata+currentOffset,sizeof(int));
					 int keycmp;
					 memcpy(&keycmp,pagedata+currentOffset+sizeof(int),sizeof(int));
					 if(keycmp==300)
					 {
						 cout<<"300";
					 }
					 int highkeycmp;
					 if(highkey!=NULL){

					 memcpy(&highkeycmp,highkey,sizeof(int));
					 if((highkeycmp<=keycmp))
									 {
										 if(!highKeyInclusive)
										 {
											 return IX_EOF;
										 }
										 else
										 {
											 if(highkeycmp>keycmp)
												 return IX_EOF;
										 }
									 }
					 }
					 else
					 {
						 nullCheck=true;
					 }

					 if(lowkey!=NULL){
						 int lowkeycmp;
						memcpy(&lowkeycmp,lowkey,sizeof(int));
						if(lowkeycmp>keycmp){
							 currentOffset=currentOffset+sizeof(int)*2+noofrids*sizeof(RID);
							 currentrid=1;
							 newentry=true;
							 currentEntry=currentEntry+1;
							 return getNextEntry(rid,key);
							}

					if(!lowKeyInclusive)
					{

					// int keycmp;
					// memcpy(&keycmp,pagedata+currentOffset+sizeof(int),sizeof(int));
					 if(lowkeycmp==keycmp)
					 {
						 currentOffset=currentOffset+sizeof(int)*2+noofrids*sizeof(RID);
						 currentEntry=currentEntry+1;
					 }
					 currentrid=1;
					 newentry=true;
					  return getNextEntry(rid,key);
					}
					 }



					currentrid=1;
					memcpy(&rid,pagedata+currentOffset+(currentrid-1)*sizeof(rid)+sizeof(int)*2,sizeof(rid));
				//	key=new unsigned char[sizeof(int)];
					memcpy(key,pagedata+currentOffset+sizeof(int),sizeof(int));
					newentry=false;

					if(currentrid==noofrids)
					{
						newentry=true;
						currentOffset=currentOffset+sizeof(int)*2+sizeof(rid)*noofrids;
						currentEntry++;

					}
					currentrid++;
					return 0;

				}
				else
				{
					if(currentrid<=noofrids)
					{
					//key=new unsigned char[sizeof(int)];
						memcpy(&rid,pagedata+currentOffset+(currentrid-1)*sizeof(rid)+sizeof(int)*2,sizeof(rid));
						memcpy(key,pagedata+currentOffset+sizeof(int),sizeof(int));
						if(currentrid==noofrids)
						{
							newentry=true;
							currentOffset=currentOffset+sizeof(int)*2+sizeof(rid)*noofrids;
							currentEntry++;
						}

						else
							currentrid++;
						return 0;
					}



				}



				break;
			case TypeReal:

				if(newentry)
				{
					memcpy(&noofrids,pagedata+currentOffset,sizeof(int));
					 float keycmp;
					 memcpy(&keycmp,pagedata+currentOffset+sizeof(int),sizeof(float));
					 float highkeycmp;
					 if(highkey!=NULL){
					 memcpy(&highkeycmp,highkey,sizeof(float));
					 if((highkeycmp<=keycmp))
										 {
											 if(!highKeyInclusive)
											 {
												 return IX_EOF;
											 }
											 else
											 {
												 if(highkeycmp>keycmp)
													 return IX_EOF;
											 }
										 }
					 }
					 else
					 {
						 nullCheck=true;
					 }

					 if(lowkey!=NULL){
						 float lowkeycmp;
						 memcpy(&lowkeycmp,lowkey,sizeof(float));
					if(!lowKeyInclusive)
					{
					 if(lowkeycmp>keycmp)
					 {
						 currentOffset=currentOffset+sizeof(int)*2+noofrids*sizeof(rid);
						 currentrid=1;
						 newentry=true;
						 currentEntry=currentEntry+1;
						 return getNextEntry(rid,key);
					 }
					// float keycmp;
					// memcpy(&keycmp,pagedata+currentOffset+sizeof(int),sizeof(float));
					 if(lowkeycmp==keycmp)
					 {
						 currentOffset=currentOffset+sizeof(int)*2+noofrids*sizeof(rid);
						 currentEntry=currentEntry+1;
					 }
					 currentrid=1;
					 newentry=true;
					  return getNextEntry(rid,key);
					}
					 }


				//	memcpy(&noofrids,pagedata+currentOffset,sizeof(int));
					currentrid=1;
					memcpy(&rid,pagedata+currentOffset+(currentrid-1)*sizeof(rid)+sizeof(int)+sizeof(float),sizeof(rid));
					//key=new unsigned char[sizeof(float)];
					memcpy(key,pagedata+currentOffset+sizeof(int),sizeof(float));
					newentry=false;

					if(currentrid==noofrids)
					{
						newentry=true;
						currentOffset=currentOffset+sizeof(int)*2+sizeof(rid)*noofrids;
						currentEntry++;
					}
					currentrid++;
					return 0;

				}
				else
				{
					if(currentrid<=noofrids)
					{
						//key=new unsigned char[sizeof(float)];
						memcpy(&rid,pagedata+currentOffset+(currentrid-1)*sizeof(rid)+sizeof(int)+sizeof(float),sizeof(rid));
						memcpy(key,pagedata+currentOffset+sizeof(int),sizeof(float));
						if(currentrid==noofrids)
						{
							newentry=true;
							currentOffset=currentOffset+sizeof(int)*2+sizeof(rid)*noofrids;
							currentEntry++;

						}

						else
							currentrid++;
						return 0;
					}



				}


				break;
			case TypeVarChar:
				int keysize;
				memcpy(&keysize,pagedata+currentOffset+sizeof(int),sizeof(int));
				if(newentry)
				{

					memcpy(&noofrids,pagedata+currentOffset,sizeof(int));
					 char* keycmp=new char[keysize+1];
					 memset(keycmp,'\0',keysize+1);
					 memcpy(keycmp,pagedata+currentOffset+sizeof(int)*2,keysize);
					 string skeycmp(keycmp);
					 delete[] keycmp;

					 int highkeysize;
					 char* highkeycmp;
					 int compare;
					 if(highkey!=NULL){
					 memcpy(&highkeysize,highkey,sizeof(int));
					 highkeycmp=new char[highkeysize+1];
					 memset(highkeycmp,'\0',highkeysize+1);
					 memcpy(highkeycmp,highkey+sizeof(int),highkeysize);
					 string shighkeycmp(highkeycmp);
					compare=shighkeycmp.compare(skeycmp);
					 delete[] highkeycmp;
					 }
					 else{
						 nullCheck=true;
					 }

					 if((compare<=0)&&(!nullCheck))
					 {
						 if(!highKeyInclusive)
						 {
							 return IX_EOF;
						 }
						 else
						 {
							 if(compare==0)
								 return IX_EOF;
						 }
					 }
					 if(lowkey!=NULL){
						 int lowkeysize;
						 memcpy(&lowkeysize,lowkey,sizeof(int));
						 char* lowkeycmp=new char[lowkeysize+1];
						 memset(lowkeycmp,'\0',lowkeysize+1);
						 memcpy(lowkeycmp,lowkey+sizeof(int),lowkeysize);
						 string slowkeycmp(lowkeycmp);
						 if(slowkeycmp.compare(skeycmp)>0)
						 {
							 currentOffset=currentOffset+sizeof(int)*2+keysize+noofrids*sizeof(rid);
							 currentrid=1;
							 newentry=true;
							 currentEntry=currentEntry+1;
							 return getNextEntry(rid,key);
						 }
					if(!lowKeyInclusive)
					{

					// float keycmp;
					// memcpy(&keycmp,pagedata+currentOffset+sizeof(int),sizeof(float));
					 if(slowkeycmp.compare(skeycmp)==0)
					 {
						 currentOffset=currentOffset+sizeof(int)*2+keysize+noofrids*sizeof(rid);
						 currentEntry=currentEntry+1;
					 }
					 currentrid=1;
					 newentry=true;
					  return getNextEntry(rid,key);
					}
					 }


					memcpy(&noofrids,pagedata+currentOffset,sizeof(int));
					currentrid=1;
					memcpy(&rid,pagedata+currentOffset+(currentrid-1)*sizeof(rid)+sizeof(int)+keysize+sizeof(int),sizeof(rid));
					//key=new unsigned char[keysize+sizeof(int)];
					memcpy(key,pagedata+currentOffset+sizeof(int),keysize+sizeof(int));
					newentry=false;

					if(currentrid==noofrids)
					{
						newentry=true;
						currentOffset=currentOffset+sizeof(int)*2+sizeof(rid)*noofrids;
						currentEntry++;
					}
					currentrid++;
					return 0;

				}
				else
				{
					if(currentrid<=noofrids)
					{
						//key=new unsigned char[keysize+sizeof(int)];
						memcpy(&rid,pagedata+currentOffset+(currentrid-1)*sizeof(rid)+sizeof(int)+sizeof(int)+keysize,sizeof(rid));
						memcpy(key,pagedata+currentOffset+sizeof(int),keysize+sizeof(int));
						if(currentrid==noofrids)
						{
							newentry=true;
							currentOffset=currentOffset+sizeof(int)*2+keysize+sizeof(rid)*noofrids;
							currentEntry++;

						}

						else
							currentrid++;
						return 0;
					}



				}

				break;
		}
	}






    return 0;
}

RC IX_ScanIterator::close()
{
	delete[] lowkey;
	delete[] highkey;
    return 0;
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

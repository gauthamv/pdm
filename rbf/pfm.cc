#include "pfm.h"
#include<iostream>
#include<fstream>
#include<cstring>
using namespace std;

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
	int createsuccess=-1;
	if(!ifstream(fileName.c_str()))
	{
		ofstream create(fileName.c_str());
		if(create.is_open())
			createsuccess=0;
		create.close();
	}
    return createsuccess;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	return remove(fileName.c_str());
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	int opensuccess=-1;

	if(ifstream(fileName.c_str()))
	{
		if(fileHandle.filehandlefree())
		{

			fileHandle.file.open(fileName.c_str(),ios::out |ios::in|ios::binary);
			opensuccess=0;
		}

	}


    return opensuccess;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	try
	{
		fileHandle.file.close();
		return 0;
	}
	catch(int e)
	{
		return -1;
	}
}


FileHandle::FileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
}

bool FileHandle::filehandlefree()
{
	return !file.is_open();
}
RC FileHandle::readHeaderPage(PageNum HeaderNum,void *data)
{
	unsigned pdBlockSize=PAGE_SIZE/sizeof(short);
	int pageoffset=PAGE_SIZE*(HeaderNum)*(pdBlockSize+1);

	file.seekg (0, ios::end);
	int length = file.tellg();
	file.seekg (0, ios::beg);

	if(length<=0)
		return -1;
	//cout<<"headeroffsetlength"<<length<<endl;
	if(pageoffset<=(length))
		{
			file.seekg(pageoffset,ios::beg);
			file.read((char*)data,PAGE_SIZE);


		}
		else
			return -1;

		return 0;

}
RC FileHandle::updateHeaderPage(PageNum HeaderNum,void *data)
{
	unsigned pdBlockSize=PAGE_SIZE/sizeof(short);
	int pageoffset=PAGE_SIZE*(HeaderNum)*(pdBlockSize+1);
	file.seekg (0, ios::end);
	int length = file.tellg();
	file.seekg (0, ios::beg);
	if(pageoffset<=(length))
		{
			file.seekg(pageoffset,ios::beg);
			file.write((char*)data,PAGE_SIZE);
			return 0;
		}
		else
			return -1;



}
RC FileHandle::readPage(PageNum pageNum, void *data)
{
	PageNum effectivePageNum=transformPageNum(pageNum);
	//cout<<"effective"<<effectivePageNum<<endl;
	unsigned char *pagedata=new unsigned char[PAGE_SIZE];

	file.seekg (0, ios::end);
	int length = file.tellg();
	file.seekg (0, ios::beg);

	int pageoffset=PAGE_SIZE*effectivePageNum;
	if(pageoffset<=(length))
	{
		file.seekg(pageoffset,ios::beg);

		file.read((char*)pagedata,PAGE_SIZE);


	}
	else
		return -1;
	memcpy((char*)data,pagedata,PAGE_SIZE);


	delete pagedata;

	readPageCounter = readPageCounter + 1;
	return 0;
}
unsigned FileHandle::transformPageNum(PageNum pageNum)
{
	unsigned pdBlockSize=PAGE_SIZE/sizeof(short);
	unsigned tNum=pageNum/pdBlockSize*(1+pdBlockSize)+pageNum%pdBlockSize+1;
	//cout<<"transform"<<tNum;
	return tNum;
}
unsigned FileHandle::originalPageNum(PageNum pageNum)
{
	unsigned pdBlockSize=PAGE_SIZE/sizeof(short)+1;
	return (pageNum-(pageNum/pdBlockSize + 1));
}
RC FileHandle::writePage(PageNum pageNum, const void *data)
{
		PageNum effectivePageNum=transformPageNum(pageNum);
		unsigned char pagedata[PAGE_SIZE],*pagecopy;
		file.seekg (0, ios::end);
		int length = file.tellg();
		file.seekg (0, ios::beg);

		int pageoffset=PAGE_SIZE*effectivePageNum;

		if(pageoffset<=(length))
		{
			file.seekg(pageoffset,ios::beg);
			pagecopy=(unsigned char*)data;
			memcpy(pagedata,pagecopy,PAGE_SIZE);
			file.write((char*)pagedata,PAGE_SIZE);
		}
		else
			return -1;


		writePageCounter=writePageCounter+1;
		return 0;
}


RC FileHandle::appendPage(const void *data)
{
	unsigned pdBlockSize=PAGE_SIZE/sizeof(short);
	short*pagedirectory;

	file.seekg (0, ios::end);
	int length = file.tellg();
	file.seekg (0, ios::beg);
	unsigned pages= (length+1)/PAGE_SIZE;
	pagedirectory=new short[pdBlockSize];
	short temp=PAGE_SIZE;

	if(pages%(pdBlockSize+1)==0)
	{

		for(unsigned int i=0;i<pdBlockSize;i++)
				pagedirectory[i]=-1;

		//memcpy(pagedirectory,&temp,sizeof(short));
		pagedirectory[0]=temp;
		temp=-1;
		for(int i=1;i<pdBlockSize;i++)
		{
		//	memcpy(pagedirectory+sizeof(short)*i,&temp,sizeof(short));
			pagedirectory[i]=temp;
		}
		file.write((char*)pagedirectory,PAGE_SIZE);

	//	cout<<"pdBlockSize"<<pdBlockSize<<endl;

	}
	else
	{
		readHeaderPage(pages/(pdBlockSize+1),pagedirectory);

		unsigned pageNumber=getNumberOfPages();
		//memcpy(pagedirectory+sizeof(short)*((pageNumber+1)%pdBlockSize),&temp,sizeof(short));//updating the page directory entry for the new page.
		pagedirectory[(pageNumber+1)%pdBlockSize]=temp;
		file.write((char*)pagedirectory,PAGE_SIZE);

	}
	delete pagedirectory;
	file.seekg (0, ios::end);
	//cout<<"len"<<file.tellg()<<endl;



	file.write((char*)data,PAGE_SIZE);

	file.seekg (0, ios::end);

	appendPageCounter=appendPageCounter+1;
	return 0;
}


unsigned FileHandle::getNumberOfPages()
{

	file.seekg (0, ios::end);
	int length = file.tellg();
	file.seekg (0, ios::beg);
	unsigned pages= (length+1)/PAGE_SIZE;
	//cout<<"page"<<pages<<endl;
	if(pages==0)
		return 0;
	else
	return (originalPageNum(pages-1)+1);

}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount=readPageCounter;
	writePageCount=writePageCounter;
	appendPageCount=appendPageCounter;
	return 0;
}

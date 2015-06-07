
#include "rbfm.h"
#include "pfm.h"
#include<cstring>
#include<map>
#include<cmath>
using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance()->openFile(fileName,fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator){

	int success;
	success = rbfm_ScanIterator.loadscandata(fileHandle,
			recordDescriptor,
			conditionAttribute,
			compOp,
			value,
			attributeNames);



	return success;

}
RC RBFM_ScanIterator::loadscandata(FileHandle &f,
		const vector<Attribute> &recordDesc,
		const string &conditionAttr,
		const CompOp CompareOp,
		const void* compvalue,
		const vector<string> &attrNm)
{
	recordDescriptor=recordDesc;
	fileHandle=&f;
	TotalPages=f.getNumberOfPages();
	conditionAttribute=conditionAttr;
	compOp=CompareOp;
	attrSize=AttributeSize(conditionAttribute,compvalue);
	//cout<<"size1"<<attrNm.size()<<endl;
		attributeNames=attrNm;
		//for(int i=0;i<attributeNames.size();i++)
			//cout<<attributeNames[i]<<endl;
		//cout<<"size"<<attributeNames.size()<<endl;
		currentPageNo=0;
		currentrid.pageNum=0;
		currentrid.slotNum=0;
		fileHandle->readPage(currentPageNo,pagedata);
		RecordBasedFileManager::instance()->gettotalslots(pagedata,&currentPageTotalSlots);
	if(attrSize==-1)
	{
		value=new unsigned char[1];
	}
	else
	{
	value=new unsigned char[attrSize];
	memcpy(value,compvalue,attrSize);
	}
	return 0;
	}
RBFM_ScanIterator::~RBFM_ScanIterator()
{
	delete value;
}
short RBFM_ScanIterator::AttrNumber(string attr)
{
	for(short i=0;i<recordDescriptor.size();i++)
		{
	//	cout<<"condition"<<attr<<endl;
	//	cout<<"recdesc"<<recordDescriptor[i].name<<endl;
			if(recordDescriptor[i].name.compare(attr)==0)
			{
				return i;
			}

		}
	return -1;
}

short RBFM_ScanIterator::AttributeSize(string conditionAttr,const void* value)
{
	for(unsigned i=0;i<recordDescriptor.size();i++)
	{
		//cout<<recordDescriptor[i].name<<endl<<conditionAttr<<endl;
		if(recordDescriptor[i].name.compare(conditionAttr)==0)
		{
			switch(recordDescriptor[i].type)
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

		}
	}
	return -1;

}
RC RBFM_ScanIterator::LoadNewPage()
{
	int success=0;;
	if(currentPageNo>=(TotalPages-1))
		return -5;
	if(currentrid.slotNum>=currentPageTotalSlots)
	{
		currentPageNo=currentPageNo+1;
		success=fileHandle->readPage(currentPageNo,pagedata);
		RecordBasedFileManager::instance()->gettotalslots(pagedata,&currentPageTotalSlots);
		currentrid.pageNum=currentPageNo;
		currentrid.slotNum=0;
		return success;
	}
	else
		return -1;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)

 {
	if (currentPageNo>=TotalPages)
		return RBFM_EOF;
	unsigned char* tempdata;
	slot slotiter;
	short noOfFields=recordDescriptor.size();
	short *offsets=new short[noOfFields];
	short nullValSize=ceil(noOfFields/8.0);
	unsigned char* nullval=new unsigned char[nullValSize];
	//cout<<"k:"<<attributeNames.size()<<endl;
	RecordBasedFileManager::instance()->getslotentry(pagedata,&slotiter,currentrid.slotNum);
	while(true)
	{
		currentrid.slotNum++;

		if(currentrid.slotNum>currentPageTotalSlots)
		{
			if(fileHandle->file.eof())
			{
				fileHandle->file.clear();
				return RBFM_EOF;
			}

			if(LoadNewPage()==-5)
				return RBFM_EOF;

			continue;
		}
		RecordBasedFileManager::instance()->getslotentry(pagedata,&slotiter,currentrid.slotNum);
		if((slotiter.offset==FREE_REC)||(slotiter.offset>=RELOC))
		{
			continue;
		}

		unsigned char* record=pagedata+slotiter.offset;
		short datalength=slotiter.length;
		tempdata=new unsigned char[datalength];
		memcpy(tempdata,record,datalength);
		memcpy(offsets,tempdata,noOfFields*sizeof(short));
		//for(int j=0;j<noOfFields;j++)
			//cout<<"offsets"<<offsets[j]<<endl;
		memcpy(nullval,tempdata+noOfFields*sizeof(short),nullValSize);
		short recordlength;
		tempdata=tempdata+noOfFields*sizeof(short);
		map<short,bool> nullcheck;
		unsigned char mask=0x80;
		int j=-1;
		for(int i=0;i<noOfFields;i++)
		{
			if(i%8==0)
			{
				mask=0x80;
				j++;
			}
			if(nullval[j] & mask)
			{
				nullcheck[i]=true;
			}
			else
			{
				nullcheck[i]=false;
			}
			mask=mask>>1;

		}
		short conditionAttrNo=AttrNumber(conditionAttribute);
		if(nullcheck[conditionAttrNo])
		{
			if(compOp!=NO_OP)
				currentrid.slotNum++;
				continue;
		}
		switch(recordDescriptor[conditionAttrNo].type)
		{
			case TypeInt:
				int idataValue;
				memcpy(&idataValue,tempdata+offsets[conditionAttrNo],attrSize);
				int icompdataValue;
				memcpy(&icompdataValue,value,attrSize);
				switch(compOp)
				{
				case EQ_OP:      // =
					if(!(idataValue==icompdataValue))
						continue;
					break;
				case LT_OP:  // <
					if(!(idataValue<icompdataValue))
						continue;
					break;
				case GT_OP:      // >
					if(!(idataValue>icompdataValue))
						continue;
					break;
				case LE_OP:      // <=
					if(!(idataValue<=icompdataValue))
						continue;
					break;
				case GE_OP:      // >=
					if(!(idataValue>=icompdataValue))
						continue;
					break;
				case NE_OP:
					if(!(idataValue!=icompdataValue))
						continue;
					break;
				case NO_OP:
					break;
				}

				break;
			case TypeReal:
				float fdataValue;
				memcpy(&fdataValue,tempdata+offsets[conditionAttrNo],attrSize);
				float fcompdataValue;
				memcpy(&fcompdataValue,value,attrSize);
				switch(compOp)
				{
				case EQ_OP:      // =
					if(!(fdataValue==fcompdataValue))
						continue;
					break;
				case LT_OP:  // <
					if(!(fdataValue<fcompdataValue))
						continue;
					break;
				case GT_OP:      // >
					if(!(fdataValue>fcompdataValue))
						continue;
					break;
				case LE_OP:      // <=
					if(!(fdataValue<=fcompdataValue))
						continue;
					break;
				case GE_OP:      // >=
					if(!(fdataValue>=fcompdataValue))
						continue;
					break;
				case NE_OP:
					if(!(fdataValue!=fcompdataValue))
						continue;
					break;
				case NO_OP:
					break;
				}

			break;
			case TypeVarChar:
				char* vdataValue=new char[attrSize-sizeof(int)+1];
				int vdataSize;
				memset(vdataValue,'\0',attrSize-sizeof(int)+1);
				memcpy(&vdataSize,tempdata+offsets[conditionAttrNo],sizeof(int));
				memcpy(vdataValue,tempdata+offsets[conditionAttrNo]+sizeof(int),vdataSize);
				//vdataValue[vdataSize-sizeof(int)]='\0';
				//cout<<*(value+5)<<endl;

				char* vcomparedataValue=new char[attrSize-sizeof(int)+1];
				memset(vcomparedataValue,'\0',attrSize-sizeof(int)+1);
				memcpy(vcomparedataValue,value+sizeof(int),attrSize-sizeof(int));
				//vcomparedataValue[attrSize-sizeof(int)]='\0';

				string vstring(vdataValue);
				string vcompstring(vcomparedataValue);
				//cout<<vstring<<endl;
				//cout<<vcompstring<<endl;

				switch(compOp)
				{
				case EQ_OP:      // =
					if(!(vstring.compare(vcompstring)==0))
						continue;
					break;
				case LT_OP:  // <
					if(!(vstring.compare(vcompstring)<0))
						continue;
					break;
				case GT_OP:      // >
					if(!(vstring.compare(vcompstring)>0))
						continue;
					break;
				case LE_OP:      // <=
					if(!(vstring.compare(vcompstring)<=0))
						continue;
					break;
				case GE_OP:      // >=
					if(!(vstring.compare(vcompstring)>=0))
						continue;
					break;
				case NE_OP:
					if(!(vstring.compare(vcompstring)!=0))
						continue;
					break;
				case NO_OP:
					break;
				}
				delete[] vdataValue;
				delete[] vcomparedataValue;
				break;
		}

		short offset=0;
		//cout<<"k1:"<<attributeNames.size()<<endl;
		short nullsize=ceil(attributeNames.size()/8.0);
		unsigned char* nullset=new unsigned char[nullsize];
		memset(nullset,0,nullsize);
		unsigned char m=0x80;
		int k=-1;
		//cout<<"k2:"<<attributeNames.size()<<endl;
		for(unsigned j=0;j<attributeNames.size();j++)
		{
			if(j%8==0)
			{
				m=0x80;
				k++;
			}
			short attrNo=AttrNumber(attributeNames[j]);
			if(nullcheck[attrNo])
			{

				nullset[k]=nullset[k]|m;
			}
			m=m>>1;

		}
		memcpy((char*)data,nullset,nullsize);
		delete nullset;
		offset=nullsize;
		//cout<<attributeNames.size()<<endl;
		for(unsigned j=0;j<attributeNames.size();j++)
		{
			//cout<<attributeNames.size()<<endl;
			//cout<<"attr"<<attributeNames[j]<<endl;
			short attrN=0;
			attrN=AttrNumber(attributeNames[j]);
			short attrS=AttributeSize(attributeNames[j],tempdata+offsets[attrN]);
			//cout<<*(tempdata+offsets[attrN]+5)<<endl;
			memcpy((char*)data+offset,tempdata+offsets[attrN],attrS);
			//cout<<*((char*)data+offset+5)<<endl;
			offset=offset+attrS;

		}
		rid=currentrid;
		//cout<<rid.pageNum<<" "<<rid.slotNum<<endl;
		break;
	}
	delete nullval;
	delete offsets;
	return 0;
 }


const void* RecordBasedFileManager::recordFormat_forwardmask(const void* data,const vector<Attribute> &recordDescriptor,short &length)
{
	//change length as well
	short noOfFields=recordDescriptor.size();
	short *offsets=new short[noOfFields];
	short nullValSize=ceil(noOfFields/8.0);
	short recordSize=nullValSize;
	length=length+noOfFields*sizeof(short);
	unsigned char* record=new unsigned char[length];
	unsigned char* nullval=new unsigned char[nullValSize];
	memcpy(nullval,data,nullValSize);
	unsigned char mask=0x80;
	map<short,bool> nullcheck;
	int j=-1;
	for(int i=0;i<noOfFields;i++)
	{
		if(i%8==0)
		{
			mask=0x80;
			j++;
		}
		if(nullval[j] & mask)
		{
			nullcheck[i]=true;
			mask=mask>>1;

		}
		else
		{
			nullcheck[i]=false;
		}


	}

	offsets[0]=nullValSize;
	for(int i=0;i<noOfFields-1;i++)
	{
		switch(recordDescriptor[i].type)
		{
		case TypeInt:
			if(nullcheck[i]==false)
				offsets[i+1]=offsets[i]+4;
			else
				offsets[i+1]=offsets[i];
			break;

		case TypeReal:
			if(nullcheck[i]==false)
				offsets[i+1]=offsets[i]+4;
			else
				offsets[i+1]=offsets[i];
			break;
		case TypeVarChar:
			if(nullcheck[i]==false)
			{
				int *varcharSize=new int;
				memcpy(varcharSize,(char*)data+offsets[i],4);
				offsets[i+1]=offsets[i]+4+*varcharSize;
				delete varcharSize;
			}
			else
				offsets[i+1]=offsets[i]+4;
		}



	}
	memcpy(record,offsets,noOfFields*2);
	memcpy(record+noOfFields*2,data,length-noOfFields*2);

	delete[] nullval;
	delete[] offsets;
	return (void*)record;
}
const void* RecordBasedFileManager::recordFormat_backwardmask(const void* data,const vector<Attribute> &recordDescriptor,short &length)
{
	short noOfFields=recordDescriptor.size();
	short offset=sizeof(short)*noOfFields;

	length=length-offset;
	//unsigned char* record=new char[length];
	//memcpy(record,data+offset,length);


	return ((char*)data+offset);


	//delete record;
}

void RecordBasedFileManager::updateslotdirectory(unsigned char *pagedata,short datalength,bool newpage,unsigned &slotNo)
{
	short *freespace=new short;
	short *slots=new short;
	*slots=1;

	if(newpage)
	{
		//cout<<"slot1**"<<endl;
		slot *slotEntry=new slot;
		slotEntry->length=datalength;
		slotEntry->offset=0;
		*freespace=datalength;

		setfreespacepointer(pagedata,freespace);
		//cout<<*freespace<<endl;

		settotalslots(pagedata,slots);


		gettotalslots(pagedata,slots);

		setslotentry(pagedata,slotEntry,1);

		delete slotEntry;


	}
	else
	{
		//cout<<"slot2**"<<endl;
		slot *slotEntry=new slot;
		getfreespacepointer(pagedata,freespace);
		slotEntry->length=datalength;
		slotEntry->offset=*freespace;
		*freespace=*freespace+datalength;
		//cout<<"freema"<<*freespace<<endl;
		setfreespacepointer(pagedata,freespace);
		gettotalslots(pagedata,slots);
		//cout<<"slots"<<*slots<<endl;
		int flag=0;
		for(short i=1;i<=*slots;i++)
		{
			slot tempslot;
			getslotentry(pagedata,&tempslot,i);
			if(tempslot.offset==FREE_REC)
			{
				setslotentry(pagedata,slotEntry,i);
				flag=1;
				break;
			}
		}
		if(flag==0)
		{
			*slots=*slots+1;
			settotalslots(pagedata,slots);
			setslotentry(pagedata,slotEntry,*slots);
		}
		delete slotEntry;
	}
	slotNo=*slots;
	delete slots;
	delete freespace;
}

void RecordBasedFileManager::copyrecord(unsigned char *pagedata,const void *data,short datalength)
{
	short *freespace=new short;
	getfreespacepointer(pagedata,freespace);

	memcpy(pagedata+*freespace,(char*)data,datalength);
	delete freespace;
	/*for(int i=0;i<datalength;i++)
		delete ((char*)data+i);*/


}
void RecordBasedFileManager::getfreespacepointer(unsigned char *pagedata,short *freespace)
{
	memcpy(freespace,pagedata+PAGE_SIZE-sizeof(*freespace),sizeof(*freespace));
}
void RecordBasedFileManager::gettotalslots(unsigned char *pagedata,short *slots)
{
	memcpy(slots,pagedata+PAGE_SIZE-sizeof(*slots)*2,sizeof(*slots));
}
void RecordBasedFileManager::setfreespacepointer(unsigned char *pagedata,short *freespace)
{
	memcpy(pagedata+PAGE_SIZE-sizeof(*freespace),freespace,sizeof(*freespace));
}
void RecordBasedFileManager::settotalslots(unsigned char *pagedata,short *slots)
{
	memcpy(pagedata+PAGE_SIZE-sizeof(*slots)*2,slots,sizeof(*slots));
}
void RecordBasedFileManager::getslotentry(unsigned char *pagedata,slot *slotEntry,short slotNo)
{
	memcpy(slotEntry,pagedata+PAGE_SIZE-sizeof(short)*2-sizeof(*slotEntry)*slotNo,sizeof(*slotEntry));

}
void RecordBasedFileManager::setslotentry(unsigned char *pagedata,slot *slotEntry,short slotNo)
{
	memcpy(pagedata+PAGE_SIZE-sizeof(short)*2-sizeof(*slotEntry)*slotNo,slotEntry,sizeof(*slotEntry));
}
void RecordBasedFileManager::updatepageheader(PageNum pageNum,unsigned char* pagedata,FileHandle &fileHandle)
{
	unsigned char* headerpage=new unsigned char[PAGE_SIZE];
	unsigned pdBlockSize=PAGE_SIZE/sizeof(short);
	fileHandle.readHeaderPage(pageNum/pdBlockSize,headerpage);
	short *freespace=new short;
	short* fp=new short;
	short* slots=new short;
	getfreespacepointer(pagedata,fp);
	gettotalslots(pagedata,slots);
	*freespace=(short)(PAGE_SIZE-*fp-(sizeof(short)*4+sizeof(slot)* (*slots))-60);
	//cout<<"updatepageheader ** pageNUM"<<pageNum<<"freespace"<<*freespace<<endl;
	if(*freespace<0)
		*freespace=0;
	memcpy((headerpage+pageNum%pdBlockSize*sizeof(short)),freespace,sizeof(short));
	//cout<<"asda";
	fileHandle.updateHeaderPage(pageNum/pdBlockSize,headerpage);
	//cout<<"asdsd";

	delete fp;
	delete freespace;
	delete headerpage;
	delete[] slots;

}
short RecordBasedFileManager::getfreepageNo(FileHandle &fileHandle,short datalength)
{
	unsigned char *pagedata=new unsigned char[PAGE_SIZE];
	unsigned noofPages=fileHandle.getNumberOfPages();
	unsigned pdBlockSize=PAGE_SIZE/sizeof(short);
	int i=0,freepage=0;
	//cout<<"No of pages**getfreepageno"<<noofPages<<endl;
	if(noofPages<0)
		return -1;

	fileHandle.readHeaderPage((noofPages-1)/pdBlockSize,pagedata);

	short *freespacelast=(short*)(pagedata+(noofPages-1)%pdBlockSize*sizeof(short));
	//cout<<"freespacelast**Getfreepageno"<<*freespacelast<<"datalength"<<datalength<<endl;

	if(*freespacelast>=datalength)
	{
		delete[] pagedata;
		return noofPages-1;
	}

	while(!fileHandle.readHeaderPage(i,pagedata))
	{
		for(short* j=(short*)pagedata;j<(short*)(pagedata+PAGE_SIZE);j++)
		{
			if(*j>=datalength)
			{
				delete[] pagedata;
				return freepage;
			}
			freepage++;
			if(freepage>=noofPages)
				break;

		}
		i++;
		if(freepage>=noofPages)
		{
			break;
		}
	}

	delete[] pagedata;
	return -1;
}
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
	//cout<<"koo"<<endl;
    unsigned noofPages=fileHandle.getNumberOfPages();
    if(noofPages>=17)
    {
    	//cout<<noofPages<<endl;\

    }

    unsigned char pagedata[PAGE_SIZE];;
    short datalength=0;
    short freePageNo=0;
    bool newpage;
    int success=-1;
    datalength=recordSize(recordDescriptor,data);
    freePageNo=getfreepageNo(fileHandle,datalength);
    if(freePageNo==-1)
    {

    	newpage=true;


		rid.pageNum=noofPages;
		const void* record=recordFormat_forwardmask(data,recordDescriptor,datalength);

		copyrecord(pagedata,record,datalength);
		updateslotdirectory(pagedata,datalength,newpage,rid.slotNum);
		//cout<<"slotno"<<rid.slotNum<<endl;

		success=fileHandle.appendPage(pagedata);

		freePageNo=noofPages;
		updatepageheader(freePageNo,pagedata,fileHandle);


    }
    else
    {
    	//cout<<"aaaaaaaaaaaaa"<<freePageNo<<endl;
    	newpage=false;


    	fileHandle.readPage(freePageNo,pagedata);

    	rid.pageNum=freePageNo;

    	const void* record=recordFormat_forwardmask(data,recordDescriptor,datalength);

    	copyrecord(pagedata,record,datalength);
    	updateslotdirectory(pagedata,datalength,newpage,rid.slotNum);
    	//cout<<"updatedone"<<endl;

    	//cout<<"freepage"<<freePageNo<<endl;
    	success=fileHandle.writePage(freePageNo,pagedata);
    	updatepageheader(freePageNo,pagedata,fileHandle);
    	//cout<<success<<"dd";
    }
    return success;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	unsigned char *temp = new unsigned char[PAGE_SIZE];
	unsigned char *tempPage = new unsigned char[PAGE_SIZE];	//Page for shuffling the data
	fileHandle.readPage(rid.pageNum,temp);

	//Check if tombstone is present
	slot temp_slot;
	getslotentry(temp,&temp_slot,rid.slotNum);
	RID newRid;

	RC success = -1;
	if(temp_slot.offset > 15000)
	{
		newRid.pageNum = temp_slot.length;
		newRid.slotNum = temp_slot.offset - 15000;
		success = deleteRecord(fileHandle,recordDescriptor,newRid);
	}
	else
	{
		if(!CompactPage(tempPage,temp,rid))			//Used for compacting space in the page
		{
			if(!fileHandle.writePage(rid.pageNum,tempPage))	//Writes the modified page in place of old page
			{
				delete temp;
				delete tempPage;
				return 0;
			}
		}
	}
	delete temp;
	delete tempPage;
   	return success;
}

RC RecordBasedFileManager::CompactPage(unsigned char *destPage,unsigned char *sourcePage,const RID &rid)
{
	 short noOfSlots;	//Used for storing the number of slots in the page
	 short freeSpacePtr=0; 	//Stores offset of freespace in page
	 slot temp_slot;		//Structure for storing slots
	 getslotentry(sourcePage,&temp_slot,rid.slotNum);
	 memcpy(&noOfSlots,(char*)sourcePage+PAGE_SIZE-4,2);	//Reading the number of slots
	 settotalslots(destPage,&noOfSlots);	//Writing the number of slots to new page
	 for(unsigned short i=1;i<=noOfSlots;i++)	//Assuming slots start from 1
	 {
		 if(i == rid.slotNum)
	     {
	    	 temp_slot.offset = FREE_REC;
	    	 temp_slot.length = 0;
	    	 setslotentry(destPage,&temp_slot,i);
	     }
	     else
	     {
	    	 getslotentry(sourcePage,&temp_slot,i);
	    	 if(temp_slot.offset < 10000)
	    	 {
	    		 memcpy((char*)destPage+freeSpacePtr,(char*)sourcePage+temp_slot.offset,temp_slot.length);	//copies the info to new page from old page
	    		 temp_slot.offset = freeSpacePtr;	//Updating the new offset for new page
	    		 setslotentry(destPage,&temp_slot,i);
	    		 freeSpacePtr+=temp_slot.length;
	    	 }
	    	 else
	    	 {
	    		 setslotentry(destPage,&temp_slot,i);
	    	 }
	     }
	 }
	 setfreespacepointer(destPage,&freeSpacePtr);
	 return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{

	unsigned char *temp = new unsigned char[PAGE_SIZE];
	unsigned char *tempPage = new unsigned char[PAGE_SIZE];	//Page for shuffling the data
	fileHandle.readPage(rid.pageNum,temp);	//Reads the page

	//Check if tombstone is present
	slot temp_slot;
	getslotentry(temp,&temp_slot,rid.slotNum);
	RID newRid;

	RC success = -1;
	if(temp_slot.offset > 15000)
	{
		newRid.pageNum = temp_slot.length;
		newRid.slotNum = temp_slot.offset - 15000;
		success = updateRecord(fileHandle,recordDescriptor,data,newRid);
	}
	else
	{
	short datalength = 0;
	datalength=recordSize(recordDescriptor,data);	//returns the record length
	const void *recordPtr = recordFormat_forwardmask(data,recordDescriptor,datalength);	//returns the updated record with field offsets
	short freeTemp=0;	//Temp variable to store free ptr
	getfreespacepointer(temp,&freeTemp);
	slot temp_slot;		//Structure for storing slots
	getslotentry(temp,&temp_slot,rid.slotNum);
	short noOfSlots;
	memcpy(&noOfSlots,(char*)temp+PAGE_SIZE-4,2);	//Reading the number of slots
	short freeSpace = PAGE_SIZE-freeTemp-(sizeof(short)*2 + noOfSlots*sizeof(slot))+temp_slot.length;
	if(freeSpace > datalength)
	{
		if(!CompactPage(tempPage,temp,rid))
		{
			getfreespacepointer(tempPage,&freeTemp);
			temp_slot.length = datalength;
			temp_slot.offset = freeTemp;
			freeTemp = freeTemp + datalength;
			copyrecord(tempPage,recordPtr,datalength);
			setfreespacepointer(tempPage,&freeTemp);
			setslotentry(tempPage,&temp_slot,rid.slotNum);
			fileHandle.writePage(rid.pageNum,tempPage);	//writing the page to disk
			delete temp;
			delete tempPage;
			//cout<<"compact Page: "<<rid.pageNum<<char(32)<<"f"<<temp_slot.offset<<char(32)<<"l"<<temp_slot.length<<char(32)<<rid.slotNum<<endl;
			return 0;
		}
	}
	else
	{
		RID rid_temp;	//temp rid to store new slot and page no
		if(!insertRecord(fileHandle,recordDescriptor,data,rid_temp))
		{
			//Tombstones for the migrated record
			temp_slot.length = rid_temp.pageNum;
			temp_slot.offset = RELOC + rid_temp.slotNum;
			setslotentry(temp,&temp_slot,rid.slotNum);	//setting the slot entry in old page
			fileHandle.writePage(rid.pageNum,temp);	//writing the page to disk
			delete temp;
			delete tempPage;
			//cout<<"Insert Record: Tobstones:"<<rid.pageNum<<char(32)<<"f"<<temp_slot.offset<<char(32)<<"l"<<temp_slot.length<<char(32)<<rid.slotNum<<endl;
			return 0;
		}
	}
	}
	delete temp;
	delete tempPage;
	return success;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
	unsigned char *temp = new unsigned char[PAGE_SIZE];
	fileHandle.readPage(rid.pageNum,temp);	//Reads the page
	slot temp_slot;	//structure Used to store the offset and length
	getslotentry(temp,&temp_slot,rid.slotNum);
	short recLength = temp_slot.length;
	const void *recPtr = (char*)temp+temp_slot.offset;
	const void *recordPtr = recordFormat_backwardmask(recPtr,recordDescriptor,recLength);	//Used for storing the orignal record without the headers
	int nullBit;	//to locate position of null bit
	int noOfFields=recordDescriptor.size();
	int nullValSize=ceil(noOfFields/8.0);
	int offset = nullValSize;
	for(int i=0;i<recordDescriptor.size();i++)
	{
		if((recordDescriptor[i].type == TypeInt) || (recordDescriptor[i].type == TypeReal))
		{
			if(recordDescriptor[i].name.compare(attributeName) == 0)
			{
				nullBit = i/8;	//CHECK THIS!!!!!!!REMEMBER
				mempcpy((char*)data+0,(char*)recordPtr+nullBit,sizeof(char));
				mempcpy((char*)data+sizeof(char),(char*)recordPtr+offset,sizeof(int));
				break;
			}
			else
			{
				offset+=sizeof(int);
			}
		}
		else
		{
			if(recordDescriptor[i].name.compare(attributeName) == 0)
			{
				int tempLength;
				nullBit = i/8;
				mempcpy((char*)data+0,(char*)recordPtr+nullBit,sizeof(char));
				memcpy(&tempLength,(char*)recordPtr+offset,sizeof(int));
				offset+=sizeof(int);
				memcpy((char*)data+sizeof(char),(char*)recordPtr+offset,tempLength);
				break;
			}
			else
			{
				int tempLength;
				memcpy(&tempLength,(char*)recordPtr+offset,sizeof(int));
				offset+=sizeof(int)+tempLength;
			}
		}
	}
	delete[] temp;
	return 0;
}

short RecordBasedFileManager::recordSize(const vector<Attribute> &recordDescriptor,const void* data)
{
	//change length as well
		int noOfFields=recordDescriptor.size();
		short nullValSize=ceil(noOfFields/8.0);
		short recordSize=nullValSize;
		unsigned char* nullval=new unsigned char[nullValSize];
		memcpy(nullval,(char*)data,nullValSize);
		unsigned char mask=0x80;
		map<short,bool> nullcheck;
		int j=-1;
		for(int i=0;i<noOfFields;i++)
		{
			if(i%8==0)
			{
				mask=0x80;
				j=j+1;
			}
			if(nullval[j] & mask)
			{
				nullcheck[i]=true;
				mask=mask>>1;
			}
			else
			{
				nullcheck[i]=false;
			}

		}


		for(int i=0;i<noOfFields;i++)
		{

			switch(recordDescriptor[i].type)
			{
			case TypeInt:

				if(nullcheck[i]==false)
					recordSize=recordSize+4;
				break;

			case TypeReal:

				if(nullcheck[i]==false)
					recordSize=recordSize+4;
				break;
			case TypeVarChar:

				if(nullcheck[i]==false)
				{
					int varcharSize;
					memcpy(&varcharSize,(char*)data+recordSize,4);

					recordSize=recordSize+4+varcharSize;
				}

			}
		}
		delete[] nullval;
		//cout<<"RecordSize**"<<recordSize<<endl;
		return recordSize;
}
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	short pageNum=rid.pageNum;
	short slotNum=rid.slotNum;
	short datalength;
	int success=1;
	unsigned char pagedata[PAGE_SIZE];
	slot* slotentry=new slot;
//	cout<<pageNum<<endl;
	success=fileHandle.readPage(pageNum,pagedata);
//	cout<<success;
	getslotentry(pagedata,slotentry,slotNum);
	if(slotentry->offset==FREE_REC)
		return -1;
	if(slotentry->offset>=RELOC)
	{
		RID newrid;
		newrid.slotNum=(slotentry->offset)-RELOC;
		newrid.pageNum=(slotentry->length);
		success=readRecord(fileHandle,recordDescriptor,newrid,data);
	}
	else
	{
		unsigned char* record=pagedata+slotentry->offset;
		datalength=slotentry->length;
		const void* maskeddata=recordFormat_backwardmask(record,recordDescriptor,datalength);
		//cout<<(int)((unsigned char*)maskeddata-record)<<endl;
		memcpy(data,maskeddata,datalength);
	}
	delete slotentry;
    return success;
}


RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {


	short noOfFields=recordDescriptor.size();
	short *offsets=new short[noOfFields];
	short nullValSize=ceil(noOfFields/8.0);
	short recordSize=nullValSize;
	unsigned char* nullval=new unsigned char[nullValSize];
	memcpy(nullval,data,nullValSize);
	unsigned char mask=0x80;
	map<short,bool> nullcheck;
	int j=-1;
	for(int i=0;i<noOfFields;i++)
	{
		if(i%8==0)
		{
			mask=0x80;
			j++;
		}
		if(nullval[j] & mask)
		{
			nullcheck[i]=true;
			mask=mask>>1;
		}
		else
		{
			nullcheck[i]=false;
		}


	}


	for(int i=0;i<noOfFields;i++)
	{
		//cout<<recordSize<<endl;
		//cout<<recordDescriptor[i].type<<endl;
		//cout<<recordDescriptor[i].name<<":";
		switch(recordDescriptor[i].type)
		{
			case TypeInt:
					if(nullcheck[i]==false)
					{

						cout<<*(int*)((char*)data+recordSize)<<endl;
						recordSize=recordSize+4;
					}
					else
					{
						cout<<"NULL"<<endl;
					}


					break;

				case TypeReal:
					if(nullcheck[i]==false)
					{
						cout<<*(float*)((char*)data+recordSize)<<endl;
						recordSize=recordSize+4;
					}
					else{
						cout<<"NULL"<<endl;
					}
					break;
				case TypeVarChar:
					if(nullcheck[i]==false)
					{
						int *varcharSize=new int;
						memcpy(varcharSize,(char*)data+recordSize,4);
						char* str=new char[*varcharSize+1];
						memcpy(str,(char*)data+recordSize+4,*varcharSize);
						str[*varcharSize]='\0';
						cout<<str<<endl;
						recordSize=recordSize+4+*varcharSize;
						delete varcharSize;
						delete[] str;
					}
					else
					{
						cout<<"NULL"<<endl;
					}
				}
	}

	delete[] nullval;
	delete[] offsets;
	return recordSize;

    return 0;
}

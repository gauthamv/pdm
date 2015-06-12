
#include "qe.h"
#include "../rbf/rbfm.h"
#include<cmath>
#include<map>


void BNLJoin::loadjoindata()
{
	currentpos=0;
	currentno=0;
	void* data;
	int offset=0;
	for(int i=0;i<RecordNum;i++)
	{
	left->getNextTuple(data);
	int size;
	size=RecordBasedFileManager::instance()->recordSize(leftAttribute,data);
	memcpy(cachedData+offset,data,size);
	offset+=size;
	}



}
RC BNLJoin::getcondAttributeValue(void* value,void* data,vector<Attribute> recordDescriptor,Attribute condAttr){
	int size;

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
			{
				if(recordDescriptor[i].name.compare(condAttr.name)==0)
				{
								value=new int;
								memcpy(value,(data(char*))+recordSize,sizeof(int));
								return 0;
							}
				recordSize=recordSize+4;
			}

			break;

		case TypeReal:

			if(nullcheck[i]==false)
			{
				if(recordDescriptor[i].name.compare(condAttr.name)==0)
						{
								value=new float;
								memcpy(value,(data(char*))+recordSize,sizeof(float));
								return 0;
							}

				recordSize=recordSize+4;
			}
			break;
		case TypeVarChar:

			if(nullcheck[i]==false)
			{
				int varcharSize;
				memcpy(&varcharSize,(char*)data+recordSize,4);
				if(recordDescriptor[i].name.compare(condAttr.name)==0)
				{

					value=new char[sizeof(int)+varcharSize];
					memcpy(value,(data(char*))+recordSize,sizeof(int)+varcharSize);
					return 0;
				}
				recordSize=recordSize+4+varcharSize;
			}

		}
	}
	delete[] nullval;

}
RC BNLJoin::joinRecords(void* dataleft,void* dataright,void* outdata,vector<Attribute> joinAttr,vector<Attribute> leftAtt,vector<Attribute> rightAtt)
{
	int leftnull=ceil(leftAtt.size()/8.0);
	int rightnull=ceil(rightAtt.size()/8.0);
	unsigned char* nullval=new unsigned char[leftnull];
	memcpy(nullval,(char*)dataleft,leftnull);
	unsigned char mask=0x80;
	map<short,bool> nullcheck;
	int j=-1;
	int i=0;
	for(i=0;i<leftAtt.size();i++)
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
	unsigned char* nullvalright=new unsigned char[rightnull];
	memcpy(nullval,(char*)dataleft,leftnull);
	unsigned char mask=0x80;
	j=-1;
	i=leftAtt.size();
	for(int k=0;k<rightAtt.size();k++)
	{

		if(k%8==0)
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
		i++;
	}
	int nullValsize=ceil((leftAtt.size()+rightAtt.size()-1)/8.0);
	unsigned char* nullJoin=new unsigned char[nullValsize];
	j=-1;
	memset(nullJoin,0,nullValsize);
	for(int k=0;k<nullcheck.size();i++)
	{
		if(k%8==0)
			{
				mask=0x80;
				j=j+1;
			}
		if(nullcheck[k])
		{
			nullJoin[j]=nullJoin[j]|mask;
		}
		mask=mask>>1;
	}


}
RC BNLJoin::compareCache(void* data,void* outdata)
{

	void* rightvalue,*leftv,*leftvalue;
	getcondAttributeValue(rightvalue,data,rightAttribute,rightcond);

	int check;
	int recordSz;
	for(int i=0;i<RecordNum;i++){


		getcondAttributeValue(leftvalue,cachedData+currentpos,leftAttribute,leftcond,joinAttribute);


		switch(leftcond.type)
			{
			case TypeInt:
				int left,right;
				memcpy(&left,leftvalue,sizeof(int));
				memcpy(&right,rightvalue,sizeof(int));
				if(left==right)
				{
					return joinRecords(cachedData+currentpos,data,outdata,joinAttribute);
				}
				break;
			case TypeReal:
				float leftf,rightf;
				memcpy(&leftf,leftvalue,sizeof(float));
				memcpy(&rightf,rightvalue,sizeof(float));
				if(left==right)
				{
					return joinRecords(cachedData+currentpos,data,outdata,joinAttribute);
				}
				break;

			case TypeVarChar:
				int varleftsize,varrightsize;
				memcpy(&varleftsize,leftvalue,sizeof(int));
				memcpy(&varrightsize,rightvalue,sizeof(int));
				char* leftvar=new char[varleftsize+1];
				char* rightvar=new char[varrightsize+1];
				memset(leftvar,'\0',varleftsize+1);
				memset(rightvar,'\0',varrightsize+1);
				string leftvars(leftvar);
				string rightvars(rightvar);
				if(leftvars.compare(rightvars)==0)
				{
					return joinRecords(cachedData+currentpos,data,outdata,joinAttribute);
				}


			}
		recordSz=RecordBasedFileManager::instance()->recordSize(leftAttribute,cachedData+currentpos);
		currentpos+=recordSz;
		currentno++;
		delete[] leftvalue;
	}
	return -1;
}
RC BNLJoin::getNextTuple(void* data)
{
	if(currentno>=RecordNum)
	{
		loadjoindata();
	}
	void* data1;
	right->getNextTuple(data1);
	return comparecache(data1,data);





}
// ... the rest of your implementations go here

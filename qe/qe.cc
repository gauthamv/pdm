#include<cstring>
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
RC BNLJoin::getcondAttributeValue(void* value,void* data,vector<Attribute> recordDescriptor,Attribute condAttr)
{
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
								memcpy(value,(char*)data+recordSize,sizeof(int));
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
								memcpy(value,(char*)data+recordSize,sizeof(float));
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
					memcpy(value,(char*)data+recordSize,sizeof(int)+varcharSize);
					return 0;
				}
				recordSize=recordSize+4+varcharSize;
			}

		}
	}
	delete[] nullval;

}
Filter::Filter(Iterator* input, const Condition &condition)
{
	iter = input;
	tempCond = condition;
}

RC Filter::getNextTuple(void *data)
{
	vector<Attribute> attrs;	//declaring the get attribute vector
	getAttributes(attrs);
	while(iter->getNextTuple(data)!=QE_EOF)
	{
		int offset=1;
		for(int i=0;i<attrs.size();i++)
		{
			if(tempCond.bRhsIsAttr == 0) //RhAttribute is not there
			{
				cout<<attrs[i].name<<char(32)<<attrs[i].type<<tempCond.lhsAttr<<endl;
				if(attrs[i].name.compare(tempCond.lhsAttr) == 0) //means there is a match
				{
					if(checkWithOperator(tempCond.op,data,offset,attrs[i],tempCond.rhsValue) == 1) //TRUE
					{
						return 0;
					}
				}
				else
				{
					if(attrs[i].type == TypeInt)
					{
						offset+=sizeof(int);
					}
					else if(attrs[i].type == TypeReal)
					{
						offset+=sizeof(int);
					}
					else
					{
						int tempLength;
						memcpy(&tempLength,(char*)data+offset,sizeof(int));
						offset+=sizeof(int)+tempLength;
					}
				}
			}
		}
	}
	return QE_EOF;
}

Project::Project(Iterator* input, const vector<string> &attrNames)
{
	iter = input;
	attributeNames = attrNames;
}

RC Project::getNextTuple(void *data)
{
	vector<Attribute> attrs;	//declaring the get attribute vector
	getAttributes(attrs);
	int noOfFields = attrs.size();
	int nullValSize=ceil(noOfFields/8.0);
	unsigned char *tempData = new unsigned char[200]; //temporary data before projection
	map<string,int> myMap;	//initializing the hash map
	for(int k=0;k<attributeNames.size();k++)
	{
		myMap[attributeNames[k]] = 1;
	}
	while(iter->getNextTuple(tempData)!=QE_EOF)
	{
		int offset = 0;
		int dataOffset = 0;
		for(int i=0;i<attrs.size();i++)
		{
			//cout<<attrs[i].name<<endl;
			if(myMap[attrs[i].name] == 1)
			{
				if(dataOffset == 0) //copying the null information
				{
					memcpy((char*)data,(char*)tempData,sizeof(char)*nullValSize);
					offset+=(sizeof(char)*nullValSize);
					dataOffset+=(sizeof(char)*nullValSize);
				}
				switch(attrs[i].type)
				{
					case TypeInt:
					{
						memcpy((char*)data+dataOffset,(char*)tempData+offset,sizeof(int));
						offset+=sizeof(int);
						dataOffset+=sizeof(int);
						break;
					}
					case TypeReal:
					{
						memcpy((char*)data+dataOffset,(char*)tempData+offset,sizeof(int));
						offset+=sizeof(int);
						dataOffset+=sizeof(int);
						break;
					}
					case TypeVarChar:
					{
						int tempLength;
						memcpy(&tempLength,(char*)tempData+offset,sizeof(int));
						memcpy((char*)data+dataOffset,(char*)tempData+offset,sizeof(int));
						dataOffset+=sizeof(int);
						offset+=sizeof(int);
						memcpy((char*)data+dataOffset,(char*)tempData+offset,tempLength);
						dataOffset+=tempLength;
						offset+=tempLength;
						break;
					}
				}
			}
			else
			{
				if(attrs[i].type == TypeInt)
				{
					offset+=sizeof(int);
				}
				else if(attrs[i].type == TypeReal)
				{
					offset+=sizeof(int);
				}
				else
				{
					int tempLength;
					memcpy(&tempLength,(char*)tempData+offset,sizeof(int));
					offset+=sizeof(int)+tempLength;
				}
			}
		}
		if(dataOffset > 0)
		{
			delete[] tempData;
			return 0;
		}
	}

	return QE_EOF;
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
}

RC Filter::checkWithOperator(CompOp op,void *data,int offset,Attribute attr,Value value)
{
	int tempInt,tempIntComp;
	float tempFloat,tempFloatComp;
	switch(op)
	{
		case EQ_OP:
			{
				if(attr.type == TypeInt)
				{
					memcpy(&tempInt,(char*)data+offset,sizeof(int));
					memcpy(&tempIntComp,(char*)(value.data),sizeof(int));
					if(tempInt == tempIntComp)
					{
						return 1;
					}
					else
					{
						return -1;
					}
				}
				else if(attr.type == TypeReal)
				{
					memcpy(&tempFloat,(char*)data+offset,sizeof(int));
					memcpy(&tempFloatComp,(char*)(value.data),sizeof(int));
					if(tempFloat == tempFloatComp)
					{
						return 1;
					}
					else
					{
						return -1;
					}
				}
				else
				{
					int length1,length2;
					memcpy(&length1,(char*)data+offset,sizeof(int));
					memcpy(&length2,(char*)(value.data),sizeof(int));
					char *str1 = new char[length1+1];
					char *str2 = new char[length2+1];
					memset(str1,'\0',length1+1);
					memset(str2,'\0',length2+1);
					memcpy(str1,(char*)data+offset+sizeof(int),length1);
					memcpy(str2,(char*)(value.data)+sizeof(int),length2);
					string tempChar = string(str1);
					string tempCharComp = string(str2);
					if(tempChar.compare(tempCharComp) == 0)
					{
						return 1;
					}
					else
					{
						return 0;
					}
				}
			}
		case LT_OP:
			{
				if(attr.type == TypeInt)
				{
					memcpy(&tempInt,(char*)data+offset,sizeof(int));
					memcpy(&tempIntComp,(char*)(value.data),sizeof(int));
					if(tempInt < tempIntComp)
					{
						return 1;
					}
					else
					{
						return -1;
					}
				}
				else if(attr.type == TypeReal)
				{
					memcpy(&tempFloat,(char*)data+offset,sizeof(int));
					memcpy(&tempFloatComp,(char*)(value.data),sizeof(int));
					if(tempFloat < tempFloatComp)
					{
						return 1;
					}
					else
					{
						return -1;
					}
				}
				else
				{
					int length1,length2;
					memcpy(&length1,(char*)data+offset,sizeof(int));
					memcpy(&length2,(char*)(value.data),sizeof(int));
					char *str1 = new char[length1];
					char *str2 = new char[length2];
					string tempChar = string(str1);
					string tempCharComp = string(str2);
					if(tempChar.compare(tempCharComp) < 0)
					{
						return 1;
					}
					else
					{
						return 0;
					}
				}
			}
		case GT_OP:
			{
				if(attr.type == TypeInt)
				{
					memcpy(&tempInt,(char*)data+offset,sizeof(int));
					memcpy(&tempIntComp,(char*)(value.data),sizeof(int));
					if(tempInt > tempIntComp)
					{
						return 1;
					}
					else
					{
						return -1;
					}
				}
				else if(attr.type == TypeReal)
				{
					memcpy(&tempFloat,(char*)data+offset,sizeof(int));
					memcpy(&tempFloatComp,(char*)(value.data),sizeof(int));
					if(tempFloat > tempFloatComp)
					{
						return 1;
					}
					else
					{
						return -1;
					}
				}
				else
				{
					int length1,length2;
					memcpy(&length1,(char*)data+offset,sizeof(int));
					memcpy(&length2,(char*)(value.data),sizeof(int));
					char *str1 = new char[length1];
					char *str2 = new char[length2];
					string tempChar = string(str1);
					string tempCharComp = string(str2);
					if(tempChar.compare(tempCharComp) > 0)
					{
						return 1;
					}
					else
					{
						return 0;
					}
				}
			}
		case LE_OP:
			{
				if(attr.type == TypeInt)
					{
						memcpy(&tempInt,(char*)data+offset,sizeof(int));
						memcpy(&tempIntComp,(char*)(value.data),sizeof(int));
						if(tempInt <= tempIntComp)
						{
							return 1;
						}
						else
						{
							return -1;
						}
					}
					else if(attr.type == TypeReal)
					{
						memcpy(&tempFloat,(char*)data+offset,sizeof(int));
						memcpy(&tempFloatComp,(char*)(value.data),sizeof(int));
						if(tempFloat <= tempFloatComp)
						{
							return 1;
						}
						else
						{
							return -1;
						}
					}
					else
					{
						int length1,length2;
						memcpy(&length1,(char*)data+offset,sizeof(int));
						memcpy(&length2,(char*)(value.data),sizeof(int));
						char *str1 = new char[length1];
						char *str2 = new char[length2];
						string tempChar = string(str1);
						string tempCharComp = string(str2);
						if(tempChar.compare(tempCharComp) <= 0)
						{
							return 1;
						}
						else
						{
							return 0;
						}
					}
			}
		case GE_OP:
			{
				if(attr.type == TypeInt)
				{
					memcpy(&tempInt,(char*)data+offset,sizeof(int));
					memcpy(&tempIntComp,(char*)(value.data),sizeof(int));
					if(tempInt >= tempIntComp)
					{
						return 1;
					}
					else
					{
						return -1;
					}
				}
				else if(attr.type == TypeReal)
				{
					memcpy(&tempFloat,(char*)data+offset,sizeof(int));
					memcpy(&tempFloatComp,(char*)(value.data),sizeof(int));
					if(tempFloat >= tempFloatComp)
					{
						return 1;
					}
					else
					{
						return -1;
					}
				}
				else
				{
					int length1,length2;
					memcpy(&length1,(char*)data+offset,sizeof(int));
					memcpy(&length2,(char*)(value.data),sizeof(int));
					char *str1 = new char[length1];
					char *str2 = new char[length2];
					string tempChar = string(str1);
					string tempCharComp = string(str2);
					if(tempChar.compare(tempCharComp) >= 0)
					{
						return 1;
					}
					else
					{
						return 0;
					}
				}
			}
		case NE_OP:
			{
				if(attr.type == TypeInt)
				{
					memcpy(&tempInt,(char*)data+offset,sizeof(int));
					memcpy(&tempIntComp,(char*)(value.data),sizeof(int));
					if(tempInt != tempIntComp)
					{
						return 1;
					}
					else
					{
						return -1;
					}
				}
				else if(attr.type == TypeReal)
				{
					memcpy(&tempFloat,(char*)data+offset,sizeof(int));
					memcpy(&tempFloatComp,(char*)(value.data),sizeof(int));
					if(tempFloat != tempFloatComp)
					{
						return 1;
					}
					else
					{
						return -1;
					}
				}
				else
				{
					int length1,length2;
					memcpy(&length1,(char*)data+offset,sizeof(int));
					memcpy(&length2,(char*)(value.data),sizeof(int));
					char *str1 = new char[length1];
					char *str2 = new char[length2];
					string tempChar = string(str1);
					string tempCharComp = string(str2);
					if(tempChar.compare(tempCharComp) != 0)
					{
						return 1;
					}
					else
					{
						return 0;
					}
				}
			}
	}
	return 0;
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
	iter->getAttributes(attrs);
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	iter->getAttributes(attrs);
}

RC BNLJoin::compareCache(void* data,void* outdata)
{

	void* rightvalue,*leftv,*leftvalue;
	getcondAttributeValue(rightvalue,data,rightAttribute,rightcond);

	int check;
	int recordSz;
	for(int i=0;i<RecordNum;i++){

		//getcondAttributeValue(leftvalue,cachedData+currentpos,leftAttribute,leftcond,joinAttribute);


		switch(leftcond.type)
			{
			case TypeInt:
				int left,right;
				memcpy(&left,leftvalue,sizeof(int));
				memcpy(&right,rightvalue,sizeof(int));
				if(left==right)
				{
					//return joinRecords(cachedData+currentpos,data,outdata,joinAttribute);
				}
				break;
			case TypeReal:
				float leftf,rightf;
				memcpy(&leftf,leftvalue,sizeof(float));
				memcpy(&rightf,rightvalue,sizeof(float));
				if(left==right)
				{
					//return joinRecords(cachedData+currentpos,data,outdata,joinAttribute);
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
					//return joinRecords(cachedData+currentpos,data,outdata,joinAttribute);
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
	return compareCache(data1,data);
}
// ... the rest of your implementations go here

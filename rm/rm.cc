
#include "rm.h"
#include "../ix/ix.h"
#include<cmath>
#include<map>

#include<cstring>
RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	tableID = 1;
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
	const string Tab = "Tables";
	const string Col = "Columns";
	const string Index = "Index";
	void *recPtr;	//ptr to hold record
	if(!RecordBasedFileManager::instance()->createFile(Tab))
	{
		FileHandle fileHandle1;	//Creating fileHandle
		if(!RecordBasedFileManager::instance()->openFile(Tab,fileHandle1))
		{
			//inserting table info
			vector<Attribute> recordDescriptor1;
			recPtr = prepareTabRecord(tableID,Tab,Tab,recordDescriptor1);
			RID dummy_rid;	//dummy RID
			RecordBasedFileManager::instance()->insertRecord(fileHandle1, recordDescriptor1, recPtr, dummy_rid);
			recordDescriptor1.clear();

			tableID++;	//Incrementing tableID to 2 for inserting column record

			//Inserting column info
			recPtr = prepareTabRecord(tableID,Col,Col,recordDescriptor1);
			RecordBasedFileManager::instance()->insertRecord(fileHandle1, recordDescriptor1, recPtr, dummy_rid);
			RecordBasedFileManager::instance()->closeFile(fileHandle1);	//closing the file
			delete[] recPtr;
		}
		else
		{
			return -1;
		}
	}
	else
	{
		return -1;
	}
	if(!RecordBasedFileManager::instance()->createFile(Col))
	{
		FileHandle fileHandle2;	//Creating fileHandle
		if(!RecordBasedFileManager::instance()->openFile(Col,fileHandle2))
		{

			//Inserting table information
			vector<Attribute> recordDescriptor2;
			RID dummy_rid;	//dummy RID
			recPtr = prepareColRecord(tableID-1,"table-id",TypeInt,sizeof(int),1,recordDescriptor2);
			RecordBasedFileManager::instance()->insertRecord(fileHandle2, recordDescriptor2, recPtr, dummy_rid);
			recordDescriptor2.clear();
			delete[] recPtr;
			recPtr = prepareColRecord(tableID-1,"table-name",TypeVarChar,50,2,recordDescriptor2);
			RecordBasedFileManager::instance()->insertRecord(fileHandle2, recordDescriptor2, recPtr, dummy_rid);
			recordDescriptor2.clear();
			delete[] recPtr;
			recPtr = prepareColRecord(tableID-1,"file-name",TypeVarChar,50,3,recordDescriptor2);
			RecordBasedFileManager::instance()->insertRecord(fileHandle2, recordDescriptor2, recPtr, dummy_rid);
			recordDescriptor2.clear();
			delete[] recPtr;

			//Inserting Column information
			recPtr = prepareColRecord(tableID,"table-id",TypeInt,sizeof(int),1,recordDescriptor2);
			RecordBasedFileManager::instance()->insertRecord(fileHandle2, recordDescriptor2, recPtr, dummy_rid);
			recordDescriptor2.clear();
			delete[] recPtr;
			recPtr = prepareColRecord(tableID,"column-name",TypeVarChar,50,2,recordDescriptor2);
			RecordBasedFileManager::instance()->insertRecord(fileHandle2, recordDescriptor2, recPtr, dummy_rid);
			recordDescriptor2.clear();
			delete[] recPtr;
			recPtr = prepareColRecord(tableID,"column-type",TypeInt,sizeof(int),3,recordDescriptor2);
			RecordBasedFileManager::instance()->insertRecord(fileHandle2, recordDescriptor2, recPtr, dummy_rid);
			recordDescriptor2.clear();
			delete[] recPtr;
			recPtr = prepareColRecord(tableID,"column-length",TypeInt,sizeof(int),4,recordDescriptor2);
			RecordBasedFileManager::instance()->insertRecord(fileHandle2, recordDescriptor2, recPtr, dummy_rid);
			recordDescriptor2.clear();
			delete[] recPtr;
			recPtr = prepareColRecord(tableID,"column-position",TypeInt,sizeof(int),5,recordDescriptor2);
			RecordBasedFileManager::instance()->insertRecord(fileHandle2, recordDescriptor2, recPtr, dummy_rid);
			recordDescriptor2.clear();
			delete[] recPtr;

			tableID++;	//incrementing tableId to 3
			RecordBasedFileManager::instance()->closeFile(fileHandle2);	//closing the file
			//return 0;
		}
		else
		{
			return -1;
		}
	}
	else
	{
		return -1;
	}
	//Creating the index Catalog File
	if(!RecordBasedFileManager::instance()->createFile(Index))
	{
		return 0;
	}
    return -1;
}

RC RelationManager::deleteCatalog()
{
	const string Tab = "Tables";
	const string Col = "Columns";
	const string Index = "Index";
	if((!PagedFileManager::instance()->destroyFile(Tab)) && (!PagedFileManager::instance()->destroyFile(Col)) && (!PagedFileManager::instance()->destroyFile(Index)))
	{
		return 0;
	}
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	//Scanning for the tableID
	//Scanning the table file foe getting the table-id
	FileHandle fileHandleTT;	//Creating fileHandle
	if(RecordBasedFileManager::instance()->openFile("Tables",fileHandleTT) != 0)
	{
		return -1;
	}
	vector<Attribute> tableDescriptor;
	formTableAttributeVector(tableDescriptor);
	RBFM_ScanIterator tableScan;
	vector<string> tableVector;
	tableVector.push_back("table-id");
	RID tableRID;
	int tableID;
	unsigned char* tabletemp=new unsigned char[1+sizeof(int)];

	if(!RecordBasedFileManager::instance()->scan(fileHandleTT,tableDescriptor,"",NO_OP,NULL,tableVector,tableScan))
	{
		while(tableScan.getNextRecord(tableRID,tabletemp) != RBFM_EOF){}
	}
	memcpy(&tableID,tabletemp+1,sizeof(int));
	tableID = tableID+1;
	if(RecordBasedFileManager::instance()->closeFile(fileHandleTT) != 0)
	{
		return -1;
	}


	RID dummy_rid;	//dummy RID
	if(!PagedFileManager::instance()->createFile(tableName))
	{
		FileHandle fileHandle1;	//Creating fileHandle
		vector<Attribute> recordDescriptor;
		if(!RecordBasedFileManager::instance()->openFile("Tables",fileHandle1))
		{
			void *recPtr = prepareTabRecord(tableID,tableName,tableName,recordDescriptor);
			RecordBasedFileManager::instance()->insertRecord(fileHandle1, recordDescriptor, recPtr, dummy_rid);
			recordDescriptor.clear();
			delete[] recPtr;
		}
		else
		{
			return -1;
		}
		FileHandle fileHandle2;
		recordDescriptor.clear();
		if(!RecordBasedFileManager::instance()->openFile("Columns",fileHandle2))
		{
			for(int i=0;i<attrs.size();i++)
			{
				void *recPtr = prepareColRecord(tableID,attrs[i].name,attrs[i].type,attrs[i].length,i+1,recordDescriptor);
				RecordBasedFileManager::instance()->insertRecord(fileHandle2, recordDescriptor, recPtr, dummy_rid);
				recordDescriptor.clear();
				delete[] recPtr;
			}
			//tableID++;	//incrementing tableid no need to do this
			return 0;
		}
		else
		{
			return -1;
		}
	}
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
	if((!tableName.compare("Tables")) || (!tableName.compare("Columns")))
	{
		return -1;
	}
	RID ridTemp; //temp rid used for deleting the table;
	FileHandle fileHandle;	//Creating fileHandle
	if(RecordBasedFileManager::instance()->openFile("Tables",fileHandle) != 0)
	{
		return -1;
	}
	vector<Attribute> tableDescriptor;	//just for passing to the function
	formTableAttributeVector(tableDescriptor);
	int success=0; //Flag to determine if the table name is deleted
	vector<string> tableVector;
	tableVector.push_back("table-id");
	RBFM_ScanIterator tableScan;
	RID tableRID;
	int varLength = tableName.length();
	unsigned char* tabletemp=new unsigned char[1+sizeof(int)];
	unsigned char *varSize = new unsigned char[varLength+sizeof(int)];
	memcpy((char*)varSize+0,&varLength,sizeof(int));
	memcpy((char*)varSize+sizeof(int),tableName.c_str(),varLength);
	if(!RecordBasedFileManager::instance()->scan(fileHandle,tableDescriptor,"table-name",EQ_OP,varSize,tableVector,tableScan))
	{
		while(tableScan.getNextRecord(tableRID,tabletemp) != 0){}
	}

	memcpy(&tableID,tabletemp+1,sizeof(int));
	delete tabletemp;

	if(!RecordBasedFileManager::instance()->deleteRecord(fileHandle,tableDescriptor,tableRID))
	{
		success = 1;
	}
	//Closing file
	if(RecordBasedFileManager::instance()->closeFile(fileHandle) != 0)
	{
		return -1;
	}



	//Read the table ID from 1st scan
	if(RecordBasedFileManager::instance()->openFile("Columns",fileHandle) != 0)
	{
		return -1;
	}
	vector<Attribute> columnDescriptor;
	formColumnAttributeVector(columnDescriptor);
	RM_ScanIterator columnScan;
	vector<string> columnVector;
	columnVector.push_back("column-name");
	columnVector.push_back("column-type");
	columnVector.push_back("column-length");
	RID columnRID;
	unsigned char *colData = new unsigned char[66];
	int tempType;

	if(!RecordBasedFileManager::instance()->scan(fileHandle,columnDescriptor,"table-id",EQ_OP,&tableID,columnVector,tableScan))
	{
		while(tableScan.getNextRecord(columnRID,colData) != RM_EOF)
		{
			if(!RecordBasedFileManager::instance()->deleteRecord(fileHandle,columnDescriptor,columnRID))
			{
				success = success & 1;
			}
		}
	}
	delete[] colData;
	//Closing file
	if(RecordBasedFileManager::instance()->closeFile(fileHandle) != 0)
	{
		return -1;
	}


	//Destroy File
	if(!RecordBasedFileManager::instance()->destroyFile(tableName))
	{
		success = success & 1;
	}
	if(success == 1)
	{
		return 0;
	}
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	FileHandle fileHandle;
	if(RecordBasedFileManager::instance()->openFile("Tables",fileHandle) != 0)
	{
		return -1;
	}
	//Scanning the table file foe getting the table-id
	vector<Attribute> tableDescriptor;
	formTableAttributeVector(tableDescriptor);
	RBFM_ScanIterator tableScan;
	vector<string> tableVector;
	tableVector.push_back("table-id");
	RID tableRID;
	int tableID;
	int varLength = tableName.length();
	unsigned char* tabletemp=new unsigned char[1+sizeof(int)];
	unsigned char *varSize = new unsigned char[varLength+sizeof(int)];
	memcpy((char*)varSize+0,&varLength,sizeof(int));
	memcpy((char*)varSize+sizeof(int),tableName.c_str(),varLength);

	if(!RecordBasedFileManager::instance()->scan(fileHandle,tableDescriptor,"table-name",EQ_OP,varSize,tableVector,tableScan))
	{
		while(tableScan.getNextRecord(tableRID,tabletemp) != 0){}
	}
	memcpy(&tableID,tabletemp+1,sizeof(int));
	delete tabletemp;

	//cout<<fileHandle.file.good()<<endl;
	//Closing file
	if(RecordBasedFileManager::instance()->closeFile(fileHandle) != 0)
	{
		return -1;
	}

	//Scanning Columns
	FileHandle fileHandle2;
	if(RecordBasedFileManager::instance()->openFile("Columns",fileHandle2) != 0)
	{
		return -1;
	}
	vector<Attribute> columnDescriptor;
	formColumnAttributeVector(columnDescriptor);
	RM_ScanIterator columnScan;
	vector<string> columnVector;
	columnVector.push_back("column-name");
	columnVector.push_back("column-type");
	columnVector.push_back("column-length");
	RID columnRID;
	unsigned char *colData = new unsigned char[66];
	int tempType;



	if(!RecordBasedFileManager::instance()->scan(fileHandle2,columnDescriptor,"table-id",EQ_OP,&tableID,columnVector,tableScan))
	{
		while(tableScan.getNextRecord(tableRID,colData) != RM_EOF)
		{
			int tempLength;
			int offset = sizeof(char);
			Attribute Temp;
			memcpy(&tempType,(char*)colData+offset,sizeof(int));

			char* tempName=new char[tempType+1];
			memset(tempName,'\0',tempType+1);

			offset+=sizeof(int);
			memcpy(tempName,(char*)colData+offset,tempType);
			offset+=tempType;
			memcpy(&tempType,(char*)colData+offset,sizeof(int));
			offset+=sizeof(int);
			memcpy(&tempLength,(char*)colData+offset,sizeof(int));
			Temp.length = tempLength;
			Temp.type = (AttrType)tempType;

			string tmp(tempName);
			Temp.name=tmp;
			//cout<<"blaah"<<Temp.name<<endl;
			attrs.push_back(Temp);
			delete[] tempName;
		}
	}
	//Closing file
	//cout<<fileHandle2.file.good()<<endl;
	if(RecordBasedFileManager::instance()->closeFile(fileHandle2) != 0)
	{
		return -1;
	}
	delete[] colData;
	delete[] varSize;
    return 0;
}

RC RelationManager::getAttributesIndex(const string &tableName, vector<string> &names)
{
	FileHandle fileHandle;
	if(RecordBasedFileManager::instance()->openFile("Index",fileHandle) != 0)
	{
		return -1;
	}
	//Scanning the index file foe getting the table-id
	vector<Attribute> indexDescriptor;
	formIndexAttributeVector(indexDescriptor);
	RBFM_ScanIterator indexScan;
	vector<string> indexVector;
	indexVector.push_back("attribute-name");
	RID indexRID;
	int indexID;
	int varLength = tableName.length();
	unsigned char* tabletemp=new unsigned char[1+50];//50 bytes is max size of attribute name
	unsigned char *varSize = new unsigned char[varLength+sizeof(int)];
	memcpy((char*)varSize+0,&varLength,sizeof(int));
	memcpy((char*)varSize+sizeof(int),tableName.c_str(),varLength);

	if(!RecordBasedFileManager::instance()->scan(fileHandle,indexDescriptor,"table-name",EQ_OP,varSize,indexVector,indexScan))
	{
		while(indexScan.getNextRecord(indexRID,tabletemp) != RM_EOF)
		{
			int tempLength;
			memcpy(&tempLength,(char*)tabletemp+sizeof(char),sizeof(int));
			char* tempName=new char[tempLength+1];
			memset(tempName,'\0',tempLength+1);
			memcpy(tempName,(char*)tabletemp+sizeof(char)+sizeof(int),tempLength);
			string attName = string(tempName);
			names.push_back(tempName);
			delete[] tempName;
		}
	}
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	int success=0;
	FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName,recordDescriptor) == -1)	//getting the attribute vector
	{
		return -1;
	}
	if(!RecordBasedFileManager::instance()->openFile(tableName,fileHandle))
	{
		if(!RecordBasedFileManager::instance()->insertRecord(fileHandle,recordDescriptor,data,rid))
		{
			success = 1;
		}
	}

	//Inserting into index
	//Creating ixFileHandle
	IXFileHandle ixfileHandle;

	//Getting the attribute names for a particular table
	vector<string> Names;
	getAttributesIndex(tableName,Names);

	if(Names.size() == 0)
	{
		//no index for this atribute
		if(success == 1)
		{
			return 0;
		}
		else
		{
			return -1;
		}
	}

	//checking all the attribute names that match
	for(int i=0;i<Names.size();i++)
	{
		cout<<"Name Size: "<<Names.size()<<endl;
		for(int j=0;j<recordDescriptor.size();j++)
		{
			cout<<"Attribute Name: "<<Names[i]<<endl;
			cout<<"recordDescriptorName: "<<recordDescriptor[j].name<<endl;
			if(Names[i].compare(recordDescriptor[j].name) == 0) //attribute name matches
			{
				string indexName = tableName + "_" + Names[i] + "_idx";
				cout<<indexName<<endl;
				if(!IndexManager::instance()->openFile(indexName,ixfileHandle))
				{
					void *key;
					//allocating space

					//getting the key
					getKey(key,data,recordDescriptor,recordDescriptor[j]);
					//writing the index entry
					if(!IndexManager::instance()->insertEntry(ixfileHandle,recordDescriptor[j],key,rid))
					{
						success = success & 1;
					}
				}
			}
		}
	}
	if(success == 1)
	{
		return 0;
	}
    return -1;
}

RC RelationManager::getKey(void *value,const void *data,vector<Attribute> recordDescriptor,Attribute attr)
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
					if(recordDescriptor[i].name.compare(attr.name)==0)
					{
						value=new int;
						int temp;
						memcpy(value,(char*)data+recordSize,sizeof(int));
						memcpy(&temp,value,sizeof(int));
						cout<<temp<<endl;
						return 0;
					}
					recordSize=recordSize+4;
				}
				break;
			case TypeReal:
				if(nullcheck[i]==false)
				{
					if(recordDescriptor[i].name.compare(attr.name)==0)
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
					if(recordDescriptor[i].name.compare(attr.name)==0)
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
	return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{

	int success = 0;
	vector<Attribute> recordDescriptor; //Dummy recordDescriptor
	if(getAttributes(tableName,recordDescriptor) == -1)	//getting the attribute vector
	{
		return -1;
	}

	//Reading the record before deleting it for indexing purposes
	FileHandle fileH;
	void *data = new unsigned char[2000]; //check this
	if(!RecordBasedFileManager::instance()->openFile(tableName,fileH))
	{
		if(!RecordBasedFileManager::instance()->readRecord(fileH,recordDescriptor,rid,data));
		{
			success = 1;
		}
	}

	FileHandle fileHandle;
	if(!RecordBasedFileManager::instance()->openFile(tableName,fileHandle))
	{
		if(!RecordBasedFileManager::instance()->deleteRecord(fileHandle,recordDescriptor,rid))
		{
			success = success & 1;
		}
	}

	//deleting from index
	//Creating ixFileHandle
	IXFileHandle ixfileHandle;

	//Getting the attribute names for a particular table
	vector<string> Names;
	getAttributesIndex(tableName,Names);

	//checking all the attribute names that match
	for(int i=0;i<Names.size();i++)
	{
		for(int j=0;j<recordDescriptor.size();j++)
		{
			if(Names[i].compare(recordDescriptor[j].name) == 0) //attribute name matches
			{
				string indexName = tableName + "_" + Names[i] + "_idx";
				if(!IndexManager::instance()->openFile(indexName,ixfileHandle))
				{
					void *key;
					//allocating space
					if((recordDescriptor[j].type == TypeInt) || (recordDescriptor[j].type == TypeReal))
					{
						key = new char[sizeof(int)]; //no null flags for index entries
					}
					else
					{
						key = new char[50+sizeof(int)]; //Max 50 bytes
					}
					//getting the key
					getKey(key,data,recordDescriptor,recordDescriptor[j]);
					//writing the index entry
					if(!IndexManager::instance()->deleteEntry(ixfileHandle,recordDescriptor[j],key,rid))
					{
						delete[] key;
						if(success == 1)
						{
							return 0;
						}
					}
					delete[] key;
				}
			}
		}
	}
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	//formAttributeVector(recordDescriptor);	//forms the attribute of vectors
	if(getAttributes(tableName,recordDescriptor) == -1)	//getting the atrribute vector
	{
		return -1;
	}
	if(!RecordBasedFileManager::instance()->openFile(tableName,fileHandle))
	{
		if(!RecordBasedFileManager::instance()->updateRecord(fileHandle,recordDescriptor,data,rid))
		{
			return 0;
		}
	}
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	FileHandle fileHandle;
	//Sanity check
	if(RecordBasedFileManager::instance()->openFile(tableName,fileHandle) != 0)
	{
		return -1;
	}
	if(RecordBasedFileManager::instance()->closeFile(fileHandle) != 0)
	{
		return -1;
	}

	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName,recordDescriptor) == -1)	//getting the attribute vector
	{
		return -1;
	}
	if(!RecordBasedFileManager::instance()->openFile(tableName,fileHandle))
	{
		if(!RecordBasedFileManager::instance()->readRecord(fileHandle,recordDescriptor,rid,data))
		{
			return 0;
		}
	}
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	if(!RecordBasedFileManager::instance()->printRecord(attrs,data))
	{
		return 0;
	}
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName,recordDescriptor) == -1)	//getting the attribute vector
	{
		return -1;
	}
	if(!RecordBasedFileManager::instance()->openFile(tableName,fileHandle))
	{
		if(!RecordBasedFileManager::instance()->readAttribute(fileHandle,recordDescriptor,rid,attributeName,data))
		{
			return 0;
		}
	}
    return -1;
}

RC RelationManager::scan(const string &tableName, const string &conditionAttribute,const CompOp compOp,const void *value,const vector<string> &attributeNames,RM_ScanIterator &rm_ScanIterator)
{
	//FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	getAttributes(tableName,recordDescriptor);	//gets the attributes

	if(!RecordBasedFileManager::instance()->openFile(tableName,rm_ScanIterator.fileHandle))
	{
		if(!RecordBasedFileManager::instance()->scan(rm_ScanIterator.fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.RBFM_iter))
		{
			return 0;
		}
	}
    return -1;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	int success = 0;
	string indexName = tableName + "_" + attributeName + "_idx"; //Creating the name for the index file
	FileHandle fileHandle;
	if(IndexManager::instance()->createFile(indexName) == -1)
	{
		return -1;
	}
	if(!RecordBasedFileManager::instance()->openFile("Index",fileHandle))
	{
		//Push this entry into index catalog
		//inserting table info
		vector<Attribute> recordDescriptor;
		void *recPtr = prepareIndexRecord(tableName,tableName,attributeName,recordDescriptor);
		RID dummy_rid;	//dummy RID
		if(!RecordBasedFileManager::instance()->insertRecord(fileHandle, recordDescriptor, recPtr, dummy_rid))
		{
			success = 1;
		}
	}
	//Check if the data table for this index is already present
	FileHandle fileHandle1;

	vector<string> attributes;
	attributes.push_back(attributeName);
	vector<Attribute> RecordDescriptor;

	if(!RecordBasedFileManager::instance()->openFile(tableName,fileHandle1))
	{
		if(fileHandle1.getNumberOfPages() > 0) //means data is present in table
		{
			if(!RecordBasedFileManager::instance()->scan(fileHandle1,indexDescriptor,"",NO_OP,varSize,indexVector,indexScan))
		}
	}


	return -1;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	if(!tableName.compare("Index"))
	{
		return -1;
	}
	string indexName = tableName + "_" +attributeName + "_idx";
	FileHandle fileHandle;	//Creating fileHandle
	if(RecordBasedFileManager::instance()->openFile("Index",fileHandle) != 0)
	{
		return -1;
	}
	vector<Attribute> indexDescriptor;	//just for passing to the function
	formIndexAttributeVector(indexDescriptor);
	int success=0; //Flag to determine if the table name is deleted
	vector<string> indexVector;
	indexVector.push_back("table-name");
	RBFM_ScanIterator indexScan;
	RID indexRID;
	int varLength = tableName.length();
	unsigned char* tabletemp=new unsigned char[1+sizeof(int)];
	unsigned char *varSize = new unsigned char[varLength+sizeof(int)];
	memcpy(varSize,&varLength,sizeof(int));
	memcpy(varSize+sizeof(int),tableName.c_str(),varLength);
	if(!RecordBasedFileManager::instance()->scan(fileHandle,indexDescriptor,"table-name",EQ_OP,varSize,indexVector,indexScan))
	{
		while(indexScan.getNextRecord(indexRID,tabletemp) != 0)
		{
			if(!RecordBasedFileManager::instance()->deleteRecord(fileHandle,indexDescriptor,indexRID))
			{
				success = 1;
			}
		}
	}
	if(IndexManager::instance()->destroyFile(tableName) == -1)
	{
		return -1;
	}
	if(success == 1)
	{
		return 0;
	}
	return -1;
}
RC RelationManager::indexScan(const string &tableName, const string &attributeName, const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive, RM_IndexScanIterator &rm_IndexScanIterator)
{
	string indexName = tableName + "_" + attributeName + "_idx";
	cout<<indexName<<endl;
	vector<Attribute> recordDescriptor;
		getAttributes(tableName,recordDescriptor);
		int i=0;//gets the attributes
		for(i=0;i<recordDescriptor.size();i++){
			if(recordDescriptor[i].name.compare(attributeName)==0)
			{
				break;
			}
		}
		if(i>=recordDescriptor.size()){
			return -1;
		}

	if(!RecordBasedFileManager::instance()->openFile(indexName,rm_IndexScanIterator.file.fileHandle))
		{
			if(!IndexManager::instance()->scan(rm_IndexScanIterator.file,recordDescriptor[i],lowKey,highKey,lowKeyInclusive,highKeyInclusive,rm_IndexScanIterator.index_iter))
			{
				return 0;
			}
		}
	    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

void* RelationManager::prepareIndexRecord(string TableName,string fileName,string attributeName,vector<Attribute> &recordDescriptor)
{
	Attribute Temp;
	//Writing the tablename part
	Temp.length = 50;
	Temp.type =	TypeVarChar;
	Temp.name = "table-name";
	recordDescriptor.push_back(Temp);

	//Writing the filename part
	Temp.length = 50;
	Temp.type =	TypeVarChar;
	Temp.name = "file-name";
	recordDescriptor.push_back(Temp);

	//Writing the filename part
	Temp.length = 50;
	Temp.type =	TypeVarChar;
	Temp.name = "attribute-name";
	recordDescriptor.push_back(Temp);

	int recSize = 3*sizeof(int) + TableName.length() + fileName.length() + attributeName.length() + sizeof(char);	//the char byte is for storing null information
	int offset = 0;	//moves the pointer within record for easy appending of data
	int tabNameLength = TableName.length();	//Length of the table name
	int fileNameLength = fileName.length();	//Length of file name
	int attributeNameLength = attributeName.length(); //Length of attribute name
	unsigned int nullField = 0;
	unsigned char *tempRec = new unsigned char[recSize];
	memcpy((char*)tempRec+offset,&nullField,sizeof(char));	//appending nullField to record
	offset+=sizeof(char);
	memcpy((char*)tempRec+offset,&tabNameLength,sizeof(int));	//appending length of tablename
	offset+=sizeof(int);
	memcpy((char*)tempRec+offset,TableName.c_str(),tabNameLength);	//appending name of table
	offset+=tabNameLength;
	memcpy((char*)tempRec+offset,&fileNameLength,sizeof(int));	//appending length of file name
	offset+=sizeof(int);
	memcpy((char*)tempRec+offset,fileName.c_str(),fileNameLength);	//appending the filename
	offset+=fileNameLength;
	memcpy((char*)tempRec+offset,&attributeNameLength,sizeof(int));	//appending the length of attributeName
	offset+=sizeof(int);
	memcpy((char*)tempRec+offset,attributeName.c_str(),attributeNameLength);	//appending the attributeName

	return tempRec;
}

void* RelationManager::prepareTabRecord(int tID,string TableName,string fileName,vector<Attribute> &recordDescriptor)
{
	Attribute Temp;
	//Writing the tid part
	Temp.length = sizeof(int);
	Temp.type =	TypeInt;
	Temp.name = "table-id";	//confirm this part
	recordDescriptor.push_back(Temp);

	//Writing the tablename part
	Temp.length = 50;
	Temp.type =	TypeVarChar;
	Temp.name = "table-name";
	recordDescriptor.push_back(Temp);

	//Writing the filename part
	Temp.length = 50;
	Temp.type =	TypeVarChar;
	Temp.name = "file-name";
	recordDescriptor.push_back(Temp);

	int recSize = 3*sizeof(int) + TableName.length() + fileName.length() + sizeof(char);	//the char byte is for storing null information
	int offset = 0;	//moves the pointer within record for easy appending of data
	int tabNameLength = TableName.length();	//Length of the table name
	int fileNameLength = fileName.length();	//Length of file name
	unsigned int nullField = 0;
	unsigned char *tempRec = new unsigned char[recSize];
	memcpy((char*)tempRec+offset,&nullField,sizeof(char));	//appending nullField to record
	offset+=sizeof(char);
	memcpy((char*)tempRec+offset,&tID,sizeof(int));	//appending table ID to record
	offset+=sizeof(int);
	memcpy((char*)tempRec+offset,&tabNameLength,sizeof(int));	//appending length of tablename
	offset+=sizeof(int);
	memcpy((char*)tempRec+offset,TableName.c_str(),tabNameLength);	//appending name of table
	offset+=tabNameLength;
	memcpy((char*)tempRec+offset,&fileNameLength,sizeof(int));	//appending length of file name
	offset+=sizeof(int);
	memcpy((char*)tempRec+offset,fileName.c_str(),fileNameLength);	//appending the filename

	return tempRec;
}

void* RelationManager::prepareColRecord(int tID,string ColName,int colType,int colLength,int colPosition,vector<Attribute> &recordDescriptor)
{
	Attribute Temp;
	//Writing the tid part
	Temp.length = sizeof(int);
	Temp.type =	TypeInt;
	Temp.name = "table-id";	//confirm this part
	recordDescriptor.push_back(Temp);

	//Writing the colname part
	Temp.length = 50;
	Temp.type =	TypeVarChar;
	Temp.name = "column-name";
	recordDescriptor.push_back(Temp);

	//Writing the column type part
	Temp.length = sizeof(int);	//Verify this
	Temp.type = TypeInt;	//Typcasting int to attrType
	Temp.name = "column-type";
	recordDescriptor.push_back(Temp);

	//Writing the column length part
	Temp.length = sizeof(int);
	Temp.type =	TypeInt;
	Temp.name = "column-length";
	recordDescriptor.push_back(Temp);

	//Writing the column length part
	Temp.length = sizeof(int);
	Temp.type =	TypeInt;
	Temp.name = "column-postion";
	recordDescriptor.push_back(Temp);

	//Appending part
	int recSize = 5*sizeof(int) + ColName.length() + sizeof(char);	//the char byte is for storing null information
	unsigned char *tempRec = new unsigned char[recSize];
	unsigned int nullField = 0;
	int colNameLength = ColName.length();	//Length of the column name
	int offset = 0;	//moves the pointer within record for easy appending of data

	memcpy((char*)tempRec+offset,&nullField,sizeof(char));	//appending nullField to record
	offset+=sizeof(char);
	memcpy((char*)tempRec+offset,&tID,sizeof(int));	//appending the table ID
	offset+=sizeof(int);
	memcpy((char*)tempRec+offset,&colNameLength,sizeof(int));	//appending the length of col name
	offset+=sizeof(int);
	memcpy((char*)tempRec+offset,ColName.c_str(),colNameLength);	//appending the col name
	offset+=colNameLength;
	memcpy((char*)tempRec+offset,&colType,sizeof(int));		//appending the col type
	offset+=sizeof(int);
	memcpy((char*)tempRec+offset,&colLength,sizeof(int));	//appending the col length
	offset+=sizeof(int);
	memcpy((char*)tempRec+offset,&colPosition,sizeof(int));		//appending the col position

	return tempRec;
}

void RelationManager::formIndexAttributeVector(vector<Attribute> &recordDescriptor)
{
	Attribute Temp;

	//Writing the tablename part
	Temp.length = 50;
	Temp.type =	TypeVarChar;
	Temp.name = "table-name";
	recordDescriptor.push_back(Temp);

	Temp.length = 50;
	Temp.type =	TypeVarChar;
	Temp.name = "file-name";
	recordDescriptor.push_back(Temp);

	Temp.length = 50;
	Temp.type =	TypeVarChar;
	Temp.name = "attribute-name";
	recordDescriptor.push_back(Temp);
}

void RelationManager::formTableAttributeVector(vector<Attribute> &recordDescriptor)
{
	Attribute Temp;

	Temp.length = sizeof(int);	//Verify this
	Temp.type = TypeInt;
	Temp.name = "table-id";
	recordDescriptor.push_back(Temp);

	//Writing the tablename part
	Temp.length = 50;
	Temp.type =	TypeVarChar;
	Temp.name = "table-name";
	recordDescriptor.push_back(Temp);

	Temp.length = 50;
	Temp.type =	TypeVarChar;
	Temp.name = "file-name";
	recordDescriptor.push_back(Temp);
}

void RelationManager::formColumnAttributeVector(vector<Attribute> &recordDescriptor)
{
	Attribute Temp;

	Temp.length = sizeof(int);	//Verify this
	Temp.type = TypeInt;
	Temp.name = "table-id";
	recordDescriptor.push_back(Temp);

	//Writing the colname part
	Temp.length = 50;
	Temp.type =	TypeVarChar;
	Temp.name = "column-name";
	recordDescriptor.push_back(Temp);

	Temp.length = sizeof(int);	//Verify this
	Temp.type = TypeInt;
	Temp.name = "column-type";
	recordDescriptor.push_back(Temp);

	Temp.length = sizeof(int);
	Temp.type =	TypeInt;
	Temp.name = "column-length";
	recordDescriptor.push_back(Temp);

	Temp.length = sizeof(int);
	Temp.type =	TypeInt;
	Temp.name = "column-position";
	recordDescriptor.push_back(Temp);
}

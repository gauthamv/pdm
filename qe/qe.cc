#include<cstring>
#include "qe.h"

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
					if(tempInt == tempIntComp)
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
					if(tempInt < tempIntComp)
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
					if(tempInt > tempIntComp)
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
						if(tempInt <= tempIntComp)
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
					if(tempInt < tempIntComp)
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
					if(tempInt != tempIntComp)
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
// ... the rest of your implementations go here

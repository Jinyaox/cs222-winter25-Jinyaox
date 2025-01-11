#include "src/include/rbfm.h"
#include <cmath>
#include <cstring>
#include <ostream>
#include <bits/ios_base.h>
#include <bits/locale_facets_nonio.h>

#include "src/include/pfm.h"

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        if (PagedFileManager::instance().createFile(fileName)==0) {
            return 0;
        }
        return -1;
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        if (PagedFileManager::instance().destroyFile(fileName)==0) {
            return 0;
        }
        return -1;
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if (PagedFileManager::instance().openFile(fileName, fileHandle)==0) {
            return 0;
        }
        return -1;
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        if (PagedFileManager::instance().closeFile(fileHandle)==0) {
            return 0;
        }
        return -1;
    }

    int RecordBasedFileManager::dataparser(const std::vector<Attribute> &recordDescriptor,
        const void *data, std::vector<std::vector<char>> &parsedData)
    {
        int numAttributes = recordDescriptor.size();
        int nullIndicatorSize = std::ceil(static_cast<double>(numAttributes) / 8);
        int offset=nullIndicatorSize;

        //get the first 8 bytes of data, now the data iteration should start at nullIndicatorSize
        char null_buffer[nullIndicatorSize]; memset(null_buffer, 0, nullIndicatorSize);
        strncpy(null_buffer,(char*)data,nullIndicatorSize);

        //now I iterate through every entry and obtain the data
        char data_buffer[2048]; memset(data_buffer, 0, 2048);

        for (int i = 0; i < numAttributes; i++) {
            //first check if that entry is null
            char null_byte=null_buffer[i/8];
            bool isOne = null_byte & (1 << 8-(i%8)-1);
            if (!isOne) {
                //retrieve information about the attribute
                //deal with non var chars first
                if (recordDescriptor[i].type!=TypeVarChar) {
                    int len=recordDescriptor[i].length;
                    std::vector<char> segment(len);
                    memcpy(segment.data(),data+offset,len);
                    parsedData.push_back(std::move(segment));
                    memset(data_buffer, 0, 2048);
                    offset+=len;
                }

                //deal with var char
                else {
                    //retrieve the first four byte for length
                    int varcharLen = 0;

                    // Copy the first 4 bytes from the char array into the integer
                    std::memcpy(&varcharLen, data+offset, sizeof(int));
                    offset += sizeof(int);

                    //do the same thing as before
                    std::vector<char> segment(varcharLen);
                    memcpy(segment.data(),data+offset,varcharLen);
                    parsedData.push_back(std::move(segment));
                    memset(data_buffer, 0, 2048);
                    offset+=varcharLen;
                }
            }
        }
        return nullIndicatorSize;
    }


    //define helper function: record creator, given a vector of information and RID
    //Structure the record as follows:

    // RID ||  length (offset 4 bytes) || nullindicator || entry size(s) || entries

    int RecordBasedFileManager::recordCreator(std::vector<std::vector<char>> &parsedData, char *data, char *orig_data, int nullsize)
    {
        int offset=6;
        //store the total length of the data string:
        int total_len=10+nullsize; //reserve for RID and length itself and null size

        //put the RID into the data first, this is a stub
        memcpy(data,"000000",6);

        //Create the field length
        std::vector<int> fieldlength;

        //first get each data length
        for (int i = 0; i < parsedData.size(); i++) {
            fieldlength.emplace_back(parsedData[i].size());
            total_len+=4; //for the entry size field
            total_len+=parsedData[i].size(); //for the entry length
        }

        //Record how many entries in this data piece
        memcpy(data+offset,&total_len,sizeof(int));
        offset += sizeof(int);

        //store the null field information
        memcpy(data+offset,orig_data,nullsize);
        offset += nullsize;

        //create the entry size for the data
        for (int i = 0; i < fieldlength.size(); i++) {
            memcpy(data+offset,&fieldlength[i],sizeof(int));
            offset += sizeof(int);
        }

        //now fill in the entries to the data
        for (int i = 0; i < parsedData.size(); i++) {
            char temp[parsedData[i].size()]; memset(temp, 0, parsedData[i].size());
            for (int j = 0; j < parsedData[i].size(); j++) {
                temp[j] = parsedData[i][j];
            }
            memcpy(data+offset,temp,parsedData[i].size());
            offset += parsedData[i].size();
        }
        return total_len;
    }


    //calculate free space for a page, given a page number, return free space in that page.
    int RecordBasedFileManager:: free_space(PageNum pg,FileHandle &fileHandle) {
        char buffer[PAGE_SIZE]; memset(buffer, 0, PAGE_SIZE);
        fileHandle.readPage(pg,buffer);

        //Read the Available Page into the buffer
        int read_cursor=6;
        int offset=0;

        //read each record and move to the end of the buffer to get the slot
        do {
            memcpy(&offset,buffer+read_cursor,sizeof(int));
            read_cursor+=offset; //since the offset already count in the 6 in its position it moves automatically
            if (read_cursor>=PAGE_SIZE) {
                return 0;
            }
        }while(offset!=0);

        read_cursor-=6;//move to the first slot
        return PAGE_SIZE-read_cursor;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        // RID || length (offset 4 bytes) || entry size(s) || entries

        char buffer[PAGE_SIZE]; memset(buffer, 0, PAGE_SIZE);
        char record[1024]; memset(record, 0, 1024);
        std::vector<std::vector<char>> parsed_data;

        //First parse the attribute descriptor to get information about data
        int nullsize=dataparser(recordDescriptor,data,parsed_data);

        //Structure the record as follows:
        // RID ||  length (offset 4 bytes) || nullindicator || entry size(s) || entries

        int size_of_rec=recordCreator(parsed_data,record,(char*)data,nullsize);

        //Search through pages, and get the free space.
        int spaces=0;
        unsigned page=0;//0 indexing pages, compatiable with the filehandle I defined
        unsigned short slot=1;

        int availablePages=fileHandle.getNumberOfPages();
        for (int i=0;i<availablePages;i++) {
            spaces=free_space(i,fileHandle);
            if (spaces>=size_of_rec) {break;}
        }

        //if no space is available, append a page
        if (spaces<size_of_rec) {
            //first write the record to the buffer and then append the buffer
            rid.pageNum=page; rid.slotNum=slot;
            std::memcpy(record, &rid.pageNum, sizeof(unsigned));
            std::memcpy(record+sizeof(unsigned), &rid.slotNum, sizeof(unsigned short));
            fileHandle.appendPage(record);
        }
        else {
            //Read the Available Page into the buffer
            int read_cursor=6;
            int offset=0;
            fileHandle.readPage(page,buffer);

            //read each record and move to the end of the buffer to get the slot
            do {
                memcpy(&offset,buffer+read_cursor,sizeof(int));
                read_cursor+=offset; //since the offset already count in the 6 in its position it moves automatically
                slot++;
            }while(offset!=0);

            slot-=1;//get rid of the last while loop inc
            read_cursor-=6;//move to the first slot
            rid.pageNum=page; rid.slotNum=slot;
            std::memcpy(record, &rid.pageNum, sizeof(unsigned));
            std::memcpy(record+sizeof(unsigned), &rid.slotNum, sizeof(unsigned short));

            std::memcpy(buffer+read_cursor, record, size_of_rec);
            fileHandle.writePage(page,buffer);
        }

        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        // RID || length (offset 4 bytes) || nullindicator || entry size(s) || entries

        char buffer[PAGE_SIZE]; memset(buffer, 0, PAGE_SIZE);

        //read page does not work (debugged Jan 11, page offset off by 1)
        fileHandle.readPage(rid.pageNum,buffer);

        //now I need to extract the record with the known slot
        int read_cursor=6;
        int record_size=0;
        int data_cursor=0;
        int write_cursor=0;

        //move the cursor to that slot
        for (int i=0;i<rid.slotNum-1;i++) {
            memcpy(&record_size,buffer+read_cursor,sizeof(int));
            read_cursor+=record_size;
        }
        read_cursor+=4;

        //first copy the null indicator
        int nullIndicatorSize=std::ceil(static_cast<double>(recordDescriptor.size()) / 8);
        memcpy(data, buffer+read_cursor, nullIndicatorSize);
        read_cursor+=nullIndicatorSize;
        write_cursor+=nullIndicatorSize;

        //initialize a null buffer to check read
        char null_buffer[nullIndicatorSize]; memset(null_buffer, 0, nullIndicatorSize);
        memcpy(null_buffer,buffer+read_cursor,nullIndicatorSize);

        //parse all the entry size and move to the entry part
        data_cursor=read_cursor+(4*recordDescriptor.size());

        //contains parsing bugs (fixed Jan 11)
        //contains data structure bug (only var char should have the initial part indicating length

        for (int i=0;i<recordDescriptor.size();i++) {
            //first check if that entry is null
            char null_byte=null_buffer[i/8];
            bool isOne = null_byte & (1 << 8-(i%8)-1);
            if (isOne) {
                continue;
            }

            if (recordDescriptor[i].type==TypeVarChar) {
                memcpy(&record_size,buffer+read_cursor,sizeof(int));//get the size
                memcpy(data+write_cursor,&record_size,sizeof(int)); //write the size
                write_cursor+=sizeof(int);
                read_cursor+=sizeof(int);
                memcpy(data+write_cursor,buffer+data_cursor,record_size);
                data_cursor+=record_size;
                write_cursor+=record_size;
            }
            else {
                memcpy(&record_size,buffer+read_cursor,sizeof(int));//get the size
                read_cursor+=sizeof(int);
                memcpy(data+write_cursor,buffer+data_cursor,record_size);
                data_cursor+=record_size;
                write_cursor+=record_size;
            }
        }
        return 0;
    }

    //this is debugged ok
    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {

        // dataparser(const std::vector<Attribute> &recordDescriptor,const void *data, std::vector<std::vector<char>> &parsedData);
        // <AttributeName1>:\s<Value1>,\s<AttributeName2>:\s<Value2>,\s<AttributeName3>:\s<Value3>\n

        //still has issue about null record
        std::vector<std::vector<char>> parsedData;
        int nullIndicatorSize=dataparser(recordDescriptor,data,parsedData);
        char null_buffer[nullIndicatorSize]; memset(null_buffer, 0, nullIndicatorSize);
        memcpy(null_buffer,data,nullIndicatorSize);
        int data_cursor=0;

        for (int i = 0; i < recordDescriptor.size(); i++) {
            //first check if that entry is null
            char null_byte=null_buffer[i/8];
            bool isOne = null_byte & (1 << 8-(i%8)-1);
            if (!isOne) {
                if (i==recordDescriptor.size()-1) {
                    if (recordDescriptor[i].type==TypeInt) {
                        int value = 0;
                        memcpy(&value, parsedData[data_cursor].data(), sizeof(int));
                        out<<recordDescriptor[i].name+std::string(": ")<<value<<"\n";
                    }
                    if (recordDescriptor[i].type==TypeReal) {
                        float val = 0;
                        memcpy(&val, parsedData[data_cursor].data(), sizeof(float));
                        out<<recordDescriptor[i].name+std::string(": ")<<val<<"\n";
                    }
                    if (recordDescriptor[i].type==TypeVarChar) {
                        out<<recordDescriptor[i].name+std::string(": ")+parsedData[data_cursor].data()+"\n";
                    }
                }
                else {
                    if (recordDescriptor[i].type==TypeInt) {
                        int value = 0;
                        memcpy(&value, parsedData[data_cursor].data(), sizeof(int));
                        out<<recordDescriptor[i].name+std::string(": ")<<value<<", ";
                        data_cursor++;
                    }
                    if (recordDescriptor[i].type==TypeReal) {
                        float val = 0;
                        memcpy(&val, parsedData[data_cursor].data(), sizeof(float));
                        out<<recordDescriptor[i].name+std::string(": ")<<val<<", ";
                        data_cursor++;
                    }
                    if (recordDescriptor[i].type==TypeVarChar) {
                        out<<recordDescriptor[i].name+std::string(": ")+parsedData[data_cursor].data()+", ";
                        data_cursor++;
                    }
                }
            }else {
                if (i==recordDescriptor.size()-1) {
                    out<<recordDescriptor[i].name+std::string(": ")+"NULL\n";
                }
                else {
                    out<<recordDescriptor[i].name+std::string(": ")+"NULL, ";
                }
            }
        }
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

} // namespace PeterDB


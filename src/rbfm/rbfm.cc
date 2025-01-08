#include "src/include/rbfm.h"
#include <cmath>
#include <cstring>
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

    //define data parser, given record descriptor and data, return a vector of each attribute
    RC RecordBasedFileManager::dataparser(const std::vector<Attribute> &recordDescriptor,const void *data) {
        int numAttributes = recordDescriptor.size();
        int nullIndicatorSize = std::ceil(static_cast<double>(numAttributes) / 8);

        //get the first 8 bytes of data, now the data iteration should start at nullIndicatorSize
        char null_buffer[nullIndicatorSize]; memset(null_buffer, 0, nullIndicatorSize);
        strncpy(null_buffer,(char*)data,nullIndicatorSize);






    }


    //define helper function: record creator, given a vector of information and RID
    //Structure the record as follows: RID\nEntry1\n......\n\null


    //calculate free space for a page, given a page number, return free space in that page.



    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {

        char buffer[PAGE_SIZE]; memset(buffer, 0, PAGE_SIZE);

        //First parse the attribute descriptor to get information about data
        //Attribute has name, type, and length
        //Structure the record as follows: RID\nEntry1\n......\n\EOR


        //Search through pages, and get the free space, return the position of the starting pt of the buffer

        return -1;
    }



























    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
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


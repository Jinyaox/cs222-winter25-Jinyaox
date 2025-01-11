// #include "/src/include/pfm.h"
#include "../include/pfm.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>

typedef unsigned PageNum;
typedef int RC;

namespace PeterDB {
    static bool Exists(const std::string &fileName) {
        std::ifstream file(fileName); // Try to open the file
        return file.is_open();        // Returns true if the file is successfully opened
    }

    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    // Create a new file
    RC PagedFileManager::createFile(const std::string &fileName) {
        if (Exists(fileName)) {return -1;}
        std::ofstream file(fileName);
        file.close();
        return 0;
    }

    // Destroy a file
    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if (std::remove(fileName.c_str()) == 0) {return 0;}
        return -1;
    }

    // Open a file
    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        fileHandle.fileName = fileName;
        fileHandle.initialize();
        return 0;
    }

    // Close a file
    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        fileHandle.finalize();
        fileHandle.fileName = "";
        return 0;
    }

//---------------------------------------------------------------------FileHandle Class------------------------------------------------//


    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        this->fileName="";
    }

    FileHandle::~FileHandle() = default;

    unsigned FileHandle::offsetCalulation(PageNum pageNum) {
        return (pageNum)*PAGE_SIZE;
    }

//------------------------------------------------Perform Initialization and Closing Operations--------------------------------------------//
    RC FileHandle::initialize() {
        if (this->fileName == "") {return -1;}

        char buffer[PAGE_SIZE];memset(buffer,0,PAGE_SIZE);

        //read the first page of the file
        std::ifstream file(this->fileName); // Open file
        file.read(buffer,PAGE_SIZE);
        file.close();

        //parse the buffer and record the values into the counters
        int pos = 0;
        int counter_pos=0;
        int buffer_pos=0;
        int len = strlen(buffer);
        char temp[1024]={};
        int storage[3];
        while (pos < len) {
            if (buffer[pos] == '\n') {
                storage[counter_pos]=std::atoi(temp);
                counter_pos++;
                buffer_pos=0;
                memset(temp,'\0',sizeof(temp));
                continue;
            }
            temp[buffer_pos]=buffer[pos];
            buffer_pos++;
            pos++;
        }
        this->readPageCounter = storage[0];
        this->writePageCounter = storage[1];
        this->appendPageCounter = storage[2];
        return 0;
    }

    RC FileHandle::finalize() {
        // Buffers to hold individual numbers and the final concatenated string
        char read_ctr[1024], write_ctr[1024], append_ctr[1024];
        char buffer[PAGE_SIZE];memset(buffer,0,PAGE_SIZE);

        // Convert integers to C-strings
        sprintf(read_ctr, "%d", this->readPageCounter);
        sprintf(write_ctr, "%d", this->writePageCounter);
        sprintf(append_ctr, "%d", this->appendPageCounter);

        // Concatenate strings with '\n' as separator
        strcat(buffer, read_ctr);
        strcat(buffer, "\n");
        strcat(buffer, write_ctr);
        strcat(buffer, "\n");
        strcat(buffer, append_ctr);
        strcat(buffer, "\n");

        //now write the buffer to the first page of the file
        std::fstream file(fileName, std::ios::in | std::ios::out);
        if (!file.is_open()) {
            return -1;
        }

        if (file.fail()){return -1;}

        for (const char i : buffer) {
            file.put(i);
        }

        file.close();
        return 0;
    }


    RC FileHandle::readPage(PageNum pageNum, void *data){

        if (this->fileName.empty()) {
            return -1; //failed read page, no file initialized
        }

        //calculate the offset
        int offset=this->offsetCalulation(pageNum+1);
        if (offset > (this->getNumberOfPages()+1)*PAGE_SIZE) {
            return -1;
        }

        std::ifstream file(this->fileName); // Open file

        if (file.fail()){return -1;}

        // Move the file pointer to the desired offset
        file.seekg(offset);

        if (file.fail()){return -1;}


        char buffer[PAGE_SIZE];memset(buffer,0,PAGE_SIZE);
        file.read(buffer,PAGE_SIZE);
        file.close();
        memcpy(data,buffer,PAGE_SIZE);
        //now we increment the counter, if successful
        FileHandle::readPageCounter++;
        return 0;
    }


    RC FileHandle::writePage(PageNum pageNum, const void *data){
        //bug still exists, cannot append the page, solved Jan 7 0920.
        char buffer[PAGE_SIZE];memset(buffer,0,PAGE_SIZE);
        if (this->fileName.empty()) {
            return -1; //failed read page, no file initialized
        }

        memcpy(buffer,data,PAGE_SIZE);

        //calculate the offset
        int offset=this->offsetCalulation(pageNum+1);
        if (offset > (this->getNumberOfPages()+1)*PAGE_SIZE) {
            return -1;
        }

        std::fstream file(fileName, std::ios::in | std::ios::out);
        if (!file.is_open()) {
            return -1;
        }

        file.seekp(offset);
        if (file.fail()){return -1;}

        for (const char i : buffer) {
            file.put(i);
        }

        file.close();
        FileHandle::writePageCounter++;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        std::fstream file(fileName, std::ios::in | std::ios::out);
        if (!file) {return -1;}
        char buffer[PAGE_SIZE]; memset(buffer,0,PAGE_SIZE);
        memcpy(buffer,data,PAGE_SIZE);
        file.seekg(0, std::ios::end); //go the the end of the file

        for (const char i : buffer) {
            file.put(i);
        }

        // Close the file
        file.close();

        FileHandle::appendPageCounter++;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        //issue exist: tellg does not go to the end of the file. Debugged success Jan 7
        std::ifstream file(this->fileName); // Open file
        file.seekg(0, std::ios::end); //go the the end of the file
        std::streamsize fileSize = file.tellg(); // Get the position of the file pointer (size in bytes)
        file.close(); // Close the file

        return fileSize / PAGE_SIZE - 1;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount){
        readPageCount=this->readPageCounter;
        writePageCount=this->writePageCounter;
        appendPageCount=this->appendPageCounter;
        return 0;
    }

} // namespace PeterDB



// int main(int argc, char *argv[]) {
//     PeterDB::FileHandle file;
//     file.fileName = "test";
//     file.writePage(0,"Hello World");
//     std::cout<<file.writePageCounter;
//     return 0;
// }
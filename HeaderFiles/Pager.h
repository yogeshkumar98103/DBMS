#ifndef DBMS_PAGER_H
#define DBMS_PAGER_H

#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <cerrno>
#include <memory>
#include <unordered_map>
#include <queue>

const uint32_t PAGE_SIZE = 4096;

struct Page{
    const char *buffer;
    bool hasUncommitedChanges;

    Page(){
        buffer = new char[PAGE_SIZE];
        hasUncommitedChanges = false;
        printf("Page Created\n");
    }

    ~Page(){
        delete[] buffer;
        printf("Page Destructed\n");
    }
};

class Pager{
    static const int pageLimit = 20;    // Maximum number of pages that can be stored at any time
    int fileDescriptor;                 // File descriptor returned by open system call
    uint32_t fileLength;                // Length of file pointed by fileDescriptor
    int32_t maxPages;                   // Maximum number of pages this file has
    std::unordered_map<int32_t, std::unique_ptr<Page>> pages;
    std::queue<int32_t> pageQueue;

    bool open(const char* fileName);

public:
    explicit Pager(const char* fileName);
    ~Pager();

    uint32_t getFileLength() const;
    bool close();
    bool flush(uint32_t pageNum);
    Page* read(uint32_t pageNum);
};

#endif //DBMS_PAGER_H

//
// Created by Yogesh Kumar on 2020-01-31.
//

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

class Pager{
    static const int pageLimit = 20;    // Maximum number of pages that can be stored at any time
    int fileDescriptor;                 // File descriptor returned by open system call
    uint32_t fileLength;                // Length of file pointed by fileDescriptor
    int32_t maxPages;                   // Maximum number of pages this file has
    std::unordered_map<int32_t, std::unique_ptr<char[]>> pages;
    std::queue<int32_t> pageQueue;

public:
    Pager();
    uint32_t getFileLength() const;
    void open(const char* fileName);
    void close(uint32_t numFullPages, uint32_t numAdditionalRows, uint32_t rowSize);
    void flush(uint32_t pageNum, uint32_t pageSize);
    char* readPage(uint32_t pageNum);

    bool writePage(uint32_t pageNum);
};

#endif //DBMS_PAGER_H

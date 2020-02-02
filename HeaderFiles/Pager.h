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
#include <errno.h>
#include <memory>

#define TABLE_MAX_PAGES 100
const uint32_t PAGE_SIZE = 4096;

class Pager{
    int fileDescriptor;
    uint32_t fileLength;
    std::unique_ptr<char[]> pages[TABLE_MAX_PAGES];

public:
    Pager();
    uint32_t getFileLength() const;
    void open(const char* fileName);
    void close(uint32_t numFullPages, uint32_t numAdditionalRows, uint32_t rowSize);
    void flush(uint32_t pageNum, uint32_t pageSize);
    char* getPage(uint32_t pageNum);
};

#endif //DBMS_PAGER_H

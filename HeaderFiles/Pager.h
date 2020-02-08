#ifndef DBMS_PAGER_H
#define DBMS_PAGER_H

/// ---------------- CLASS DESCRIPTION ----------------
/// Pager directly deals with File IO
/// It can read/write given page in a file
/// It also maintains a cache of recently used pages

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
#include <list>
#include "Constants.h"

struct Page{
    std::unique_ptr<char[]> buffer;
    bool hasUncommitedChanges;
    int32_t pageNum;

    Page(){
        buffer = std::make_unique<char[]>(PAGE_SIZE);
        hasUncommitedChanges = false;
        pageNum = 0;
        printf("Page Created\n");
    }

    ~Page(){
        printf("Page Destructed\n");
    }
};

template <typename page_t>
class Pager{
protected:
    using list_t     = std::list<std::unique_ptr<page_t>>;
    using iterator_t = typename list_t::iterator;
    const int pageLimit;                // Maximum number of pages that can be stored at any time
    int fileDescriptor;                 // File descriptor returned by open system call
    int64_t fileLength;                 // Length of file pointed by fileDescriptor
    int32_t maxPages;                   // Maximum number of pages this file has
    std::unordered_map<int32_t, iterator_t> pageMap;
    list_t pageQueue;
    bool open(const char* fileName);

public:
    std::unique_ptr<Page> header;

    Pager(const char* fileName, int pageLimit_ = DEFAULT_PAGE_LIMIT);
    ~Pager();

    int64_t getFileLength();
    virtual bool getHeader();
    bool close();
    bool flush(uint32_t pageNum);
    virtual bool flushPage(page_t* page);
    bool flushAll();
    page_t* read(uint32_t pageNum);
};

#endif //DBMS_PAGER_H

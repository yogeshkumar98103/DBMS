#include "HeaderFiles/Pager.h"

Pager::Pager(const char* fileName):pageLimit(DEFAULT_PAGE_LIMIT){
    this->fileDescriptor = -1;
    this->fileLength = 0;
    this->maxPages = 0;
    if(!this->open(fileName)){
        throw std::runtime_error("Unable to Open Table");
    }
}

Pager::Pager(const char* fileName, int pageLimit_):pageLimit(pageLimit_){
    this->fileDescriptor = -1;
    this->fileLength = 0;
    this->maxPages = 0;
    if(!this->open(fileName)){
        throw std::runtime_error("Unable to Open Table");
    }
}

Pager::~Pager(){
    this->close();
}

int64_t Pager::getFileLength(){
    off_t fileLength_ = lseek(fileDescriptor, 0, SEEK_END);
    this->fileLength = static_cast<int64_t>(fileLength_);
    return this->fileLength;
};

bool Pager::open(const char* fileName){
    int openFlags = O_RDWR | O_CREAT;
    mode_t filePerms = S_IWUSR | S_IRUSR;
    int fd = ::open(fileName, openFlags, filePerms);
    if (fd == -1) {
        return false;
    }
    off_t fileLength_ = lseek(fd, 0, SEEK_END);
    this->fileDescriptor = fd;
    this->fileLength = static_cast<int64_t>(fileLength_);
    return true;
}

bool Pager::close(){
    if(this->fileDescriptor == -1) return false;

    while(!pageQueue.empty()){
        int32_t pageNum = pageQueue.front();
        pageQueue.pop();
        if(this->pages[pageNum]->hasUncommitedChanges){
            this->flush(pageNum);
        }
        pages.erase(pageNum);
    }

    int result = ::close(fileDescriptor);
    this->fileDescriptor = -1;
    return (result != -1);
}

/// This return the asked page from opened file
/// You have to manually look for required table row or other information in the logic that calls this function.
/// pageNum is 0 indexed
Page* Pager::read(uint32_t pageNum){
    if(this->fileDescriptor == -1) return nullptr;
    if(pages.find(pageNum) == pages.end()){
        // Cache miss. Allocate memory and load from file.
        pages[pageNum] = std::make_unique<Page>();
        this->fileLength = static_cast<uint32_t>(lseek(fileDescriptor, 0, SEEK_END));
        this->maxPages = (this->fileLength + PAGE_SIZE - 1) / PAGE_SIZE;

        if(pageNum < maxPages){
            // This page reside in memory so read it
            lseek(fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
            ssize_t bytesRead = ::read(fileDescriptor, pages[pageNum]->buffer.get(), PAGE_SIZE);
            if(bytesRead == -1){
                printf("Error reading file: %d\n", errno);
                pages.erase(pageNum);
                return nullptr;
            }
        }

        // Handle Page Queue
        pageQueue.push(pageNum);
        if(pageQueue.size() >= pageLimit){
            // Remove the page that was used earliest
            int pageNumToRemove = pageQueue.front();
            pageQueue.pop();
            if(pages[pageNumToRemove]->hasUncommitedChanges){
                this->flush(pageNumToRemove);
            }
            pages.erase(pageNumToRemove);
        }
    }

    return pages[pageNum].get();
}

/// This flushes the given page to storage if it is open
bool Pager::flush(uint32_t pageNum){
    if(this->fileDescriptor == -1) return false;
    if(pages.find(pageNum) == pages.end()){
        // Page Not Loaded Yet
        printf("Tried To write page which is not read: %d\n", errno);
        return false;
    }

    off_t offset = lseek(fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        return false;
    }

    ssize_t bytesWritten = write(fileDescriptor, pages[pageNum]->buffer.get(), PAGE_SIZE);
    if (bytesWritten == -1) {
        printf("Error writing: %d\n", errno);
        return false;
    }

    pages[pageNum]->hasUncommitedChanges = false;
    return true;
}

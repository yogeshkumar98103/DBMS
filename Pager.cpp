#include "HeaderFiles/Pager.h"

template<typename page_t>
Pager<page_t>::Pager(int pageLimit_): pageLimit(pageLimit_){
    this->fileDescriptor = -1;
    this->fileLength = 0;
    this->maxPages = 0;
}

template <typename page_t>
Pager<page_t>::Pager(const char* fileName, int pageLimit_): pageLimit(pageLimit_){
    this->fileDescriptor = -1;
    this->fileLength = 0;
    this->maxPages = 0;
    if(!this->open(fileName)){
        throw std::runtime_error("Unable to Open Table");
    }
    this->getHeader();
}

template <typename page_t>
Pager<page_t>::~Pager(){
    this->close();
}

template <typename page_t>
int64_t Pager<page_t>::getFileLength(){
    off_t fileLength_ = lseek(fileDescriptor, 0, SEEK_END);
    this->fileLength = static_cast<int64_t>(fileLength_);
    return this->fileLength;
};

template <typename page_t>
bool Pager<page_t>::open(const char* fileName){
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

template <typename page_t>
bool Pager<page_t>::close(){
    if(this->fileDescriptor == -1) return false;
    flushAll();
    pageQueue.clear();
    pageMap.clear();
    int result = ::close(fileDescriptor);
    this->fileDescriptor = -1;
    return (result != -1);
}

/// This return the asked page from opened file
/// You have to manually look for required table row or other information in the logic that calls this function.
/// pageNum is 0 indexed
template <typename page_t>
bool Pager<page_t>::getHeader(){
    header = std::make_unique<page_t>();
    this->fileLength = static_cast<uint32_t>(lseek(fileDescriptor, 0, SEEK_END));
    this->maxPages = (this->fileLength + PAGE_SIZE - 1) / PAGE_SIZE;
    if(maxPages > 0){
        lseek(fileDescriptor, 0, SEEK_SET);
        ssize_t bytesRead = ::read(fileDescriptor, header->buffer.get(), PAGE_SIZE);
        if(bytesRead == -1){
            printf("Error reading Header: %d\n", errno);
            return false;
        }
    }
    return true;
}

template <typename page_t>
page_t* Pager<page_t>::read(uint32_t pageNum, std::function<void(page_t*)> callback){
    if(this->fileDescriptor == -1) return nullptr;
    if(pageNum == 0) return this->header.get();
    std::unique_ptr<page_t> page;
    auto itr = pageMap.find(pageNum);
    if(itr == pageMap.end()){
        // Cache miss. Allocate memory and load from file.
        page = std::make_unique<page_t>();
        page->pageNum = pageNum;
        this->fileLength = static_cast<uint32_t>(lseek(fileDescriptor, 0, SEEK_END));
        this->maxPages = (this->fileLength + PAGE_SIZE - 1) / PAGE_SIZE;

        if(pageNum < maxPages){
            // This page reside in memory so read it
            lseek(fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
            ssize_t bytesRead = ::read(fileDescriptor, page->buffer.get(), PAGE_SIZE);
            if(bytesRead == -1){
                printf("Error reading file: %d\n", errno);
                return nullptr;
            }
            if(callback) callback(page.get());
        }

        // Handle Page Queue
        if (pageQueue.size() >= pageLimit) {
            auto it = std::move(pageQueue.back());
            if(it->hasUncommitedChanges){
                this->flushPage(it.get());
            }
            pageQueue.pop_back();
            pageMap.erase(it->pageNum);
        }
    }
    else{
        page = std::move(*(itr->second));
        pageQueue.erase(itr->second);
    }
    pageQueue.emplace_front(std::move(page));
    pageMap[pageNum] = pageQueue.begin();
    return pageQueue.begin()->get();
}

/// This flushes the given page to storage if it is open
template <typename page_t>
bool Pager<page_t>::flush(uint32_t pageNum){
    if(this->fileDescriptor == -1) return false;
    if(pageNum == 0){
        return flushPage(header.get());
    }
    if(pageMap.find(pageNum) == pageMap.end()){
        // Page Not Loaded Yet
        printf("Tried To write page which is not read: %d\n", errno);
        return false;
    }
    auto it = pageMap[pageNum]->get();
    return flushPage(it);
}

template <typename page_t>
bool Pager<page_t>::flushAll(){
    if(this->fileDescriptor == -1) return false;
    flushPage(header.get());
    int size = pageQueue.size();
    for(auto& it: pageQueue){
        int32_t pageNum = it->pageNum;
        if(it->hasUncommitedChanges){
            if(!flushPage(it.get())) return false;
        }
    }
    return true;
}

template <typename page_t>
bool Pager<page_t>::flushPage(page_t* page){
    off_t offset = lseek(fileDescriptor, ((page->pageNum) * PAGE_SIZE), SEEK_SET);
    if (offset == -1) return false;
    ssize_t bytesWritten = write(fileDescriptor, page->buffer.get(), PAGE_SIZE);
    if (bytesWritten == -1) return false;
    page->hasUncommitedChanges = false;
    return true;
}

//void Pager::printQueue(){
//    for(auto& it: pageQueue){
//        std::cout << it->pageNum << "->";
//    }
//    std::cout << std::endl;
//}

//void testPager(){
//    Pager pager("Mytable", 4);
//    std::cout << pager.read(0)->pageNum << std::endl;
//    pager.printQueue();
//    std::cout << pager.read(1)->pageNum << std::endl;
//    pager.printQueue();
//    std::cout << pager.read(2)->pageNum << std::endl;
//    pager.printQueue();
//    std::cout << pager.read(3)->pageNum << std::endl;
//    pager.printQueue();
//    std::cout << pager.read(4)->pageNum << std::endl;
//    pager.printQueue();
//    std::cout << pager.read(2)->pageNum << std::endl;
//    pager.printQueue();
//    std::cout << pager.read(0)->pageNum << std::endl;
//    pager.printQueue();
//    std::cout << pager.read(1)->pageNum << std::endl;
//    pager.printQueue();
//    std::cout << pager.read(2)->pageNum << std::endl;
//    pager.printQueue();
//    std::cout << pager.read(3)->pageNum << std::endl;
//    pager.printQueue();
//    std::cout << pager.read(1)->pageNum << std::endl;
//    pager.printQueue();
//    std::cout << pager.read(2)->pageNum << std::endl;
//    pager.printQueue();
//    std::cout << pager.read(3)->pageNum << std::endl;
//    pager.printQueue();
//}

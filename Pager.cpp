//
// Created by Yogesh Kumar on 2020-01-31.
//
#include "HeaderFiles/Pager.h"

Pager::Pager(){
    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        this->pages[i] = nullptr;
    }
};

uint32_t Pager::getFileLength() const{
    return this->fileLength;
};

void Pager::open(const char* fileName){
    int openFlags = O_RDWR | O_CREAT;
    mode_t filePerms = S_IWUSR | S_IRUSR;
    int fd = ::open(fileName, openFlags, filePerms);
    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }
    off_t fileLength = lseek(fd, 0, SEEK_END);
    this->fileDescriptor = fd;
    this->fileLength = static_cast<uint32_t>(fileLength);

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        this->pages[i] = nullptr;
    }
}

void Pager::close(uint32_t numFullPages, uint32_t numAdditionalRows, uint32_t rowSize){
    for(uint32_t i = 0; i < numFullPages; i++) {
        if(pages[i] == nullptr)continue;
        flush(i, PAGE_SIZE);
        pages[i].reset();
        pages[i] = nullptr;
    }

    if(numAdditionalRows > 0) {
        uint32_t pageNum = numFullPages;
        if (pages[pageNum] != nullptr) {
            flush(pageNum, numAdditionalRows * rowSize);
            pages[pageNum].reset();
            pages[pageNum] = nullptr;
        }
    }

    int result = ::close(fileDescriptor);
    if(result == -1){
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        if(pages[i] != nullptr){
            pages[i].reset();
            pages[i] = nullptr;
        }
    }
}

void Pager::flush(uint32_t pageNum, uint32_t pageSize){
    if (pages[pageNum] == nullptr){
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }
    ssize_t bytesWritten = write(fileDescriptor, pages[pageNum].get(), pageSize);
    if (bytesWritten == -1) {
        printf("Error writing: %d\n", errno);
        (EXIT_FAILURE);
    }
}

char* Pager::getPage(uint32_t pageNum){
    if(pageNum > TABLE_MAX_PAGES){
        printf("Tried to fetch page number out of bounds. %d > %d\n", pageNum, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    if(pages[pageNum] == nullptr){
        // Cache miss. Allocate memory and load from file.
        pages[pageNum] = std::make_unique<char[]>(PAGE_SIZE);

        uint32_t numPages = fileLength / PAGE_SIZE;
        if(fileLength % PAGE_SIZE) numPages += 1;

        if(pageNum <= numPages){
            lseek(fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
            ssize_t bytesRead = read(fileDescriptor, pages[pageNum].get(), PAGE_SIZE);
            if(bytesRead == -1){
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
    }
    return pages[pageNum].get();
}

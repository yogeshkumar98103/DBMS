//
// Created by Yogesh Kumar on 2020-01-31.
//
#include "HeaderFiles/Cursor.h"

Cursor::Cursor(Table* table){
    this->table = table;
    this->page = nullptr;
    this->row = 0;
    this->endOfTable = false;
}

Cursor Cursor::operator++(){
    if(this->row < this->table->numRows){
        ++this->row;
        this->endOfTable = true;
    }
    return (*this);
}

char* Cursor::value(){
    uint32_t pageNum = row / table->rowsPerPage;
    this->page = table->pager->read(pageNum);
    if(page == nullptr){return nullptr;}
    // Read Successful
    uint32_t rowOffset = row % table->rowsPerPage;
    uint32_t byteOffset = rowOffset * table->rowSize;
    return page->buffer.get() + byteOffset;
}

void Cursor::addedChangesToCommit(){
    if(page != nullptr) page->hasUncommitedChanges = true;
}

void Cursor::commitChanges(){
    if(page != nullptr){
        uint32_t pageNum = row / table->rowsPerPage;
        this->table->pager->flush(pageNum);
    }
}
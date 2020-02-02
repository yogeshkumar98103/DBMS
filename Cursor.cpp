//
// Created by Yogesh Kumar on 2020-01-31.
//
#include "HeaderFiles/Cursor.h"

Cursor::Cursor(Table* table){
    this->table = table;
    this->rowNum = 0;
    this->endOfTable = false;
}

Cursor Cursor::operator++(){
    this->rowNum += 1;
    if(this->rowNum >= this->table->numRows){
        this->endOfTable = true;
    }
    return (*this);
}

char* Cursor::value(){
    uint32_t pageNum = rowNum / table->rowsPerPage;
    char* page = table->pager->getPage(pageNum);
    uint32_t rowOffset = rowNum % table->rowsPerPage;
    uint32_t byteOffset = rowOffset * ROW_SIZE;
    return page + byteOffset;
}


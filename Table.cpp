//
// Created by Yogesh Kumar on 2020-01-31.
//
#include "HeaderFiles/Table.h"

// =============================================
//                  TABLE
// =============================================

Table::Table(const std::string& fileName){
    this->pager = std::make_unique<Pager>();
    this->open(fileName.c_str());
}

Table::~Table(){
    this->close();
}

bool Table::open(const std::string& filename){
    tableName = filename;
    if(!pager->open(filename.c_str())) return false;
    this->numRows = pager->getFileLength() / ROW_SIZE;
    printf("Number Of Rows: %d\n",this->numRows);
    return true;
}

bool Table::close(){
    uint32_t numFullPages = numRows / rowsPerPage;
    uint32_t numAdditionalRows = numRows % rowsPerPage;
    return pager->close(numFullPages, numAdditionalRows, ROW_SIZE);
}

Cursor Table::start(){
    Cursor cursor(this);
    cursor.rowNum = 0;
    cursor.endOfTable = (numRows == 0);
    return cursor;
}

Cursor Table::end(){
    Cursor cursor(this);
    cursor.rowNum = numRows;
    cursor.endOfTable = true;
    return cursor;
}

void Table::createColumns(std::vector<std::string>&& columnNames, std::vector<DataType>&& columnTypes, std::vector<uint32_t>&& columnSize) {
    this->columnNames = std::move(columnNames);
    this->columnSize = std::move(columnSize);
    this->columnTypes = std::move(columnTypes);
    createColumnIndex();
}

void Table::createColumnIndex(){
    for(int index = 0; index < columnNames.size(); ++index){
        columnIndex[columnNames[index]] = index;
    }
}

// =============================================
//                  ROW
// =============================================

Row::Row(char* source){
    deserialize(source);
}

void Row::serialize(char* destination) {
//    memcpy(destination + ID_OFFSET, &(this->id), ID_SIZE);
//    strncpy(destination + USERNAME_OFFSET, this->username, USERNAME_SIZE);
//    strncpy(destination + EMAIL_OFFSET, this->email, EMAIL_SIZE);
}

void Row::deserialize(char* source){
//    memcpy(&(this->id), source + ID_OFFSET, ID_SIZE);
//    memcpy(&(this->username), source + USERNAME_OFFSET, USERNAME_SIZE);
//    memcpy(&(this->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void Row::print(){
    printf("Id: %d | Username: %s | Email: %s\n", id, username, email);
}
//
// Created by Yogesh Kumar on 2020-01-31.
//
#include "HeaderFiles/Table.h"

// =============================================
//                  TABLE
// =============================================

Table::Table(std::string tableName, const std::string& fileName){
    try{
        this->pager = std::make_unique<Pager>(fileName.c_str());
    }
    catch(...){

        throw;
    }
    // TODO: Read first page to get metadata
    // Metadata:-
    // 1. Column Names
    // 2. Column Types
    // 3. Column Size
    // 4. Empty Rows
    this->tableOpen = true;
    this->tableName = std::move(tableName);
    this->numRows = 0;
    this->rowSize = 0;
    this->rowsPerPage = 0;
}

Table::~Table(){
    this->close();
}

bool Table::close(){
    if(tableOpen) return pager->close();
    return false;
}

Cursor Table::start(){
    Cursor cursor(this);
    cursor.row = 0;
    cursor.endOfTable = (numRows == 0);
    return cursor;
}

Cursor Table::end(){
    Cursor cursor(this);
    cursor.row = numRows;
    cursor.endOfTable = true;
    return cursor;
}

void Table::createColumns(std::vector<std::string>&& columnNames_, std::vector<DataType>&& columnTypes_, std::vector<uint32_t>&& columnSizes_){
    this->columnNames = std::move(columnNames_);
    this->columnSizes = std::move(columnSizes_);
    this->columnTypes = std::move(columnTypes_);
    this->createColumnIndex();
}

void Table::createColumnIndex(){
    for(int index = 0; index < columnNames.size(); ++index){
        columnIndex[columnNames[index]] = index;
    }
}

void Table::storeMetadata() {
    Page* page = pager->read(0);
    serailizeColumnMetadata(page->buffer.get());
    page->hasUncommitedChanges = true;
    pager->flush(0);
}

void Table::loadMetadata() {
    Page* page = pager->read(0);
    deSerailizeColumnMetadata(page->buffer.get());
}

void Table::serailizeColumnMetadata(char* buffer){
    int32_t size1 = columnNames.size();
    int32_t size2 = columnTypes.size();
    int32_t size3 = columnSizes.size();

    if(!(size1 == size2 && size2 == size3)){
        printf("Failed To Serialize Metadata.\nInconsistent Metadata\n");
        return;
    }
    static const int32_t sizeInt = sizeof(uint32_t);
    static const int32_t sizeDatatype = sizeof(DataType);

    int32_t offset = 0;
    memcpy(buffer, &size1, sizeInt);
    buffer += sizeInt;

    for(const std::string& str: columnNames){
        strncpy(buffer + offset, str.c_str(), MAX_COLUMN_SIZE);
        offset += MAX_COLUMN_SIZE;
    }

    for(uint32_t size: columnSizes){
        memcpy(buffer + offset, &size, sizeInt);
        offset += sizeInt;
    }

    for(const DataType& type: columnTypes){
        memcpy(buffer + offset, &type, sizeDatatype);
        offset += sizeDatatype;
    }
}

void Table::deSerailizeColumnMetadata(char* metadataBuffer) {
    static const int32_t sizeInt = sizeof(uint32_t);
    static const int32_t sizeDatatype = sizeof(DataType);
    int32_t offset = 0;
    int32_t size;
    memcpy(&size, &metadataBuffer, sizeInt);
    metadataBuffer += sizeInt;

    columnNames.reserve(size);
    columnSizes.reserve(size);
    columnTypes.reserve(size);

    char colName[MAX_COLUMN_SIZE + 1];
    for(int32_t i = 0; i < size; ++i){
        strncpy(colName, (metadataBuffer + offset), MAX_COLUMN_SIZE);
        columnNames.emplace_back(colName);
        offset += MAX_COLUMN_SIZE;
    }

    char colSize[sizeInt + 1];
    for(int32_t i = 0; i < size; ++i){
        strncpy(colSize, (metadataBuffer + offset), sizeInt);
        int32_t colSizeInt = std::stoi(colSize);
        columnSizes.emplace_back(colSizeInt);
        offset += sizeInt;
    }

    char colType[sizeDatatype + 1];
    for(int32_t i = 0; i < size; ++i){
        strncpy(colType, (metadataBuffer + offset), sizeInt);
        auto colType_ = static_cast<DataType>(std::stoi(colType));
        columnTypes.emplace_back(colType_);
        offset += sizeInt;
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
//    printf("Id: %d | Username: %s | Email: %s\n", id, username, email);
}
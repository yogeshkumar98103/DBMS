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

bool Table::createIndexPager(int32_t index, const std::string& fileName){
    if(!indexed[index]) return false;
    try{
        this->indexPagers[index] = std::make_unique<Pager>(fileName.c_str());
    }
    catch(...){
        return false;
    }
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
    Page* page = pager->header.get();
    serailizeColumnMetadata(page->buffer.get());
    page->hasUncommitedChanges = true;
    pager->flush(0);
    calculateRowInfo();
}

void Table::storeIndexMetadata(int32_t index){
    Page* page = indexPagers[index]->header.get();
    serializeIndexMetadata(page->buffer.get(), index);
    page->hasUncommitedChanges = true;
    pager->flush(0);
}

void Table::loadMetadata() {
    Page* page = pager->header.get();
    deSerailizeColumnMetadata(page->buffer.get());
    this->createColumnIndex();
    calculateRowInfo();
}

void Table::calculateRowInfo(){
    this->rowSize = 0;
    for(int32_t size: columnSizes){
        this->rowSize += size;
    }
    this->rowsPerPage = PAGE_SIZE/rowSize;
}

void Table::serailizeColumnMetadata(char* buffer){
    int32_t size1 = columnNames.size();
    int32_t size2 = columnTypes.size();
    int32_t size3 = columnSizes.size();

    if(!(size1 == size2 && size2 == size3)){
        printf("Failed To Serialize Metadata.\nInconsistent Metadata\n");
        return;
    }

    int32_t offset = 0;

    // Write Number of Rows
    this->numRows = 0;
    memcpy(buffer + offset, &numRows, sizeof(row_t));
    offset += sizeof(row_t);

    // Write Number of columns
    memcpy(buffer + offset, &size1, sizeof(int32_t));
    offset += sizeof(int32_t);

    for(const std::string& str: columnNames){
        strncpy(buffer + offset, str.c_str(), MAX_COLUMN_SIZE);
        offset += MAX_COLUMN_SIZE;
    }

    for(uint32_t size: columnSizes){
        memcpy(buffer + offset, &size, sizeof(int32_t));
        offset += sizeof(int32_t);
    }

    for(const DataType& type: columnTypes){
        memcpy(buffer + offset, &type, sizeof(DataType));
        offset += sizeof(DataType);
    }
    rowStackPtr = 0;
    memcpy(buffer + offset, &rowStackPtr, sizeof(int32_t));
    rowStackOffset = offset + sizeof(int32_t);
}

void Table::deSerailizeColumnMetadata(char* metadataBuffer) {
    char colName[MAX_COLUMN_SIZE + 1] = {0};
    int32_t colSize;
    DataType colType;
    int32_t offset = 0;
    int32_t size;

    memcpy(&this->numRows, metadataBuffer + offset, sizeof(row_t));
    offset += sizeof(row_t);

    memcpy(&size, metadataBuffer + offset, sizeof(int32_t));
    offset += sizeof(int32_t);

    columnNames.reserve(size);
    columnSizes.reserve(size);
    columnTypes.reserve(size);

    for(int32_t i = 0; i < size; ++i){
        strncpy(colName, (metadataBuffer + offset), MAX_COLUMN_SIZE);
        columnNames.emplace_back(colName);
        offset += MAX_COLUMN_SIZE;
    }

    for(int32_t i = 0; i < size; ++i){
        memcpy(&colSize, (metadataBuffer + offset), sizeof(int32_t));
        columnSizes.emplace_back(colSize);
        offset += sizeof(int32_t);
    }

    for(int32_t i = 0; i < size; ++i){
        memcpy(&colType, (metadataBuffer + offset), sizeof(DataType));
        columnTypes.emplace_back(colType);
        offset += sizeof(DataType);
    }
    memcpy(&rowStackPtr, (metadataBuffer + offset), sizeof(int32_t));
    rowStackOffset = offset + sizeof(int32_t);
}

void Table::serializeIndexMetadata(char* buffer, int32_t index){
    // 1. Stack Ptr
    // 2. Blank Spaces
    int32_t offset = 0;
    memcpy(buffer + offset, &stackPtr[index], sizeof(int32_t));
    offset += sizeof(int32_t);
}

void Table::deSerializeIndexMetadata(char* buffer, int32_t index){
    // 1. Stack Ptr
    // 2. Blank Spaces
    int32_t offset = 0;
    memcpy(&stackPtr[index], buffer + offset, sizeof(int32_t));
    offset += sizeof(int32_t);
}

row_t Table::nextFreeIndexLocation(int32_t index){
    if(stackPtr[index] == 0) return numRows;
    Page* page = indexPagers[index]->header.get();
    char* buffer = page->buffer.get();
    int32_t offset = (stackPtr[index] - 1) * sizeof(row_t) + sizeof(int32_t);
    row_t nextRow;
    memcpy(&nextRow, buffer + offset, sizeof(int32_t));
    stackPtr[index]--;
    memcpy(buffer, &stackPtr[index], sizeof(int32_t));
    return nextRow;
}

row_t Table::nextFreeRowLocation(){
    if(rowStackPtr == 0) return numRows;
    Page* page = pager->header.get();
    char* buffer = page->buffer.get();
    int32_t offset = (rowStackPtr - 1) * sizeof(row_t) + rowStackOffset;
    row_t nextRow;
    memcpy(&nextRow, buffer + offset, sizeof(int32_t));
    rowStackPtr--;
    memcpy(buffer, &rowStackPtr, sizeof(int32_t));
    return nextRow;
}

void Table::addFreeRowLocation(row_t location){
    Page* page = pager->header.get();
    char* buffer = page->buffer.get();
    int32_t offset = rowStackPtr * sizeof(row_t) + sizeof(int32_t);
    rowStackPtr++;
    if(offset + sizeof(row_t) > PAGE_SIZE){
        printf("Stack Overflow occurred in main table.\n");
        throw std::runtime_error("STACK OVERFLOWS HEADER PAGE");
    }
    memcpy(buffer + offset, &location, sizeof(row_t));
}

void Table::addFreeIndexLocation(row_t location, int index){
    Page* page = indexPagers[index]->header.get();
    char* buffer = page->buffer.get();
    int32_t offset = stackPtr[index] * sizeof(row_t) + sizeof(int32_t);
    stackPtr[index]++;
    if(offset + sizeof(row_t) > PAGE_SIZE){
        printf("Stack Overflow occurred at %d\n", index);
        throw std::runtime_error("STACK OVERFLOWS HEADER PAGE");
    }
    memcpy(buffer + offset, &location, sizeof(row_t));
}

int32_t Table::getRowSize() const{
    return this->rowSize;
}

void Table::increaseRowCount() {
    this->numRows++;
    Page* page = pager->header.get();
    char* buffer = page->buffer.get();
    memcpy(buffer, &numRows, sizeof(int64_t));
    page->hasUncommitedChanges = true;
}
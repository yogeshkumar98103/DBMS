//
// Created by Yogesh Kumar on 2020-01-31.
//

#ifndef DBMS_TABLE_H
#define DBMS_TABLE_H

#include <memory>
#include <string>
#include <vector>
#include <map>
#include "Pager.h"
#include "DataTypes.h"
#include "../BTree.cpp"

class Table;

class Cursor{
public:
    /// This is pointer is the parent Table of whose row this cursor is pointing to
    Table* table;

    /// This is pointer is the page this cursor is pointing to
    Page* page;

    /// Row this cursor is pointing to
    /// Row is zero indexed
    row_t row;

    /// This is true if cursor is pointing to last row
    /// calling operator++ at this point won't increase row further
    bool endOfTable;

    explicit Cursor(Table* table);

    /// This increase the member row if not pointing last row
    Cursor operator++();

    /// Step1: This calculates the page on which it is pointing to
    /// Step2; It calls table->pager to load required page
    /// Step3: It calculates offset on page to find current row
    /// Step4: It returns pointer to that row in memory location
    char* value();

    void addedChangesToCommit();
    void commitChanges();
};

class Table{
    friend class Cursor;
    friend class TableManager;
    int32_t rowSize;
    int32_t rowsPerPage;
    row_t numRows = 0;
    int32_t rowStackPtr;
    int32_t rowStackOffset;
    bool tableOpen;

    std::string tableName;
    std::unique_ptr<Pager> pager;
public:

    std::vector<std::string> columnNames;
    std::vector<DataType> columnTypes;
    std::vector<uint32_t> columnSizes;
    std::map<std::string, int> columnIndex;
    std::vector<bool> indexed;
    std::vector<std::unique_ptr<Pager>> indexPagers;
    std::vector<int32_t> stackPtr;
    std::vector<std::unique_ptr<BaseBPTree>> bPlusTrees;

    Table(std::string tableName, const std::string& fileName);
    ~Table();

    bool close();
    void storeMetadata();
    void storeIndexMetadata(int32_t index);
    void loadMetadata();
    void createColumns(std::vector<std::string>&& columnNames, std::vector<DataType>&& columnTypes, std::vector<uint32_t>&& columnSizes);

    int32_t getRowSize() const;
    void increaseRowCount();
    row_t nextFreeIndexLocation(int32_t index);
    row_t nextFreeRowLocation();
    void addFreeRowLocation(row_t location);
    void addFreeIndexLocation(row_t location, int index);
    bool insertBTree(std::vector<std::string>& data, row_t row);
    Cursor start();
    Cursor end();

private:
    void createColumnIndex();
    void calculateRowInfo();
    bool createIndexPager(int32_t index, const std::string& fileName);
    void serailizeColumnMetadata(char* buffer);
    void deSerailizeColumnMetadata(char* buffer);
    void serializeIndexMetadata(char* buffer, int32_t index);
    void deSerializeIndexMetadata(char* buffer, int32_t index);
};

#endif //DBMS_TABLE_H

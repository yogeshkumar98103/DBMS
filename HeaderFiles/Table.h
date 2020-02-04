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

class Table;

class Cursor{
public:
    /// This is pointer is the parent Table of whose row this cursor is pointing to
    Table* table;

    /// This is pointer is the page this cursor is pointing to
    Page* page;

    /// Row this cursor is pointing to
    /// Row is zero indexed
    int32_t row;

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

class Row{
public:
    Table* table;


    Row() = default;
    explicit Row(char* source);
    void serialize(char* destination);
    void deserialize(char* source);
    void print();
};

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

class Table{
    friend class Cursor;
    friend class TableManager;
    int32_t rowSize;
    int32_t rowsPerPage;
    int32_t numRows = 0;
    bool tableOpen;

    std::string tableName;
    std::unique_ptr<Pager> pager;
public:
    std::vector<std::string> columnNames;
    std::vector<DataType> columnTypes;
    std::vector<uint32_t> columnSizes;
    std::map<std::string, int> columnIndex;

    Table(std::string tableName, const std::string& fileName);
    ~Table();

    bool close();
    void storeMetadata();
    void loadMetadata();
    void createColumns(std::vector<std::string>&& columnNames, std::vector<DataType>&& columnTypes, std::vector<uint32_t>&& columnSizes);

    int32_t getRowSize() const;
    void increaseRowCount();

    Cursor start();
    Cursor end();

private:
    void createColumnIndex();
    void calculateRowInfo();
    void serailizeColumnMetadata(char* buffer);
    void deSerailizeColumnMetadata(char* buffer);
};

#endif //DBMS_TABLE_H

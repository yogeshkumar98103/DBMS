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
#include "BTree.h"
#include "Constants.h"

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

#define BTREE_HANDLER(res, tree, handler)                                    \
    case DataType::Int:                                                     \
        res = dynamic_cast<BPTree<int>*>(tree)->handler;                    \
        break;                                                              \
    case DataType::Float:                                                   \
        res = dynamic_cast<BPTree<float>*>(tree)->handler;                 \
        break;                                                              \
    case DataType::Char:                                                    \
        res = dynamic_cast<BPTree<char>*>(tree)->handler;                   \
        break;                                                              \
    case DataType::Bool:                                                    \
        res = dynamic_cast<BPTree<bool>*>(tree)->handler;                   \
        break;                                                              \
    case DataType::String:                                                  \
        res = dynamic_cast<BPTree<dbms::string>*>(tree)->handler;           \
        break;


class Table{
    friend class Cursor;
    friend class TableManager;
    int32_t rowSize;
    int32_t rowsPerPage;
    row_t numRows = 0;
    // int32_t rowStackPtr;
    // int32_t rowStackOffset;

    row_t* rowStack;
    int32_t stackSize;

    bool tableOpen;
    std::string tableName;
    pkey_t nextPKey;

public:
    bool tableIsIndexed;
    int32_t anyIndex;

    std::vector<std::string> columnNames;
    std::vector<DataType> columnTypes;
    std::vector<uint32_t> columnSizes;
    std::map<std::string, int> columnIndex;
    std::vector<bool> indexed;
    std::unique_ptr<Pager<Page>> pager;
    std::vector<int32_t> stackPtr;
    std::vector<std::unique_ptr<BPlusTreeBase>> trees;

    Table(std::string tableName, const std::string& fileName);
    ~Table();

    bool close();
    void storeMetadata();
    void loadMetadata();
    void createColumns(std::vector<std::string>&& columnNames, std::vector<DataType>&& columnTypes, std::vector<uint32_t>&& columnSizes);

    int32_t getRowSize() const;
    void increaseRowCount();
    row_t nextFreeRowLocation();
    void addFreeRowLocation(row_t location);
    bool deleteRow(row_t row);
    bool insertBTree(std::vector<std::string>& data, row_t row);
    bool removeBTree(int index, std::string& key);
    bool updateBTree(std::vector<std::string>& data, row_t row);
    Cursor start();
    Cursor end();

private:
    void createColumnIndex();
    bool createIndex(int index, const std::string& filename);
    void calculateRowInfo();
    void serailizeColumnMetadata(char* buffer);
    void deSerailizeColumnMetadata(char* buffer);
};

#endif //DBMS_TABLE_H

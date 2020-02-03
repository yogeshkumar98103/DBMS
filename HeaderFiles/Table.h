//
// Created by Yogesh Kumar on 2020-01-31.
//

#ifndef DBMS_TABLE_H
#define DBMS_TABLE_H

#include <memory>
#include <string>
#include <vector>
#include "Pager.h"
#include "DataTypes.h"
#include <cstdlib>
#include <cstdio>
//#include "Cursor.h"

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

class Table;

class Cursor{
public:
    Table* table;
    uint32_t rowNum;
    bool endOfTable;

    explicit Cursor(Table* table);
    Cursor operator++();
    char* value();
};

class Row{
public:
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];

    Row() = default;
    explicit Row(char* source);
    void serialize(char* destination);
    void deserialize(char* source);
    void print();
};

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

class Table{
public:
    friend class Cursor;
    const uint32_t rowsPerPage = PAGE_SIZE / ROW_SIZE;
    const uint32_t maxRows = rowsPerPage * TABLE_MAX_PAGES;
    uint32_t numRows = 0;

    std::string openedTable;
    std::unique_ptr<Pager> pager;
    std::vector<std::string> columnNames;
    std::vector<DataType> columnTypes;
    std::vector<uint32_t> columnSize;

    Table() = default;
    ~Table();
    explicit Table(const char* filename);
    void open(const char* filename);
    void close();

    Cursor start();
    Cursor end();
};

#endif //DBMS_TABLE_H

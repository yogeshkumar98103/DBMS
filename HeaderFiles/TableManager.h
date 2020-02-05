//
// Created by Yogesh Kumar on 03/02/20.
//

#ifndef DBMS_TABLEMANAGER_H
#define DBMS_TABLEMANAGER_H

#include <memory>
#include <unordered_map>
#include <queue>
#include <filesystem>
#include "Table.h"
#include "Constants.h"

/// ----------------- CLASS DESCRIPTION -----------------
/// Table Manager deals with tasks like opening/closing/deleting table
/// It can find and return required Table object from table name
/// Usually every Database will have a single Table Manager

/// ---------------- FILE NAMING SCHEME ----------------
/// 1. Base Table => <baseURL>/<table-name>.db
/// 2. Index on col => <baseURL>/<table-name>_<col-number>.idx

enum class TableManagerResult{
    tableNotFound,
    tableAlreadyExists,
    openedSuccessfully,
    closedSuccessfully,
    openingFaliure,
    closingFaliure,
    droppedSuccessfully,
    tableCreatedSuccessfully,
    tableCreationFaliure,
    droppingFaliure
};

enum class TableFileType{
    indexFile,
    baseTable
};

class TableManager {
    /// When Database is opened all table names are stored in tableMap with all entries pointing nullptr
    /// With usage tables are opened and pointers are changed
    /// When a table is closed pointer is again set in nullptr
    std::unordered_map<std::string, std::shared_ptr<Table>> tableMap{};

    /// This stores the baseURL where all database files are stored
    std::string baseURL;

public:

    explicit TableManager(std::string baseURL_);

    /// This opens the table when given tableName if not open already
    /// And share its ownership with table parameter passed as reference
    /// It fails if table does not exist
    TableManagerResult open(const std::string &tableName, std::shared_ptr<Table> &table);

    /// This creates and shares ownership of table with table parameter passed as reference
    /// This fails if table already exists
    TableManagerResult create(const std::string &tableName,
                              std::vector<std::string> &&columnNames_,
                              std::vector<DataType> &&columnTypes_,
                              std::vector<uint32_t> &&columnSize_);

    TableManagerResult drop(const std::string &tableName);

    TableManagerResult close(const std::string &tableName);
    bool createIndex(std::shared_ptr<Table>& table, int32_t index);
    TableManagerResult closeAll();
    void flushAll();

private:

    /// This is helper function to get proper file names
    std::string getFileName(const std::string &tableName, TableFileType type, int index = -1);
};


#endif //DBMS_TABLEMANAGER_H

//
// Created by Yogesh Kumar on 03/02/20.
//

#ifndef DBMS_TABLEMANAGER_H
#define DBMS_TABLEMANAGER_H

#include <memory>
#include <unordered_map>
#include <queue>
#include "Table.h"

enum class TableManagerResult{
    tableNotFound,
    tableAlreadyExists,
    openedSuccessfully,
    closedSuccessfully,
    openingFaliure,
    closingFaliure,
    droppedSuccessfully,
    droppingFaliure
};

enum class TableFileType{
    indexFile,
    baseTable
};

class TableManager{
    std::unordered_map<std::string, std::shared_ptr<Table>> tableMap;

public:
//    static TableManager shared;

    TableManagerResult open(const std::string& tableName, std::shared_ptr<Table>& table);

    TableManagerResult create(const std::string& tableName, std::shared_ptr<Table>& table);

    TableManagerResult drop(const std::string& tableName);

    TableManagerResult close(const std::string& tableName);

    static std::string getFileName(const std::string& tableName, TableFileType type);
};


#endif //DBMS_TABLEMANAGER_H

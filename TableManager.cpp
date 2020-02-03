//
// Created by Yogesh Kumar on 03/02/20.
//

#include "HeaderFiles/TableManager.h"

TableManagerResult TableManager::open(const std::string& tableName, std::shared_ptr<Table>& table){
    if(tableMap.find(tableName) == tableMap.end()){
        return TableManagerResult::tableNotFound;
    }

    table = tableMap[tableName];
    if(table == nullptr){
        table = std::make_shared<Table>();
        if(!table->open(getFileName(tableName, TableFileType::baseTable))){
            table.reset();
            return TableManagerResult::openingFaliure;
        }
        tableMap[tableName] = table;
    }

    return TableManagerResult::openedSuccessfully;
}

TableManagerResult TableManager::create(const std::string& tableName, std::shared_ptr<Table>& table){
    if(tableMap.count(tableName) != 0){
        return TableManagerResult::tableAlreadyExists;
    }
    table = std::make_shared<Table>(getFileName(tableName, TableFileType::baseTable));
    tableMap[tableName] = table;
    return TableManagerResult::openedSuccessfully;
}

TableManagerResult TableManager::drop(const std::string& tableName){
    std::shared_ptr<Table> table;
    auto res = open(tableName, table);
    if(res != TableManagerResult::openedSuccessfully){
        return res;
    }
    // if(table.drop()) return TableManagerResult::droppingFaliure;
    return TableManagerResult::droppedSuccessfully;
}

TableManagerResult TableManager::close(const std::string& tableName){
    std::shared_ptr<Table> table;
    if(tableMap.count(tableName) == 0){
        return TableManagerResult::tableNotFound;
    }
    if(tableMap[tableName] != nullptr){
        if(!tableMap[tableName]->close()){
            return TableManagerResult::closingFaliure;
        }
    }
    return TableManagerResult::closedSuccessfully;
}

std::string TableManager::getFileName(const std::string& tableName, TableFileType type){
    switch(type){
        case TableFileType::indexFile:
            return tableName + "_" + ".idx";
            break;
        case TableFileType::baseTable:
            return tableName + ".bin";
            break;
    }
}


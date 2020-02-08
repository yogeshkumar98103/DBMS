//
// Created by Yogesh Kumar on 03/02/20.
//

#include "HeaderFiles/TableManager.h"
#include <ncurses.h>

TableManager::TableManager(std::string baseURL_):baseURL(std::move(baseURL_)){
    // Create Directory if it doesn't exist
    if(!std::filesystem::exists(baseURL)){
        if(!std::filesystem::create_directory(baseURL)){
            printf("Failed to open Database\n");
            return;
        }
    }

    std::string indexURL = baseURL + "/indexes";
    if(!std::filesystem::exists(baseURL + "/indexes")){
        if(!std::filesystem::create_directory(indexURL)){
            printf("Failed to create Index Folder\n");
            return;
        }
    }
    // Read all files in this directory
    printf("Opened Database at \"%s\" Successfully\n", baseURL.c_str());
    for (auto& itr: std::filesystem::directory_iterator(baseURL)){
        if(itr.is_regular_file() && itr.path().extension() == ".bin"){
            std::string name = itr.path().stem().string();
            tableMap[name] = nullptr;
            printw("Found Table \"%s\"\n", name.c_str());
        }
    }
}

void TableManager::loadIndexes(const std::shared_ptr<Table>& table){
    std::string indexURL = baseURL + "/indexes";
    for (auto& itr: std::filesystem::directory_iterator(indexURL)){
        if(itr.is_regular_file()){
            std::string indexFileName = itr.path().stem().string();
            int i = (int)indexFileName.size() - 1;
            while(i >= 0 && indexFileName[i] != '_') --i;
            std::string foundTableName = indexFileName.substr(0, i);
            if(table->tableName != foundTableName) continue;
            try{
                int32_t colNum = std::stoi(indexFileName.substr(i+1, indexFileName.size()));
                if(colNum < table->columnSizes.size()){
                    table->indexed[colNum] = true;
                    table->createIndex(colNum, itr.path().string());
                    printw("Found Indexfile on column %d\n", colNum + 1);
                }
            }
            catch(...){
                continue;
            }
        }
    }
}

TableManagerResult TableManager::open(const std::string& tableName, std::shared_ptr<Table>& table){
    if(tableMap.find(tableName) == tableMap.end()){
        return TableManagerResult::tableNotFound;
    }

    table = tableMap[tableName];
    if(table == nullptr){
        try{
            table = std::make_shared<Table>(tableName,
                                            getFileName(tableName, TableFileType::baseTable));
            table->loadMetadata();
        }
        catch(...){
            printw("Table Opening Faliure\n");
            return TableManagerResult::openingFaliure;
        }
        tableMap[tableName] = table;
        loadIndexes(table);
    }

    return TableManagerResult::openedSuccessfully;
}

TableManagerResult TableManager::create(const std::string& tableName,
                                        std::vector<std::string>&& columnNames_,
                                        std::vector<DataType>&& columnTypes_,
                                        std::vector<uint32_t>&& columnSize_){
    if(tableMap.find(tableName) != tableMap.end()){
        return TableManagerResult::tableAlreadyExists;
    }
    std::shared_ptr<Table> table;
    try{
        table = std::make_shared<Table>(tableName, getFileName(tableName, TableFileType::baseTable));
    }catch(...){
//        printw("Faliure Allocation Table");
        return TableManagerResult::tableCreationFaliure;
    }

    // Store metadata in first page
    table->createColumns(std::move(columnNames_), std::move(columnTypes_), std::move(columnSize_));
    table->storeMetadata();
    tableMap[tableName] = table;
    return TableManagerResult::tableCreatedSuccessfully;
}

TableManagerResult TableManager::drop(const std::string& tableName){
    std::shared_ptr<Table> table;
    auto res = open(tableName, table);
    if(res != TableManagerResult::openedSuccessfully){
        return res;
    }
    int removeRes = std::remove(getFileName(tableName, TableFileType::baseTable).c_str());
    if(removeRes != 0){
        return TableManagerResult::droppingFaliure;
    }
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

TableManagerResult TableManager::closeAll(){
    for(auto& table: tableMap){
        if(table.second != nullptr && table.second->tableOpen){
            table.second->close();
            table.second.reset();
        }
    }
    tableMap.clear();
    return TableManagerResult::closedSuccessfully;
}
void TableManager::flushAll(){
    for(auto& table: tableMap){
        if(table.second != nullptr && table.second->tableOpen){
            table.second->pager->flushAll();
        }
    }
}

bool TableManager::createIndex(std::shared_ptr<Table>& table, int32_t index){
    if(table == nullptr || index < 0) return false;
    bool res = table->createIndex(index, getFileName(table->tableName, TableFileType::indexFile, index));
    if(!res) return false;
    return true;
}

std::string TableManager::getFileName(const std::string& tableName, TableFileType type, int32_t index){
    switch(type){
        case TableFileType::indexFile:
            if(index < 0) throw std::runtime_error("Invalid Index");
            char fileName[255];
            sprintf(fileName, "%s/indexes/%s_%d.idx", baseURL.c_str(), tableName.c_str(), index);
            return fileName;
        case TableFileType::baseTable:
            return baseURL + "/" + tableName + ".bin";
    }
}
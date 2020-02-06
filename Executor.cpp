//
// Created by Yogesh Kumar on 04/02/20.
//
#include "Parser.cpp"

enum class ExecuteResult{
    success,
    tableFull,
    faliure,
    typeMismatch,
    stringTooLarge,
    invalidColumnName,
    unexpectedError
};

struct ErrorHandler{
    static void handleTableManagerError(const TableManagerResult& res){
        switch(res){
            case TableManagerResult::tableCreatedSuccessfully:
                printf("Table Created Successfully\n");
                break;
            case TableManagerResult::tableAlreadyExists:
                printf("This Table already exists\n");
                break;
            case TableManagerResult::tableCreationFaliure:
                printf("Failed To Create Table\n");
                break;
            case TableManagerResult::tableNotFound:
                printf("Table Not Found\n");
                break;
            case TableManagerResult::openingFaliure:
                printf("Failed To Open Table\n");
                break;
            case TableManagerResult::closedSuccessfully:
                printf("Closed Table Successfully\n");
                break;
            case TableManagerResult::closingFaliure:
                printf("Closed Table Successfully\n");
                break;
            case TableManagerResult::droppingFaliure:
                printf("Error Dropping Table\n");
                break;
            case TableManagerResult::openedSuccessfully:
                break;
            case TableManagerResult::droppedSuccessfully:
                break;
            default:
                printf("Unknown Error Occurred\n");
                break;
        }
    }

    static void handleTableMismatchError(int32_t actualSize, int32_t expectedSize){
        printf("Number of values provided does not match number of columns\n"
               "Expected %d value(s). Got %d value(s)\n", expectedSize, actualSize);
    }

    static void indexCreationError(const std::string& colName){
        printf("Failed to create index on %s\n", colName.c_str());
    }

    static void indexCreationSuccessful(const std::string& colName){
        printf("Successfully created index on %s\n", colName.c_str());
    }
};

template <typename T>
T convert(const std::string& str);

template <>
int convert(const std::string& str){
    return std::stoi(str);
}

template <>
char convert(const std::string& str){
    return str[0];
}

template <>
bool convert(const std::string& str){
    return str == "true";
}

template <>
float convert(const std::string& str){
    return std::stof(str);
}

class Executor{
public:
    std::unique_ptr<TableManager> sharedManager;
    explicit Executor(const std::string& baseURL){
        sharedManager = std::make_unique<TableManager>(baseURL);
        acutalSize = 0;
        expectedSize = 0;
    }

    ExecuteResult execute(Parser& parser){
        ExecuteResult res;
        switch(parser.type){
            case StatementType::insert:
                res = executeInsert(parser.statement);
                break;
            case StatementType::select:
                res = executeSelect(parser.statement);
                break;
            case StatementType::remove:
                res = executeRemove(parser.statement);
                break;
            case StatementType::create:
                res = executeCreate(parser.statement);
                break;
            case StatementType::index:
                res = executeIndex(parser.statement);
            case StatementType::update:
                res = executeUpdate(parser.statement);
                break;
            case StatementType::drop:
                res = executeDrop(parser.statement);
                break;
        }
        return res;
    }

    int32_t acutalSize;
    int32_t expectedSize;

private:

    ExecuteResult executeCreate(std::unique_ptr<QueryStatement>& statement){
        auto createStatement = dynamic_cast<CreateStatement*>(statement.get());
        auto res = sharedManager->create(createStatement->tableName,
                                         std::move(createStatement->colNames),
                                         std::move(createStatement->colTypes),
                                         std::move(createStatement->colSize));

        ErrorHandler::handleTableManagerError(res);
        if(res == TableManagerResult::tableCreatedSuccessfully){
            return ExecuteResult::success;
        }
        return ExecuteResult::faliure;
    }

    ExecuteResult executeIndex(std::unique_ptr<QueryStatement>& statement){
        std::shared_ptr<Table> table;
        auto res = sharedManager->open(statement->tableName, table);
        if(res != TableManagerResult::openedSuccessfully) {
            return ExecuteResult::faliure;
        }

        auto insertStatement = dynamic_cast<IndexStatement*>(statement.get());
        for(auto& colName: insertStatement->colNames){
            auto itr = table->columnIndex.find(colName);
            if(itr == table->columnIndex.end()){
                return ExecuteResult::invalidColumnName;
            }
            int32_t index = itr->second;
            if(!table->indexed[index]){
                table->indexed[index] = true;
                if(!sharedManager->createIndex(table, index)){
                    ErrorHandler::indexCreationError(colName);
                    return ExecuteResult::faliure;
                }
                else{
                    ErrorHandler::indexCreationSuccessful(colName);
                }
            }
        }
        return ExecuteResult::success;
    }

    ExecuteResult executeInsert(std::unique_ptr<QueryStatement>& statement){
        std::shared_ptr<Table> table;
        auto res = sharedManager->open(statement->tableName, table);
        if(res != TableManagerResult::openedSuccessfully) {
            return ExecuteResult::faliure;
        }

        auto insertStatement = dynamic_cast<InsertStatement*>(statement.get());
        int32_t columnCount = table->columnNames.size();
        int32_t actualSize = insertStatement->data.size();
        if(columnCount != actualSize){
            ErrorHandler::handleTableMismatchError(actualSize, columnCount);
            return ExecuteResult::faliure;
        }

        // Serialise Data;
        Cursor cursor(table.get());
        cursor.row = table->nextFreeRowLocation();
        char* buffer = cursor.value();
        if(buffer == nullptr) return ExecuteResult::unexpectedError;
        auto serializeRes = serializeRow(buffer, table.get(), insertStatement->data);
        if(serializeRes != ExecuteResult::success) return serializeRes;
        table->increaseRowCount();
        cursor.addedChangesToCommit();

        // if(!table->insertBTree(insertStatement->data, cursor.row))
        return ExecuteResult::success;
    }

    ExecuteResult executeSelect(std::unique_ptr<QueryStatement>& statement){
        std::shared_ptr<Table> table;
        auto res = sharedManager->open(statement->tableName, table);
        if(res != TableManagerResult::openedSuccessfully) {
            return ExecuteResult::faliure;
        }

        auto selectStatement = dynamic_cast<SelectStatement*>(statement.get());

        std::vector<int32_t> indices;
        if(!selectStatement->selectAllCols){
            for(auto& str: selectStatement->colNames){
                int32_t index = table->columnIndex[str];
                indices.emplace_back(index);
            }
        }

        int32_t size = selectStatement->selectAllCols ? table->columnNames.size(): indices.size();
        auto callback = [&](row_t row)->bool{
            Cursor cursor(table.get());
            cursor.row = row;
            char* buffer = cursor.value();
            std::vector<std::string> data(size);
            bool deserializeRes = deserializeRow(buffer, table, indices, data, selectStatement->selectAllCols);
            if(!deserializeRes) return false;
            for(auto& str: data){
                std::cout << str << " | ";
            }
            std::cout << std::endl;
            return true;
        };

        if(selectStatement->selectAllRows){

        }
        auto condition = std::move(selectStatement->condition);
        int32_t index = table->columnIndex[condition.col];
        if(condition.isCompound){

        }
        else{
            switch(condition.compType1){
                case ComparisonType::equal:
                    table->bPlusTrees[index]
                    break;
                case ComparisonType::notEqual:
                    break;
                case ComparisonType::lessThan:
                    break;
                case ComparisonType::greaterThan:
                    break;
                case ComparisonType::lessThanOrEqual:
                    break;
                case ComparisonType::greaterThanOrEqual:
                    break;
                default: break;
            }
        }

        return ExecuteResult::success;
    }

    ExecuteResult executeUpdate(std::unique_ptr<QueryStatement>& statement){
        std::shared_ptr<Table> table;
        auto res = sharedManager->open(statement->tableName, table);
        if(res != TableManagerResult::openedSuccessfully) {
            return ExecuteResult::faliure;
        }

        auto updateStatement = dynamic_cast<UpdateStatement*>(statement.get());
        std::vector<int32_t> indices;
        for(auto& str: updateStatement->colNames){
            int32_t index = table->columnIndex[str];
            indices.emplace_back(index);
        }
        // TODO: Search Btree for given condition
        //       Read Matched Records

        // Call this lambda on every matched row
        auto updateCallback = [&](Cursor& cursor)->bool{
            char* buffer = cursor.value();
            if(buffer == nullptr) return false;
            auto tempBuffer = std::make_unique<char[]>(cursor.table->getRowSize() + 1);
            strncpy(tempBuffer.get(), buffer, table->getRowSize());
            auto serializeRes = serializeRow(tempBuffer.get(), cursor.table, updateStatement->colValues, false , &indices);
            if(serializeRes != ExecuteResult::success) return false;
            strncpy(buffer, tempBuffer.get(), table->getRowSize());
            cursor.addedChangesToCommit();
            return true;
        };
        return ExecuteResult::success;
    }

    ExecuteResult executeRemove(std::unique_ptr<QueryStatement>& statement){
        std::shared_ptr<Table> table;
        auto res = sharedManager->open(statement->tableName, table);
        if(res != TableManagerResult::openedSuccessfully) {
            return ExecuteResult::faliure;
        }

        auto deleteStatement = dynamic_cast<DeleteStatement*>(statement.get());
        // TODO: Search Btree for given condition
        //       Read Matched Records
        //       Write Updated Value
        return ExecuteResult::faliure;
    }

    ExecuteResult executeDrop(std::unique_ptr<QueryStatement>& statement){
        auto res = sharedManager->drop(statement->tableName);
        ErrorHandler::handleTableManagerError(res);
        if(res == TableManagerResult::droppedSuccessfully){
            return ExecuteResult::success;
        }
        return ExecuteResult::faliure;
    }

private:
    static ExecuteResult serializeRow(char* buffer, Table* table, std::vector<std::string>& data, bool serializeAll = true, std::vector<int32_t>* indices = nullptr){
        int32_t offset = 0;
        int32_t j = 0;
        int32_t size = table->columnNames.size();
        for(int32_t i = 0; i < size; ++i){
            if(serializeAll || (*indices)[j] == i) {
                switch (table->columnTypes[i]) {
                    case DataType::Int:
                        try {
                            int32_t dataInt = std::stoi(data[j]);
                            memcpy(buffer + offset, &dataInt, sizeof(int32_t));
                        } catch (...) {
                            return ExecuteResult::typeMismatch;
                        }
                        break;

                    case DataType::Float:
                        try {
                            float dataInt = std::stof(data[j]);
                            memcpy(buffer + offset, &dataInt, sizeof(float));
                        } catch (...) {
                            return ExecuteResult::typeMismatch;
                        }
                        break;

                    case DataType::Char:
                        if (data[i].size() != 1) {
                            return ExecuteResult::typeMismatch;
                        }
                        memcpy(buffer + offset, &data[j][0], sizeof(char));
                        break;

                    case DataType::Bool:
                        bool dataBool;
                        if (strncmp(data[j].c_str(), "true", 4) == 0) {
                            dataBool = true;
                        } else if (strncmp(data[j].c_str(), "false", 4) == 0) {
                            dataBool = false;
                        } else {
                            return ExecuteResult::typeMismatch;
                        }
                        memcpy(buffer + offset, &dataBool, sizeof(bool));

                        break;

                    case DataType::String:
                        if (data[j].size() > table->columnSizes[i]) {
                            return ExecuteResult::stringTooLarge;
                        }
                        strncpy(buffer + offset, data[j].c_str(), table->columnSizes[i]);
                        break;
                }
                ++j;
            }
            offset += table->columnSizes[i];
        }
        return ExecuteResult::success;
    }

    static bool deserializeRow(char* buffer, std::shared_ptr<Table>& table, std::vector<int32_t>& indices, std::vector<std::string>& row, bool selectAll){
        try{
            int32_t size = table->columnNames.size();
            int j = 0;
            int32_t offset = 0;
            for(int32_t i = 0; i < size; ++i){
                if(selectAll || indices[j] == i){
                    switch(table->columnTypes[i]){
                        case DataType::Int:
                            int32_t dataInt;
                            memcpy(&dataInt, buffer + offset, table->columnSizes[i]);
                            row[j] = std::move(std::to_string(dataInt));
                            break;

                        case DataType::Float:
                            float dataFloat;
                            memcpy(&dataFloat, buffer + offset, table->columnSizes[i]);
                            row[j] = std::move(std::to_string(dataFloat));
                            break;

                        case DataType::Char:
                            char dataChar;
                            memcpy(&dataChar, buffer + offset, table->columnSizes[i]);
                            row[j] = std::move(std::string(1, dataChar));
                            break;

                        case DataType::Bool:
                            bool dataBool;
                            memcpy(&dataBool, buffer + offset, table->columnSizes[i]);
                            row[j] = dataBool? "true": "false";
                            break;

                        case DataType::String:
                            std::string dataString(table->columnSizes[i], '\0');
                            memcpy((void*)dataString.c_str(), buffer + offset, table->columnSizes[i]);
                            row[j] = std::move(dataString);
                            break;
                    }
                    ++j;
                }
                offset += table->columnSizes[i];
            }
        }
        catch(...){
            return false;
        }
        return true;
    }
};
//
// Created by Yogesh Kumar on 04/02/20.
//
#include "Parser.cpp"

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
};

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
        Cursor tableEnd = table->end();
        char* buffer = tableEnd.value();
        if(buffer == nullptr) return ExecuteResult::unexpectedError;
        int32_t offset = 0;
        for(int32_t i = 0; i < columnCount; ++i){
            switch(table->columnTypes[i]){
                case DataType::Int:
                    try{
                        int32_t dataInt = std::stoi(insertStatement->data[i]);
                        memcpy(buffer + offset, &dataInt, sizeof(int32_t));
                        offset += sizeof(int32_t);
                    }catch(...){
                        return ExecuteResult::typeMismatch;
                    }
                    break;

                case DataType::Float:
                    try{
                        float dataInt = std::stof(insertStatement->data[i]);
                        memcpy(buffer + offset, &dataInt, sizeof(float));
                        offset += sizeof(float);
                    }catch(...){
                        return ExecuteResult::typeMismatch;
                    }
                    break;

                case DataType::Char:
                    if(insertStatement->data[i].size() != 1){
                        return ExecuteResult::typeMismatch;
                    }
                    memcpy(buffer + offset, &insertStatement->data[i][0], sizeof(char));
                    offset += sizeof(char);
                    break;

                case DataType::Bool:
                    bool dataBool;
                    if(strncmp(insertStatement->data[i].c_str(), "true", 4) == 0){
                        dataBool = true;
                    }
                    else if(strncmp(insertStatement->data[i].c_str(), "false", 4) == 0){
                        dataBool = false;
                    }
                    else{
                        return ExecuteResult::typeMismatch;
                    }
                    memcpy(buffer + offset, &dataBool, sizeof(bool));
                    offset += sizeof(bool);
                    break;

                case DataType::String:
                    if(insertStatement->data[i].size() > table->columnSizes[i]){
                        return ExecuteResult::stringTooLarge;
                    }
                    strncpy(buffer + offset, insertStatement->data[i].c_str(), table->columnSizes[i]);
                    offset += table->columnSizes[i];
                    break;
            }
        }
        table->increaseRowCount();
        tableEnd.addedChangesToCommit();

        // TODO: Call B+ Tree to insert this row to index;
        return ExecuteResult::success;
    }

    ExecuteResult executeSelect(std::unique_ptr<QueryStatement>& statement){
        std::shared_ptr<Table> table;
        auto res = sharedManager->open(statement->tableName, table);
        if(res != TableManagerResult::openedSuccessfully) {
            return ExecuteResult::faliure;
        }

        auto selectStatement = dynamic_cast<SelectStatement*>(statement.get());

        std::vector<int> indices;
        if(!selectStatement->selectAllCols){
            for(auto& str: selectStatement->colNames){
                int32_t index = table->columnIndex[str];
                indices.emplace_back(index);
            }
        }

        if(selectStatement->selectAllRows){
            Cursor cursor = table->start();
            int32_t size = selectStatement->selectAllCols? table->columnNames.size(): indices.size();
            std::vector<std::string> row(size);
            while(!cursor.endOfTable){
                char* buffer = cursor.value();
                bool deserializeRes = deserializeRow(buffer, table, indices, row, selectStatement->selectAllCols);
                if(!deserializeRes) return ExecuteResult::unexpectedError;
                for(auto& str: row){
                    std::cout << str << " | ";
                }
                std::cout << std::endl;
                ++cursor;
            }
        }
        return ExecuteResult::success;
    }

    ExecuteResult executeUpdate(std::unique_ptr<QueryStatement>& statement){
        return ExecuteResult::faliure;
    }

    ExecuteResult executeRemove(std::unique_ptr<QueryStatement>& statement){
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
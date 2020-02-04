//
// Created by Yogesh Kumar on 2020-01-31.
//

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include "HeaderFiles/DataTypes.h"
#include "HeaderFiles/Table.h"
#include "HeaderFiles/TableManager.h"

#define MAX_FIELD_SIZE 512
#define MAX_TABLE_NAME_LEN 50

enum class StatementType{
    insert,
    select,
    update,
    remove,
    create,
    drop
};

enum class PrepareResult{
    success,
    unrecognized,
    syntaxError,
    stringTooLong,
    negativeID,
    invalidType,
    noSizeForString,
    invalidOperator,
    comparisonOnDifferentRows,
    cannotCreateEmptyTable,
    noTableName,
    noInsertData,
    noUpdateData,
    noCondition
};

enum class ExecuteResult{
    success,
    tableFull,
    faliure
};

/*
 *  ---------------------- COMMANDS ----------------------
 *  create table <table-name>{<col-1>:<DATATYPE>, <col-2>:<DATATYPE>, ...}
 *  insert into <table-name>{<col-1-data>, <col-1-data>, ...}
 *  update <table-name> set {<col-1> = <data-1>, <col-1> = <data-1>, ...}
 *  update <table-name> set {<col-1> = <data-1>, <col-1> = <data-1>, ...} where <CONDITION>
 *  delete from <table-name> where <CONDITION>
 *  delete table <table-name>
 *  drop table <table-name>
 *  select (<col-1>, <col-2>, ...) from <table-name> where <CONDITION>
 *  select * from <table-name> where <CONDITION>
 *
 *  --------------------- DATA TYPES ---------------------
 *  1. string(<length>)
 *  2. int
 *  3. float
 *  4. long
 *  5. bool
 *  6. char
 *
 *  --------------------- CONDITION ---------------------
 *  <col-1> == <data-1>
 *  <col-1> != <data-1>
 *  <col-1> > <data-1>
 *  <col-1> < <data-1>
 *  <col-1> <= <data-1>
 *  <col-1> >= <data-1>
 *  <CONDITION> && <CONDITION>
 *
 */

enum class ComparisonType{
    equal,
    notEqual,
    lessThan,
    greaterThan,
    lessThanOrEqual,
    greaterThanOrEqual,
    error
};

ComparisonType findConparisonType(const char* op){
    if(strcmp(op, "==") == 0){
        return ComparisonType::equal;
    }
    else if(strcmp(op, "!=") == 0){
        return ComparisonType::notEqual;
    }
    else if(strcmp(op, ">") == 0){
        return ComparisonType::greaterThan;
    }
    else if(strcmp(op, "<") == 0){
        return ComparisonType::lessThan;
    }
    else if(strcmp(op, ">=") == 0){
        return ComparisonType::greaterThanOrEqual;
    }
    else if(strcmp(op, "<=") == 0){
        return ComparisonType::lessThanOrEqual;
    }
    return ComparisonType::error;
}

struct Condition{
    bool isCompound{};
    std::string col;
    void* data1{};
    void* data2{};
    ComparisonType compType1{};
    ComparisonType compType2{};
};

struct QueryStatement{
    std::string tableName;
    Table* table{};
    virtual ~QueryStatement() = default;
};

struct CreateStatement: public QueryStatement{
    std::vector<std::string> colNames;
    std::vector<DataType> colTypes;
    std::vector<uint32_t> colSize;
};

struct InsertStatement: public QueryStatement{
    std::vector<std::string> data;
};

struct SelectStatement: public QueryStatement{
    std::vector<std::string> colNames;
    Condition condition;
    bool selectAllRows{};
    bool selectAllCols{};
};

struct UpdateStatement: public QueryStatement{
    std::vector<std::string> colNames;
    std::vector<std::string> colValues;
    Condition condition;
    bool updateAll{};
};

struct DeleteStatement: public QueryStatement{
    Condition condition;
    bool deleteAll{};
};

struct DropStatement:   public QueryStatement{

};

void release(std::vector<void*>& data, std::vector<DataType>& type, std::vector<uint32_t>& size){
    for(int i = 0; i < data.size(); ++i){
        if(data[i] == nullptr) return;
        switch(type[i]){
            case DataType::Int:
                delete (int*)data[i];
                break;
            case DataType::Float:
                delete (float*)data[i];
                break;
            case DataType::Char:
                delete (char*)data[i];
                break;
            case DataType::Bool:
                delete (bool*)data[i];
                break;
            case DataType::String:
                uint32_t len = size[i] + 1;
                delete[] static_cast<char*>(data[i]);
                break;
        }
    }
}

class Parser{
    char tableName[MAX_TABLE_NAME_LEN]{};

public:
    StatementType type;
    std::unique_ptr<QueryStatement> statement;

    Parser() = default;

    PrepareResult parse(InputBuffer &inputBuffer){
        PrepareResult res;
        if(strncmp(inputBuffer.buffer.c_str(), "insert into", 11) == 0) {
            res = parseInsert(inputBuffer);
        }
        else if(strncmp(inputBuffer.buffer.c_str(), "select", 6) == 0) {
            res = parseSelect(inputBuffer);
        }
        else if(strncmp(inputBuffer.buffer.c_str(), "create table", 12) == 0){
            res = parseCreate(inputBuffer);
        }
        else if(strncmp(inputBuffer.buffer.c_str(), "update", 6) == 0){
            res = parseUpdate(inputBuffer);
        }
        else if(strncmp(inputBuffer.buffer.c_str(), "delete from", 11) == 0){
            res = parseDelete(inputBuffer);
        }
        else if(strncmp(inputBuffer.buffer.c_str(), "delete table", 12) == 0){
            res = parseDeleteAll(inputBuffer);
        }
        else if(strncmp(inputBuffer.buffer.c_str(), "drop table", 10) == 0){
            res = parseDrop(inputBuffer);
        }
        else{
            res = PrepareResult::unrecognized;
        }
        if(res == PrepareResult::success) statement->tableName = tableName;
        return res;
    }

private:
    bool getTableName(const char** ptr, const std::string& prefix){
        int n = 0;
        std::string formatString = prefix + " %255[^ \t\n{] %n";
        if(sscanf(*ptr, formatString.c_str(), tableName, &n) != 1){
            return false;
        }
        (*ptr) += n;
        return true;
    }

    // HELPER FUNCTIONS
    static bool checkOpeningBrace(const char** ptr){
        int n = 0;
        char val[2];
        if(sscanf(*ptr, " %1[{] %n", val, &n) != 1){
            return false;
        }
        (*ptr) += n;
        return true;
    }
    static bool parseFormatString(const char** ptr, char* field, const char* formatString){
        int n = 0;
        if(sscanf(*ptr, formatString, field, &n) != 1) return false;
        (*ptr) += n;
        return true;
    }
    static bool getNextValue(const char** ptr, char* field){
        int n = 0;
        char val[2];
        // Get Opening quote
        if(sscanf(*ptr, " %1[\"]%n", val, &n) != 1) return false;
        (*ptr) += n;

        // Get String
        sscanf(*ptr, "%255[^\"]%n", field, &n);
        (*ptr) += n;

        // Get Closing quote
        if(sscanf(*ptr, "%1[\"]%n ", val, &n) != 1) return false;
        (*ptr) += n;

        return true;
    }
    static bool getSeperator(const char** ptr, char* val){
        int n = 0;
        if(sscanf(*ptr, " %1s %n", val, &n) != 1) return false;
        (*ptr) += n;
        return true;
    }

    PrepareResult parseCreate(InputBuffer& inputBuffer){
        // SYNTAX:- create table <table-name>{<col-1>:<DATATYPE>, <col-2>:<DATATYPE>, ...}
        this->type = StatementType::create;
        const char *ptr = inputBuffer.str();
        std::vector<std::string> colNames;
        std::vector<DataType> colTypes;
        std::vector<uint32_t> colSize;
        char name[50], type[20];
        int n;
        if(sscanf(ptr, "create table %50[^ \n\t{] { %n", tableName, &n) != 1){
            return PrepareResult::noTableName;
        }
        ptr += n;
        int col = 0;
        while(sscanf(ptr, " %255[^: \t\n] %n", name, &n) == 1){
            ptr += n;
            ++col;
            if(*ptr != ':'){
                return PrepareResult::syntaxError;
            }
            ptr++;
            if(sscanf(ptr, " %19[^, }] %n", type, &n) != 1){
                return PrepareResult::syntaxError;
            }

            colNames.push_back(name);
            if(strcmp(type, "int") == 0){
                colTypes.push_back(DataType::Int);
                colSize.push_back(4);
            }
            else if(strcmp(type, "float") == 0){
                colTypes.push_back(DataType::Float);
                colSize.push_back(4);
            }
            else if(strcmp(type, "bool") == 0){
                colTypes.push_back(DataType::Bool);
                colSize.push_back(1);
            }
            else if(strcmp(type, "char") == 0){
                colTypes.push_back(DataType::Char);
                colSize.push_back(1);
            }
            else if(strncmp(type, "string", 6) == 0){
                int end;
                int len;
                if(sscanf(type, " string( %d %n", &len, &end) != 1){
                    return PrepareResult::syntaxError;
                }
                if(*(type + end) != ')'){
                    return PrepareResult::syntaxError;
                }
                colTypes.push_back(DataType::String);
                colSize.push_back(len);
            }
            else{
                return PrepareResult::invalidType;
            }
            ptr += n;
            if(*ptr == '}'){
                break;
            }
            ++ptr;
        }
        if(col == 0){
            return PrepareResult::cannotCreateEmptyTable;
        }
        auto createStatement = std::make_unique<CreateStatement>();
        createStatement->colNames = std::move(colNames);
        createStatement->colTypes = std::move(colTypes);
        createStatement->colSize  = std::move(colSize);
        this->statement = std::move(createStatement);
        return PrepareResult::success;
    }

    PrepareResult parseInsert(InputBuffer& inputBuffer){
        // SYNTAX :- insert into <table-name>{<col-1-data>, <col-1-data>, ...}
        this->type = StatementType::insert;
        const char *ptr = inputBuffer.str();
        char field[MAX_FIELD_SIZE + 1];
        char seperator[2];
        std::vector<std::string> data;

        if(!getTableName(&ptr, "insert into")) return PrepareResult::noTableName;
        if(!Parser::checkOpeningBrace(&ptr)) return PrepareResult::syntaxError;

        while(true){
            if(!getNextValue(&ptr, field)){
                if(data.empty()) return PrepareResult::noInsertData;
                return PrepareResult::syntaxError;
            }
            printf("Parsed Field : \"%s\"\n", field);

            data.emplace_back(field);

            if(!getSeperator(&ptr, seperator)) return PrepareResult::syntaxError;
            if(seperator[0] == ',') continue;
            if(seperator[0] == '}') break;
            else return PrepareResult::syntaxError;
        }

        auto insertStatement = std::make_unique<InsertStatement>();
        insertStatement->data = std::move(data);
        this->statement = std::move(insertStatement);

        return PrepareResult::success;
    }

    PrepareResult parseUpdate(InputBuffer& inputBuffer){
        // SYNTAX:- update <table-name> {<col-1> = <data-1>, <col-1> = <data-1>, ...}
        //          update <table-name> {<col-1> = <data-1>, <col-1> = <data-1>, ...} where <CONDITION>
        this->type = StatementType::update;

        const char *ptr = inputBuffer.str();
        std::vector<std::string> colNames;
        std::vector<std::string> colValues;
        char colName[MAX_FIELD_SIZE + 1];
        char colValue[MAX_FIELD_SIZE + 1];
        char seperator[20];
        char colFormatStr[] = " %255[^=} \t\n] %n";
        int col = 0;

        if(!getTableName(&ptr, "update")) return PrepareResult::noTableName;
        if(!Parser::checkOpeningBrace(&ptr)) return PrepareResult::syntaxError;

        while(true) {
            // Get Column Name
            if(!parseFormatString(&ptr, colName, colFormatStr)){
                if(col == 0){return PrepareResult::noUpdateData;}
                return PrepareResult::syntaxError;
            }

            // Get '=' seperator
            if(!getSeperator(&ptr, seperator)) return PrepareResult::syntaxError;
            if(seperator[0] != '=') return PrepareResult::syntaxError;

            // Get Column Value
            if(!getNextValue(&ptr, colValue)) return PrepareResult::syntaxError;

            ++col;
            colNames.emplace_back(colName);
            colValues.emplace_back(colValue);
            printf("Parsed Column: \"%s\" | Value: \"%s\"\n", colName, colValue);

            // Get Column Seperator ',' or '}'
            if(!getSeperator(&ptr, seperator)) return PrepareResult::syntaxError;
            if(seperator[0] == ',') continue;
            if(seperator[0] == '}') break;
            else return PrepareResult::syntaxError;
        }

        auto updateStatement = std::make_unique<UpdateStatement>();
        updateStatement->colNames = std::move(colNames);
        updateStatement->colValues = std::move(colValues);

        if(!parseFormatString(&ptr, seperator, " %20s %n")){
            updateStatement->updateAll = true;
        }
        else if(strcmp(seperator, "where") == 0){
            updateStatement->updateAll = false;
            auto res = Parser::parseCondition(ptr, updateStatement->condition);
            if(res != PrepareResult::success) return res;
        }
        else{
            return PrepareResult::syntaxError;
        }

        this->statement = std::move(updateStatement);
        return PrepareResult::success;
    }

    PrepareResult parseDelete(InputBuffer& inputBuffer){
        // SYNTAX:- delete from <table-name> where <CONDITION>
        this->type = StatementType::remove;
        const char *ptr = inputBuffer.str();
        char keyword[20];
        int n;

        if(!getTableName(&ptr, "delete from")) return PrepareResult::noTableName;
        auto deleteStatement = std::make_unique<DeleteStatement>();

        if(!parseFormatString(&ptr, keyword, " %20s %n")) return PrepareResult::noCondition;
        if(strcmp(keyword, "where") == 0){
            deleteStatement->deleteAll = false;
            auto res = Parser::parseCondition(ptr, deleteStatement->condition);
            if(res != PrepareResult::success) return res;
        }
        else{
            return PrepareResult::syntaxError;
        }

        this->statement = std::move(deleteStatement);
        return PrepareResult::success;
    }

    PrepareResult parseDeleteAll(InputBuffer& inputBuffer){
        this->type = StatementType::remove;
        const char *ptr = inputBuffer.str();
        if(!getTableName(&ptr, "delete table")) return PrepareResult::noTableName;
        auto deleteStatement = std::make_unique<DeleteStatement>();
        deleteStatement->deleteAll = true;
        this->statement = std::move(deleteStatement);
        return PrepareResult::success;
    }

    PrepareResult parseDrop(InputBuffer& inputBuffer){
        // SYNTAX:- drop table <table-name>
        this->type = StatementType::drop;
        const char *ptr = inputBuffer.str();
        if(!getTableName(&ptr, "drop table")) return PrepareResult::noTableName;
        auto dropStatement = std::make_unique<DropStatement>();
        this->statement = std::move(dropStatement);
        return PrepareResult::success;
    }

    PrepareResult parseSelect(InputBuffer& inputBuffer){
        // SYNTAX:- select {<col-1>, <col-2>, ...} from <table-name> where <CONDITION>
        //          select * from <table-name> where <CONDITION>
        //          select {*} from <table-name> where <CONDITION>
        this->type = StatementType::select;
        const char *ptr = inputBuffer.str();
        char keyword[20];
        int n = 0;
        auto selectStatement = std::make_unique<SelectStatement>();

        sscanf(ptr, "select %n", &n);
        ptr += n;

        if( (sscanf(ptr, " %1[*] %n", keyword, &n) == 1) ||
            (sscanf(ptr, " { %1[*] } %n", keyword, &n) == 1)){
            ptr += n;
            selectStatement->selectAllCols = true;
        }
        else{
            if(!checkOpeningBrace(&ptr)) return PrepareResult::syntaxError;
            selectStatement->selectAllCols = false;
            std::vector<std::string> colNames;
            char colName[MAX_FIELD_SIZE + 1];
            char colFormatStr[] = " %255[^,} \t\n] %n";
            while(true){
                // Get Column Name
                if (!parseFormatString(&ptr, colName, colFormatStr)){
                    return PrepareResult::syntaxError;
                }

                colNames.emplace_back(colName);
                printf("Parsed Column: \"%s\"\n", colName);

                // Get Column Seperator ',' or '}'
                if (!getSeperator(&ptr, keyword)) return PrepareResult::syntaxError;
                if (keyword[0] == ',') continue;
                if (keyword[0] == '}') break;
                else return PrepareResult::syntaxError;
            }
            selectStatement->colNames = std::move(colNames);
        }

        if(!getTableName(&ptr, " from ")) return PrepareResult::noTableName;
        if(!parseFormatString(&ptr, keyword, " %20s %n")){
            selectStatement->selectAllRows = true;
        }
        if(strcmp(keyword, "where") == 0){
            selectStatement->selectAllRows = false;
            auto res = parseCondition(ptr, selectStatement->condition);
            if(res != PrepareResult::success) return res;
        }
        else{
            return PrepareResult::syntaxError;
        }

        this->statement = std::move(selectStatement);
        return PrepareResult::success;
    }

    static PrepareResult parseCondition(const char* ptr, Condition& cond){
        char col1[255], col2[255];
        char val1[255], val2[255];
        char op1[3], op2[3];
        char combineOperator[3];
        int count = sscanf(ptr, "%[^><=!& ] %2[><=!] %2[^&] %[&] %[^><=!& ] %2[><=!] %[^&\n] ", col1, op1, val1, combineOperator,col2, op2, val2);
        // printf("%d\n", count);
        // printf("\"%s\"  \"%s\"  \"%s\" \"%s\" \"%s\"  \"%s\"  \"%s\"\n", col1, op1 ,val1, combineOperator, col2, op2, val2);
        if(count == 3){
            // printf("\"%s\"  \"%s\"  \"%s\"\n", col1, op1 ,val1);
            cond.isCompound = false;
            cond.col = col1;
            cond.data1 = val1;
            cond.compType1 = findConparisonType(op1);
            if(cond.compType1 == ComparisonType::error){
                return PrepareResult::invalidOperator;
            }
        }
        else if(count == 7){
            // printf("\"%s\"  \"%s\"  \"%s\" \"%s\" \"%s\"  \"%s\"  \"%s\"\n", col1, op1 ,val1, combineOperator, col2, op2, val2);
            cond.isCompound = true;
            if(strcmp(col1, col2) != 0){
                return PrepareResult::comparisonOnDifferentRows;
            }
            if(strcmp(combineOperator, "&&") != 0){
                return PrepareResult::syntaxError;
            }
            cond.col = col1;
            cond.data1 = val1;
            cond.data2 = val2;
            cond.compType1 = findConparisonType(op1);
            cond.compType2 = findConparisonType(op2);
            if(cond.compType1 == ComparisonType::error || cond.compType2 == ComparisonType::error){
                return PrepareResult::invalidOperator;
            }
        }
        else{
            return PrepareResult::syntaxError;
        }
        return PrepareResult::success;
    }
};

class Executor{
public:
    std::unique_ptr<TableManager> sharedManager;
    explicit Executor(const std::string& baseURL){
        sharedManager = std::make_unique<TableManager>(baseURL);
    }

    ExecuteResult execute(Parser& parser){
        switch(parser.type){
            case StatementType::insert:
                return executeInsert(parser.statement);
                break;
            case StatementType::select:
                return executeSelect(parser.statement);
                break;
            case StatementType::remove:
                return executeRemove(parser.statement);
                break;
            case StatementType::create:
                return executeCreate(parser.statement);
                break;
            case StatementType::update:
                return executeUpdate(parser.statement);
                break;
            case StatementType::drop:
                return executeDrop(parser.statement);
                break;
        }
    }

private:

    ExecuteResult executeInsert(std::unique_ptr<QueryStatement>& statement){
        std::shared_ptr<Table> table;
        auto res = sharedManager->open(statement->tableName, table);
        if(res == TableManagerResult::openedSuccessfully){

        }
        switch(res){
            case TableManagerResult::tableNotFound:
                printf("Table Not Found");
            case TableManagerResult::openingFaliure:
                printf("Failed To Open Table");
            default:
                printf("Unknown Error Occurred");
        }
        return ExecuteResult::faliure;
//        auto insertStatement = dynamic_cast<SelectStatement*>(statement.get());
//        Row rowToInsert{};
//        // TODO: Insert data in rowToInsert from data in statement
//        auto insert = dynamic_cast<InsertStatement*>(statement.get());
//        rowToInsert.serialize(table->end().value());
        return ExecuteResult::success;
    }

    ExecuteResult executeSelect(std::unique_ptr<QueryStatement>& statement){
//        Row row{};
//        Cursor cursor(statement->table);
//        for (uint32_t i = 0; i < statement->table->numRows; i++){
//            row.deserialize(cursor.value());
//            row.print();
//            ++cursor;
//        }
        return ExecuteResult::faliure;
    }

    ExecuteResult executeUpdate(std::unique_ptr<QueryStatement>& statement){
        return ExecuteResult::faliure;
    }

    ExecuteResult executeCreate(std::unique_ptr<QueryStatement>& statement){
        auto createStatement = dynamic_cast<CreateStatement*>(statement.get());
        auto res = sharedManager->create(createStatement->tableName,
                std::move(createStatement->colNames),
                std::move(createStatement->colTypes),
                std::move(createStatement->colSize));

        switch(res){
            case TableManagerResult::tableCreatedSuccessfully:
                printf("Table Created Successfully\n");
                return ExecuteResult::success;
                break;
            case TableManagerResult::tableAlreadyExists:
                printf("This Table already exists\n");
                return ExecuteResult::faliure;
                break;
            case TableManagerResult::tableCreationFaliure:
                printf("Failed To Create Table\n");
                return ExecuteResult::faliure;
                break;
            default: break;
        }
        return ExecuteResult::faliure;
    }

    ExecuteResult executeRemove(std::unique_ptr<QueryStatement>& statement){
        return ExecuteResult::faliure;
    }

    ExecuteResult executeDrop(std::unique_ptr<QueryStatement>& statement){
        return ExecuteResult::faliure;
    }
};

/*
 * while(sscanf(ptr, " %255[^,}]%n", field, &n) == 1){
            ptr += n;
            ++col;
            if(col == numCols){
                if(*ptr != '}'){
                    release(data, table->columnTypes, table->columnSize);
                    return PrepareResult::syntaxError;
                }
                break;
            }
            ++ptr;

            switch (table->columnTypes[col - 1]) {
                case DataType::Int:
                    try {
                        int *it = new int;
                        *it = std::stoi(field);
                        data[col] = (void *) it;
                    }
                    catch (...) {
                        release(data, table->columnTypes, table->columnSize);
                        return PrepareResult::syntaxError;
                    }
                    break;

                case DataType::Float:
                    break;
                case DataType::Char:
                    break;
                case DataType::Bool:
                    break;

                case DataType::String:
                    int len = table->columnSize[col];
                    if (n == 0){
                        release(data, table->columnTypes, table->columnSize);
                        return PrepareResult::syntaxError;
                    }
                    if (n > len){
                        release(data, table->columnTypes, table->columnSize);
                        return PrepareResult::stringTooLong;
                    }
                    char *it = new char[len + 1];
                    strncpy(it, field, n);
                    data[col] = (void *)it;
                    break;
            }
        }

        ptr += n;
                if (!(*ptr == ',' || *ptr == '}')) {
                    return PrepareResult::syntaxError;
                }
                colNames.emplace_back(colName);
                if (*ptr == '}') {
                    ptr++;
                    break;
                }
                ++ptr;

 */
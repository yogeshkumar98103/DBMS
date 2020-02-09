#include <cstdio>
#include <ncurses.h>
//#include "HeaderFiles/Constants.h"
#include "Executor.cpp"
//#include "CommandHistory.cpp"

//void sigintHandler(int sig_num)
//{
//    signal(SIGINT, sigintHandler);
//    /*
//        Yeh Function run hoga
//    */
//    printf("\n Cannot be terminated using Ctrl+C \n");
//    fflush(stdout);
//}

void func(){
    static InputBuffer inputBuffer;
    static Parser parser;
    static Executor executor("./MyDatabase");

    printPrompt();
    inputBuffer.readInput();

    if(inputBuffer.isMetaCommand()){
        switch(inputBuffer.performMetaCommand()){
            case MetaCommandResult::exit:
                executor.sharedManager->closeAll();
                printw("Exited Successfully\n");
                endwin();
                exit(EXIT_SUCCESS);

            case MetaCommandResult::flush:
                printw("Flushed All Opened Tables.\n");
                executor.sharedManager->flushAll();

            case MetaCommandResult::empty:
                return;

            case MetaCommandResult::unrecognized:
                printw("Unrecognized command '%s'.\n", inputBuffer.str());
                return;
        }
    }

    switch(parser.parse(inputBuffer)){
        case PrepareResult::success:
            break;
        case PrepareResult::syntaxError:
            printw("Syntax Error. Could not parse statement\n");
            return;
        case PrepareResult::stringTooLong :
            printw("String is too long.\n");
            return;
        case PrepareResult::negativeID:
            printw("ID must be positive.\n");
            return;
        case PrepareResult::invalidType:
            printw("Invalid Data Type.\n");
            return;
        case PrepareResult::noSizeForString:
            printw("No size provided for string.\n");
            return;
        case PrepareResult::unrecognized:
            printw("Unrecognized keyword at start of '%s'.\n", inputBuffer.str());
            return;
        case PrepareResult::invalidOperator:
            printw("Invalid Operator\n");
            return;
        case PrepareResult::comparisonOnDifferentRows:
            printw("Comparison on different rows is not allowed\n");
            return;
        case PrepareResult::cannotCreateEmptyTable:
            printw("Cannot Create Empty Table. Please add some columns\n");
            return;
        case PrepareResult::noTableName:
            printw("Please provide Table Name\n");
            return;
        case PrepareResult::noInsertData:
            printw("No Data Provided to Insert\n");
            return;
        case PrepareResult::noUpdateData:
            printw("No Data Provided to Update\n");
            return;
        case PrepareResult::noCondition:
            printw("Provide Condition To Delete Selected Table using `where` clause.\n"
                   "To delete all entries use `delete table` instead\n");
            return;
    }

    switch(executor.execute(parser)){
        case ExecuteResult::success:
            printw("Executed.\n");
            break;
        case ExecuteResult::tableFull:
            printw("Error: Table full.\n");
            break;
        case ExecuteResult::faliure:
            printw("Action Failed\n");
            break;
        case ExecuteResult::typeMismatch:
            printw("Type Mismatch Occured\n");
            break;
        case ExecuteResult::stringTooLarge:
            printw("String Too Large\n");
            break;
        case ExecuteResult::invalidColumnName:
            printw("Column names don't match table column names\n");
            break;
        case ExecuteResult::tableNotIndexed:
            printw("There are no indexes for this table.\n"
                   "Create atleast one and then try again.\n");
            break;
        case ExecuteResult::unexpectedError:
            printw("Unexpected Error occured\n");
            break;
    }
}

int main() {
//    signal(SIGINT, sigintHandler);
//    CommandInput commandInput;
//    int c;
//    initscr();
//    commandInput.initialize();
    while(true){
        func();
//        c = getch();
//        commandInput.handleKeys(c);
    }
    endwin();
    return 0;
}
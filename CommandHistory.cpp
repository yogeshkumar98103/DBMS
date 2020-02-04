#include <deque>
#include<ncurses.h>
#include<string>
#include "Interface.cpp"
#include "Parser.cpp"

class CommandHistory{
	int maxHistorySize;
	std::deque<string> commandsList;
	int positionInDeq; 
	friend class CommandInput;

	CommandHistory(int maxHistorySize_){
		maxHistorySize = maxHistorySize_;
		positionInDeq = -1;
	}	

	void insertIntoCommandsList(std::string command){
		commandsList.push_front(s);
	    if(commandsList.size() > maxHistorySize) 
	        commandsList.pop_back();
	}

	std::string lastCommand(){
		if(positionInDeq<((int)(commandsList.size())-1)){
        	return commandsList[++positionInDeq];
        }
        positionInDeq = commandsList.size();
        return "";
	}

	std::string downCommand(){
		if(positionInDeq>0){
        	return commandsList[--positionInDeq];
        }
        positionInDeq = -1;
        return "";
	}

	void enterKeyCommand(std::string command){
		if(command.compare(""))
        	insertIntoCommandsList(command);
        positionInDeq = -1;
	}
}


class CommandInput{
	std::string command;
	InputBuffer inputBuffer;
    Parser parser;
    Executor executor(".");
    CommandHistory commandHistory(20);

	void clearLine(){
		printw("\r");
		clrtoeol();
	}
	void upKeyPressed(){
		clearLine();
		printw("\rdb> %s", commandHistory.lastCommand().c_str());
	}

	void downKeyPressed(){
		clearLine();
		printw("\rdb> %s", commandHistory.downCommand().c_str());
	}


	void enterKeyPressed(std::string command){
		commandHistory.enterKeyCommand(command);
		printw("db> %s\n", command.c_str());
        printw("\rdb> ");
        inputBuffer.buffer = command;
        if(inputBuffer.isMetaCommand()){
            switch(inputBuffer.performMetaCommand()){
                case MetaCommandResult::exit:
                    executor.sharedManager->closeAll();
                    printw("Exited Successfully\n");
                    exit(EXIT_SUCCESS);

                case MetaCommandResult::empty:
                    continue;

                case MetaCommandResult::unrecognized:
                    printw("Unrecognized command '%s'.\n", inputBuffer.str());
                    continue;
            }
        }

        switch(parser.parse(inputBuffer)){
            case PrepareResult::success:
                break;
            case PrepareResult::syntaxError:
                printw("Syntax Error. Could not parse statement\n");
                continue;
            case PrepareResult::stringTooLong :
                printw("String is too long.\n");
                continue;
            case PrepareResult::negativeID:
                printw("ID must be positive.\n");
                continue;
            case PrepareResult::invalidType:
                printw("Invalid Data Type.\n");
                continue;
            case PrepareResult::noSizeForString:
                printw("No size provided for string.\n");
                continue;
            case PrepareResult::unrecognized:
                printw("Unrecognized keyword at start of '%s'.\n", inputBuffer.str());
                continue;
            case PrepareResult::invalidOperator:
                printw("Invalid Operator\n");
                continue;
            case PrepareResult::comparisonOnDifferentRows:
                printw("Comparison on different rows is not allowed\n");
                continue;
            case PrepareResult::cannotCreateEmptyTable:
                printw("Cannot Create Empty Table. Please add some columns\n");
                continue;
            case PrepareResult::noTableName:
                printw("Please provide Table Name\n");
                continue;
            case PrepareResult::noInsertData:
                printw("No Data Provided to Insert\n");
                continue;
            case PrepareResult::noUpdateData:
                printw("No Data Provided to Update\n");
                continue;
            case PrepareResult::noCondition:
                printw("Provide Condition To Delete Selected Table using `where` clause.\n"
                       "To delete all entries use `delete table` instead\n");
                continue;
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
        }
	}

	void backKeyPressed(){
		if(!command.empty()){
            command.pop_back();
            printw("\b");
        }
    	printw("\b\b");
    	clrtoeol();
	}

	void defaultKey(int character){
		command += (char)character;
	}

public:
	inline void initialize(){
		printw("Welcome\ndb> ");
	}

	void handleKeys(int character){
		switch(character){
			case 65: // UP key
			upKeyPressed();
			break; 
			case 66: // DOWN key
			downKeyPressed();
			break; 
			case 127: // BACKSPACE key
			backKeyPressed();
			break; 
			case 10: // ENTER key
			enterKeyPressed();
			break;
			default:
			defaultKey();
			break;  
		}	
	}

	void clearCommand(){
		command.clear();
	}
}
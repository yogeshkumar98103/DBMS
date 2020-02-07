#include <deque>
#include <ncurses.h>
#include <string>
#include "Interface.cpp"
#include "Executor.cpp"

class CommandHistory{
	int maxHistorySize;
	std::deque<std::string> commandsList;
	int positionInDeq; 
	friend class CommandInput;

	explicit CommandHistory(int maxHistorySize_){
		maxHistorySize = maxHistorySize_;
		positionInDeq = -1;
	}	

	void insertIntoCommandsList(const std::string& command){
		commandsList.push_front(command);
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

	void enterKeyCommand(const std::string& command){
		if(command.compare(""))
        	insertIntoCommandsList(command);
        positionInDeq = -1;
	}
};


class CommandInput{
    std::string commandToTheRight;
    std::string command;
	int positionInCommand;
	InputBuffer inputBuffer;
    Parser parser;
    Executor executor;
    CommandHistory commandHistory;
    WINDOW* win;

	void clearLine(){
		printw("\r");
		clrtoeol();
	}

    void printRightCommand(){
        int currentLine=0, currentPositionInLine=1;
        getyx(win, currentLine, currentPositionInLine);
        printw("%s", commandToTheRight.c_str());
        move(currentLine, currentPositionInLine);
    }

	void upKeyPressed(){
		clearLine();
        command = commandHistory.lastCommand();
        positionInCommand = command.size();
		printw("\rdb> %s", command.c_str());
	}

	void downKeyPressed(){
		clearLine();
        command = commandHistory.downCommand();
        positionInCommand = command.size();
		printw("\rdb> %s", command.c_str());
	}

    void rightKeyPressed(){
        //TODO: Handle right key pressed on cmd
        int currentLine=0, currentPositionInLine=1;
        getyx(win, currentLine, currentPositionInLine);
        // printw("%d\n", positionInCommand);
        if(positionInCommand < command.size()){
            commandToTheRight.erase(0,1);
            move(currentLine, currentPositionInLine+1);
            positionInCommand++;
        }
	}

    void leftKeyPressed(){
        //TODO: Handle left key pressed on cmd
        //printw("\b\b\b\b");
        int currentLine=0, currentPositionInLine=1;
        getyx(win, currentLine, currentPositionInLine);
        if(positionInCommand > 0){
            move(currentLine, currentPositionInLine-1);
            positionInCommand--;
            std::string st(1,command[positionInCommand]); 
            commandToTheRight.insert(0,st);
        }
        
    }

	void enterKeyPressed(){
		commandHistory.enterKeyCommand(command);
        printw("\rdb> %s\n", command.c_str());
        // printw("\n");
		// printw("db> %s\n", command.c_str());
        // printw("REPly of the statment\n");
        inputBuffer.buffer = command;
        if(inputBuffer.isMetaCommand()){
            switch(inputBuffer.performMetaCommand()){
                case MetaCommandResult::exit:
                    executor.sharedManager->closeAll();
                    printw("Exited Successfully\n");
                    endwin();
                    exit(EXIT_SUCCESS);

                case MetaCommandResult::flush:
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
            case ExecuteResult::unexpectedError:
                printw("Unexpected Error occured\n");
                break;
        }
        // printw("My name is\n");
        // printw("Helloo\n");
        printw("\rdb> ");
        command.clear();
        commandToTheRight.clear();
        positionInCommand = 0; 
    //    printw("%d ",scrl(1));
	}

	void backKeyPressed(){
		      
        if(positionInCommand > 0){
            command.erase(--positionInCommand,1);
            printw("\b");
        }
    	clrtoeol();
        
        if(commandToTheRight.size() > 0){
            int currentLine=0, currentPositionInLine=1;
            getyx(win, currentLine, currentPositionInLine);
            printw("%s", commandToTheRight.c_str());
            move(currentLine, currentPositionInLine);
        }
	}

    void deleteKeyPressed() {
        if(positionInCommand < command.size()){
            command.erase(positionInCommand,1);
            commandToTheRight.erase(0,1);
            clrtoeol();
            printRightCommand();
        }
    }

	void defaultKey(int character){
        std::string characterString(1,character); 
        command.insert(positionInCommand, characterString);
        positionInCommand++;
        
        
        printw("%c", character);
        if(commandToTheRight.size() > 0){
            printRightCommand();
        }

		// command += (char)character;
	}

public:
    CommandInput():commandHistory(20), positionInCommand(0){}
	inline void initialize(WINDOW* win_){
        win = win_;
		printw("Welcome\ndb> ");
	}

	void handleKeys(int character){
	    int temp;
		switch(character){
			case '\033':
                getch();
                temp = getch();
                if(temp == 65){
                    upKeyPressed();     // UP key
                }
                else if(temp == 66){
                    downKeyPressed();   // DOWN key
                }
                else if(temp == 67){
                    rightKeyPressed();   // RIGHT KEY
                }
                else if(temp == 68){
                    leftKeyPressed();   // LEFT KEY
                }
                break;
			case 127: // BACKSPACE key
                backKeyPressed();
                break;
			case 10: // ENTER key
                enterKeyPressed();
                break;
            case '~': // ENTER key
                deleteKeyPressed();
                break;
			default:
                defaultKey(character);
                break;
		}	
	}

	void clearCommand(){
		command.clear();
	}
};
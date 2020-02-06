#include <cstdio>
#include <ncurses.h>
#include "CommandHistory.cpp"

void sigintHandler(int sig_num)
{
    signal(SIGINT, sigintHandler);
    /*
        Yeh Function run hoga 
    */
    printf("\n Cannot be terminated using Ctrl+C \n");
    fflush(stdout);
}

int main() {
//    signal(SIGINT, sigintHandler);
    CommandInput commandInput;
    int c;
    initscr();
    commandInput.initialize();
    while(true){
        c = getch();
        commandInput.handleKeys(c);
    }
    endwin();
    return 0;
}
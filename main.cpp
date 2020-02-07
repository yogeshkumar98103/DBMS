#include <cstdio>
#include <ncurses.h>
#include "CommandHistory.cpp"
#include <signal.h>

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
    WINDOW* win;
    win = initscr();
//    scrollok(win,TRUE);
    noecho();
    commandInput.initialize(win);
    while(true){
        c = getch();
        commandInput.handleKeys(c);
    }
    endwin();
    return 0;
}
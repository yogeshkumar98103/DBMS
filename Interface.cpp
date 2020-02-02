//
// Created by Yogesh Kumar on 2020-01-31.
//

#include <iostream>
#include <cstdio>
#include <string>

enum class MetaCommandResult{
    exit,
    empty,
    unrecognized
};

class InputBuffer{
public:
    std::string buffer;

    InputBuffer() = default;

    void readInput(){
        std::getline(std::cin, buffer);
    }

    const char* str() const{
        return buffer.c_str();
    }

    bool isMetaCommand(){
        return (buffer.empty() || buffer[0] == '.');
    }

    MetaCommandResult performMetaCommand(){
        if(buffer == ".exit"){
            return MetaCommandResult::exit;
        }
        else if(buffer.empty()){
            return MetaCommandResult::empty;
        }
        else{
            return MetaCommandResult::unrecognized;
        }
    }
};

void printPrompt(){
    printf("db > ");
}
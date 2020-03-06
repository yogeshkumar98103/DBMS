//
// Created by Yogesh Kumar on 2020-01-31.
//

#ifndef DBMS_DATATYPES_H
#define DBMS_DATATYPES_H
#include <cstring>
#include <iostream>

enum class DataType{
    Int,
    Float,
    Char,
    Bool,
    String,
};

template <typename T>
T convertDataType(const std::string& str);

namespace dbms{
    class string{
    public:
        char* str_;
        int32_t size;

        string() = default;

        string(char* memoryPool, int32_t size_){
            this->size = size_;
            str_ = new(memoryPool) char[size_];
        }

        void setBuffer(char* memoryPool, int32_t size_){
            this->size = size_;
            str_ = new(memoryPool) char[size];
        }

        string(const string& s){
            strcpy(str_, s.str_);
            size = s.size;
        }

        // string(const string& s) = delete;

        void refcopy(string& s){
            s.str_ = str_;
            s.size = size;
        }

        string(const std::string& s){
            strcpy(str_, s.c_str());
            size = s.size();
        }

        string& operator=(const string& s){
            strcpy(str_, s.str_);
            size = s.size;
            return (*this);
        }
        string& operator=(const char* s){
            strcpy(str_, s);
            size = strlen(s);
            return (*this);
        }

        string& operator=(const std::string& s){
            strcpy(str_, s.c_str());
            size = s.size();
            return (*this);
        }

        bool operator<(const string& other) const{
            return (strcmp(str_, other.str_) == -1);
        }

        bool operator<=(const string& other) const{
            return (strcmp(str_, other.str_) != 1);
        }

        bool operator>(const string& other) const{
            return (strcmp(str_, other.str_) == 1);
        }

        bool operator>=(const string& other) const{
            return (strcmp(str_, other.str_) != -1);
        }

        bool operator!=(const string& other) const{
            return (strcmp(str_, other.str_) != 0);
        }

        bool operator==(const string& other) const{
            return (strcmp(str_, other.str_) == 0);
        }

        // friend std::ostream & operator << (std::ostream &out, const string &c);
    };
}

std::ostream & operator << (std::ostream &out, const dbms::string &c);


#endif //DBMS_DATATYPES_H

//
// Created by Yogesh Kumar on 2020-01-31.
//

#ifndef DBMS_DATATYPES_H
#define DBMS_DATATYPES_H

enum class DataType{
    Int,
    Float,
    Char,
    Bool,
    String,
};

namespace dbms{
    class string{
    public:
        char* str_;
        int32_t size;
        string() = default;

        string(char* memoryPool, int32_t size_){
            this->size = size_;
            str_ = new(memoryPool) char[size];
        }

        void setBuffer(char* memoryPool, int32_t size_){
            this->size = size_;
            str_ = new(memoryPool) char[size];
        }

        string(const string& s) = delete;

        string(const std::string& s){
            strcpy(str_, s.c_str());
        }

        string& operator=(const string& s){
            strcpy(str_, s.str_);
            return (*this);
        }
        string& operator=(const char* s){
            strcpy(str_, s);
            return (*this);
        }

        string& operator=(const std::string& s){
            strcpy(str_, s.c_str());
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
    };
}

#endif //DBMS_DATATYPES_H

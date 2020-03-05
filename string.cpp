//
// Created by Yogesh Kumar on 06/03/20.
//

#include "HeaderFiles/DataTypes.h"

std::ostream & operator << (std::ostream &out, const dbms::string &c){
    out << c.str_;
    return out;
}

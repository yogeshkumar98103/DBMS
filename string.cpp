#include "HeaderFiles/DataTypes.h"

// CONVERT TEMPLATE SECIALIZATION
template <> inline int convertDataType<int>(const std::string& str)    {  return std::stoi(str);  }
template <> inline char convertDataType<char>(const std::string& str)  {  return str[0];          }
template <> inline bool convertDataType<bool>(const std::string& str)  {  return str == "true";   }
template <> inline float convertDataType<float>(const std::string& str){  return std::stof(str);  }
template <> inline dbms::string convertDataType<dbms::string>(const std::string& str){  return dbms::string(str);  }


std::ostream & operator << (std::ostream &out, const dbms::string &c){
    out << c.str_;
    return out;
}

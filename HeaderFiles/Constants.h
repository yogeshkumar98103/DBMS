//
// Created by Yogesh Kumar on 03/02/20.
//

#ifndef DBMS_CONSTANTS_H
#define DBMS_CONSTANTS_H

#include <cinttypes>

#define MAX_COLUMN_SIZE 50
const uint32_t PAGE_SIZE = 4096;
const int DEFAULT_PAGE_LIMIT = 20;

using row_t = int32_t;
using pkey_t = int32_t;
#define printw printf

#endif //DBMS_CONSTANTS_H

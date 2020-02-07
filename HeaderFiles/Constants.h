//
// Created by Yogesh Kumar on 03/02/20.
//

#ifndef DBMS_CONSTANTS_H
#define DBMS_CONSTANTS_H

#define MAX_COLUMN_SIZE 50
const uint32_t PAGE_SIZE = 4096;
const int DEFAULT_PAGE_LIMIT = 20;

using row_t = int32_t;

#ifndef LLONG_MAX
#define LLONG_MAX 9223372036854775807LL
#endif

#ifndef SIGINT
#define SIGINT 2
#endif


#endif //DBMS_CONSTANTS_H

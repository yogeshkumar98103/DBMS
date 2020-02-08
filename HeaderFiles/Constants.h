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

const int32_t BPTNodeSizeOffset         = sizeof(bool);
const int32_t BPTNodeleftSiblingOffset  = BPTNodeSizeOffset + sizeof(int32_t);
const int32_t BPTNoderightSiblingOffset = BPTNodeleftSiblingOffset + sizeof(row_t);
const int32_t BPTNodeHeaderSize         = BPTNoderightSiblingOffset + sizeof(row_t);

#define BRANCHING_FACTOR(x) (PAGE_SIZE - BPTNodeHeaderSize + sizeof(pkey_t) + x) / (2 * (sizeof(row_t) + x + sizeof(pkey_t)))
#define P_KEY_OFFSET(x) BPTNodeHeaderSize + (2 * BRANCHING_FACTOR(x) - 1) * x
#define CHILD_OFFSET(x) PAGE_SIZE - 2 * BRANCHING_FACTOR(x) * sizeof(row_t)

const int32_t intBranchingFactor    = BRANCHING_FACTOR(sizeof(int32_t));
const int32_t floatBranchingFactor  = BRANCHING_FACTOR(sizeof(float));
const int32_t charBranchingFactor   = BRANCHING_FACTOR(sizeof(char));
const int32_t boolBranchingFactor   = BRANCHING_FACTOR(sizeof(bool));

#endif //DBMS_CONSTANTS_H

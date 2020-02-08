//
// Created by Pranav Gupta on 06/02/20.
//

#ifndef DBMS_BTREE_H
#define DBMS_BTREE_H

#include <iostream>
#include <cstring>
#include <vector>
#include <memory>
#include <utility>
#include "Constants.h"
#include "Table.h"
#include "BPTreeNodeManager.h"

template <typename T>
T convert(const std::string& str);

/*
 * -------------------- BPTNode --------------------
 * 1. isLeaf            => bool
 * 2. size              => int32_t
 * 3. leftSibling       => row_t
 * 4. rightSibling      => row_T
 * 5.
 */

static const BPTNodeSizeOffset         = sizeof(bool)
static const BPTNodeleftSiblingOffset  = BPTNodeSizeOffset + sizeof(in32_t);
static const BPTNoderightSiblingOffset = leftSiblingOffset + sizeof(row_t)
static const BPTNodeHeaderSize         = rightSiblingOffset + sizeof(row_t);

template <typename key_t>
class BPTNode: Page{
    using Node = BPTNode<key_t>;
    using keyRNPair = std::pair<key_t, pkey_t>;

    bool isLeaf;
    int size;
    row_t leftSibling_;
    row_t rightSibling_;

    static int32_t childOffset = 0;
    BPTreeNodeManager* nodeManager;

    template <typename o_key_t>
    friend class BPTree;

    template <typename o_key_t>
    friend class BPTreeNodeManager;

public:

    // Setters and Getters
    Node* getChildNode(int32_t index);
    row_t getChildRow(int32_t index);
    keyRNPair getKey(int32_t index);
    void setChild(int32_t index, Node* child);
    void setChild(int32_t index, row_t pageNum);
    void setKey(int32_t index, const keyRNPair& key);
    void readHeader();
    void writeHeader();

    BPTNode(){
        isLeaf = false;
        size = 1;
        leftSibling_ = 0;
        rightSibling_ = 0;
    }

    explicit BPTNode(int32_t pageNo):BPTNode(){
        this->pageNo = pageNo;
    }
};

template <typename key_t>
struct SearchResult {
    int index; // between branchingFactor-1 and 2*branchingFactor-1
    BPTNode<key_t>* node;

    SearchResult(){
        index = -1;
        node = nullptr;
    }
};

class BPlusTreeBase{
public:
    virtual ~BPlusTreeBase() = default;
    virtual void traverseAllWithKey(std::string){}
};

template <typename key_t>
class BPTree: public BPlusTreeBase{
    using Node      = BPTNode<key_t>;
    using result_t  = SearchResult<key_t>;
    using keyRNPair = std::pair<key_t, long long int>;
    using manager_t = BPTreeNodeManager<key_t>;

    manager_t manager;
    int32_t branchingFactor;

public:
    BPTree(const char* filename, int32_t branchingFactor_);
    bool insert(const std::string& keyStr, pkey_t pkey, row_t row);
    bool search(const std::string& str);
    void traverseAllWithKey(const std::string& strKey);
    void bfsTraverse();

private:

    result_t searchUtil(const keyRNPair& key);
    void incrementLinkedList(result_t& currentPosition);
    void decrementLinkedList(result_t& currentPosition);
    int32_t binarySearch(Node* node, const keyRNPair& key);
    void splitRoot();
    void splitNode(Node* parent, Node* child, int indexFound);
    void bfsTraverseUtil(Node* start);


//    bool remove(const keyRNPair& key);


//    void greaterThanEquals(const key_t& key);
//
//    void smallerThanEquals(const key_t& key);
//
//    void greaterThan(const key_t& key);
//
//    void smallerThan(const key_t& key);

//    void removeWithKey(const key_t& key);

private:
    // MARK:- HELPER FUNCTIONS
//   void removeMultipleAtLeaf(Node* leaf, int startIndex, int countToDelete);

//    bool deleteAtLeaf(Node* leaf, int index);


//    void borrowFromLeftSibling(int indexFound, Node* parent, Node* child);

//    void borrowFromRightSibling(int indexFound, Node* parent, Node* child);

//    void mergeWithSibling(int indexFound, Node*& parent, Node* child);

//    void iterateLeftLeaf(Node* node, int startIndex);

//    void iterateRightLeaf(Node* node, int startIndex);
};

#endif //DBMS_BTREE_H

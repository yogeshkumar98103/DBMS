//
// Created by Yogesh Kumar on 07/02/20.
//

#ifndef DBMS_BPTREENODEMANAGER_H
#define DBMS_BPTREENODEMANAGER_H

#include "BTree.h"
#include "Constants.h"
#include <unordered_map>
#include <list>
#include <memory>

template <typename key_t>
class BPTreeNodeManager: Pager<BPTNode<key_t>>{
    using node_t     = BPTNode<key_t>;
    using list_t     = std::list<std::unique_ptr<node_t>>;
    using iterator_t = typename list_t::iterator;
    using base_t     = Pager<BPTNode<key_t>>;

public:

    int32_t keySize;
    int32_t stackPtr;
    int32_t branchingFactor;
    int32_t stackPtrOffset;
    row_t numPages;
    row_t rootPageNum;
    std::unique_ptr<node_t> root;

    explicit BPTreeNodeManager(const char* fileName, int nodeLimit_ = DEFAULT_PAGE_LIMIT);
    ~BPTreeNodeManager();
    row_t nextFreeIndexLocation();
    void addFreeIndexLocation(row_t location);
    node_t* read(int32_t pageNo);
    bool flushPage(node_t* node) override;
    bool getRoot();
    void setRoot(node_t* newRoot);
    bool getHeader() override;
    void copyKeys(node_t* destination, int32_t startD, node_t* source, int32_t startS, int32_t size);
    void copyChildren(node_t* destination, int32_t startD, node_t* source, int32_t startS, int32_t size);
//    template<typename callback_t>
    void shiftKeysRight(node_t* node, int32_t index);
    void shiftKeysLeft(node_t* node, int32_t index);
    void shiftChildrenRight(node_t* node, int32_t index);
    void shiftChildrenLeft(node_t* node, int32_t index);
};

#endif //DBMS_BPTREENODEMANAGER_H

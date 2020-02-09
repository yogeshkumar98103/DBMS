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

template <typename node_t>
class BPTreeNodeManager: public Pager<node_t>{
    using list_t     = std::list<std::unique_ptr<node_t>>;
    using iterator_t = typename list_t::iterator;
    using base_t     = Pager<node_t>;

public:

    int32_t keySize;
    int32_t stackPtr;
    int32_t branchingFactor;
    int32_t stackPtrOffset;
    row_t numPages;
    row_t rootPageNum;
    std::unique_ptr<node_t> root;

    BPTreeNodeManager(const char* fileName, int32_t branchingFactor_, int32_t keySize_,int nodeLimit_ = DEFAULT_PAGE_LIMIT);
    ~BPTreeNodeManager();
    row_t nextFreeIndexLocation();
    void addFreeIndexLocation(row_t location);
    node_t* read(int32_t pageNo);
    node_t* readChild(node_t* parent, int32_t childIndex);
    bool flushPage(node_t* node) override;
    bool flush(uint32_t pageNum);
    bool flushAll();
    bool getRoot();
    void setRoot(node_t* newRoot);
    bool getHeader();
    void incrementPageNum();
    void decrementPageNum();
    void deserializeHeaderMetaData();
    void serializeHeaderMetaData();
};

#include "../BPTreeNodeManager.cpp"

#endif //DBMS_BPTREENODEMANAGER_H

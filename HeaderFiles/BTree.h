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

template <typename T>
T convert(const std::string& str);

template <typename key_t>
class BPTNode{
    using Node = BPTNode<key_t>;
    using keyRNPair = std::pair<key_t, long long int>;
    bool isLeaf;
    int size;
    keyRNPair* keys;     // key and row Number
    std::unique_ptr<Node>* child;
    Node* leftSibling_;
    Node* rightSibling_;

    template <typename o_key_t>
    friend class BPTree;

public:
    explicit BPTNode(int branchingFactor):isLeaf(false),size(1),leftSibling_(nullptr),rightSibling_(nullptr){
        keys = new keyRNPair[(2 * branchingFactor - 1)];
        child = new std::unique_ptr<Node>[2 * branchingFactor];
    }

    ~BPTNode(){
        delete[] keys;
        delete[] child;
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

    std::unique_ptr<Node> root;
    int branchingFactor;

public:
    explicit BPTree(int branchingFactor_);

    bool insert(const keyRNPair& key);

    bool remove(const keyRNPair& key);

    void bfsTraverse();

    void greaterThanEquals(const key_t& key);

    void smallerThanEquals(const key_t& key);

    void greaterThan(const key_t& key);

    void smallerThan(const key_t& key);

    bool search(const std::string& str);

    void traverseAllWithKey(const key_t& key);

    void removeWithKey(const key_t& key);

private:
    // MARK:- HELPER FUNCTIONS
   void removeMultipleAtLeaf(Node* leaf, int startIndex, int countToDelete);

    result_t searchUtil(const keyRNPair& key);

    void rightPosition(result_t& currentPosition);

    void leftPosition(result_t& currentPosition);

    int binarySearch(Node* node, const keyRNPair& key);

    void splitRoot();

    void splitNode(Node* parent, Node* child, int indexFound);

    bool deleteAtLeaf(Node* leaf, int index);

    void bfsTraverseUtil(Node* start);

    void borrowFromLeftSibling(int indexFound, Node* parent, Node* child);

    void borrowFromRightSibling(int indexFound, Node* parent, Node* child);

    void mergeWithSibling(int indexFound, Node*& parent, Node* child);

    void iterateLeftLeaf(Node* node, int startIndex);

    void iterateRightLeaf(Node* node, int startIndex);
};

#endif //DBMS_BTREE_H

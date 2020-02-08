//
// Created by Yogesh Kumar on 07/02/20.
//

#include "HeaderFiles/BTree.h"
#include "HeaderFiles/Constants.h"

// CONVERT TEMPLATE SECIALIZATION
template <> int convert(const std::string& str)  {  return std::stoi(str);  }
template <> char convert(const std::string& str) {  return str[0];          }
template <> bool convert(const std::string& str) {  return str == "true";   }
template <> float convert(const std::string& str){  return std::stof(str);  }

template <typename key_t>
BPTree<key_t>::BPTree(const char* filename, int branchingFactor_):manager(filename){
    this->branchingFactor = branchingFactor_;
}


// ------------------------ GETTERS AND SETTERS ------------------------
template <typename key_t>
BPTNode<key_t>* BPTNode<key_t>::getChildNode(int32_t index){
    int32_t offset = childOffset + index * sizeof(row_t);
    row_t childRow = 0;
    memcpy(&childRow, buffer.get() + offset, sizeof(row_t));
    return nodeManager->read(childRow);
}

row_t BPTNode<key_t>::getChildRow(int32_t index){
    int32_t offset = childOffset + index * sizeof(row_t);
    row_t childRow = 0;
    memcpy(&childRow, buffer.get() + offset, sizeof(row_t));
    return childRow;
}

template <typename key_t>
std::pair<key_t, pkey_t> BPTNode<key_t>::getKey(int32_t index){
    int32_t offset = BPTNodeHeaderSize + index * nodeManager->keySize;
    key_t key = 0;
    pkey_t pkey = 0;
    memcpy(&key, buffer.get() + offset, sizeof(key_t));
    offset += sizeof(key_t);
    memcpy(&pkey, buffer.get() + offset, sizeof(pkey_t));
    return std::make_pair(key, pkey);
}

template <typename key_t>
void BPTNode<key_t>::setChild(int32_t index, Node* child){
    int32_t offset = childOffset + index * sizeof(row_t);
    memcpy(buffer.get() + offset, &child->pageNum, sizeof(row_t));
}

template <typename key_t>
void BPTNode<key_t>::setChild(int32_t index, row_t pageNum){
    int32_t offset = childOffset + index * sizeof(row_t);
    memcpy(buffer.get() + offset, &pageNum, sizeof(row_t));
}

template <typename key_t>
void BPTNode<key_t>::setKey(int32_t index, const std::pair<key_t, pkey_t>& key){
    int32_t offset = BPTNodeHeaderSize + index * nodeManager->keySize;
    memcpy(buffer.get() + offset, &key.first, sizeof(key_t));
    offset += sizeof(key_t);
    memcpy(buffer.get() + offset, &key.second, sizeof(pkey_t));
}


// ------------------------ READ / WRITE HEADER ------------------------
template<typename key_t>
void BPTNode<key_t>::writeHeader() {
    char* buffer = this->buffer->get();
    int32_t offset = 0;

    memcpy(buffer + offset, &isLeaf, sizeof(isLeaf));
    offset += sizeof(bool);
    memcpy(buffer + offset, &size, sizeof(size));
    offset += sizeof(int32_t);
    memcpy(buffer + offset, &leftSibling_, sizeof(leftSibling_));
    offset += sizeof(row_t);
    memcpy(buffer + offset, &rightSibling_, sizeof(rightSibling_));
    offset += sizeof(row_t);
}

template<typename key_t>
void BPTNode<key_t>::readHeader() {
    char* buffer = this->buffer->get();
    int32_t offset = 0;

    memcpy(&isLeaf, buffer + offset, sizeof(isLeaf));
    offset += sizeof(bool);
    memcpy(&size, buffer + offset, sizeof(size));
    offset += sizeof(int32_t);
    memcpy(&leftSibling_, buffer + offset, sizeof(leftSibling_));
    offset += sizeof(row_t);
    memcpy(&rightSibling_, buffer + offset, sizeof(rightSibling_));
    offset += sizeof(row_t);
}


// ------------------------ INSERT ------------------------
template <typename key_t>
bool BPTree<key_t>::insert(const std::string& keyStr, pkey_t pKey, row_t row) {
    auto key = std::make_pair(convert<key_t>(keyStr), pKey);
    auto root = manager.root.get();
    if(root->size == 1){
        root->setKey(0, key);
        root->isLeaf = true;
        return true;
    }

    // If root is full create a new root and split this root
    int maxSize = 2*branchingFactor - 1;
    if(root->size == maxSize){
        splitRoot();
    }

    auto current = root;
    Node* child;

    while(!current->isLeaf) {
        int indexFound = binarySearch(current, key);
        //child = current->child[indexFound].get();
        child = current->getChildNode(indexFound);
        if(child->size < maxSize){
            current = child;
            continue;
        }

        // Child is full. Split it first and then go down
        splitNode(current, child, indexFound);

        if(key <= current->keys[indexFound]) {
            // current = current->child[indexFound].get();
            current = current->getChildNode(indexFound);
        }
        else{
            // current = current->child[indexFound+1].get();
            current = current->getChildNode(indexFound + 1);
        }
    }

    int insertAtIndex = 0;
    for(int i = current->size-1; i >= 0; --i){
        if(key > current.getKey(i)){
            insertAtIndex = i+1;
            break;
        }
    }
    manager.shiftKeysRight(current, insertAtIndex);
    current->keys[insertAtIndex] = key;
    current->size++;
    return true;
}

template <typename key_t>
void BPTree<key_t>::splitRoot(){
    // root =>      newRoot
    //              /     \
    //            root   newNode

    int32_t nextPageLocation = manager.nextFreeIndexLocation();
    auto newRoot = manager.read(nextPageLocation);
    nextPageLocation = manager.nextFreeIndexLocation();
    auto newNode = manager.read(nextPageLocation);
    auto root = manager.root.get();
    if (root->isLeaf) {
        newRoot->isLeaf = true;
        newNode->isLeaf = true;
    }

    // Copy right half keys to newNode
    int maxSize = 2*branchingFactor-1;
    manager.copyKeys(newNode, 0, root, branchingFactor, branchingFactor - 2);
    manager.copyChildren(newNode, 0, root, branchingFactor, branchingFactor - 2);

    newRoot->setKey(0, root->getKey(branchingFactor - 1));
    newNode->setChild(branchingFactor-1, root->getChildRow(maxSize));
    newNode->size = branchingFactor - 1;

    root->rightSibling_ = newNode->pageNum;
    newNode->leftSibling_ = root->pageNum;

    if(!(root->isLeaf)){
        root->size = branchingFactor - 1;
    }
    else{
        root->size = branchingFactor;
    }

    newRoot->setChild(1, newNode->pageNum);
    newRoot->setChild(0, root->pageNum);
    manager.setRoot(newRoot);
    manager.root->isLeaf = false;
}

template <typename key_t>
void BPTree<key_t>::splitNode(Node* parent, Node* child, int indexFound){
    int maxSize = 2*branchingFactor - 1;

    // Shift keys right to accommodate a key from child
    manager.shiftKeysRight(parent, indexFound);
    manager.shiftChildRight(parent, indexFound + 1);
    parent->setKey(indexFound, child->getKey(branchingFactor - 1));

    int32_t nextPageNo = manager.nextFreeIndexLocation();
    auto newSibling = manager.read(nextPageNo);
    newSibling->isLeaf = child->isLeaf;

    // Copy right half keys to newNode
    manager.copyKeys(newSibling, 0 , child, branchingFactor, branchingFactor - 2);
    manager.copyChildren(newSibling, 0 , child, branchingFactor, branchingFactor - 2);
    newSibling->setChild(branchingFactor - 1, child->getChildRow(maxSize));

    newSibling->size = branchingFactor-1;
    parent->size++;
    child->size = branchingFactor;
    if(!child->isLeaf) --(child->size);

    newSibling->leftSibling_ = parent->getChildRow(indexFound);
    newSibling->rightSibling_ = parent->getChildNode(indexFound)->rightSibling_;
    if(newSibling->rightSibling_){
        newSibling->getChildNode(newSibling->rightSibling_)->leftSibling_ = newSibling->pageNum;
    }

    child->rightSibling_ = newSibling.pageNum;
    parent->setChild(indexFound + 1, newSibling->pageNum);
}


// ------------------------ SEARCH ------------------------
template <typename key_t>
bool BPTree<key_t>::search(const std::string& str){
    key_t key = convert<key_t>(str);
    auto searchRes = searchUtil(std::make_pair(key,-1));
    if(searchRes.node->size == searchRes.index) {
        searchRes.index--;
        incrementLinkedList(searchRes);
    }
    return (searchRes.node->getKey(searchRes.index).first == key);
}

template <typename key_t>
void BPTree<key_t>::traverseAllWithKey(const std::string& strKey){
    key_t key = convert<key_t>(strKey);
    auto searchRes = searchUtil(std::make_pair(key,-1));
    if(searchRes.node->size == searchRes.index) {
        searchRes.index--;
        incrementLinkedList(searchRes);
    }

    keyRNPair pair = searchRes.node->getKey(searchRes.index);
    while(searchRes.node && pair.first == key){
        std::cout << "( " << pair.first << ","<< pair.second << ") ";
        incrementLinkedList(searchRes);
        pair = searchRes.node->getKey(searchRes.index);
    }
    std::cout << std::endl;
}

template <typename key_t>
SearchResult<key_t> BPTree<key_t>::searchUtil(const keyRNPair& key){
    result_t searchRes{};

    if(manager.root != nullptr){
        auto node = manager.root.get();

        while(!(node->isLeaf)) {
            int indexFound = binarySearch(node, key);
            node = node->getChild(indexFound); //node->child[indexFound].get();
        }

        int indexFound = binarySearch(node, key);
        searchRes.index = indexFound;
        searchRes.node = node;
    }
    return searchRes;
}

template <typename key_t>
int32_t BPTree<key_t>::binarySearch(Node* node, const keyRNPair& key) {
    int l = 0;
    int r = node->size - 1;
    int mid;
    int ans = node->size;

    while(l <= r) {
        mid = (l+r)/2;
        if(key <= node->getKey(mid)){
            r = mid-1;
            ans = mid;
        }
        else{
            l = mid+1;
        }
    }
    return ans;
}


// ----------------------- TRAVERSAL ----------------------
template <typename key_t>
void BPTree<key_t>::bfsTraverse(){
    bfsTraverseUtil(manager.root->get());
    std::cout << std::endl;
}

template <typename key_t>
void BPTree<key_t>::bfsTraverseUtil(Node* start){
    if(start == nullptr) return;

    printf("%d# ", start->size);
    for(int32_t i = 0; i < start->size; ++i){
        auto keyPair = start->getKey(i);
        std::cout << keyPair.first << "(" << keyPair.second << ") ";
    }
    std::cout << std::endl;

    if(!start->isLeaf) {
        for (int i = 0; i < start->size + 1; ++i) {
            bfsTraverseUtil(start->getChild(i));
        }
    }
}


// ----------------------- HELPERS ----------------------
template <typename key_t>
void BPTree<key_t>::incrementLinkedList(result_t& currentPosition){
    if(currentPosition.index < currentPosition.node->size-1) {
        currentPosition.index++;
    }
    else {
        if(currentPosition.node->rightSibling_){
            currentPosition.node = currentPosition.node->rightSibling_;
            currentPosition.index = 0;
        }
        else {
            currentPosition.node = nullptr;
            currentPosition.index = -1;
        }
    }
}

template <typename key_t>
void BPTree<key_t>::decrementLinkedList(result_t& currentPosition){
    if(currentPosition.index>0) {
        currentPosition.index--;
    }
    else {
        if(currentPosition.node->leftSibling_){
            currentPosition.node = currentPosition.node->leftSibling_;
            currentPosition.index = currentPosition.node->size-1;
        }
        else {
            currentPosition.node = nullptr;
            currentPosition.index = -1;
        }
    }
}

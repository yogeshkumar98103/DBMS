//
// Created by Yogesh Kumar on 07/02/20.
//

#include "HeaderFiles/BTree.h"

template <typename key_t> int32_t BPTNode<key_t>::pKeyOffset = P_KEY_OFFSET(sizeof(key_t));
template <typename key_t> int32_t BPTNode<key_t>::childOffset = CHILD_OFFSET(sizeof(key_t));

// CONVERT TEMPLATE SECIALIZATION
template <> inline int convertDataType<int>(const std::string& str)    {  return std::stoi(str);  }
template <> inline char convertDataType<char>(const std::string& str)  {  return str[0];          }
template <> inline bool convertDataType<bool>(const std::string& str)  {  return str == "true";   }
template <> inline float convertDataType<float>(const std::string& str){  return std::stof(str);  }
template <> inline dbms::string convertDataType<dbms::string>(const std::string& str){  return dbms::string(str);  }

template <typename key_t>
BPTree<key_t>::BPTree(const char* filename, int32_t branchingFactor_, int32_t keySize_):manager(filename, branchingFactor_, keySize_){
    this->branchingFactor = branchingFactor_;
    this->keySize = keySize_;
}


// ------------------------ GETTERS AND SETTERS ------------------------
template <typename key_t>
BPTNode<key_t>* BPTNode<key_t>::getChildNode(manager_t& manager, int32_t index){
    return manager.read(child[index]);
}

template <typename key_t>
BPTNode<key_t>* BPTNode<key_t>:: getRightSibling(manager_t& manager){
    if(rightSibling_ <= 0) return nullptr;
    return manager.read(rightSibling_);
}

template <typename key_t>
BPTNode<key_t>* BPTNode<key_t>:: getLeftSibling(manager_t& manager){
    if(leftSibling_ <= 0) return nullptr;
    return manager.read(leftSibling_);
}

// ------------------------ READ / WRITE HEADER ------------------------
template<typename key_t>
void BPTNode<key_t>::writeHeader() {
    char* buffer = this->buffer.get();
    int32_t offset = 0;

    memcpy(buffer + offset, &isLeaf, sizeof(isLeaf));
    offset += sizeof(bool);
    memcpy(buffer + offset, &size, sizeof(size));
    offset += sizeof(int32_t);
    memcpy(buffer + offset, &leftSibling_, sizeof(row_t));
    offset += sizeof(row_t);
    memcpy(buffer + offset, &rightSibling_, sizeof(row_t));
    offset += sizeof(row_t);
}

template<typename key_t>
void inline BPTNode<key_t>::readHeader(int32_t maxSize, int32_t keySize) {
    char* buffer = this->buffer.get();
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

template<typename key_t>
void inline BPTNode<key_t>::allocate(int32_t maxSize, int32_t keySize){
    char* buffer = this->buffer.get();
    keys = new(buffer + BPTNodeHeaderSize) key_t[maxSize];
    pkeys = new(buffer + pKeyOffset) pkey_t[maxSize];
    child = new(buffer + childOffset) row_t[maxSize + 1];
}

template<>
void inline BPTNode<dbms::string>::allocate(int32_t maxSize, int32_t keySize){
    char* buffer = this->buffer.get();
    keys = new dbms::string[size];
    for(int i = 0; i < maxSize; ++i){
        keys[i].setBuffer(buffer + BPTNodeHeaderSize + keySize * i, keySize);
    }
    pkeys = new(buffer + pKeyOffset) pkey_t[maxSize];
    child = new(buffer + childOffset) row_t[maxSize + 1];
}

// ------------------------ INSERT ------------------------
template <typename key_t>
bool BPTree<key_t>::insert(const std::string& keyStr, pkey_t pkey, row_t row) {
    auto key = convertDataType<key_t>(keyStr);
    auto root = manager.root.get();
    if(root->size == 0){
        root->keys[0] = key;
        root->pkeys[0] = pkey;
        root->child[0] = row;
        root->isLeaf = true;
        root->size++;
        root->hasUncommitedChanges = true;
        return true;
    }

    // If root is full create a new root and split this root
    int maxSize = 2*branchingFactor - 1;
    if(root->size == maxSize){
        splitRoot();
    }

    auto current = manager.root.get();;
    Node* child;

    while(!current->isLeaf) {
        int indexFound = binarySearch(current, key, pkey);
        child = current->getChildNode(manager, indexFound);
        if(child->size < maxSize){
            current = child;
            continue;
        }

        // Child is full. Split it first and then go down
        splitNode(current, child, indexFound);

        if(key <= current->keys[indexFound]) {
            current = current->getChildNode(manager, indexFound);
        }
        else{
            current = current->getChildNode(manager, indexFound + 1);
        }
    }

    int insertAtIndex = 0;
    for(int i = current->size-1; i >= 0; --i){
        if(key <= current->keys[i] || (key == current->keys[i] && pkey <= current->pkeys[i])){
            current->keys[i+1] = current->keys[i];
            current->child[i+1] = current->child[i];
            current->pkeys[i+1] = current->pkeys[i];
        }
        else {
            insertAtIndex = i+1;
            break;
        }
    }

    current->keys[insertAtIndex] = key;
    current->pkeys[insertAtIndex] = pkey;
    current->child[insertAtIndex] = row;
    current->size++;
    current->hasUncommitedChanges = true;
    return true;
}

template <typename key_t>
void BPTree<key_t>::splitRoot(){
    // root =>      newRoot
    //              /     \
    //            root   newNode

//    int32_t nextPageLocation = manager.nextFreeIndexLocation();
//    auto newRoot = manager.read(nextPageLocation);
//    manager.incrementPageNum();
//    nextPageLocation = manager.nextFreeIndexLocation();
//    auto newNode = manager.read(nextPageLocation);
//    manager.incrementPageNum();
    auto newRoot = manager.newNode();
    auto newNode = manager.newNode();

    auto root = manager.root.get();
    if (root->isLeaf) {
        newRoot->isLeaf = true;
        newNode->isLeaf = true;
    }

    
    // Copy right half keys to newNode
    int maxSize = 2*branchingFactor-1;
    for(int i = branchingFactor; i < maxSize; ++i){
        newNode->keys[i-branchingFactor]  = root->keys[i];
        newNode->pkeys[i-branchingFactor]  = root->pkeys[i];
        newNode->child[i-branchingFactor] = root->child[i];
    }

    newRoot->keys[0]  = root->keys[branchingFactor - 1];
    newRoot->pkeys[0]  = root->pkeys[branchingFactor - 1];
    newRoot->size = 1;

    newNode->size = branchingFactor - 1;

    if(!newNode->isLeaf){ 
        newNode->child[branchingFactor-1] = root->child[maxSize];
    }

    root->rightSibling_ = newNode->pageNum;
    newNode->leftSibling_ = root->pageNum;

    if(!(root->isLeaf)){
        root->size = branchingFactor - 1;
    }
    else{
        root->size = branchingFactor;
    }

    newRoot->child[1] = newNode->pageNum;
    newRoot->child[0] = root->pageNum;
    manager.setRoot(newRoot);
    manager.root->isLeaf = false;

    root->hasUncommitedChanges = true;
    newNode->hasUncommitedChanges = true;
    newRoot->hasUncommitedChanges = true;
}

template <typename key_t>
void BPTree<key_t>::splitNode(Node* parent, Node* child, int indexFound){
    int maxSize = 2*branchingFactor - 1;

    // Shift keys right to accommodate a key from child
    for(int i = parent->size - 1; i >= indexFound; --i){
        parent->keys[i+1]  = parent->keys[i];
        parent->pkeys[i+1]  = parent->pkeys[i];
        parent->child[i+2] = parent->child[i+1];
    }
    parent->keys[indexFound] = child->keys[branchingFactor - 1];
    parent->pkeys[indexFound] = child->pkeys[branchingFactor - 1];

    int32_t nextPageNo = manager.nextFreeIndexLocation();
    Node* newSibling = manager.read(nextPageNo);
    manager.incrementPageNum();
    newSibling->isLeaf = child->isLeaf;

    // Copy right half keys to newNode
    for(int i = branchingFactor; i < maxSize; ++i) {
        newSibling->keys[i-branchingFactor]  = child->keys[i];
        newSibling->pkeys[i-branchingFactor]  = child->pkeys[i];
        newSibling->child[i-branchingFactor] = child->child[i];
    }

    newSibling->size = branchingFactor-1;
    parent->size++;
    child->size = branchingFactor;
    if(!child->isLeaf) {
        --(child->size);
        newSibling->child[branchingFactor-1] = child->child[maxSize];
    }

    newSibling->leftSibling_ = parent->child[indexFound];
    newSibling->rightSibling_ = parent->getChildNode(manager, indexFound)->rightSibling_;
    if(newSibling->rightSibling_){
        newSibling->getChildNode(manager, newSibling->rightSibling_)->leftSibling_ = newSibling->pageNum;
    }

    child->rightSibling_ = newSibling->pageNum;
    parent->child[indexFound+1] = newSibling->pageNum;

    newSibling->hasUncommitedChanges = true;
    parent->hasUncommitedChanges = true;
    child->hasUncommitedChanges = true;
}


// ------------------------ SEARCH ------------------------
template <typename key_t>
bool BPTree<key_t>::search(const std::string& strKey){
    key_t key = convertDataType<key_t>(strKey);
    auto searchRes = searchUtil(key, -1);
    if(searchRes.node->size == searchRes.index) {
        searchRes.index--;
        incrementLinkedList(searchRes);
    }
    return (searchRes.node->key[searchRes.index] == key);
}

template <typename key_t>
void BPTree<key_t>::traverseAllWithKey(const std::string& strKey){
    key_t key = convertDataType<key_t>(strKey);
    auto searchRes = searchUtil(key,-1);
    if(searchRes.node->size == searchRes.index) {
        searchRes.index--;
        incrementLinkedList(searchRes);
    }

    while(searchRes.node && searchRes.node->keys[searchRes.index] == key){
        std::cout << "( " << searchRes.node->keys[searchRes.index] << ","<< searchRes.node->pkeys[searchRes.index] << ") ";
        incrementLinkedList(searchRes);
    }
    std::cout << std::endl;
}

template <typename key_t>
SearchResult<key_t> BPTree<key_t>::searchUtil(const key_t& key, const pkey_t& pkey){
    result_t searchRes{};

    if(manager.root != nullptr){
        auto node = manager.root.get();

        while(!(node->isLeaf)) {
            int indexFound = binarySearch(node, key, pkey);
            node = node->getChildNode(manager, indexFound);
        }

        int indexFound = binarySearch(node, key, pkey);
        searchRes.index = indexFound;
        searchRes.node = node;
    }
    return searchRes;
}

template <typename key_t>
int32_t BPTree<key_t>::binarySearch(Node* node, const key_t& key, const pkey_t pkey) {
    int l = 0;
    int r = node->size - 1;
    int mid;
    int ans = node->size;

    while(l <= r) {
        mid = (l+r)/2;
        if((key < node->keys[mid]) || (key == node->keys[mid] && pkey < node->pkeys[mid])){
            r = mid-1;
            ans = mid;
        }
        else{
            l = mid+1;
        }
    }
    return ans;
}

// ----------------------- DELETE ----------------------
template <typename key_t>
bool BPTree<key_t>::remove(const std::string& keyStr, const pkey_t pkey){
    auto key = convertDataType<key_t>(keyStr);
    Node* root = manager.root.get();
    if(root == nullptr || root->size == 0){
        return false;
    }

    Node* current = root;
    Node* child;
    int maxSize = 2*branchingFactor - 1;
    while(!current->isLeaf){
        int indexFound = binarySearch(current, key, -1);
        child = current->getChildNode(manager, indexFound);

        if(child->size != branchingFactor - 1){
            current = child;
            continue;
        }

        // If child is of size branchingFactor-1 fix it and then traverse in
        bool flag = false;
        Node *leftSibling = nullptr, *rightSibling = nullptr;

        if(indexFound > 0 && (leftSibling = current->getChildNode(manager, indexFound - 1)) && leftSibling->size > branchingFactor-1){
            borrowFromLeftSibling(indexFound, current, child, leftSibling);
            current = child;
        }
        else if(indexFound < current->size && (rightSibling = current->getChildNode(manager, indexFound + 1)) && rightSibling->size > branchingFactor-1){
            borrowFromRightSibling(indexFound, current, child, rightSibling);
            current = child;
        }
        else{
            mergeWithSibling(indexFound, current, child, leftSibling, rightSibling);
        }
    }

    // Now we are in a leaf node
    int indexFound = binarySearch(current, key, pkey);
    if(indexFound < current->size) {
        if (current->keys[indexFound] == key){
            deleteAtLeaf(current, indexFound);
            return true;
        }
    }
    return false;
}

template <typename key_t>
template <typename callback_t>
bool BPTree<key_t>::remove(const std::string& keyStr, const callback_t& callback, const pkey_t pkey){
    auto key = convertDataType<key_t>(keyStr);
    while(true){
        Node* root = manager.root.get();
        if(root == nullptr || root->size == 0){
            return true;
        }

        Node* current = root;
        Node* child;
        int maxSize = 2*branchingFactor - 1;
        while(!current->isLeaf){
            int indexFound = binarySearch(current, key, -1);
            child = current->getChildNode(manager, indexFound);

            if(child->size != branchingFactor - 1){
                current = child;
                continue;
            }

            // If child is of size branchingFactor-1 fix it and then traverse in
            bool flag = false;
            Node* leftSibling = nullptr, *rightSibling = nullptr;

            if(indexFound > 0 && (leftSibling = current->getChildNode(manager, indexFound - 1)) && leftSibling->size > branchingFactor-1){
                borrowFromLeftSibling(indexFound, current, child, leftSibling);
                current = child;
            }
            else if(indexFound < current->size && (rightSibling = current->getChildNode(manager, indexFound + 1)) && rightSibling->size > branchingFactor-1){
                borrowFromRightSibling(indexFound, current, child, rightSibling);
                current = child;
            }
            else{
                mergeWithSibling(indexFound, current, child, leftSibling, rightSibling);
            }
        }

        // Now we are in a leaf node
        int indexFound = binarySearch(current, key, -1);
        if(indexFound < current->size) {
            if (current->keys[indexFound] == key){
                auto row = deleteAtLeaf(current, indexFound);
                if(!callback(row)) return false;
            }
            if(pkey != -1) return true;
        }
        else return true;
    }
}

template <typename key_t>
row_t BPTree<key_t>::deleteAtLeaf(Node* node, int index){
    Node* root = manager.root.get();
    int res = node->child[index];
    if(root->isLeaf && root->size == 1){
        root->size = 0;
        root->hasUncommitedChanges = true;
        return res;
    }
    for(int i = index; i < node->size-1; ++i){
        node->keys[i] = node->keys[i+1];
        node->pkeys[i] = node->pkeys[i+1];
        node->child[i] = node->child[i+1];
    }
    node->size--;
    node->hasUncommitedChanges = true;
    return res;
}

template <typename key_t>
void BPTree<key_t>::borrowFromLeftSibling(int indexFound, Node* parent, Node* child, Node* leftSibling){
    if(child->isLeaf){
        for(int i = child->size-1; i >= 0; --i){
            child->keys[i+1] = child->keys[i];
            child->pkeys[i+1] = child->pkeys[i];
            child->child[i+1] = child->child[i];
        }
        child->keys[0] = leftSibling->keys[leftSibling->size-1];
        child->pkeys[0] = leftSibling->pkeys[leftSibling->size-1];
        child->child[0] = leftSibling->child[leftSibling->size-1];
        parent->keys[indexFound-1] = leftSibling->keys[leftSibling->size-2];
        parent->pkeys[indexFound-1] = leftSibling->pkeys[leftSibling->size-2];
    }
    else {
        for(int i=child->size-1;i>=0;i--){
            child->keys[i+1]  = child->keys[i];
            child->pkeys[i+1]  = child->pkeys[i];
        }
        child->child[1] = child->child[0];
        child->keys[0]  = leftSibling->keys[leftSibling->size-1];
        child->pkeys[0]  = leftSibling->pkeys[leftSibling->size-1];
        child->child[0] = leftSibling->child[leftSibling->size-1];
        parent->keys[indexFound-1] = leftSibling->keys[leftSibling->size-2];
        parent->pkeys[indexFound-1] = leftSibling->pkeys[leftSibling->size-2];
    }
    leftSibling->size--;
    child->size++;
    child->hasUncommitedChanges = true;
    parent->hasUncommitedChanges = true;
    leftSibling->hasUncommitedChanges = true;
}

template <typename key_t>
void BPTree<key_t>::borrowFromRightSibling(int indexFound, Node* parent, Node* child, Node* rightSibling){
    if (child->isLeaf) {
        parent->keys[indexFound] = rightSibling->keys[0];
        parent->pkeys[indexFound] = rightSibling->pkeys[0];
        child->keys[child->size] = rightSibling->keys[0];
        child->pkeys[child->size] = rightSibling->pkeys[0];
        child->child[child->size] = rightSibling->child[0];
        for(int i=0;i<rightSibling->size-1;i++){
            rightSibling->keys[i] = rightSibling->keys[i+1];
            rightSibling->pkeys[i] = rightSibling->pkeys[i+1];
            rightSibling->child[i] = rightSibling->child[i+1];
        }

    }
    else {
        child->keys[child->size]    = parent->keys[indexFound];
        child->pkeys[child->size]    = parent->pkeys[indexFound];
        child->child[child->size+1] = rightSibling->child[0];
        parent->keys[indexFound] = rightSibling->keys[0];
        parent->pkeys[indexFound] = rightSibling->pkeys[0];

        for(int i=0;i<rightSibling->size-1;i++){
            rightSibling->keys[i] = rightSibling->keys[i+1];
            rightSibling->pkeys[i] = rightSibling->pkeys[i+1];
            rightSibling->child[i] = rightSibling->child[i+1];
        }
        rightSibling->child[rightSibling->size-1] = rightSibling->child[rightSibling->size];
    }
    child->size++;
    rightSibling->size--;
    child->hasUncommitedChanges = true;
    parent->hasUncommitedChanges = true;
    rightSibling->hasUncommitedChanges = true;
}

template <typename key_t>
void BPTree<key_t>::mergeWithSibling(int indexFound, Node*& parent, Node* child, Node* leftSibling, Node* rightSibling){
    int maxSize = 2 * branchingFactor - 1;
    if(indexFound > 0){
        leftSibling->rightSibling_ =  child->rightSibling_;
        if(leftSibling->rightSibling_) {
            rightSibling->leftSibling_ = leftSibling->pageNum;
            rightSibling->hasUncommitedChanges = true;
        }
        if(leftSibling->isLeaf){
            for(int i = 0; i < child->size; ++i){
                leftSibling->keys[branchingFactor+i-1]  = child->keys[i];
                leftSibling->pkeys[branchingFactor+i-1] = child->pkeys[i];
                leftSibling->child[branchingFactor+i-1] = child->child[i];
            }

            for(int i = indexFound-1; i < parent->size-1; ++i){
                parent->keys[i]    = parent->keys[i+1];
                parent->pkeys[i]   = parent->pkeys[i+1];
                parent->child[i+1] = parent->child[i+2];
            }
            leftSibling->size = maxSize - 1;
        }
        else{
            leftSibling->keys[branchingFactor-1] = parent->keys[indexFound-1];
            leftSibling->pkeys[branchingFactor-1] = parent->pkeys[indexFound-1];
            leftSibling->child[branchingFactor] = child->child[0];

            for(int i = 0; i < child->size; ++i){
                leftSibling->keys[branchingFactor+i] = child->keys[i];
                leftSibling->pkeys[branchingFactor+i] = child->pkeys[i];
                leftSibling->child[branchingFactor+i+1]  = child->child[i+1];
            }

            for(int i = indexFound-1; i < parent->size-1; ++i){
                parent->keys[i]    = parent->keys[i+1];
                parent->pkeys[i]    = parent->pkeys[i+1];
                parent->child[i+1] = parent->child[i+2];
            }
            leftSibling->size = maxSize;
        }

        parent->size--;
        if(parent->size == 0){
            // happens only when parent is root
            // manager.addFreeIndexLocation(parent->pageNum);
            manager.deleteNode(parent);
            manager.setRoot(leftSibling);
        }
        manager.deleteNode(child);
        // manager.addFreeIndexLocation(child->pageNum);
        leftSibling->hasUncommitedChanges = true;
        parent->hasUncommitedChanges = true;
        // child->hasUncommitedChanges = false;
        parent = leftSibling;
    }
    else if(indexFound < parent->size){
        rightSibling->leftSibling_ = child->leftSibling_;
        if(leftSibling){
            leftSibling->rightSibling_ = rightSibling->pageNum;
            leftSibling->hasUncommitedChanges = true;
        }
        if(rightSibling->isLeaf){
            for(int i = rightSibling->size - 1 ; i >= 0; i--){
                rightSibling->keys[branchingFactor+i-1] = rightSibling->keys[i];
                rightSibling->pkeys[branchingFactor+i-1] = rightSibling->pkeys[i];
                rightSibling->child[branchingFactor+i-1] = rightSibling->child[i];
            }
            for(int i = 0; i < child->size ; i++){
                rightSibling->keys[i] = child->keys[i];
                rightSibling->pkeys[i] = child->pkeys[i];
                rightSibling->child[i] = child->child[i];
            }

            rightSibling->size = maxSize - 1;
            parent->child[indexFound] = rightSibling->pageNum;
            for(int i = indexFound; i < parent->size-1; ++i){
                parent->keys[i]    = parent->keys[i+1];
                parent->pkeys[i]   = parent->pkeys[i+1];
                parent->child[i+1] = parent->child[i+2];
            }
        }
        else {
            rightSibling->child[branchingFactor+rightSibling->size] = rightSibling->child[rightSibling->size];
            for(int i = rightSibling->size - 1 ; i >= 0; i--){
                rightSibling->keys[branchingFactor+i]  = rightSibling->keys[i];
                rightSibling->pkeys[branchingFactor+i] = rightSibling->pkeys[i];
                rightSibling->child[branchingFactor+i] = rightSibling->child[i];
            }
            rightSibling->keys[branchingFactor-1] = parent->keys[indexFound];
            rightSibling->pkeys[branchingFactor-1] = parent->pkeys[indexFound];
            rightSibling->child[branchingFactor-1] = child->child[branchingFactor-1];

            for(int i = 0; i < child->size; ++i){
                rightSibling->keys[i]  = child->keys[i];
                rightSibling->pkeys[i] = child->pkeys[i];
                rightSibling->child[i] = child->child[i];
            }
            rightSibling->size = maxSize;
            parent->child[indexFound] = rightSibling->pageNum;
            for(int i = indexFound; i < parent->size-1; ++i){
                parent->keys[i]    = parent->keys[i+1];
                parent->pkeys[i]   = parent->pkeys[i+1];
                parent->child[i+1] = parent->child[i+2];
            }
        }


        parent->size--;
        if(parent->size == 0) {
            // happens only when current is root
            manager.setRoot(rightSibling);
            manager.deleteNode(parent);
            // manager.addFreeIndexLocation(parent->pageNum);
        }
        manager.deleteNode(child);
        // manager.addFreeIndexLocation(child->pageNum);
        rightSibling->hasUncommitedChanges = true;
        parent->hasUncommitedChanges = true;
        // child->hasUncommitedChanges = false;
        parent = rightSibling;
    }
}


// ----------------------- TRAVERSAL ----------------------
template <typename key_t>
bool BPTree<key_t>::traverse(const std::function<bool(row_t row)>& callback){
    Node* root = manager.root.get();
    if(root->size == 0) return true;
    while(!root->isLeaf) root = root->getChildNode(manager, 0);
    return iterateRightLeaf(root,0, callback);
}

template <typename key_t>
bool BPTree<key_t>::BFStraverse(const std::function<bool(row_t row)>& callback){
    return traverseUtil(manager.root.get(), callback);
}

template <typename key_t>
bool BPTree<key_t>::traverseUtil(Node* start, const std::function<bool(row_t row)>& callback){
    if(start == nullptr) return true;

    if(start->isLeaf){
        for(int32_t i = 0; i < start->size; ++i){
            if(!callback(start->child[i])) return false;
        }
    }

    if(!start->isLeaf) {
        for (int i = 0; i < start->size + 1; ++i) {
            if(!traverseUtil(start->getChildNode(manager, i), callback)) return false;
        }
    }
    return true;
}

template <typename key_t>
void BPTree<key_t>::bfsTraverseDebug(){
    bfsTraverseUtilDebug(manager.root.get());
    std::cout << std::endl;
}

template <typename key_t>
void BPTree<key_t>::bfsTraverseUtilDebug(Node* start){
    if(start == nullptr) return;

    printf("%d# ", start->size);
    for(int32_t i = 0; i < start->size; ++i){
        std::cout << start->keys[i] << "(" << start->pkeys[i] << ") ";
    }
    std::cout << std::endl;

    if(!start->isLeaf) {
        for (int i = 0; i < start->size + 1; ++i) {
            bfsTraverseUtil(start->getChildNode(manager, i));
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

template <typename key_t>
bool BPTree<key_t>::iterateRightLeaf(Node* node, int startIndex, const std::function<bool(row_t row)>& callback){
    while(node!=nullptr){
        for(int i=startIndex;i<node->size;i++){
            if(!callback(node->child[i])) return false;
        }
        node = node->getRightSibling(manager);
        startIndex=0;
    }
    return true;
}

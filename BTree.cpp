//
// Created by Pranav Gupta on 2020-01-31.
//

#include "HeaderFiles/BTree.h"
#include "HeaderFiles/Constants.h"

template <>
int convert(const std::string& str){
    return std::stoi(str);
}

template <>
char convert(const std::string& str){
    return str[0];
}

template <>
bool convert(const std::string& str){
    return str == "true";
}

template <>
float convert(const std::string& str){
    return std::stof(str);
}

template <typename key_t>
BPTree<key_t>::BPTree(int branchingFactor_):branchingFactor(branchingFactor_), root(nullptr){}

template <typename key_t>
bool BPTree<key_t>::insert(const keyRNPair& key, row_t row) {
    if(root == nullptr){
        root = std::make_unique<Node>(branchingFactor);
        root->keys[0] = key;
        root->isLeaf = true;
        return true;
    }

    // If root is full create a new root and split this root
    int maxSize = 2*branchingFactor - 1;
    if(root->size == maxSize){
        splitRoot();
    }

    auto current = root.get();
    Node* child;

    while(!current->isLeaf) {
        int indexFound = binarySearch(current, key);
        child = current->child[indexFound].get();

        if(child->size < maxSize){
            current = child;
            continue;
        }

        // Child is full. Split it first and then go down
        splitNode(current, child, indexFound);

        if(key <= current->keys[indexFound]) {
            current = current->child[indexFound].get();
        }
        else{
            current = current->child[indexFound+1].get();
        }
    }

    int insertAtIndex = 0;
    for(int i = current->size-1; i >= 0; --i){
        if(key <= current->keys[i]){
            current->keys[i+1] = current->keys[i];
        }
        else {
            insertAtIndex = i+1;
            break;
        }
    }

    current->keys[insertAtIndex] = key;
    current->size++;
    return true;
}

template <typename key_t>
bool BPTree<key_t>::remove(const keyRNPair& key) {
    if(root == nullptr){
        return false;
    }

    auto current = root.get();
    Node* child;
    int maxSize = 2*branchingFactor - 1;
    while(!current->isLeaf){
        int indexFound = binarySearch(current, key);
        child = current->child[indexFound].get();

        if(child->size != branchingFactor - 1){
            current = child;
            continue;
        }

        // If child is of size branchingFactor-1 fix it and then traverse in
        bool flag = false;
        auto leftSibling  = current->child[indexFound-1].get();
        auto rightSibling = current->child[indexFound+1].get();

        if(indexFound > 0 && leftSibling->size > branchingFactor-1){
            borrowFromLeftSibling(indexFound, current, child);
            current = child;
        }
        else if(indexFound < current->size && rightSibling->size > branchingFactor-1){
            borrowFromRightSibling(indexFound, current, child);
            current = child;
        }
        else{
            mergeWithSibling(indexFound, current, child);
        }
    }

    // Now we are in a leaf node
    int indexFound = binarySearch(current, key);
    if(indexFound < current->size) {
        if (current->keys[indexFound] == key){
            return deleteAtLeaf(current,indexFound);
        }
    }
    return false;
}

template <typename key_t>
void BPTree<key_t>::bfsTraverse() {
    bfsTraverseUtil(root.get());
    std::cout << std::endl;
}

template <typename key_t>
void BPTree<key_t>::greaterThanEquals(const key_t& key) {
    auto searchRes = searchUtil(std::make_pair(key,-1));
    if(searchRes.node->size == searchRes.index) {
        searchRes.index--;
        incrementLinkedList(searchRes);
    }
    iterateRightLeaf(searchRes.node, searchRes.index);
    printf("\n");
}

template <typename key_t>
void BPTree<key_t>::smallerThanEquals(const key_t& key) {
    auto searchRes = searchUtil(std::make_pair(key,LLONG_MAX));
    if(searchRes.node->size == searchRes.index) {
        searchRes.index--;
    }
    else{
        decrementLinkedList(searchRes);
    }
    iterateLeftLeaf(searchRes.node, searchRes.index);
    printf("\n");
}

template <typename key_t>
void BPTree<key_t>::greaterThan(const key_t& key) {
    auto searchRes = searchUtil(std::make_pair(key,LLONG_MAX));
    if(searchRes.node->size == searchRes.index) {
        searchRes.index--;
        incrementLinkedList(searchRes);
    }
    iterateRightLeaf(searchRes.node, searchRes.index);
    printf("\n");
}

template <typename key_t>
void BPTree<key_t>::smallerThan(const key_t& key) {
    auto searchRes = searchUtil(std::make_pair(key,-1));
    if(searchRes.node->size == searchRes.index) {
        searchRes.index--;
    }
    else {
        decrementLinkedList(searchRes);
    }
    iterateLeftLeaf(searchRes.node, searchRes.index);
    printf("\n");
}

template <typename key_t>
bool BPTree<key_t>::search(const std::string& str){
    key_t key = convert<key_t>(str);
    auto searchRes = searchUtil(std::make_pair(key,-1));
    if(searchRes.node->size == searchRes.index) {
        searchRes.index--;
        incrementLinkedList(searchRes);
    }
    if(searchRes.node->keys[searchRes.index].first != key) {
        return false;
    }
    return true;
}

template <typename key_t>
void BPTree<key_t>::traverseAllWithKey(const key_t& key){
    // key_t key = convert<key_t>(str);
    auto searchRes = searchUtil(std::make_pair(key,-1));
    if(searchRes.node->size == searchRes.index) {
        searchRes.index--;
        incrementLinkedList(searchRes);
    }
    while(searchRes.node && searchRes.node->keys[searchRes.index].first == key){
        printf("%d(%ld) ", searchRes.node->keys[searchRes.index].first,searchRes.node->keys[searchRes.index].second);
        incrementLinkedList(searchRes);
    }
    printf("\n");
}

    // result_t findFirstKey(const key_t& key){
    //     auto searchRes = searchUtil(std::make_pair(key,-1));
    //     if(searchRes.node->size == searchRes.index || searchRes.node->keys[searchRes.index].first != key) {
    //         searchRes.node = nullptr;
    //         searchRes.index = -1;
    //     }
    //     return searchRes;
    // }


    // result_t findLastKey(const key_t& key){
    //     auto searchRes = searchUtil(std::make_pair(key,LLONG_MAX));
    //     if(searchRes.node->size == searchRes.index || searchRes.node->keys[searchRes.index].first != key) {
    //         return false;
    //     }
    //     return true;
    // }

template <typename key_t>
void BPTree<key_t>::removeWithKey(const key_t& key){
        auto searchRes = searchUtil(std::make_pair(key,-1));
        if(searchRes.node->size == searchRes.index) {
            searchRes.index--;
            incrementLinkedList(searchRes);
        }
        int startIndexNode = searchRes.index;
        int endIndexNode = startIndexNode;
        while(endIndexNode < searchRes.node->size){
            if(searchRes.node->keys[endIndexNode].first != key){
                break;
            }
            endIndexNode++;
        }

        int countCanDelete = searchRes.node->size - (branchingFactor-1);

     }

// MARK:- HELPER FUNCTIONS

template <typename key_t>
void removeMultipleAtLeaf(BPTNode<key_t>* leaf, int startIndex, int countToDelete){
    for(int i = index; i < leaf->size-1; ++i){
        leaf->keys[i] = leaf->keys[i+countToDelete];
    }
    leaf->size-=countToDelete;
}

template <typename key_t>
SearchResult<key_t> BPTree<key_t>::searchUtil(const keyRNPair& key){
    result_t searchRes{};

    if(root != nullptr){
        auto node = root.get();

        while(!(node->isLeaf)) {
            int indexFound = binarySearch(node, key);
            node = node->child[indexFound].get();
        }

        int indexFound = binarySearch(node, key);
        searchRes.index = indexFound;
        searchRes.node = node;
    }
    return searchRes;
}

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
int BPTree<key_t>::binarySearch(Node* node, const keyRNPair& key) {
    int l = 0;
    int r = node->size - 1;
    int mid;
    int ans = node->size;

    while(l <= r) {
        mid = (l+r)/2;
        if(key <= node->keys[mid]){
            r = mid-1;
            ans = mid;
        }
        else{
            l = mid+1;
        }
    }
    return ans;
}

template <typename key_t>
void BPTree<key_t>::splitRoot(){
    // root =>      newRoot
    //              /     \
    //            root   newNode

    auto newRoot = std::make_unique<Node>(branchingFactor);
    auto newNode = std::make_unique<Node>(branchingFactor);
    if (root->isLeaf) {
        newRoot->isLeaf = true;
        newNode->isLeaf = true;
    }

    // Copy right half keys to newNode
    int maxSize = 2*branchingFactor-1;
    for(int i = branchingFactor; i < maxSize; ++i){
        newNode->keys[i-branchingFactor]  = root->keys[i];
        newNode->child[i-branchingFactor] = std::move(root->child[i]);
    }

    newRoot->keys[0]  = root->keys[branchingFactor - 1];
    newNode->size     = branchingFactor - 1;
    newNode->child[branchingFactor-1] = std::move(root->child[maxSize]);

    root->rightSibling_ = newNode.get();
    newNode->leftSibling_ = root.get();

    if(!(root->isLeaf)){
        root->size = branchingFactor - 1;
    }
    else{
        root->size = branchingFactor;
    }

    newRoot->child[1] = std::move(newNode);
    newRoot->child[0] = std::move(root);
    this->root        = std::move(newRoot);
    root->isLeaf      = false;
}

template <typename key_t>
void BPTree<key_t>::splitNode(Node* parent, Node* child, int indexFound){
    int maxSize = 2*branchingFactor - 1;
    // Shift keys right to accommodate a key from child
    for(int i = parent->size - 1; i >= indexFound; --i){
        parent->keys[i+1]  = parent->keys[i];
        parent->child[i+2] = std::move(parent->child[i+1]);
    }
    parent->keys[indexFound] = child->keys[branchingFactor - 1];

    auto newSibling = std::make_unique<Node>(branchingFactor);
    newSibling->isLeaf = child->isLeaf;

    // Copy right half keys to newNode
    for(int i = branchingFactor; i < maxSize; ++i) {
        newSibling->keys[i-branchingFactor]  = child->keys[i];
        newSibling->child[i-branchingFactor] = std::move(child->child[i]);
    }
    newSibling->child[branchingFactor-1] = std::move(child->child[maxSize]);

    newSibling->size = branchingFactor-1;
    parent->size++;
    child->size = branchingFactor;
    if(!child->isLeaf) --(child->size);

    newSibling->leftSibling_ = parent->child[indexFound].get();
    newSibling->rightSibling_ = parent->child[indexFound].get()->rightSibling_;
    if(newSibling->rightSibling_){
        newSibling->rightSibling_->leftSibling_ = newSibling.get();
    }

    child->rightSibling_ = newSibling.get();
    parent->child[indexFound+1] = std::move(newSibling);
}

template <typename key_t>
bool BPTree<key_t>::deleteAtLeaf(Node* leaf, int index){
    if(root->isLeaf && root->size == 1){
        root.reset(nullptr);
        return true;
    }

    for(int i = index; i < leaf->size-1; ++i){
        leaf->keys[i] = leaf->keys[i+1];
    }
    leaf->size--;
    return true;
}

template <typename key_t>
void BPTree<key_t>::bfsTraverseUtil(Node* start){
    if(start == nullptr) return;

    printf("%d# ", start->size);
    for(int i = 0; i < start->size; ++i) {
        std::cout << start->keys[i].first << "(" << start->keys[i].second << ") ";
    }
    std::cout << std::endl;

    if(!start->isLeaf) {
        for (int i = 0; i < start->size + 1; ++i) {
            bfsTraverseUtil(start->child[i].get());
        }
    }
}

template <typename key_t>
void BPTree<key_t>::borrowFromLeftSibling(int indexFound, Node* parent, Node* child){
    auto leftSibling = parent->child[indexFound-1].get();
    if(child->isLeaf){
        for(int i = child->size-1; i >= 0; --i){
            child->keys[i+1] = child->keys[i];
        }
        child->keys[0] = leftSibling->keys[leftSibling->size-1];
        parent->keys[indexFound-1] = leftSibling->keys[leftSibling->size-2];
    }
    else {
        for(int i=child->size-1;i>=0;i--){
            child->keys[i+1]  = child->keys[i];
            child->child[i+2] = std::move(child->child[i+1]);
        }
        child->child[1] = std::move(child->child[0]);
        child->keys[0]  = leftSibling->keys[leftSibling->size-1];
        child->child[0] = std::move(leftSibling->child[leftSibling->size-1]);
        parent->keys[indexFound-1] = leftSibling->keys[leftSibling->size-2];
    }
    leftSibling->size--;
    child->size++;
}

template <typename key_t>
void BPTree<key_t>::borrowFromRightSibling(int indexFound, Node* parent, Node* child){
    auto rightSibling = parent->child[indexFound+1].get();
    if (child->isLeaf) {
        parent->keys[indexFound] = rightSibling->keys[0];
        child->keys[child->size] = parent->keys[indexFound];
        for(int i=0;i<rightSibling->size-1;i++){
            rightSibling->keys[i] = rightSibling->keys[i+1];
        }

    }
    else {
        child->keys[child->size]    = parent->keys[indexFound];
        child->child[child->size+1] = std::move(rightSibling->child[0]);
        parent->keys[indexFound] = rightSibling->keys[0];

        for(int i=0;i<rightSibling->size-1;i++){
            rightSibling->keys[i] = rightSibling->keys[i+1];
            rightSibling->child[i] = std::move(rightSibling->child[i+1]);
        }
        rightSibling->child[rightSibling->size-1] = std::move(rightSibling->child[rightSibling->size]);
    }

    child->size++;
    rightSibling->size--;
}

template <typename key_t>
void BPTree<key_t>::mergeWithSibling(int indexFound, Node*& parent, Node* child){
    int maxSize = 2 * branchingFactor - 1;
    if(indexFound > 0){
        Node* leftSibling = std::move(parent->child[indexFound-1]).get();

        leftSibling->rightSibling_ =  child->rightSibling_;
        if(leftSibling->rightSibling_) {
            leftSibling->rightSibling_->leftSibling_ = leftSibling;
        }
        if(leftSibling->isLeaf){
            for(int i = 0; i < child->size; ++i){
                leftSibling->keys[branchingFactor+i-1] = child->keys[i];
            }

            for(int i = indexFound-1; i < parent->size-1; ++i){
                parent->keys[i]    = parent->keys[i+1];
                parent->child[i+1] = std::move(parent->child[i+2]);
            }
            leftSibling->size = maxSize - 1;

        }
        else{
            leftSibling->keys[branchingFactor-1] = parent->keys[indexFound-1];
            leftSibling->child[branchingFactor] = std::move(child->child[0]);

            for(int i = 0; i < child->size; ++i){
                leftSibling->keys[branchingFactor+i] = child->keys[i];
                leftSibling->child[branchingFactor+i+1]  = std::move(child->child[i+1]);
            }

            for(int i = indexFound-1; i < parent->size-1; ++i){
                parent->keys[i]    = parent->keys[i+1];
                parent->child[i+1] = std::move(parent->child[i+2]);
            }
            leftSibling->size = maxSize;
        }

        parent->size--;
        if(parent->size == 0){
            // happens only when parent is root
            this->root = std::move(parent->child[indexFound-1]);
        }
        parent = leftSibling;
    }
    else if(indexFound < parent->size){
        auto rightSibling = std::move(parent->child[indexFound+1]);
        child->rightSibling_ = rightSibling->rightSibling_;
        if(child->rightSibling_) {
            child->rightSibling_->leftSibling_ = child;
        }
        if(rightSibling->isLeaf){

            for(int i = 0; i < rightSibling->size; ++i){
                child->keys[branchingFactor+i-1] = rightSibling->keys[i];
            }
            child->size = maxSize - 1;

            for(int i = indexFound; i < parent->size-1; ++i){
                parent->keys[i]    = parent->keys[i+1];
                parent->child[i+1] = std::move(parent->child[i+2]);
            }
        }
        else {
            child->keys[branchingFactor-1] = parent->keys[indexFound];
            child->child[branchingFactor] = std::move(rightSibling->child[0]);

            for(int i = 0; i < rightSibling->size; ++i){
                child->keys[branchingFactor+i] = rightSibling->keys[i];
                child->child[branchingFactor+i+1]  = std::move(rightSibling->child[i+1]);
            }
            child->size = maxSize;

            for(int i = indexFound; i < parent->size-1; ++i){
                parent->keys[i]    = parent->keys[i+1];
                parent->child[i+1] = std::move(parent->child[i+2]);
            }
        }


        parent->size--;
        if(parent->size == 0) {
            // happens only when current is root
            this->root = std::move(parent->child[indexFound]);
        }
        parent = child;
    }
}

template <typename key_t>
void BPTree<key_t>::iterateLeftLeaf(Node* node, int startIndex){
    while(node!=nullptr){
        for(int i=startIndex;i>=0;i--){
            printf("%d ", node->keys[i].first);
        }
        node = node->leftSibling_;
        if(node){
            startIndex = node->size-1;
        }
    }
}

template <typename key_t>
void BPTree<key_t>::iterateRightLeaf(Node* node, int startIndex){
    while(node!=nullptr){
        for(int i=startIndex;i<node->size;i++){
            printf("%d ", node->keys[i].first);
        }
        node = node->rightSibling_;
        startIndex=0;

    }
}

// void nextKey(Node* node, int index){
//     if(index < node->size-1){

//     }
// }


void BPTreeTest(){
   BPTree<int> bt(2);
   bt.insert({10,1});
   bt.bfsTraverse();
   bt.insert({20,2});
   bt.bfsTraverse();
   bt.insert({5,3});
   bt.bfsTraverse();
   bt.insert({15,4});
   bt.bfsTraverse();
   bt.insert({11,5});
   bt.bfsTraverse();
//    bt.insert({21,6});
//    bt.bfsTraverse();
//    bt.insert({51,6});
//    bt.bfsTraverse();
//    bt.insert({17,7});
//    bt.bfsTraverse();
   bt.insert({71,5});
   bt.bfsTraverse();
   bt.insert({71,6});
   bt.bfsTraverse();
   bt.insert({71,7});
   bt.bfsTraverse();
   bt.insert({71,8});
   bt.bfsTraverse();
   bt.insert({71,9});
   bt.bfsTraverse();
   bt.insert({11,10});
   bt.bfsTraverse();
   bt.insert({10,11});
   bt.bfsTraverse();

   std::cout << "Insert done" << std::endl;

    // std::cout << bt.remove(71) << std::endl;
    // bt.bfsTraverse();
    // std::cout << bt.remove(21) << std::endl;
    // bt.bfsTraverse();
    // std::cout << bt.remove(51) << std::endl;
    // bt.bfsTraverse();
    // std::cout << bt.remove(11) << std::endl;
    // bt.bfsTraverse();
    // bt.insert(11);
    // bt.bfsTraverse();
    // auto searchRes = bt.searchUtil({15,-1});
    // std::cout << searchRes.node << std::endl;
    // std::cout << searchRes.index << std::endl;
    // bt.iterateRightLeaf(searchRes.node, searchRes.index);

   // bt.smallerThanEquals(11);
   // bt.smallerThan(11);
   // bt.greaterThanEquals(11);
   // bt.greaterThan(11);
   // bt.smallerThanEquals(12);
   // bt.smallerThan(12);
   // bt.greaterThanEquals(12);
   // bt.greaterThan(12);
   // bt.smallerThanEquals(71);
   // bt.smallerThan(71);
   // bt.greaterThanEquals(71);
   // bt.greaterThan(71);
   // bt.smallerThanEquals(20);
   // bt.smallerThan(20);
   // bt.greaterThanEquals(20);
   // bt.greaterThan(20);
   // bt.smallerThanEquals(4);
   // bt.smallerThan(4);
   // bt.greaterThanEquals(4);
   // bt.greaterThan(4);
   // bt.smallerThanEquals(100);
   // bt.smallerThan(100);
   // bt.greaterThanEquals(100);
   // bt.greaterThan(100);

//   std::cout<<bt.search("4")<<std::endl;
//   std::cout<<bt.search("20")<<std::endl;

   //  bt.bfsTraverse();
    bt.remove({20,2});
    bt.bfsTraverse();
    bt.insert({71,10});
    bt.bfsTraverse();
    bt.remove({71,9});
    bt.bfsTraverse();
//     std::cout<<bt.search(71)<<std::endl;
//     bt.traverseAllWithKey(12);
   //  std::cout << bt.remove(5) << std::endl;
   //  bt.bfsTraverse();

   // bt.smallerThan(11);
   // bt.greaterThan(17);
}

int main(){
   BPTreeTest();
   return 0;
}
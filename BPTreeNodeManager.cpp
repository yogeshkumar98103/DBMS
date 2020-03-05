//
// Created by Yogesh Kumar on 07/02/20.
//

#include "HeaderFiles/BPTreeNodeManager.h"

/*
 * ------------------ B Plus Tree HEADER ------------------
 * 1. Number of pages           =>  row_t
 * 2. Root Node Page Number     =>  row_t
 * 3. Key size                  =>  int32_t
 * 4. Branching Factor          =>  int32_t
 * 5. Stack Pointer             =>  int32_t
 *
 */

template <typename node_t>
BPTreeNodeManager<node_t>::BPTreeNodeManager(const char* fileName, int32_t branchingFactor_, int32_t keySize_, int nodeLimit): base_t(nodeLimit){
    this->rootPageNum = 1;
    this->numPages = 0;
    this->branchingFactor = branchingFactor_;
    this->keySize = keySize_;
    // this->stackPtr = 0;
    // this->stackPtrOffset = 0;
    this->stackSize = 0;
    this->indexStack = nullptr;

    if(!this->open(fileName)){
        throw std::runtime_error("Unable to Open Table");
    }
    this->getRoot();
}

template <typename node_t>
BPTreeNodeManager<node_t>::~BPTreeNodeManager(){
    flushAll();
};

template <typename node_t>
bool BPTreeNodeManager<node_t>::getRoot(){
    if(this->root != nullptr) return true;
    if(this->header == nullptr){
        if(!this->getHeader()) return false;
    }

    root = std::make_unique<node_t>();
    // root->isLeaf = true;
    this->fileLength = static_cast<uint32_t>(lseek(this->fileDescriptor, 0, SEEK_END));
    this->maxPages = (this->fileLength + PAGE_SIZE - 1) / PAGE_SIZE;
    if(this->maxPages > rootPageNum){
        lseek(this->fileDescriptor, rootPageNum * PAGE_SIZE, SEEK_SET);
        char* buffer = this->root->buffer.get();
        ssize_t bytesRead = ::read(this->fileDescriptor, buffer, PAGE_SIZE);
        if(bytesRead == -1){
            printf("Error reading Root Node: %d\n", errno);
            return false;
        }
        root->hasUncommitedChanges = false;
        root->readHeader(2 * branchingFactor - 1, keySize);
    }
    else{
        incrementPageNum();
    }
    root->pageNum = this->rootPageNum;
    root->allocate(2 * branchingFactor - 1, keySize);
    return true;
}

template <typename node_t>
bool BPTreeNodeManager<node_t>::getHeader(){
    if(this->header != nullptr) return true;
    this->header = std::make_unique<node_t>();
    this->fileLength = static_cast<uint32_t>(lseek(this->fileDescriptor, 0, SEEK_END));
    this->maxPages = (this->fileLength + PAGE_SIZE - 1) / PAGE_SIZE;
    if(this->maxPages > 0){
        lseek(this->fileDescriptor, 0, SEEK_SET);
        char* buffer = this->header->buffer.get();
        ssize_t bytesRead = ::read(this->fileDescriptor, buffer, PAGE_SIZE);
        if(bytesRead == -1){
            printf("Error reading Root Node: %d\n", errno);
            return false;
        }
        deserializeHeaderMetaData();
        this->header->hasUncommitedChanges = false;
    }
    else{
        serializeHeaderMetaData();
        this->header->hasUncommitedChanges = true;
    }
    return true;
}

template <typename node_t>
void BPTreeNodeManager<node_t>::deserializeHeaderMetaData(){
    // Deserialize Metadata
    char* buffer = this->header->buffer.get();
    int32_t offset = 0;

    memcpy(&this->numPages, buffer + offset, sizeof(row_t));
    offset += sizeof(row_t);

    memcpy(&this->rootPageNum, buffer + offset, sizeof(row_t));
    offset += sizeof(row_t);

    stackSize = (PAGE_SIZE - offset)/sizeof(row_t);
    indexStack = new(buffer + offset) row_t[stackSize];
    // memcpy(&this->stackPtr, buffer + offset, sizeof(int32_t));
    // this->stackPtrOffset = offset;
    // offset += sizeof(int32_t);
}

template <typename node_t>
void BPTreeNodeManager<node_t>::serializeHeaderMetaData(){
    // serialize Metadata
    char* buffer = this->header->buffer.get();
    int32_t offset = 0;

    memcpy(buffer + offset, &this->numPages, sizeof(row_t));
    offset += sizeof(row_t);

    memcpy(buffer + offset, &this->rootPageNum, sizeof(row_t));
    offset += sizeof(row_t);

    // this->stackPtrOffset = offset;
    if(indexStack == nullptr){
        stackSize = (PAGE_SIZE - offset)/sizeof(row_t);
        indexStack = new(buffer + offset) row_t[stackSize];
        indexStack[0] = 0;
    }
    // int32_t stackIndex = indexStack == nullptr ? 0 : indexStack[0];
    // memcpy( buffer + offset, &stackIndex, sizeof(int32_t));
    // offset += sizeof(int32_t);
}

template <typename node_t>
bool BPTreeNodeManager<node_t>::flush(uint32_t pageNum){
    if(this->fileDescriptor == -1) return false;
    if(pageNum == 0){
        return flushPage(this->header.get());
    }
    else if(pageNum == rootPageNum){
        return flushPage(root.get());
    }
    if(this->pageMap.find(pageNum) == this->pageMap.end()){
        // Page Not Loaded Yet
        printf("Tried To write page which is not read: %d\n", errno);
        return false;
    }
    auto it = this->pageMap[pageNum]->get();
    return flushPage(it);
}

template <typename node_t>
bool BPTreeNodeManager<node_t>::flushAll(){
    base_t::flushAll();
    flushPage(root.get());
    return true;
}

template <typename node_t>
bool BPTreeNodeManager<node_t>::flushPage(node_t* node){
    off_t offset = lseek(this->fileDescriptor, node->pageNum * PAGE_SIZE, SEEK_SET);
    if (offset == -1) return false;
    if(node->pageNum != 0) node->writeHeader();
    ssize_t bytesWritten = write(this->fileDescriptor, node->buffer.get(), PAGE_SIZE);
    if (bytesWritten == -1) return false;
    node->hasUncommitedChanges = false;
    return true;
}

template <typename node_t>
row_t BPTreeNodeManager<node_t>::nextFreeIndexLocation(){
    if(indexStack[0] == 0) return numPages + 1;
    row_t nextRow = indexStack[indexStack[0]];
    indexStack[0]--;
    this->header->hasUncommitedChanges = true;
    return nextRow;
//    if(stackPtr == 0) return numPages + 1;
//    Page* page = this->header.get();
//    char* buffer = page->buffer.get();
//    int32_t offset = stackPtrOffset + (stackPtr - 1) * sizeof(row_t) + sizeof(int32_t);
//    row_t nextRow;
//    memcpy(&nextRow, buffer + offset, sizeof(row_t));
//    stackPtr--;
//    memcpy(buffer, &stackPtr, sizeof(int32_t));
//    return nextRow;
}

template <typename node_t>
void BPTreeNodeManager<node_t>::incrementPageNum(){
    numPages++;
    memcpy(this->header->buffer.get(), &this->numPages, sizeof(row_t));
    this->header->hasUncommitedChanges = true;
}

template <typename node_t>
void BPTreeNodeManager<node_t>::decrementPageNum(){
    numPages--;
    memcpy(this->header->buffer.get(), &this->numPages, sizeof(row_t));
    this->header->hasUncommitedChanges = true;
}

template <typename node_t>
void BPTreeNodeManager<node_t>::addFreeIndexLocation(row_t location){
    ++indexStack[0];
    if(indexStack[0] >= stackSize){
        printf("Stack Overflow occurred in main table.\n");
        throw std::runtime_error("STACK OVERFLOWS HEADER PAGE");
    }
    indexStack[indexStack[0]] = location;
    this->header->hasUncommitedChanges = true;

//    Page* page = this->header.get();
//    char* buffer = page->buffer.get();
//    int32_t offset = stackPtrOffset + stackPtr * sizeof(row_t) + sizeof(int32_t);
//    ++stackPtr;
//    if(offset + sizeof(row_t) > PAGE_SIZE){
//        printf("Stack Overflow occurred at %d\n", location);
//        throw std::runtime_error("STACK OVERFLOWS HEADER PAGE");
//    }
//    memcpy(buffer + offset, &location, sizeof(row_t));
}

template<typename node_t>
node_t* BPTreeNodeManager<node_t>::newNode(){
    row_t pageNum = nextFreeIndexLocation();
    incrementPageNum();
    return read(pageNum);
}

template<typename node_t>
void BPTreeNodeManager<node_t>::deleteNode(node_t* node){
    decrementPageNum();
    addFreeIndexLocation(node->pageNum);
    node->hasUncommitedChanges = false;
}

template<typename node_t>
void BPTreeNodeManager<node_t>::setRoot(node_t* newNode){
    // Remove newRoot from LRU Cache
    auto itr = this->pageMap.find(newNode->pageNum);
    auto temp = std::move(*(itr->second));
    this->pageQueue.erase(itr->second);
    this->pageMap.erase(newNode->pageNum);

    // Add oldRoot to LRU Cache
    this->pageQueue.emplace_front(std::move(root));
    this->pageMap[rootPageNum] = this->pageQueue.begin();

    // Set root to newRoot
    root = std::move(temp);
    this->rootPageNum = root->pageNum;
    Page* page = this->header.get();
    char* buffer = page->buffer.get();
    memcpy(buffer + sizeof(row_t), &this->rootPageNum, sizeof(row_t));
    this->header->hasUncommitedChanges = true;
}

template <typename node_t>
node_t* BPTreeNodeManager<node_t>::read(int32_t pageNum){
    if(pageNum == rootPageNum) return root.get();
    if(pageNum < 0) return nullptr;
    auto node = base_t::read(pageNum, [&](node_t* node){
        node->readHeader(2 * branchingFactor - 1, keySize);
    });
    node->allocate(2 * branchingFactor - 1, keySize);
    return node;
}

template <typename node_t>
node_t* BPTreeNodeManager<node_t>::readChild(node_t* parent, int32_t childIndex){
    return read(parent->child[childIndex].pageNum);
}

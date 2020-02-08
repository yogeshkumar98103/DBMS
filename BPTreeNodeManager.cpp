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

template <typename key_t>
BPTreeNodeManager<key_t>::BPTreeNodeManager(const char* fileName, int nodeLimit): base_t(fileName, nodeLimit){
    this->getRoot();
}

template <typename key_t>
BPTreeNodeManager<key_t>::~BPTreeNodeManager() = default;

template <typename key_t>
bool BPTreeNodeManager<key_t>::getRoot(){
    if(this->root != nullptr) return true;
    char* buffer = this->header->buffer();
    if(this->header == nullptr){
        if(!this->getHeader()) return false;
    }

    root = std::make_unique<node_t>();
    this->fileLength = static_cast<uint32_t>(lseek(this->fileDescriptor, rootPageNum * PAGE_SIZE, SEEK_END));
    this->maxPages = (this->fileLength + PAGE_SIZE - 1) / PAGE_SIZE;
    if(this->maxPages > 0){
        lseek(this->fileDescriptor, 0, SEEK_SET);
        ssize_t bytesRead = ::read(this->fileDescriptor, this->root->buffer.get(), PAGE_SIZE);
        if(bytesRead == -1){
            printf("Error reading Root Node: %d\n", errno);
            return false;
        }
    }
    return true;
}

template <typename key_t>
bool BPTreeNodeManager<key_t>::getHeader(){
    if(this->header != nullptr) return true;
    if(!base_t::getHeader()) return false;

    // Deserialize Metadata
    char* buffer = this->header->buffer;
    int32_t offset = 0;

    memcpy(&this->numPages, buffer + offset, sizeof(row_t));
    offset += sizeof(row_t);

    memcpy(&this->rootPageNum, buffer + offset, sizeof(row_t));
    offset += sizeof(row_t);

    memcpy(&this->keySize, buffer + offset, sizeof(int32_t));
    offset += sizeof(int32_t);

    memcpy(&this->branchingFactor, buffer + offset, sizeof(int32_t));
    offset += sizeof(row_t);

    memcpy(&this->stackPtr, buffer + offset, sizeof(int32_t));
    this->stackPtrOffset = offset;
    offset += sizeof(int32_t);

    return true;
}

template <typename key_t>
bool BPTreeNodeManager<key_t>::flushPage(node_t* node){
    off_t offset = lseek(this->fileDescriptor, node->pageNum * PAGE_SIZE, SEEK_SET);
    if (offset == -1) return false;
    node->writeHeader();
    ssize_t bytesWritten = write(this->fileDescriptor, node->buffer.get(), PAGE_SIZE);
    if (bytesWritten == -1) return false;
    node->hasUncommitedChanges = false;
    return true;
}

template <typename key_t>
row_t BPTreeNodeManager<key_t>::nextFreeIndexLocation(){
    if(stackPtr[index] == 0) return numPages;
    Page* page = this->header.get();
    char* buffer = page->buffer.get();
    int32_t offset = stackPtrOffset + (stackPtr - 1) * sizeof(row_t) + sizeof(int32_t);
    row_t nextRow;
    memcpy(&nextRow, buffer + offset, sizeof(row_t));
    stackPtr--;
    memcpy(buffer, &stackPtr[index], sizeof(int32_t));
    return nextRow;
}

template <typename key_t>
void BPTreeNodeManager<key_t>::addFreeIndexLocation(row_t location){
    Page* page = this->header.get();
    char* buffer = page->buffer.get();
    int32_t offset = stackPtrOffset + stackPtr * sizeof(row_t) + sizeof(int32_t);
    ++stackPtr;
    if(offset + sizeof(row_t) > PAGE_SIZE){
        printf("Stack Overflow occurred at %d\n", index);
        throw std::runtime_error("STACK OVERFLOWS HEADER PAGE");
    }
    memcpy(buffer + offset, &location, sizeof(row_t));
}

template <typename key_t>
void BPTreeNodeManager<key_t>::copyKeys(node_t* destination, int32_t startD, node_t* source, int32_t startS, int32_t size){
    int32_t offsetD = BPTNodeHeaderSize + startD * sizeof(row_t);
    int32_t offsetS = BPTNodeHeaderSize + startS * sizeof(row_t);
    memcpy(destination + startD, source + startS, size * (sizeof(key_t) + sizeof(pkey_t)));
}

template <typename key_t>
void BPTreeNodeManager<key_t>::copyChildren(node_t* destination, int32_t startD, node_t* source, int32_t startS, int32_t size){
    int32_t childOffset = BPTNode<key_t>::childOffset;
    int32_t offsetD = childOffset + startD * sizeof(row_t);
    int32_t offsetS = childOffset + startS * sizeof(row_t);
    memcpy(destination + startD, source + startS, size * sizeof(row_t));
}

template<typename key_t>
void BPTreeNodeManager<key_t>::setRoot(node_t* newNode){
    // Remove newRoot from LRU Cache
    auto itr = this->pageMap[newNode->pageNum];
    this->pageQueue.erase(itr);
    this->pageMap.erase(newNode->pageNum);

    // Add oldRoot to LRU Cache
    auto temp = std::move(*itr);
    this->pageQueue.emplace_front(std::move(root));
    this->pageMap[root->pageNum] = this->pageQueue.begin();

    // Set root to newRoot
    root = std::move(temp);
    this->rootPageNum = root->pageNum;
}

template <typename key_t>
BPTNode<key_t>* BPTreeNodeManager<key_t>::read(int32_t pageNum){
    if(pageNum == rootPageNum) return root.get();
    auto node = base_t::read(pageNum);
    return node;
}

template<typename key_t>
void BPTreeNodeManager<key_t>::shiftKeysRight(BPTreeNodeManager::node_t *node, int32_t index) {
    char* buffer = node->buffer.get();
    int32_t itemsToShift = (node->size - index);
    const int32_t KSize = (sizeof(key_t) + sizeof(pkey_t));
    int offset = BPTNodeHeaderSize + (node->size - 1) * KSize;
    key_t key = 0;

    for(int32_t i = 0; i < itemsToShift; ++i){
        memcpy(buffer + offset + KSize, buffer + offset, KSize);
        offset -= KSize;
    }
}

template<typename key_t>
void BPTreeNodeManager<key_t>::shiftKeysLeft(BPTreeNodeManager::node_t *node, int32_t index) {

}

template<typename key_t>
void BPTreeNodeManager<key_t>::shiftChildrenRight(BPTreeNodeManager::node_t *node, int32_t index) {
    char* buffer = node->buffer.get();
    int32_t itemsToShift = (node->size - index + 1);
    const int32_t KSize = sizeof(row_t);
    int offset = BPTNode<key_t>::childOffset + node->size * KSize;
    for(int32_t i = 0; i < itemsToShift; ++i){
        memcpy(buffer + offset + KSize, buffer + offset, KSize);
        offset -= KSize;
    }
}

template<typename key_t>
void BPTreeNodeManager<key_t>::shiftChildrenLeft(BPTreeNodeManager::node_t *node, int32_t index) {

}

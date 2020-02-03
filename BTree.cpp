//
// Created by Yogesh Kumar on 2020-01-31.
//
#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <memory>

template <typename key_t>
class RowData{
    key_t x{};

public:
    RowData() = default;

    void setData(key_t x_){
        this->x = x_;
    }

    void displayData() {
        printf("Data: %d\n", x);
    }

    bool operator < (RowData const &obj) {
        return x < obj.x;
    }

    bool operator <= (RowData const &obj) {
        return x <= obj.x;
    }

    bool operator == (RowData const &obj) {
        return x == obj.x;
    }
};

template <typename key_t>
class BPTNode{
    using Node = BPTNode<key_t>;
    bool isLeaf;
    int size;
    key_t* keys;
    std::unique_ptr<Node>* child;

    template <typename o_key_t>
    friend class BPTree;

public:
    explicit BPTNode(int branchingFactor):isLeaf(false),size(1){
        keys = new key_t[(2 * branchingFactor - 1)];
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

template <typename key_t>
class BPTree{
    using Node      = BPTNode<key_t>;
    using result_t  = SearchResult<key_t>;

    std::unique_ptr<Node> root;
    int branchingFactor;

public:
    explicit BPTree(int branchingFactor_):branchingFactor(branchingFactor_), root(nullptr){}

    result_t search(const key_t& key){
        result_t searchRes{};

        if(root != nullptr){
            auto node = root.get();

            while(!(node->isLeaf)) {
                int indexFound = binarySearch(node, key);
                node = node->child[indexFound].get();
            }

            int indexFound = binarySearch(node, key);
            if((indexFound != node->size) && (node->keys[indexFound] == key)){
                searchRes.index = indexFound;
                searchRes.node = node;
            }
        }
        return searchRes;
    }

    bool insert(const key_t& key) {
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

    bool remove(const key_t& key) {
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

    void bfsTraverse() {
        bfsTraverseUtil(root.get());
        std::cout << std::endl;
    }

private:
    // MARK:- HELPER FUNCTIONS
    int binarySearch(Node* node, const key_t& key) {
        int l = 0;
        int r = node->size - 1;
        int mid;
        int ans = node->size;

        while(l <= r) {
            mid = (l+r)/2;
            if(key <= node->keys[mid]) {
                r = mid-1;
                ans = mid;
            }
            else{
                l = mid+1;
            }
        }
        return ans;
    }

    void splitRoot(){
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

    void splitNode(Node* parent, Node* child, int indexFound){
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

        parent->child[indexFound+1] = std::move(newSibling);
    }

    bool deleteAtLeaf(Node* leaf, int index){
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

    void bfsTraverseUtil(Node* start){
        if(start == nullptr) return;

        printf("%d# ", start->size);
        for(int i = 0; i < start->size; ++i) {
            std::cout << start->keys[i] << " ";
        }
        std::cout << std::endl;

        if(!start->isLeaf) {
            for (int i = 0; i < start->size + 1; ++i) {
                bfsTraverseUtil(start->child[i].get());
            }
        }
    }

    void borrowFromLeftSibling(int indexFound, Node* parent, Node* child){
        auto leftSibling = parent->child[indexFound-1].get();
        if(child->isLeaf){
            for(int i = child->size-1; i >= 0; --i){
                child->keys[i+1] = child->keys[i];
            }
            child->keys[0] = parent->keys[indexFound-1];
            parent->keys[indexFound-1] = leftSibling->keys[leftSibling->size-2];
        }
        else {
            for(int i=child->size-1;i>=0;i--){
                child->keys[i+1]  = child->keys[i];
                child->child[i+2] = std::move(child->child[i+1]);
            }
            child->child[1] = std::move(child->child[0]);
            child->keys[0]  = parent->keys[indexFound-1];
            child->child[0] = std::move(leftSibling->child[leftSibling->size-1]);
            parent->keys[indexFound-1] = leftSibling->keys[leftSibling->size-2];
        }
        leftSibling->size--;
        child->size++;
    }

    void borrowFromRightSibling(int indexFound, Node* parent, Node* child){
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

    void mergeWithSibling(int indexFound, Node*& parent, Node* child){
        int maxSize = 2 * branchingFactor - 1;
        if(indexFound > 0){
            Node* leftSibling = std::move(parent->child[indexFound-1]).get();

            if(leftSibling->isLeaf){
                // leftSibling->child[branchingFactor-1] = std::move(child->child[0]);

                for(int i = 0; i < child->size; ++i){
                    leftSibling->keys[branchingFactor+i-1] = child->keys[i];
                    // leftSibling->child[branchingFactor+i]  = std::move(child->child[i+1]);
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

            if(rightSibling->isLeaf){
                // child->child[branchingFactor-1] = std::move(rightSibling->child[0]);

                for(int i = 0; i < rightSibling->size; ++i){
                    child->keys[branchingFactor+i-1] = rightSibling->keys[i];
                    // child->child[branchingFactor+i]  = std::move(rightSibling->child[i+1]);
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
                child->size = maxSize - 1;

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
};

void BPTreeTest(){
    BPTree<int> bt(3);
    bt.insert(10);
    bt.bfsTraverse();
    bt.insert(20);
    bt.bfsTraverse();
    bt.insert(5);
    bt.bfsTraverse();
    bt.insert(15);
    bt.bfsTraverse();
    bt.insert(11);
    bt.bfsTraverse();
    bt.insert(21);
    bt.bfsTraverse();
    bt.insert(51);
    bt.bfsTraverse();
    bt.insert(17);
    bt.bfsTraverse();
    bt.insert(71);
    bt.bfsTraverse();

    std::cout << "Insert done" << std::endl;
    std::cout << bt.remove(71) << std::endl;
    bt.bfsTraverse();
    std::cout << bt.remove(21) << std::endl;
    bt.bfsTraverse();
    std::cout << bt.remove(51) << std::endl;
    bt.bfsTraverse();
    std::cout << bt.remove(11) << std::endl;
    bt.bfsTraverse();
    bt.insert(11);
    bt.bfsTraverse();
    auto searchRes = bt.search(11);
    std::cout << searchRes.node << std::endl;
    std::cout << searchRes.index << std::endl;

    std::cout << bt.remove(10) << std::endl;
    bt.bfsTraverse();
    std::cout << bt.remove(5) << std::endl;
    bt.bfsTraverse();
}

int main(){
    BPTreeTest();
    return 0;
}

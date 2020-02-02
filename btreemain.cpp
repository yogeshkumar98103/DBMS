#include<iostream>
#include<fstream>
#include<strings.h>

using namespace std;
#define o2(a,b) cout<<(a)<<" "<<(b)<<endl
#define o3(a,b,c) cout<<(a)<<" "<<(b)<<" "<<(c)<<endl
#define o4(a,b,c,d) cout<<(a)<<" "<<(b)<<" "<<(c)<<" "<<(d)<<endl
#define cl cout<<endl



class RowData {
		int x;

		friend class BPTNode;
		friend class BPTree;

public:

		RowData() {
			x = 0;
		}

		void setData(int _x) {
			x = _x;
		}

        void displayData() {
            cout<<"x";
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


class BPTNode {
	bool is_leaf;
	RowData* data;
	int size;
	BPTNode** ptr;

    friend class BPTree;

public:
	BPTNode(int t) {
		is_leaf = false;
		ptr = new BPTNode*[2*t];
		data = new RowData[2*t-1];
		size=1;

		for(int i=0;i<=(2*t);i++) {
			ptr[i] = nullptr;
		}

	}

	~BPTNode() {
		cout<<"Deleted"<<endl;
		delete[] data;
		delete[] ptr;
	}


};


struct SearchResult {
	int index; // between t-1 and 2t-1
	BPTNode* bptnode;

	SearchResult() {
		index = -1;
		bptnode = nullptr;
	}
};


class BPTree {
	BPTNode* root;
	int t;

	int binSearch(BPTNode* temp, RowData* keyData) {
		int l=0, r=temp->size-1, mid, ans=temp->size;
		while(l<=r) {
            mid = (l+r)/2;
			if((*keyData) <= temp->data[mid]) {
				r = mid-1;
				ans = mid;
			}
			else {
				l = mid+1;
			}
		}
		return ans;
	}

public:

	BPTree(int _t) {
		t = _t;
		root = nullptr;
	}



	SearchResult search(RowData* keyData) {
		SearchResult searchRes;

		if(root == nullptr) {
			// Not Found

			return searchRes;
		}
		else {
			BPTNode* temp = root;

			while( !(temp->is_leaf) ) {
				int indexFound = binSearch(temp, keyData);
				temp = temp->ptr[indexFound];

			}

			int indexFound = binSearch(temp, keyData);

			if(indexFound == temp->size || (!(temp->data[indexFound] == *(keyData))) ) {
				// Not Found

				return searchRes;
			}
			else {
				// Found
				searchRes.index = indexFound;
				searchRes.bptnode = temp;
				return searchRes;
			}

		}
	}


	bool insert(RowData* keyData) {
		if (root == nullptr) {
			root = new BPTNode(t);
			root->data[0] = (*keyData);
			root->is_leaf = true;
			// Inserted
			return true;
		}
		else {

			if(root->size == (2*t-1)) {
				// If root is full create a new root and split this root
				BPTNode* newRoot = new BPTNode(t);
				BPTNode* nBnode = new BPTNode(t);
				if (root->is_leaf) {
                    newRoot->is_leaf = true;
                    nBnode->is_leaf = true;
				}
				for(int i=t;i<=(2*t-2);i++) {
					nBnode->data[i-t] = root->data[i];
					nBnode->ptr[i-t] = root->ptr[i];
				}


				newRoot->size = 1;
				newRoot->data[0] = root->data[t-1];
				newRoot->ptr[1]=nBnode;
				newRoot->ptr[0]=root;
				nBnode->ptr[t-1]=root->ptr[2*t-1];
				nBnode->size = t-1;

				if(!(root->is_leaf)) {
					root->size = t-1;
					root->ptr[t]=nullptr;
				}
				else {
					root->size = t;
				}
				root = newRoot;
				root->is_leaf = false;

			}

			BPTNode* temp = root;
			BPTNode *child;


			while( !(temp->is_leaf) ) {

				int indexFound = binSearch(temp, keyData);
				child = temp->ptr[indexFound];

				if (child->size == (2*t-1)) {
					// If child is full, split it first and then go down

					for(int i=temp->size-1;i>=indexFound;i--) {
						temp->data[i+1] = temp->data[i];
						temp->ptr[i+2] = temp->ptr[i+1];
					}
					temp->data[indexFound] = child->data[t-1];
					BPTNode* nBnode = new BPTNode(t);
					nBnode->is_leaf = child->is_leaf;
					for(int i=t;i<=(2*t-2);i++) {
						nBnode->data[i-t] = child->data[i];
						nBnode->ptr[i-t] = child->ptr[i];
					}
					temp->ptr[indexFound+1]=nBnode;
					nBnode->ptr[t-1]=child->ptr[2*t-1];
					nBnode->size = t-1;
					temp->size++;
					if(!(child->is_leaf)) {
						child->size = t-1;

					}
					else {
						child->size = t;
					}
					child->ptr[t]=nullptr;

					if((*keyData) <= temp->data[indexFound]) {
						temp = temp->ptr[indexFound];
					}
					else {
						temp = temp->ptr[indexFound+1];
					}
				}
				else {
					temp = child;
				}

			}


			int insertAtIndex = 0;
			for(int i=temp->size-1;i>=0;i--){
				if((*keyData) <= temp->data[i]) {
					temp->data[i+1]=temp->data[i];
				}
				else {
					insertAtIndex=i+1;
					break;
				}
			}

			temp->data[insertAtIndex] = (*keyData);
			temp->size++;

			// Inserted
			return true;




		}
	}

	bool deleteAtLeaf(BPTNode* temp, int index) {

		if(root->is_leaf && root->size == 1) {
			root->~BPTNode();
			root = nullptr;
			return true;
		}
		else {
			 for(int i=0;i<temp->size;i++) {
			 	cout<<temp->data[i].x<<" ";
			 }cl;cl;
			for(int i=index;i<temp->size-1;i++) {
				temp->data[i] = temp->data[i+1];
			}
			temp->size--;
			return true;
		}

		// else {
		// 	BPTNode* lchild = temp->ptr[index];
		// 	BPTNode* rchild = temp->ptr[index+1];

		// 	if(lchild->size > t-1) {
		// 		temp->data[index] = lchild->data[lchild->size-1];
		// 		return findAndDelete(&(temp->data[index]), lchild);
		// 	}
		// 	else if(rchild->size > t-1) {
		// 		temp->data[index] = rchild->data[0];
		// 		return findAndDelete(&(temp->data[index]), rchild);
		// 	}
		// 	else {

		// 	}
		//  }
	}

	bool findAndDelete(RowData* keyData) {
		if(root == nullptr) {
			return false;
		}

		BPTNode* temp = root;
		BPTNode *child;


		while( !(temp->is_leaf) ) {

			int indexFound = binSearch(temp, keyData);

			child = temp->ptr[indexFound];

			if (child->size == (t-1)) {
				// If child is of size t-1 fix it and then traverse in
				bool flag=0;
				if(indexFound>0) {
					// check left sibling of child
					BPTNode* lsibling = temp->ptr[indexFound-1];
					if(lsibling->size > t-1) {
						if(child->is_leaf){
							for(int i=child->size-1;i>=0;i--){
								child->data[i+1]=child->data[i];
							}
							temp->data[indexFound-1] = lsibling->data[lsibling->size-1];
							child->data[0]=temp->data[indexFound-1];

						}
						else {
							for(int i=child->size-1;i>=0;i--){
								child->data[i+1]=child->data[i];
								child->ptr[i+2]=child->ptr[i+1];
							}
							child->ptr[1]=child->ptr[0];
							child->data[0]=temp->data[indexFound-1];
							child->ptr[0]=lsibling->ptr[lsibling->size];
							lsibling->ptr[lsibling->size]=nullptr;
							temp->data[indexFound-1] = lsibling->data[lsibling->size-1];
						}
						lsibling->size--;
						child->size++;
						flag=1;
						temp = child;

					}

				}

				if(flag == 0 && indexFound < temp->size){

					BPTNode* rsibling = temp->ptr[indexFound+1];
					if(rsibling->size > t-1) {
						if (child->is_leaf) {
							temp->data[indexFound] = rsibling->data[0];
							child->data[child->size]=temp->data[indexFound];
							for(int i=0;i<rsibling->size-1;i++){
								rsibling->data[i]=rsibling->data[i+1];
							}

						}
						else {
							child->data[child->size]=temp->data[indexFound];
							child->ptr[child->size+1]=rsibling->ptr[0];
							temp->data[indexFound] = rsibling->data[0];
							for(int i=0;i<rsibling->size-1;i++){
								rsibling->data[i]=rsibling->data[i+1];
								rsibling->ptr[i]=rsibling->ptr[i+1];
							}
							rsibling->ptr[rsibling->size-1]=rsibling->ptr[rsibling->size];

						}

						child->size++;
						rsibling->size--;
						flag=1;
						temp = child;

					}
				}

				if(flag==0) {
					if(indexFound>0) {
						BPTNode* lsibling = temp->ptr[indexFound-1];
						//lsibling->data[t] = temp->data[indexFound-1];
						lsibling->ptr[t-1] = child->ptr[0];

						for(int i=0;i<child->size;i++) {
							lsibling->data[t+i-1] = child->data[i];
							lsibling->ptr[t+i] = child->ptr[i+1];
						}

						for(int i=indexFound-1;i<temp->size-1;i++){
							temp->data[i]=temp->data[i+1];
							temp->ptr[i+1]=temp->ptr[i+2];
						}
						temp->ptr[temp->size]=nullptr;
						temp->size--;
						if(temp->size==0) {
							// happens only when temp is root
							temp->~BPTNode();
							root = lsibling;
						}
						flag=1;
						temp = lsibling;
						lsibling->size = (2*t-2);
						child->~BPTNode();
					}
					else if(indexFound < temp->size){
						BPTNode* rsibling = temp->ptr[indexFound+1];
						child->ptr[t-1] = rsibling->ptr[0];

						for(int i=0;i<rsibling->size;i++) {
							child->data[t+i-1] = rsibling->data[i];
							child->ptr[t+i] = rsibling->ptr[i+1];
						}
						child->size=(2*t-2);


						for(int i=indexFound;i<temp->size-1;i++){
							temp->data[i]=temp->data[i+1];
							temp->ptr[i+1]=temp->ptr[i+2];
						}
						temp->ptr[temp->size]=nullptr;
						temp->size--;
						if(temp->size==0) {
							// happens only when temp is root
							temp->~BPTNode();
							root = child;
						}
						flag=1;
						temp = child;
						rsibling->~BPTNode();
					}
					else {
						cout<<"FUCK"<<endl;
					}
				}

				if(flag==0) cout<<"FUCKKKKKKKKKKK"<<endl;

			}
			else {
				temp = child;
			}
		}

		// Now we are in a leaf node
		int indexFound = binSearch(temp, keyData);
		//o3("F", keyData->x, indexFound);
		if(indexFound < temp->size) {
			if (temp->data[indexFound] == *(keyData)) {
				return deleteAtLeaf(temp,indexFound);
			}
		}

		return false;
	}

    void bfsTraverse() {
        bfsTraverseUtil(root);
    }

	void bfsTraverseUtil(BPTNode* start) {
		if(start != nullptr) {
             cout<<start->size<<"# ";
			 for(int i=0;i<start->size;i++) {
			 	cout<<start->data[i].x<<" ";
			 }cout<<endl;

			if (!(start->is_leaf)) {
				for(int i=0;i<start->size+1;i++) {
					bfsTraverseUtil(start->ptr[i]);
				}
			}

		}
	}

};


int main(){
	// BPTree bt(3);
	// RowData rd;
	// rd.setData(10);
	// bt.insert(&rd);
	// rd.setData(20);
	// bt.insert(&rd);
	// rd.setData(5);
	// bt.insert(&rd);
	// rd.setData(15);
	// bt.insert(&rd);
	// //bt.bfsTraverse();
	// rd.setData(11);
	// bt.insert(&rd);
	// //bt.bfsTraverse();
	// rd.setData(21);
	// bt.insert(&rd);
	// bt.bfsTraverse();
	// rd.setData(51);
	// bt.insert(&rd);
	// bt.bfsTraverse();
	// rd.setData(17);
	// bt.insert(&rd);
	// bt.bfsTraverse();
	// rd.setData(71);
	// bt.insert(&rd);
	// bt.bfsTraverse();

 //    cout<<"Insert done"<<endl;
 //     rd.setData(51);
	//  SearchResult rs = bt.search(&rd);
	//  cout<<rs.index<<endl;

	//  cout<<bt.findAndDelete(&rd)<<endl;
	//  bt.bfsTraverse();
 //     rd.setData(71);
	//  cout<<bt.findAndDelete(&rd)<<endl;
	//  bt.bfsTraverse();
	//  rd.setData(51);
	//  cout<<bt.findAndDelete(&rd)<<endl;
	//  bt.bfsTraverse();
	//  rd.setData(21);

	//  cout<<bt.findAndDelete(&rd)<<endl;
	//  bt.bfsTraverse();




}


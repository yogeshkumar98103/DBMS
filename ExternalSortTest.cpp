#include "HeaderFiles/ExternalSort.h"
#include <cstdlib>
#include <chrono>
#include <iostream>
#include <ctime>

void generateDummyData(const std::string& filename, const std::string& databaseName, off_t headerSize, int numRows){
    auto file = databaseName + "/" + filename;
    auto txtFile = "original.txt";
    int fd = open(file.c_str(), O_WRONLY | O_CREAT, S_IRWXU);
    int fd2 = open(txtFile, O_WRONLY | O_CREAT, S_IRWXU);
    lseek(fd, headerSize, SEEK_SET);
    char c = 'A';
    char buffer[PAGE_SIZE] = {0};
    int offset = 0;
    int rowSize = sizeof(int) + sizeof(int) + sizeof(char) + sizeof(pkey_t);
    srand(time(0));
    for(pkey_t i = 0; i < numRows; ++i){
        if(offset + rowSize > PAGE_SIZE){
            write(fd, buffer, PAGE_SIZE);
            offset = 0;
        }
        int x = rand();
        int y = rand();
        memcpy(buffer + offset, &x, sizeof(int));
        offset += sizeof(int);
        memcpy(buffer + offset, &y, sizeof(int));
        offset += sizeof(int);
        memcpy(buffer + offset, &c, sizeof(char));
        offset += sizeof(char);
        memcpy(buffer + offset, &i, sizeof(pkey_t));
        offset += sizeof(pkey_t);
        auto str = std::to_string(y) + "\n";
        write(fd2, str.c_str(), str.size());
    }
    if(offset != 0){
        write(fd, buffer, PAGE_SIZE);
    }
    close(fd2);
    close(fd);
}

int main(){
    std::string filename = "table2.bin";
    std::string databaseName = "Mydatabase";
    std::string finalName = "extSortTemp/finalOutput.bin";
    int headerOffset = PAGE_SIZE;
    int keySize = sizeof(int32_t);
    int rowOffset = sizeof(int32_t) + sizeof(int32_t) + sizeof(char) + sizeof(pkey_t);
    int columnOffset = sizeof(int32_t);
    int numRows = 100000;
    auto t1 = std::chrono::high_resolution_clock::now();
//    generateDummyData(filename, databaseName, headerOffset, numRows);
    auto t2 = std::chrono::high_resolution_clock::now();
    std::cout << "Time For Data Generation: " << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()/1000.0 << std::endl;
    int rowStack[] = {0};
    auto t3 = std::chrono::high_resolution_clock::now();
    ExternalSort<int> sorter(databaseName, filename, finalName, 1, numRows, rowStack);
    sorter.sort(rowOffset, columnOffset, headerOffset, keySize);
    auto t4 = std::chrono::high_resolution_clock::now();
    std::cout << "Time for Sorting: " << std::chrono::duration_cast<std::chrono::milliseconds>(t4 - t3).count()/1000.0 << std::endl;
    convertToText(finalName, "finalOutput.txt");
    return 0;
}

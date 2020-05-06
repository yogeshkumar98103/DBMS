#include "HeaderFiles/ExternalSort.h"
#include <cstdlib>
#include <chrono>
#include <iostream>
#include <ctime>
#include <fstream>

uint32_t headerOffset = PAGE_SIZE;
row_t numRows         = 10000000;

/// ===> BENCHMARK RESULTS
/// ===============================================
///             Async I/0       ||    Sync I/0
/// ===============================================
/// Run #1      9.404           ||    10.093
/// Run #2      8.673           ||    8.913
/// Run #3      8.712           ||    9.082
/// ===============================================

void generateDummyData(){
    char databaseFile[]   = "Mydatabase/table.bin";

    int databaseFD  = open(databaseFile, O_WRONLY | O_CREAT, S_IRWXU);
    std::ofstream generatedFile("generated.txt");

    lseek(databaseFD, headerOffset, SEEK_SET);
    char c = 'A';
    char buffer[PAGE_SIZE] = {0};
    int offset = 0;
    int rowSize = sizeof(int) + sizeof(int) + sizeof(char) + sizeof(pkey_t);
    srand(time(0));

    int numRowsPerPage = PAGE_SIZE / rowSize;
    int numFullPages = numRows/ numRowsPerPage;
    int rowsOnLastPage = numRows - numFullPages * numRowsPerPage;

    for(int pageNo = 0; pageNo < numFullPages; ++pageNo){
        for(int i = 0; i < numRowsPerPage; ++i){
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
            generatedFile << y << "\n";
        }

        write(databaseFD, buffer, PAGE_SIZE);
        memset(buffer, 0, sizeof(buffer));
        offset = 0;
    }

    for(int i = 0; i < rowsOnLastPage; ++i){
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
        generatedFile << y << "\n";
    }

    write(databaseFD, buffer, PAGE_SIZE);
    close(databaseFD);
    generatedFile.close();
}

int main(){
    std::string finalName = "extSortTemp/finalOutput.bin";
    int keySize = sizeof(int32_t);
    int rowOffset = sizeof(int32_t) + sizeof(int32_t) + sizeof(char) + sizeof(pkey_t);
    int columnOffset = sizeof(int32_t);
//    generateDummyData();

    int rowStack[] = {0};
    auto t1 = std::chrono::high_resolution_clock::now();
    ExternalSort<int> sorter("Mydatabase", "table.bin", finalName, numRows, rowStack);
    sorter.sort(rowOffset, columnOffset, keySize, headerOffset);
    auto t2 = std::chrono::high_resolution_clock::now();
    std::cout << "Time for Sorting: " << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()/1000.0 << std::endl;
//    convertToText<int>(finalName, "final.txt", keySize, numRows);
    return 0;
}

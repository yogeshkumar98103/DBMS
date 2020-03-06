#ifndef DBMS_EXTERNALSORT_H
#define DBMS_EXTERNALSORT_H


#include <algorithm>
#include <set>
#include <memory>
#include <vector>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <queue>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include "DataTypes.h"
#include "Constants.h"

#define EXTERNAL_SORTING_K 20
#define EXTERNAL_SORTING_PAGE_PER_BLOCK 1
static const uint32_t blockSize = PAGE_SIZE * EXTERNAL_SORTING_PAGE_PER_BLOCK;
static const uint32_t seqBlockSize = EXTERNAL_SORTING_K * blockSize;

/// This is responsible for sequentially reading table file
/// This is double buffered
class SeqPageReader{
    int inFileDescriptor;
    int outFileDescriptor;

    std::mutex outputMutex;
    std::mutex inputMutex;
    bool finishedFetching = false;

    std::unique_ptr<char[]> secondaryInputBuffer;
    std::unique_ptr<char[]> secondaryOutputBuffer;

public:

    std::unique_ptr<char[]> primaryInputBuffer;
    std::unique_ptr<char[]> primaryOutputBuffer;
    int32_t bufferSize;
    bool finished = false;

    SeqPageReader() = default;
    ~SeqPageReader();
    void initialise(const char* inFileName, const char* outFileName, uint32_t headerOffset);

    void fetchInput();
    void flushOutput();
    void flushOutputToStorage();

private:
    static bool open(const char* fileName, int& fd, int mode);
    void fetchFromSecondary();
    void fetchFromStorage();
    void flushOutputToSecondary();
};


template <typename key_t>
struct KRPair{
    row_t row;
    key_t key;

    bool operator<(const KRPair<key_t>& kr2){
        return key < kr2.key;
    }

    bool operator>(const KRPair<key_t>& kr2){
        return key > kr2.key;
    }
};

template <>
struct KRPair<char[]>{
    row_t row;
    char key[];
    bool operator<(const KRPair<char[]>& kr2){
        return (strcmp(key, kr2.key) < 0);
    }

    bool operator>(const KRPair<char[]>& kr2){
        return (strcmp(key, kr2.key) > 0);
    }
};


/// This is responsible for
/// 1. maintaining Input (double Buffered) and Output Buffers
/// 2. Filling primary and secondary buffers
/// 3. All I/O Operations
class ExtSortPager{
    int inFileDescriptor;
    int outFileDescriptor;
    int blocksPerBuffer;

    std::mutex inputMutex[EXTERNAL_SORTING_K];
    std::mutex outputMutex;
    std::mutex queueMutex;
    std::condition_variable condition;
    std::queue<int> fillRequests;

    std::unique_ptr<char[]> secondaryInputBuffer[EXTERNAL_SORTING_K];
    std::unique_ptr<char[]> secondaryOutputBuffer;

    bool finishedFetching[EXTERNAL_SORTING_K] = {false};
    bool finished[EXTERNAL_SORTING_K] = {false};
    bool finishedFetchingAll = false;
    int fullyFetchedBufferCount = 0;

public:
    std::unique_ptr<char[]> primaryInputBuffer[EXTERNAL_SORTING_K];
    std::unique_ptr<char[]> primaryOutputBuffer;
    int bufferSize[EXTERNAL_SORTING_K];
    bool finishedAll = false;

    ExtSortPager();
    ~ExtSortPager();
    void initialise(const char* inFileName, const char* outFileName, int blocksPerBuffer_);
    void fetchInput(int bufferNo);
    void flushOutput();
    void flushOutputToStorage();

private:
    static bool open(const char* fileName, int& fd, int mode);
    void storageFetcher();
    void fetchFromSecondary(int bufferNo);
    void fetchFromStorage(int bufferNo);
    void flushOutputToSecondary();
};

#endif //DBMS_EXTERNALSORT_H

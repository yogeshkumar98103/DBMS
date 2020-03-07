#ifndef DBMS_EXTERNALSORT_H
#define DBMS_EXTERNALSORT_H


#include <algorithm>
#include <set>
#include <memory>
#include <vector>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <future>
#include <queue>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include "DataTypes.h"
#include "Constants.h"

#define SEQ_READ_ASYNC
#define SEQ_WRITE_ASYNC
#define EXT_READ_ASYNC
#define EXT_WRITE_ASYNC

#define EXTERNAL_SORTING_K 5                                    // Constant K for K-way merge
#define EXTERNAL_SORTING_BLOCK_SIZE 1                           // Num of Pages in ext sort block
#define SEQ_READ_BLOCKS  EXTERNAL_SORTING_BLOCK_SIZE * 5        // Num of Pages in seq read block
#define SEQ_WRITE_BLOCKS EXTERNAL_SORTING_BLOCK_SIZE * 5        // Num of Pages in seq write block

static const uint32_t blockSize = PAGE_SIZE * EXTERNAL_SORTING_BLOCK_SIZE;
static const uint32_t seqReadBlockSize = SEQ_READ_BLOCKS * blockSize;
static const uint32_t seqWriteBlockSize = SEQ_WRITE_BLOCKS * blockSize;

using block_t = int;

/// This is responsible for sequentially reading table file
/// This is double buffered
class SeqPageReader{
    int inFileDescriptor;
    int outFileDescriptor;

    std::thread readThread;
    std::thread writeThread;
    bool finishedFetching = false;

    std::unique_ptr<char[]> secondaryInputBuffer;
    std::unique_ptr<char[]> secondaryOutputBuffer;

public:

    std::unique_ptr<char[]> primaryInputBuffer;
    std::unique_ptr<char[]> primaryOutputBuffer;
    off_t bufferSize = 0;
    bool finished = false;

    SeqPageReader() = default;
    ~SeqPageReader();
    void initialise(const char* inFileName, const char* outFileName, uint32_t headerOffset);

    void fetchInput();
    void flushOutput(off_t outputBuffSize = seqWriteBlockSize);
    void flushOutputToStorage(off_t outputBuffSize);
    off_t getOutputFileSize();

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

    bool operator<(const KRPair<key_t>& kr2) const {
        return key < kr2.key;
    }

    bool operator>(const KRPair<key_t>& kr2) const {
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
    off_t fileSize;
    off_t offset;

    std::thread readThread;
    std::thread writeThread;
    std::mutex inputMutex[EXTERNAL_SORTING_K];
    std::mutex queueMutex;
    std::condition_variable condition;
    std::queue<int> fillRequests;

    std::unique_ptr<char[]> secondaryInputBuffer[EXTERNAL_SORTING_K];
    std::unique_ptr<char[]> secondaryOutputBuffer;
    int secondaryBufferSize[EXTERNAL_SORTING_K];
    int timesFetched[EXTERNAL_SORTING_K] = {0};
    bool finishedFetching[EXTERNAL_SORTING_K] = {false};
    bool finishedFetchingAll = false;
    int fullyFetchedBufferCount = 0;

public:
    std::unique_ptr<char[]> primaryInputBuffer[EXTERNAL_SORTING_K];
    std::unique_ptr<char[]> primaryOutputBuffer;
    int bufferSize[EXTERNAL_SORTING_K];
    bool finished[EXTERNAL_SORTING_K] = {false};
    bool finishedAll = false;

    ExtSortPager();
    ~ExtSortPager();
    void initialise(const char* inFileName, const char* outFileName, int blocksPerBuffer_, off_t offset_);
    void fetchInput(int bufferNo);
    void flushOutput(off_t outputBuffSize = blockSize);
    void flushOutputToStorage(off_t outputBuffSize);

private:
    static bool open(const char* fileName, int& fd, int mode);
    void storageFetcher();
    void fetchFromSecondary(int bufferNo);
    void fetchFromStorage(int bufferNo);
    void flushOutputToSecondary();
};

template<typename key_t>
class ExternalSort{
    using data_t      = KRPair<key_t>;
    using heap_data_t = std::pair<KRPair<key_t>, int>;
    using heap_t      = std::priority_queue <heap_data_t, std::vector<heap_data_t>, std::greater<>>;

    std::string fileName;                   /// File name of table to sort
    int colNo;                              /// Column Number on which sorting is done

public:
    ExternalSort(const std::string& fileName_, const std::string& databaseName_, int colNo_, int numRows_, int* rowStack);
    void sort(int rowOffset, int columnOffset, uint32_t headerOffset, int32_t keySize);                            /// Wrapper which calls other functions

private:
    std::string tempFileName;                  /// FileName of file containing extracted rows
    std::string partiallySortedFileName[2];    /// Alternate b/w these two file names
    std::string finalSortedFileName;

    ExtSortPager pager;                        /// Handles disk I/O for partially sorted File
    std::vector<int> deletedRows;              /// Contains rows numbers of deleted rows
    row_t numRows;

    /// Reads the main table and copy all valid entries to another file
    /// only (key, rowNo) is copied not the entire row
    block_t getData(int rowOffset, int columnOffset, uint32_t headerOffset, int32_t keySize);

    /// Reads the given input file and and performs k way merge
    /// Write the output to output file
    void kWayMerge(const std::string& inFileName, const std::string& outfileName, off_t offset, int blockPerBuffer, int32_t keySize);
    int shiftDataToBlockBoundary(char* buffer, int count);
};

#include "../ExternalSort.cpp"

#endif //DBMS_EXTERNALSORT_H

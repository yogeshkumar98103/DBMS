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
#include <filesystem>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include "DataTypes.h"
#include "Constants.h"

//#define SEQ_READ_ASYNC
//#define SEQ_WRITE_ASYNC
//#define EXT_READ_ASYNC
//#define EXT_WRITE_ASYNC

#define MAX_MEMORY_USAGE                (1 << 27)                          // 64MB
#define EXTERNAL_SORTING_K              6                                  // Constant K for K-way merge


#define SEQ_READ_BLOCKS                 2                                  // Num of Blocks in seq read block
#define SEQ_WRITE_BLOCKS                1                                  // Num of Blocks in seq write block
#define SORTING_BUFFER_BLOCKS           2                                  // Num of Blocks in sorting buffers

static const uint64_t seqBlockSize      = (MAX_MEMORY_USAGE / 8);
static const uint64_t seqReadBlockSize  = SEQ_READ_BLOCKS * seqBlockSize;
static const uint64_t seqWriteBlockSize = SEQ_WRITE_BLOCKS * seqBlockSize;

#define EXT_READ_BLOCKS                 1                                  // Num of Blocks in seq read block
#define EXT_WRITE_BLOCKS                2                                  // Num of Blocks in seq write block

static const uint32_t extBlockSize      = (MAX_MEMORY_USAGE / 16);

using block_t = int64_t;

/// This is responsible for sequentially reading table file
/// This is double buffered
class SeqPageReader{
    int inFileDescriptor;
    int outFileDescriptor;
    int64_t inputFileSize;
    int64_t outputFileSize;
    int requiredNumberOfFetches;
    int currentFetchNumber;

    std::thread readThread;
    std::thread writeThread;
    bool finishedFetching = false;

    std::unique_ptr<char[]> secondaryInputBuffer;
    std::unique_ptr<char[]> secondaryOutputBuffer;

public:

    std::unique_ptr<char[]> primaryInputBuffer;
    std::unique_ptr<char[]> primaryOutputBuffer;
    int64_t bufferSize = 0;
    bool finished = false;

    SeqPageReader() = default;
    ~SeqPageReader();
    void initialise(const char* inFileName, const char* outFileName, uint32_t headerOffset);

    void fetchInput();
    void flushOutput(int64_t outputBuffSize = seqWriteBlockSize);
    void flushOutputToStorage(int64_t outputBuffSize);
    void flushRemaining();

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

    KRPair() = default;
    KRPair(key_t&& key_, row_t row_): key(std::move(key_)), row(row_) {}

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
    int64_t blocksPerBuffer;
    int64_t fileSize;
    int64_t offset;

    std::thread readThread;
    std::thread writeThread;

    std::mutex inputMutex[EXTERNAL_SORTING_K];
    std::mutex queueMutex;

    std::condition_variable condition;
    std::queue<std::pair<std::promise<bool>, int>> fillRequests;

    std::unique_ptr<char[]> secondaryInputBuffer[EXTERNAL_SORTING_K];
    std::unique_ptr<char[]> secondaryOutputBuffer;
    std::future<bool> futures[EXTERNAL_SORTING_K];

    int timesFetched[EXTERNAL_SORTING_K] = {0};
    bool finishedFetchingAll = false;
    int k;

public:
    std::unique_ptr<char[]> primaryInputBuffer[EXTERNAL_SORTING_K];
    std::unique_ptr<char[]> primaryOutputBuffer;
    size_t readSize;

    ExtSortPager();
    ~ExtSortPager();
    void initialise(const char* inFileName, const char* outFileName, int64_t blocksPerBuffer_, uint64_t offset_, int k_);
    void fetchInput(int bufferNo, bool fetchMore);
    void flushOutput(off_t outputBuffSize);
    void flushOutputToStorage(uint64_t outputBuffSize);
    void flushRemaining();
    void endFetching();

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

public:
    ExternalSort(const std::string& fileName_,
                 const std::string& databaseName_,
                 const std::string& finalSortedFileName_,
                 row_t numRows_, int* rowStack);

    /// Wrapper which calls other functions
    void sort(int rowSize_, int columnOffset_, int32_t keySize, uint32_t headerOffset);

private:
    std::string tempFileName;                  /// FileName of file containing extracted rows
    std::string partiallySortedFileName[2];    /// Alternate b/w these two file names
    std::string finalSortedFileName;

    ExtSortPager pager;                        /// Handles disk I/O for partially sorted File
    std::vector<int> deletedRows;              /// Contains rows numbers of deleted rows
    row_t numRows;
    int64_t fileSize;
    int keySize;

    /// Reads the main table and copy all valid entries to another file
    /// only (key, rowNo) is copied not the entire row
    void getData(uint32_t headerOffset);

    SeqPageReader seqReader;
    char* inputBuffer;

    row_t currentReadBufferNumber;
    row_t currentReadPageNumber;
    row_t currentReadRowInPage;
    row_t currentReadRow;
    row_t rowsInSinglePage;
    row_t currentWriteRowInSortingBuffer;
    row_t currentWriteRow;
    row_t rowsInSingleBlock;
    row_t numPagesInInputBuffer;
    int64_t readOffset;
    int columnOffset;
    int rowSize;

    std::vector<data_t> parsedData;

    void initReader();
    bool readNextRow(key_t& key);
    void initWriter();
    void writeNextRow(key_t& key, row_t row);
    void sortBufferAndWrite(row_t rows);

    /// Reads the given input file and and performs k way merge
    /// Write the output to output file
    row_t rowsPerInputBlock;
    row_t rowsPerOutputBlock;
    int fileIdx = 0;

    void kWayMerge(row_t sortedRows, int64_t mergeIdx);
};

#include "../ExternalSort.cpp"

#endif //DBMS_EXTERNALSORT_H

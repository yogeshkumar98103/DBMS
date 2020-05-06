#include "HeaderFiles/ExternalSort.h"
#include <fstream>

// CONVERT TEMPLATE SECIALIZATION
template <> inline int convertDataType<int>(const std::string& str)    {  return std::stoi(str);  }
template <> inline char convertDataType<char>(const std::string& str)  {  return str[0];          }
template <> inline bool convertDataType<bool>(const std::string& str)  {  return str == "true";   }
template <> inline float convertDataType<float>(const std::string& str){  return std::stof(str);  }
template <> inline dbms::string convertDataType<dbms::string>(const std::string& str){  return dbms::string(str);  }


// ---------------------- SeqPageReader ----------------------

SeqPageReader::~SeqPageReader(){
    flushRemaining();
}

void SeqPageReader::flushRemaining(){
    if(readThread.joinable()) readThread.join();
    if(writeThread.joinable()) writeThread.join();
    if(inFileDescriptor != -1)  ::close(inFileDescriptor);
    if(outFileDescriptor != -1) ::close(outFileDescriptor);
    primaryOutputBuffer.reset();
    secondaryInputBuffer.reset();
    primaryOutputBuffer.reset();
    secondaryOutputBuffer.reset();
}

void SeqPageReader::initialise(const char* inFileName, const char* outFileName, uint32_t headerOffset){
    open(inFileName, inFileDescriptor, O_RDONLY);
    open(outFileName, outFileDescriptor, O_WRONLY);

    primaryInputBuffer = std::make_unique<char[]>(seqReadBlockSize);
    secondaryInputBuffer = std::make_unique<char[]>(seqReadBlockSize);
    primaryOutputBuffer = std::make_unique<char[]>(seqWriteBlockSize);
    secondaryOutputBuffer = std::make_unique<char[]>(seqWriteBlockSize);

    inputFileSize = lseek(inFileDescriptor, 0, SEEK_END);
    int numDataPagesInInputFile = (inputFileSize - headerOffset) / PAGE_SIZE;
    int numberPagesSeqBlock = SEQ_READ_BLOCKS * (seqBlockSize / PAGE_SIZE);
    requiredNumberOfFetches = (numDataPagesInInputFile + numberPagesSeqBlock - 1) / numberPagesSeqBlock;
    currentFetchNumber = 0;

    lseek(inFileDescriptor, headerOffset, SEEK_SET);
    fetchFromStorage();
}

bool SeqPageReader::open(const char* fileName, int& fd, int mode){
    int openFlags = O_CREAT | mode;
    mode_t filePerms = S_IWUSR | S_IRUSR;
    fd = ::open(fileName, openFlags, filePerms);
    return (fd == -1);
}

void SeqPageReader::fetchFromSecondary(){
    auto temp = std::move(primaryInputBuffer);
    primaryInputBuffer = std::move(secondaryInputBuffer);
    secondaryInputBuffer = std::move(temp);
}

void SeqPageReader::fetchFromStorage(){
    if(finishedFetching) return;
    bufferSize = ::read(inFileDescriptor, secondaryInputBuffer.get(), seqReadBlockSize);
    if(bufferSize == -1){
        printf("Error reading file\n");
        throw std::runtime_error("Error reading file");
    }

    ++currentFetchNumber;
    if(currentFetchNumber == requiredNumberOfFetches){
        finishedFetching = true;
    }
}

void SeqPageReader::flushOutputToStorage(int64_t outputBuffSize){
    ssize_t bytesWritten = ::write(outFileDescriptor, secondaryOutputBuffer.get(), outputBuffSize);
}

void SeqPageReader::flushOutputToSecondary(){
    auto temp = std::move(primaryOutputBuffer);
    primaryOutputBuffer = std::move(secondaryOutputBuffer);
    secondaryOutputBuffer = std::move(temp);
}

//off_t SeqPageReader::getOutputFileSize(){
//    return outputFileSize;
//}

#ifdef SEQ_READ_ASYNC
void SeqPageReader::fetchInput(){
    if(finished) return;
    if(readThread.joinable()) readThread.join();

    fetchFromSecondary();
    if(finishedFetching) finished = true;
    else readThread = std::thread (&SeqPageReader::fetchFromStorage, this);
}
#else
void SeqPageReader::fetchInput(){
    if(finished) return;
    fetchFromSecondary();
    if(finishedFetching) finished = true;
    else fetchFromStorage();
}
#endif

#ifdef SEQ_WRITE_ASYNC
void SeqPageReader::flushOutput(off_t outputBuffSize){
    if(writeThread.joinable()) writeThread.join();
    flushOutputToSecondary();
    writeThread = std::thread(&SeqPageReader::flushOutputToStorage, this, outputBuffSize);
}
#else
void SeqPageReader::flushOutput(off_t outputBuffSize){
    flushOutputToSecondary();
    flushOutputToStorage(outputBuffSize);
}
#endif


// ---------------------- ExtSortPager ----------------------

ExtSortPager::ExtSortPager(){
    inFileDescriptor = -1;
    outFileDescriptor = -1;
}

ExtSortPager::~ExtSortPager(){
    flushRemaining();
}

void ExtSortPager::flushRemaining(){
    if(readThread.joinable()) readThread.join();
    if(writeThread.joinable()) writeThread.join();
    if(inFileDescriptor != -1) ::close(inFileDescriptor);
    if(outFileDescriptor != -1) ::close(outFileDescriptor);
};

void ExtSortPager::initialise(const char* inFileName, const char* outFileName, int64_t blocksPerBuffer_, uint64_t offset_, int k_){
    flushRemaining();
    this->blocksPerBuffer = blocksPerBuffer_;
    this->offset = offset_;
    this->k = k_;

    open(inFileName, inFileDescriptor, O_RDONLY);
    open(outFileName, outFileDescriptor, O_WRONLY);

    fileSize = lseek(inFileDescriptor, 0, SEEK_END);
    lseek(outFileDescriptor, offset, SEEK_SET);

    primaryOutputBuffer     = std::make_unique<char[]>(EXT_WRITE_BLOCKS * seqBlockSize);
    secondaryOutputBuffer   = std::make_unique<char[]>(EXT_WRITE_BLOCKS * seqBlockSize);
    finishedFetchingAll = false;

    for(int buffNo = 0; buffNo < k; ++buffNo){
        primaryInputBuffer[buffNo]      = std::make_unique<char[]>(EXT_READ_BLOCKS * seqBlockSize);
        secondaryInputBuffer[buffNo]    = std::make_unique<char[]>(EXT_READ_BLOCKS * seqBlockSize);
        timesFetched[buffNo]            = 0;
        fetchFromStorage(buffNo);
    }


    #ifdef EXT_READ_ASYNC
        readThread = std::thread(&ExtSortPager::storageFetcher, this);
    #endif
}

bool ExtSortPager::open(const char* fileName, int& fd, int mode){
    int openFlags = O_CREAT | mode;
    mode_t filePerms = S_IWUSR | S_IRUSR;
    fd = ::open(fileName, openFlags, filePerms);
    return fd != -1;
}

void ExtSortPager::fetchFromSecondary(int bufferNo){
    auto temp = std::move(primaryInputBuffer[bufferNo]);
    primaryInputBuffer[bufferNo] = std::move(secondaryInputBuffer[bufferNo]);
    secondaryInputBuffer[bufferNo] = std::move(temp);
}

void ExtSortPager::fetchFromStorage(int bufferNo){
    uint64_t offset_ = this->offset + (bufferNo * blocksPerBuffer + timesFetched[bufferNo]) * EXT_READ_BLOCKS * extBlockSize;
    if(offset_ > fileSize) return;
    lseek(inFileDescriptor, offset_, SEEK_SET);
    auto len = ::read(inFileDescriptor, secondaryInputBuffer[bufferNo].get(), readSize);
    if(len == -1) throw std::runtime_error("Error reading file");
    ++timesFetched[bufferNo];
}

void ExtSortPager::flushOutputToStorage(uint64_t outputBuffSize){
    ssize_t bytesWritten = ::write(outFileDescriptor, secondaryOutputBuffer.get(), outputBuffSize);
}

void ExtSortPager::flushOutputToSecondary(){
    auto temp = std::move(primaryOutputBuffer);
    primaryOutputBuffer = std::move(secondaryOutputBuffer);
    secondaryOutputBuffer = std::move(temp);
}

#ifdef EXT_READ_ASYNC
void ExtSortPager::storageFetcher(){
    while(true){
        std::unique_lock<std::mutex> lock(queueMutex);
        condition.wait(lock, [&](){return !fillRequests.empty() || finishedFetchingAll;});
        if(finishedFetchingAll) break;
        auto request = std::move(fillRequests.front());
        fillRequests.pop();
        lock.unlock();
        fetchFromStorage(request.second);
        request.first.set_value(true);
    }
}

void ExtSortPager::fetchInput(int bufferNo, bool fetchMore){
    if(futures[bufferNo].valid()) futures[bufferNo].get();
    fetchFromSecondary(bufferNo);

    if(fetchMore){
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            std::promise<bool> fetchPromise;
            futures[bufferNo] = fetchPromise.get_future();
            fillRequests.emplace(std::move(fetchPromise), bufferNo);
        }
        condition.notify_one();
    }
}

void ExtSortPager::endFetching(){
    finishedFetchingAll = true;
    condition.notify_one();
}

#else
void ExtSortPager::fetchInput(int bufferNo, bool fetchMore){
    fetchFromSecondary(bufferNo);
    if(fetchMore) fetchFromStorage(bufferNo);
}

void ExtSortPager::endFetching() {}
#endif

#ifdef EXT_WRITE_ASYNC
void ExtSortPager::flushOutput(off_t outputBuffSize){
    if(writeThread.joinable()) writeThread.join();
    flushOutputToSecondary();
    writeThread = std::thread (&ExtSortPager::flushOutputToStorage, this, outputBuffSize);
}
#else
void ExtSortPager::flushOutput(off_t outputBuffSize){
    flushOutputToSecondary();
    flushOutputToStorage(outputBuffSize);
}
#endif

template <typename key_t>
void convertToText(const std::string& infileName, const std::string& outFileName, int keySize, row_t rowCount){
    int fd = open(infileName.c_str(), O_RDONLY);
    std::ofstream fout(outFileName);

    char* buffer = new char[seqBlockSize];

    int rowSize = (keySize + sizeof(row_t));
    row_t rowInOneGo = seqBlockSize / rowSize;
    uint64_t readSize = rowInOneGo * rowSize;
    row_t row = 0;
    row_t reads = (rowCount + rowInOneGo - 1) / rowInOneGo;
    key_t key;
    for(int i = 0; i < reads; ++i){
        ::read(fd, buffer, readSize);
        if(i == reads - 1) rowInOneGo = rowCount - row;
        uint64_t offset = 0;

        for(int j = 0; j < rowInOneGo; ++j){
            memcpy(&key, buffer + offset, keySize);
            offset += (keySize + sizeof(row_t));
            fout << key << "\n";
            ++row;
        }
    }

    close(fd);
    fout.close();
    delete[] buffer;
}


// ---------------------- ExternalSort ----------------------
template <typename key_t>
ExternalSort<key_t>::ExternalSort(const std::string& databaseName_, const std::string& fileName_, const std::string& finalSortedFileName_, int numRows_, int* rowStack)
:deletedRows(rowStack[0]){
    this->finalSortedFileName   = finalSortedFileName_;
    this->fileName              = databaseName_ + "/" + fileName_;
    this->numRows               = numRows_;
    for(int i = 0; i < rowStack[0]; ++i){
        deletedRows[i] = rowStack[i + 1];
    }
    std::sort(deletedRows.begin(), deletedRows.end());
    this->partiallySortedFileName[0] = std::string("extSortTemp/") + "_0_" + fileName_;
    this->partiallySortedFileName[1] = std::string("extSortTemp/") + "_1_" + fileName_;
}

template <typename key_t>
void ExternalSort<key_t>::sort(int rowSize_, int columnOffset_, int32_t keySize_, uint32_t headerOffset){
    rowSize             = rowSize_;
    columnOffset        = columnOffset_;
    keySize             = keySize_;
    rowsPerInputBlock   = EXT_READ_BLOCKS * extBlockSize / (sizeof(row_t) + keySize);
    rowsPerOutputBlock  = EXT_WRITE_BLOCKS * extBlockSize / (sizeof(row_t) + keySize);
    fileIdx             = 0;
    getData(headerOffset);
    // convertToText<key_t>(partiallySortedFileName[0], "initial.txt", keySize, numRows);

    pager.readSize = rowsPerInputBlock * (sizeof(row_t) + keySize);
    row_t sortedRows = rowsInSingleBlock;
    while(sortedRows < numRows){
        int sortedRowsInOneMerge = sortedRows * EXTERNAL_SORTING_K;
        int numMerges = (numRows + sortedRowsInOneMerge - 1) / sortedRowsInOneMerge;

        for(int mergeIdx = 0; mergeIdx < numMerges; ++mergeIdx){
            kWayMerge(sortedRows, mergeIdx);
        }

        sortedRows *= EXTERNAL_SORTING_K;
        fileIdx = 1 - fileIdx;
    }

    std::filesystem::remove(partiallySortedFileName[1 - fileIdx]);
    std::filesystem::rename(partiallySortedFileName[fileIdx], finalSortedFileName);
}

template <typename key_t>
void ExternalSort<key_t>::getData(uint32_t headerOffset){
    seqReader.initialise(fileName.c_str(), partiallySortedFileName[0].c_str(), headerOffset);
    initReader();
    initWriter();

    key_t key;
    auto nextDeletedRow = deletedRows.begin();

    while(readNextRow(key)){
        // Check if this row is deleted
        if(nextDeletedRow != deletedRows.end() && currentReadRow == *nextDeletedRow){
            ++nextDeletedRow;
        }
        else{
            writeNextRow(key, currentReadRow);
        }
    }

    // Write Remaining Data
    sortBufferAndWrite(currentWriteRowInSortingBuffer);
    seqReader.flushRemaining();
}

template <typename key_t>
void ExternalSort<key_t>::kWayMerge(row_t sortedRows, int64_t mergeIdx){
    int64_t timesFetchingReqd = sortedRows / rowsPerInputBlock;
    row_t rowsProcessed = mergeIdx * sortedRows * EXTERNAL_SORTING_K;
    row_t rowsToProcess = std::min(sortedRows * EXTERNAL_SORTING_K, numRows - rowsProcessed);
    int k = (rowsToProcess + sortedRows - 1) / sortedRows;
    uint64_t offset = rowsProcessed * (sizeof(row_t) + keySize);

    pager.initialise(partiallySortedFileName[fileIdx].c_str(),
                     partiallySortedFileName[1 - fileIdx].c_str(),
                     timesFetchingReqd, offset, k);

    std::vector<data_t> buffers[k];
    std::vector<data_t> outputBuffer(rowsPerOutputBlock);
    row_t bufferIdx[k];
    row_t remRows[k];
    row_t outputBufferIdx = 0;
    heap_t heap;

    for(int buffNo = 0; buffNo < k; ++buffNo){
        remRows[buffNo] = (buffNo == k - 1) ? (rowsToProcess - (k - 1) * sortedRows) : sortedRows;

        // Deserialize Input Buffer
        buffers[buffNo].resize(rowsPerInputBlock);
        pager.fetchInput(buffNo, (remRows[buffNo] - rowsPerInputBlock > 0));
        auto ipBuffer = pager.primaryInputBuffer[buffNo].get();
        uint64_t ipOffset = 0;
        for(int i = 0; i < rowsPerInputBlock; ++i){
            memcpy(&buffers[buffNo][i].key, ipBuffer + ipOffset, keySize);
            ipOffset += keySize;
            memcpy(&buffers[buffNo][i].row, ipBuffer + ipOffset, sizeof(row_t));
            ipOffset += sizeof(row_t);
        }

        // Add Initial Values to heap
        heap.emplace(buffers[buffNo][0], buffNo);
        bufferIdx[buffNo] = 1;
        --remRows[buffNo];
    }

    while(!heap.empty()){
        auto next = heap.top();
        heap.pop();

        int buffNo = next.second;
        outputBuffer[outputBufferIdx++] = std::move(next.first);

        if(outputBufferIdx == rowsPerOutputBlock){
            // Serialize Output
            auto opBuffer = pager.primaryOutputBuffer.get();
            uint64_t opOffset = 0;
            for(int i = 0; i < rowsPerOutputBlock; ++i){
                memcpy(opBuffer + opOffset, &outputBuffer[i].key, keySize);
                opOffset += keySize;
                memcpy(opBuffer + opOffset, &outputBuffer[i].row, sizeof(row_t));
                opOffset += sizeof(row_t);
            }

            pager.flushOutput(opOffset);
            outputBufferIdx = 0;
        }

        if(remRows[buffNo] > 0){
            if(bufferIdx[buffNo] == rowsPerInputBlock){
                bool fetchMore = (remRows[buffNo] - rowsPerInputBlock > 0);
                pager.fetchInput(buffNo, fetchMore);
                auto ipBuffer = pager.primaryInputBuffer[buffNo].get();
                uint64_t ipOffset = 0;
                for(int i = 0; i < rowsPerInputBlock; ++i){
                    memcpy(&buffers[buffNo][i].key, ipBuffer + ipOffset, keySize);
                    ipOffset += keySize;
                    memcpy(&buffers[buffNo][i].row, ipBuffer + ipOffset, sizeof(row_t));
                    ipOffset += sizeof(row_t);
                }
                bufferIdx[buffNo] = 0;
            }

            heap.emplace(buffers[buffNo][bufferIdx[buffNo]], buffNo);
            ++bufferIdx[buffNo];
            --remRows[buffNo];
        }
    }

    if(outputBufferIdx != 0){
        auto opBuffer = pager.primaryOutputBuffer.get();
        uint64_t opOffset = 0;
        for(int i = 0; i < outputBufferIdx; ++i){
            memcpy(opBuffer + opOffset, &outputBuffer[i].key, keySize);
            opOffset += keySize;
            memcpy(opBuffer + opOffset, &outputBuffer[i].row, sizeof(row_t));
            opOffset += sizeof(row_t);
        }
        pager.flushOutput(opOffset);
    }
    pager.endFetching();
    pager.flushRemaining();
}

template <typename key_t>
void ExternalSort<key_t>::initReader(){
    readOffset = 0;
    currentReadBufferNumber = 0;
    currentReadPageNumber = 0;
    currentReadRowInPage = 0;
    currentReadRow = 0;
    readOffset = columnOffset;

    rowsInSinglePage = PAGE_SIZE / rowSize;
    numPagesInInputBuffer = SEQ_READ_BLOCKS * (seqBlockSize / PAGE_SIZE);

    seqReader.fetchInput();
    inputBuffer = seqReader.primaryInputBuffer.get();
}

template <typename key_t>
bool ExternalSort<key_t>::readNextRow(key_t& key){
    // Check if all rows are read
    if(currentReadRow == numRows){
        return false;
    }

    // Read data at read offset
    memcpy(&key, inputBuffer + readOffset, keySize);
    // TODO: This will cause bug with dbms::string
    //       Create different set of functions for dbms::string using flexible array member

    readOffset += rowSize;
    ++currentReadRowInPage;
    ++currentReadRow;

    // Change Page
    if(currentReadRowInPage == rowsInSinglePage){
        currentReadRowInPage = 0;
        ++currentReadPageNumber;
        readOffset = currentReadPageNumber * PAGE_SIZE + columnOffset;
    }

    // Change Input Buffer
    if(currentReadPageNumber == numPagesInInputBuffer){
        ++currentReadBufferNumber;
        currentReadPageNumber = 0;
        readOffset = columnOffset;
        seqReader.fetchInput();
        inputBuffer = seqReader.primaryInputBuffer.get();
    }

    return true;
}

template <typename key_t>
void ExternalSort<key_t>::initWriter(){
    currentWriteRowInSortingBuffer = 0;
    currentWriteRow = 0;

    rowsInSingleBlock = SORTING_BUFFER_BLOCKS * seqBlockSize / (keySize + sizeof(row_t));
    parsedData.resize(rowsInSingleBlock);
}

template <typename key_t>
void ExternalSort<key_t>::writeNextRow(key_t& key, row_t row){
    parsedData[currentWriteRowInSortingBuffer] = data_t(std::move(key), row);

    ++currentWriteRowInSortingBuffer;
    ++currentWriteRow;

    // Change Block
    if(currentWriteRowInSortingBuffer == rowsInSingleBlock){
        sortBufferAndWrite(rowsInSingleBlock);
        currentWriteRowInSortingBuffer = 0;
    }
}

template <typename key_t>
void ExternalSort<key_t>::sortBufferAndWrite(row_t rows){
    std::sort(parsedData.begin(), parsedData.begin() + rows);

    row_t size = SEQ_WRITE_BLOCKS * seqBlockSize / (sizeof(row_t) + keySize);
    int fullWrites = rows / size;
    for(int writeNo = 0; writeNo < fullWrites; ++writeNo){
        auto buffer = seqReader.primaryOutputBuffer.get();
        uint64_t offset = 0;
        row_t start = writeNo * size;
        row_t end = start + size;
        for(int i = start; i < end; ++i){
            memcpy(buffer + offset,  &parsedData[i].key, keySize);
            // TODO: This won't work for strings.
            offset += keySize;
            memcpy(buffer + offset,  &parsedData[i].row, sizeof(row_t));
            offset += sizeof(row_t);
        }
        seqReader.flushOutput(offset);
    }

    row_t remaining = rows % size;
    if(remaining > 0) {
        auto buffer = seqReader.primaryOutputBuffer.get();
        uint64_t offset = 0;
        row_t start = fullWrites * size;
        row_t end = start + remaining;
        for(int i = start; i < end; ++i){
            memcpy(buffer + offset,  &parsedData[i].key, keySize);
            // TODO: This won't work for strings.
            offset += keySize;
            memcpy(buffer + offset,  &parsedData[i].row, sizeof(row_t));
            offset += sizeof(row_t);
        }

        seqReader.flushOutput(offset);
    }
}

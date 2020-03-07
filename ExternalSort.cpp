#include "HeaderFiles/ExternalSort.h"

// CONVERT TEMPLATE SECIALIZATION
template <> inline int convertDataType<int>(const std::string& str)    {  return std::stoi(str);  }
template <> inline char convertDataType<char>(const std::string& str)  {  return str[0];          }
template <> inline bool convertDataType<bool>(const std::string& str)  {  return str == "true";   }
template <> inline float convertDataType<float>(const std::string& str){  return std::stof(str);  }
template <> inline dbms::string convertDataType<dbms::string>(const std::string& str){  return dbms::string(str);  }


// ---------------------- SeqPageReader ----------------------

SeqPageReader::~SeqPageReader(){
    if(inFileDescriptor != -1)  ::close(inFileDescriptor);
    if(outFileDescriptor != -1) ::close(outFileDescriptor);
    if(readThread.joinable()) readThread.join();
    if(writeThread.joinable()) writeThread.join();
}

void SeqPageReader::initialise(const char* inFileName, const char* outFileName, uint32_t headerOffset){
    open(inFileName, inFileDescriptor, O_RDONLY);
    open(outFileName, outFileDescriptor, O_WRONLY);

    primaryInputBuffer = std::make_unique<char[]>(seqReadBlockSize);
    secondaryInputBuffer = std::make_unique<char[]>(seqReadBlockSize);
    primaryOutputBuffer = std::make_unique<char[]>(seqWriteBlockSize);
    secondaryOutputBuffer = std::make_unique<char[]>(seqWriteBlockSize);

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
    if(bufferSize < seqReadBlockSize){
        finishedFetching = true;
    }
}

void SeqPageReader::flushOutputToStorage(off_t outputBuffSize){
    ssize_t bytesWritten = ::write(outFileDescriptor, secondaryOutputBuffer.get(), outputBuffSize);
}

void SeqPageReader::flushOutputToSecondary(){
    auto temp = std::move(primaryOutputBuffer);
    primaryOutputBuffer = std::move(secondaryOutputBuffer);
    secondaryOutputBuffer = std::move(temp);
}

off_t SeqPageReader::getOutputFileSize(){
    return lseek(outFileDescriptor, 0, SEEK_END);
}


#ifdef SEQ_READ_ASYNC
void SeqPageReader::fetchInput(){
    if(readThread.joinable()) readThread.join();
    fetchFromSecondary();
    if(finishedFetching){
        finished = true;
        return;
    }
    readThread = std::thread (&SeqPageReader::fetchFromStorage, this);
}
#else
void SeqPageReader::fetchInput(){
    fetchFromSecondary();
    if(finishedFetching){
        finished = true;
        return;
    }
    fetchFromStorage();
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
#endif  // SEQ_WRITE_ASYNC


void convertToText(const std::string& infileName, const std::string& outFileName){
    int fd = open(infileName.c_str(), O_RDONLY);
    int fd2 = open(outFileName.c_str(), O_WRONLY | O_TRUNC | O_CREAT, S_IRWXU);
    char buffer[blockSize];
    KRPair<int>* data;
    int size = blockSize/sizeof(KRPair<int>);
    data = new(buffer) KRPair<int>[size];
    std::cout << size * SEQ_WRITE_BLOCKS << std::endl;
    while(read(fd, buffer, blockSize) > 0){
        for(int i = 0; i < size; ++i){
            auto key = std::to_string(data[i].key) + "\n";
            write(fd2, key.c_str(), key.size());
        }
    }
    close(fd);
    close(fd2);
}


// ---------------------- ExtSortPager ----------------------

ExtSortPager::ExtSortPager(){
    inFileDescriptor = -1;
    outFileDescriptor = -1;
}

ExtSortPager::~ExtSortPager(){
    if(inFileDescriptor != -1) ::close(inFileDescriptor);
    if(outFileDescriptor != -1) ::close(outFileDescriptor);
    if(readThread.joinable()) readThread.join();
    if(writeThread.joinable()) writeThread.join();
}

void ExtSortPager::initialise(const char* inFileName, const char* outFileName, int blocksPerBuffer_, off_t offset_){
    if(inFileDescriptor != -1) ::close(inFileDescriptor);
    if(outFileDescriptor != -1) ::close(outFileDescriptor);
    if(readThread.joinable()) readThread.join();
    if(writeThread.joinable()) writeThread.join();

    this->blocksPerBuffer = blocksPerBuffer_;
    this->offset = offset_;
    open(inFileName, inFileDescriptor, O_RDONLY);
    open(outFileName, outFileDescriptor, O_WRONLY);
    fileSize = lseek(inFileDescriptor, 0, SEEK_END);

    primaryOutputBuffer = std::make_unique<char[]>(blockSize);
    secondaryOutputBuffer = std::make_unique<char[]>(blockSize);
    for(int buffNo = 0; buffNo < EXTERNAL_SORTING_K; ++buffNo){
        primaryInputBuffer[buffNo] = std::make_unique<char[]>(blockSize);
        secondaryInputBuffer[buffNo] = std::make_unique<char[]>(blockSize);
        finishedFetching[buffNo] = false;
        finished[buffNo] = false;
        timesFetched[buffNo] = 0;
        fetchFromStorage(buffNo);
    }
    finishedFetchingAll = false;
    finishedAll = false;
    fullyFetchedBufferCount = 0;


    #ifdef EXT_READ_ASYNC
        readThread = std::thread(&ExtSortPager::storageFetcher, this);
    #endif

    for(int buffNo = 0; buffNo < EXTERNAL_SORTING_K; ++buffNo){
        fetchInput(buffNo);
    }
}

bool ExtSortPager::open(const char* fileName, int& fd, int mode){
    int openFlags = O_CREAT | mode;
    mode_t filePerms = S_IWUSR | S_IRUSR;
    fd = ::open(fileName, openFlags, filePerms);
    if (fd == -1) {
        return false;
    }
    // off_t fileLength_ = lseek(fd, 0, SEEK_END);
    // this->fileDescriptor = fd;
    // this->fileLength = static_cast<int64_t>(fileLength_);
    return true;
}

void ExtSortPager::fetchFromSecondary(int bufferNo){
    auto temp = std::move(primaryInputBuffer[bufferNo]);
    primaryInputBuffer[bufferNo] = std::move(secondaryInputBuffer[bufferNo]);
    secondaryInputBuffer[bufferNo] = std::move(temp);
    bufferSize[bufferNo] = secondaryBufferSize[bufferNo];
}

void ExtSortPager::fetchFromStorage(int bufferNo){
    if(finishedFetching[bufferNo]) return;
    std::lock_guard<std::mutex> lock(inputMutex[bufferNo]);
    uint64_t offset_ = this->offset + bufferNo * blocksPerBuffer * blockSize;
    if(offset >= fileSize){
        finishedFetching[bufferNo] = true;
        ++fullyFetchedBufferCount;
        finishedFetchingAll = (fullyFetchedBufferCount == EXTERNAL_SORTING_K);
        return;
    }

    lseek(inFileDescriptor, offset_, SEEK_SET);
    secondaryBufferSize[bufferNo] = ::read(inFileDescriptor, secondaryInputBuffer[bufferNo].get(), blockSize);

    if(secondaryBufferSize[bufferNo] == -1){
        printf("Error reading file\n");
        throw std::runtime_error("Error reading file");
    }

    ++timesFetched[bufferNo];
    if(timesFetched[bufferNo] == blocksPerBuffer || secondaryBufferSize[bufferNo] < blockSize){
        finishedFetching[bufferNo] = true;
        ++fullyFetchedBufferCount;
        finishedFetchingAll = (fullyFetchedBufferCount == EXTERNAL_SORTING_K);
    }
}

void ExtSortPager::flushOutputToStorage(off_t outputBuffSize){
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
        condition.wait(lock, [&](){return !fillRequests.empty();});
        int buffNo = fillRequests.front();
        fillRequests.pop();
        lock.unlock();
        fetchFromStorage(buffNo);
        if(finishedFetchingAll) break;
    }
    finishedAll = true;
}
void ExtSortPager::fetchInput(int bufferNo)
{
    {
        std::lock_guard<std::mutex> lock(inputMutex[bufferNo]);
        fetchFromSecondary(bufferNo);
        if(finishedFetching[bufferNo]){
            finished[bufferNo] = true;
            return;
        }
    }
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        fillRequests.push(bufferNo);
    }
    condition.notify_one();
}
#else
void ExtSortPager::fetchInput(int bufferNo){
    fetchFromSecondary(bufferNo);
    if(finishedFetching[bufferNo]){
        finished[bufferNo] = true;
        return;
    }
    fetchFromStorage(bufferNo);
}
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


// ---------------------- ExternalSort ----------------------
template <typename key_t>
ExternalSort<key_t>::ExternalSort(const std::string& databaseName_, const std::string& fileName_, int colNo_, int numRows_, int* rowStack)
:deletedRows(rowStack[0]){
    this->fileName  = databaseName_ + "/" + fileName_;
    this->colNo     = colNo_;
    this->numRows   = numRows_;
    for(int i = 0; i < rowStack[0]; ++i){
        deletedRows[i] = rowStack[i + 1];
    }
    std::sort(deletedRows.begin(), deletedRows.end());
    this->partiallySortedFileName[0] = "extSortTemp/" + std::to_string(colNo_) + "_0_" + fileName_;
    this->partiallySortedFileName[1] = "extSortTemp/" + std::to_string(colNo_) + "_1_" + fileName_;
}

template <typename key_t>
void ExternalSort<key_t>::sort(int rowOffset, int columnOffset, uint32_t headerOffset, int32_t keySize){
    block_t totalBlocks = getData(rowOffset, columnOffset, headerOffset, keySize);
    int blockPerBuffer = seqWriteBlockSize/blockSize;
    off_t fileSize = totalBlocks * blockSize;
    int in  = 0;
    int out = 1;
    while(blockPerBuffer < totalBlocks){
        off_t offset = 0;
        while(offset < fileSize){
            kWayMerge(partiallySortedFileName[in], partiallySortedFileName[out], offset, blockPerBuffer, keySize);
            offset += (blockPerBuffer * blockSize * EXTERNAL_SORTING_K);
            convertToText(partiallySortedFileName[out], "output2.txt");
        }
        blockPerBuffer *= EXTERNAL_SORTING_K;
        std::swap(in, out);
    }
}

template <typename key_t>
block_t ExternalSort<key_t>::getData(int rowOffset, int columnOffset, uint32_t headerOffset, int32_t keySize){
    SeqPageReader seqReader;
    seqReader.initialise(fileName.c_str(), partiallySortedFileName[0].c_str(), headerOffset);
    char* buffer;
    int32_t bufferSize, currentOffset;
    data_t* data;
    key_t key;
    row_t currentRow = 0, dataIdx = 0;
    int deleteSize = deletedRows.size();
    int deleteIdx = 0;

    int32_t maxDataSize = SEQ_WRITE_BLOCKS * (blockSize/sizeof(data_t));
    data = new(seqReader.primaryOutputBuffer.get()) data_t[maxDataSize];
    block_t blockCount = 0;
    while(currentRow < numRows){
        seqReader.fetchInput();
        buffer = seqReader.primaryInputBuffer.get();
        bufferSize = seqReader.bufferSize;
        currentOffset = columnOffset;

        while(currentOffset < bufferSize && currentRow < numRows){
            if(deleteIdx < deleteSize && currentRow == deletedRows[deleteIdx]){
                ++deleteIdx;
                currentOffset += rowOffset;
                continue;
            }

            memcpy(&key, buffer + currentOffset, keySize);
            // TODO: This will cause bug with dbms::string
            //       Create different set of functions for dbms::string using flexible array member
            data[dataIdx].key = key;
            data[dataIdx].row = currentRow;
            currentOffset += rowOffset;
            ++dataIdx;
            ++currentRow;

            if(dataIdx == maxDataSize){
                std::sort(data, data + maxDataSize);
                int numBlocks = shiftDataToBlockBoundary(seqReader.primaryOutputBuffer.get(), maxDataSize);
                blockCount += numBlocks;
                seqReader.flushOutput();
                data = new(seqReader.primaryOutputBuffer.get()) data_t[maxDataSize];
                dataIdx = 0;
            }
        }
    }

    if(dataIdx != 0){
        std::sort(data, data + dataIdx);
        int numBlocks = shiftDataToBlockBoundary(seqReader.primaryOutputBuffer.get(), dataIdx);
        blockCount += numBlocks;
        seqReader.flushOutput(numBlocks * blockSize);
    }

    return blockCount;
}

template<typename key_t>
int ExternalSort<key_t>::shiftDataToBlockBoundary(char* buffer, int count){
    int countPerBlock = blockSize / sizeof(data_t);
    int blocksReqd = (count + countPerBlock - 1) / countPerBlock;
    if(blockSize % sizeof(data_t) == 0) return blocksReqd;
    int lastBlockCount = count - (blocksReqd - 1) * countPerBlock;
    off_t from  =  (count - lastBlockCount) * sizeof(data_t);
    off_t to    =  (blocksReqd - 1) * blockSize;
    memcpy(buffer + to, buffer + from, lastBlockCount * sizeof(data_t));
    for(int i = 0; i < blocksReqd - 2; ++i){
        from -= countPerBlock * sizeof(data_t);
        to -= blockSize;
        memcpy(buffer + to, buffer + from, countPerBlock * sizeof(data_t));
    }
    return blocksReqd;
}

template <typename key_t>
void ExternalSort<key_t>::kWayMerge(const std::string& inFileName, const std::string& outfileName, off_t offset, int blockPerBuffer, int32_t keySize){
    pager.initialise(inFileName.c_str(), outfileName.c_str(), blockPerBuffer, offset);
    heap_t heap;
    data_t* buffers[EXTERNAL_SORTING_K];
    data_t* outputBuffer;
    int bufferIdx[EXTERNAL_SORTING_K];
    int outputBufferIdx = 0;
    uint32_t maxBufferSize = blockSize/sizeof(data_t);
    outputBuffer = new(pager.primaryOutputBuffer.get()) data_t[maxBufferSize];
    for(int buffNo = 0; buffNo < EXTERNAL_SORTING_K; ++buffNo){
        buffers[buffNo] = new(pager.primaryInputBuffer[buffNo].get()) data_t[maxBufferSize];
        if(pager.bufferSize[buffNo] == 0) continue;
        heap.emplace(buffers[buffNo][0], buffNo);
        bufferIdx[buffNo] = 1;
    }

    int blockCount = 0;
    while(!heap.empty()){
        auto next = heap.top();
        heap.pop();
        int buffNo = next.second;
        outputBuffer[outputBufferIdx] = next.first;
        ++outputBufferIdx;
        if(outputBufferIdx == maxBufferSize){
            pager.flushOutput();
            outputBuffer = new(pager.primaryOutputBuffer.get()) KRPair<key_t>[maxBufferSize];
            outputBufferIdx = 0;
            ++blockCount;
        }

        if(pager.finished[buffNo]) continue;
        heap.emplace(buffers[buffNo][bufferIdx[buffNo]], buffNo);
        ++bufferIdx[buffNo];
        if(bufferIdx[buffNo] == pager.bufferSize[buffNo]){
            pager.fetchInput(buffNo);
            buffers[buffNo] = new(pager.primaryInputBuffer[buffNo].get()) KRPair<key_t>[maxBufferSize];
            bufferIdx[buffNo] = 0;
        }
    }

    if(outputBufferIdx != 0) pager.flushOutput(outputBufferIdx * sizeof(data_t));
}

//    #ifdef F_FULLFSYNC
//        fcntl(outFileDescriptor, F_FULLFSYNC);   // For MacOS X
//    #else
//        fdatasync(outFileDescriptor);
//    #endif
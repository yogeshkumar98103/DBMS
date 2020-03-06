#include "HeaderFiles/ExternalSort.h"

// ---------------------- SeqPageReader ----------------------

SeqPageReader::~SeqPageReader(){
    if(inFileDescriptor != -1)  ::close(inFileDescriptor);
    if(outFileDescriptor != -1) ::close(outFileDescriptor);
}

void SeqPageReader::initialise(const char* inFileName, const char* outFileName, uint32_t headerOffset){
    open(inFileName, inFileDescriptor, O_RDONLY);
    open(outFileName, outFileDescriptor, O_APPEND);

    primaryInputBuffer = std::make_unique<char[]>(seqBlockSize);
    secondaryInputBuffer = std::make_unique<char[]>(seqBlockSize);
    primaryOutputBuffer = std::make_unique<char[]>(seqBlockSize);
    secondaryOutputBuffer = std::make_unique<char[]>(seqBlockSize);

    lseek(inFileDescriptor, headerOffset, SEEK_SET);
    fetchFromStorage();
    fetchInput();
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
    std::lock_guard<std::mutex> lock(inputMutex);
    bufferSize = ::read(inFileDescriptor, secondaryInputBuffer.get(), seqBlockSize);
    if(bufferSize == -1){
        printf("Error reading file\n");
        throw std::runtime_error("Error reading file");
    }
    if(bufferSize < seqBlockSize){
        finishedFetching = true;
    }
}

void SeqPageReader::fetchInput(){
    {
        std::lock_guard<std::mutex> lock(inputMutex);
        fetchFromSecondary();
    }
    if(finishedFetching){
        finished = true;
        return;
    }
    std::thread readFromDiskThread(&SeqPageReader::fetchFromSecondary, this);
    readFromDiskThread.detach();
}

void SeqPageReader::flushOutput(){
    {
        std::lock_guard<std::mutex> lock(outputMutex);
        flushOutputToSecondary();
    }
    std::thread flushToDiskThread(&SeqPageReader::flushOutputToStorage, this);
    flushToDiskThread.detach();
}

void SeqPageReader::flushOutputToStorage(){
    std::lock_guard<std::mutex> lock(outputMutex);
    ssize_t bytesWritten = ::write(outFileDescriptor, secondaryOutputBuffer.get(), seqBlockSize);
    #ifdef F_FULLFSYNC
        fcntl(outFileDescriptor, F_FULLFSYNC);   // For MacOS X
    #else
        fdatasync(outFileDescriptor);
    #endif
}

void SeqPageReader::flushOutputToSecondary(){
    auto temp = std::move(primaryOutputBuffer);
    primaryOutputBuffer = std::move(secondaryOutputBuffer);
    secondaryOutputBuffer = std::move(temp);
}





// ---------------------- ExtSortPager ----------------------

ExtSortPager::ExtSortPager(){
    inFileDescriptor = -1;
    outFileDescriptor = -1;
}

ExtSortPager::~ExtSortPager(){
    if(inFileDescriptor != -1) ::close(inFileDescriptor);
    if(outFileDescriptor != -1) ::close(outFileDescriptor);
}

void ExtSortPager::initialise(const char* inFileName, const char* outFileName, int blocksPerBuffer_){
    if(inFileDescriptor != -1) ::close(inFileDescriptor);
    if(outFileDescriptor != -1) ::close(outFileDescriptor);

    this->blocksPerBuffer = blocksPerBuffer_;
    open(inFileName, inFileDescriptor, O_RDONLY);
    open(outFileName, outFileDescriptor, O_APPEND);

    primaryOutputBuffer = std::make_unique<char[]>(blockSize);
    secondaryOutputBuffer = std::make_unique<char[]>(blockSize);
    for(int buffNo = 0; buffNo < EXTERNAL_SORTING_K; ++buffNo){
        primaryInputBuffer[buffNo] = std::make_unique<char[]>(blockSize);
        secondaryInputBuffer[buffNo] = std::make_unique<char[]>(blockSize);
        finishedFetching[buffNo] = false;
        finished[buffNo] = false;
        fetchFromStorage(buffNo);
    }
    finishedFetchingAll = false;
    finishedAll = false;
    fullyFetchedBufferCount = 0;


    std::thread dataFetchThread(&ExtSortPager::storageFetcher, this);
    dataFetchThread.detach();

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

void ExtSortPager::fetchInput(int bufferNo){
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

void ExtSortPager::fetchFromSecondary(int bufferNo){
    primaryInputBuffer[bufferNo] = std::move(secondaryInputBuffer[bufferNo]);
}

void ExtSortPager::fetchFromStorage(int bufferNo){
    if(finishedFetching[bufferNo]) return;
    std::lock_guard<std::mutex> lock(inputMutex[bufferNo]);
    uint64_t offset = bufferNo * blocksPerBuffer * blockSize;
    lseek(inFileDescriptor, offset, SEEK_SET);
    bufferSize[bufferNo] = ::read(inFileDescriptor, secondaryInputBuffer[bufferNo].get(), blockSize);

    if(bufferSize[bufferNo] == -1){
        printf("Error reading file\n");
        throw std::runtime_error("Error reading file");
    }
    if(bufferSize[bufferNo] < blockSize){
        finishedFetching[bufferNo] = true;
        ++fullyFetchedBufferCount;
        if(fullyFetchedBufferCount == EXTERNAL_SORTING_K){
            finishedFetchingAll = true;
        }
    }
}

void ExtSortPager::flushOutput(){
    {
        std::lock_guard<std::mutex> lock(outputMutex);
        flushOutputToSecondary();
    }
    std::thread flushToDiskThread(&ExtSortPager::flushOutputToStorage, this);
    flushToDiskThread.detach();
}

void ExtSortPager::flushOutputToStorage(){
    std::lock_guard<std::mutex> lock(outputMutex);
    ssize_t bytesWritten = ::write(outFileDescriptor, secondaryOutputBuffer.get(), blockSize);
    #ifdef F_FULLFSYNC
        fcntl(outFileDescriptor, F_FULLFSYNC);   // For MacOS X
    #else
        fdatasync(outFileDescriptor);
    #endif
}

void ExtSortPager::flushOutputToSecondary(){
    auto temp = std::move(primaryOutputBuffer);
    primaryOutputBuffer = std::move(secondaryOutputBuffer);
    secondaryOutputBuffer = std::move(temp);
}





// ---------------------- ExternalSort ----------------------

template<typename key_t>
class ExternalSort{
    using heap_t = std::priority_queue <std::pair<KRPair<key_t>, int>, std::vector<int>, std::greater<>>;
    std::string fileName;                   /// File name of table to sort
    int colNo;                              /// Column Number on which sorting is done

    ExternalSort(const std::string& fileName_, int colNo_, int numRows_, int* rowStack);
    void sort(int rowOffset, int columnOffset, uint32_t headerOffset, int32_t keySize);                            /// Wrapper which calls other functions

private:
    std::string tempFileName;                  /// FileName of file containing extracted rows
    std::string partiallySortedFileName[2];    /// Alternate b/w these two file names
    std::string finalSortedFileName;

    SeqPageReader seqReader;                   /// Handles disk I/O for original Table
    ExtSortPager pager;                        /// Handles disk I/O for partially sorted File
    std::vector<int> deletedRows;                   /// Contains rows numbers of deleted rows
    key_t* keys[EXTERNAL_SORTING_K];           /// Array conatining keys in buffer
    row_t* rows[EXTERNAL_SORTING_K];           /// Array conatining rows in buffer
    row_t numRows;

    /// Reads the main table and copy all valid entries to another file
    /// only (key, rowNo) is copied not the entire row
    void getData(int rowOffset, int columnOffset, uint32_t headerOffset, int32_t keySize);

    /// Reads the given input file and and performs k way merge
    /// Write the output to output file
    void kWayMerge(const std::string& inFileName, const std::string& outfileName, int blockPerBuffer);
};

template <typename key_t>
ExternalSort<key_t>::ExternalSort(const std::string& fileName_, int colNo_, int numRows_, int* rowStack)
:deletedRows(rowStack[0]){
    this->fileName  = fileName_;
    this->colNo     = colNo_;
    this->numRows   = numRows_;
    for(int i = 0; i < rowStack[0]; ++i){
        deletedRows[i] = rowStack[i + 1];
    }
    std::sort(deletedRows.begin(), deletedRows.end());
}

template <typename key_t>
void ExternalSort<key_t>::sort(int rowOffset, int columnOffset, uint32_t headerOffset, int32_t keySize){
    getData(rowOffset, columnOffset, headerOffset, keySize);
    //kWayMerge();
}

template <typename key_t>
void ExternalSort<key_t>::getData(int rowOffset, int columnOffset, uint32_t headerOffset, int32_t keySize){
    seqReader.initialise(fileName.c_str(), tempFileName.c_str(), headerOffset);
    char* buffer;
    int32_t bufferSize, currentOffset;
    row_t currentRow = 0;
    KRPair<key_t>* data;
    row_t i = 0;
    int deleteIdx = 0;
    char keyStr[keySize];

    // TODO: Confirm this later
    int32_t dataArraySize = seqBlockSize/(2 * std::max((int)sizeof(row_t), keySize)); // seqBlockSize/sizeof(KRPair<key_t>);
    data = new(seqReader.primaryOutputBuffer.get()) KRPair<key_t>[dataArraySize];
    int deleteSize = deletedRows.size();

    while(!seqReader.finished){
        seqReader.fetchInput();

        buffer = seqReader.primaryInputBuffer.get();
        bufferSize = seqReader.bufferSize;
        currentOffset = columnOffset;

        while(currentOffset < bufferSize){
            if(deleteIdx < deleteSize && currentRow == deletedRows[deleteIdx]){
                ++deleteIdx;
                currentOffset += rowOffset;
                continue;
            }

            memcpy(keyStr, buffer + currentOffset, keySize);
            // TODO: This will cause bug with dbms::string
            //       Create different set of functions for dbms::string using flexible array member
            data[i].key = convertDataType<key_t>(keyStr);
            data[i].row = currentRow;
            if(i == dataArraySize - 1){
                std::sort(data, data + dataArraySize);
                seqReader.flushOutput();
                data = new(seqReader.primaryOutputBuffer.get()) KRPair<key_t>[dataArraySize];
            }
            currentOffset += rowOffset;
            ++i;
        }
    }
}

template <typename key_t>
void ExternalSort<key_t>::kWayMerge(const std::string& inFileName, const std::string& outfileName, int blockPerBuffer){
    pager.initialise(inFileName.c_str(), outfileName.c_str(), blockPerBuffer);
    heap_t heap;
}

int main(){
    return 0;
}
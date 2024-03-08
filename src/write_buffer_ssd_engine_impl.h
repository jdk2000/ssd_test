//
// Created by 86185 on 2024/3/9.
//

#ifndef SSDENGINE_WRITE_BUFFER_SSD_ENGINE_IMPL_H
#define SSDENGINE_WRITE_BUFFER_SSD_ENGINE_IMPL_H


#include <unordered_map>
#include <list>
#include <unordered_set>
#include <fstream>
#include "ssd_engine.h"


class WriteBufferSSDEngineImpl : public SSDEngine {
public:
    WriteBufferSSDEngineImpl() = default;

    WriteBufferSSDEngineImpl(uint64_t embSize, const std::string &saveDir);

    ~WriteBufferSSDEngineImpl();

    void InsertEmbeddings(std::vector<uint64_t> keys, std::vector<float *> addrs) override;

    std::vector<std::vector<float>> FetchEmbeddings(std::vector<uint64_t> keys) override;

    void DeleteEmbeddings(std::vector<uint64_t> keys) override;

private:
    void CreateNewFile();

    void FlushWriteBuffer();

    struct EmbPos {
        bool flushed;
        uint32_t fileID;
        size_t offset;
    };
    struct FileInfo {
        std::unordered_set<uint64_t> validKeys;
    };
    uint64_t embSize{};
    std::string saveDir;
    std::unordered_map<uint64_t, EmbPos> key2Pos;
    const uint32_t maxFileSize = 32 * 1024 * 1024;
    uint32_t fileCnt = 0;
    std::unordered_map<uint32_t, FileInfo> fileInfoMap;
    std::fstream curFile;

    const uint64_t BUFFER_SIZE = maxFileSize;
    char *write_buffer;
    size_t buffer_off = 0;
    std::vector<uint64_t> keysInBuffer;
};


#endif //SSDENGINE_WRITE_BUFFER_SSD_ENGINE_IMPL_H

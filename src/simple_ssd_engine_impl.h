//
// Created by 86185 on 2024/3/8.
//

#ifndef SSDENGINE_SIMPLE_SSD_ENGINE_IMPL_H
#define SSDENGINE_SIMPLE_SSD_ENGINE_IMPL_H

#include <unordered_map>
#include <list>
#include <unordered_set>
#include <fstream>
#include "ssd_engine.h"


class SimpleSSDEngineImpl : public SSDEngine {
public:
    SimpleSSDEngineImpl() = default;

    SimpleSSDEngineImpl(uint64_t embSize, const std::string &saveDir);

    ~SimpleSSDEngineImpl();

    void InsertEmbeddings(std::vector<uint64_t> keys, std::vector<float *> addrs) override;

    std::vector<std::vector<float>> FetchEmbeddings(std::vector<uint64_t> keys) override;

    void DeleteEmbeddings(std::vector<uint64_t> keys) override;

private:
    void CreateNewFile();

    struct EmbPos {
        uint32_t fileID;
        size_t offset;
    };
    struct FileInfo {
        size_t curOffset;
        std::unordered_set<uint64_t> validKeys;
    };

    uint64_t embSize{};
    std::string saveDir;
    std::unordered_map<uint64_t, EmbPos> key2Pos;
    const uint32_t maxFileSize = 32 * 1024 * 1024;
    uint32_t fileCnt = 0;
    std::unordered_map<uint32_t, FileInfo> fileInfoMap;
    std::fstream curFile;
};


#endif //SSDENGINE_SIMPLE_SSD_ENGINE_IMPL_H

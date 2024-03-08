//
// Created by 86185 on 2024/3/8.
//

#include "simple_ssd_engine_impl.h"
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

SimpleSSDEngineImpl::SimpleSSDEngineImpl(uint64_t embSize, const std::string &saveDir) : embSize(embSize),
                                                                                         saveDir(saveDir) {
    fs::path savePath(saveDir);
    if (fs::exists(savePath)) {
        fs::remove_all(savePath);
    }
    fs::create_directories(savePath);
}

SimpleSSDEngineImpl::~SimpleSSDEngineImpl() {
    if (curFile.is_open()) {
        curFile.close();
    }
}

void SimpleSSDEngineImpl::InsertEmbeddings(std::vector<uint64_t> keys, std::vector<float *> addrs) {
    uint64_t memSize = embSize * sizeof(float);
    for (uint64_t i = 0; i < keys.size(); i++) {
        if (key2Pos.count(keys[i])) {
            auto embPos = key2Pos[keys[i]];
            fileInfoMap[embPos.fileID].validKeys.erase(keys[i]);
        }
        if (fileInfoMap.empty() || fileInfoMap[fileCnt].curOffset + memSize > maxFileSize) {
            CreateNewFile();
        }
        auto &curFileInfo = fileInfoMap[fileCnt];

        curFile.seekp(curFileInfo.curOffset);
        curFile.write(reinterpret_cast<const char *>(addrs[i]), memSize);
        curFile.flush();
        key2Pos[keys[i]] = {fileCnt, curFileInfo.curOffset};

        curFileInfo.curOffset += memSize;
        curFileInfo.validKeys.insert(keys[i]);
    }
}

std::vector<std::vector<float>> SimpleSSDEngineImpl::FetchEmbeddings(std::vector<uint64_t> keys) {
    uint64_t memSize = embSize * sizeof(float);
    std::vector<std::vector<float>> embs;
    for (uint64_t key :keys) {
        if (key2Pos.find(key) == key2Pos.end()) {
            throw std::runtime_error("FetchEmbeddings failed. key not in file");
        }
        auto embPos = key2Pos[key];
        std::string fileName = saveDir + "/emb-" + std::to_string(embPos.fileID);
        std::fstream file = std::fstream(fileName, std::ios::binary | std::ios::in);
        file.seekg(embPos.offset);
        std::vector<float> emb(embSize);
        file.read(reinterpret_cast<char *>(emb.data()), memSize);
        embs.push_back(emb);
        file.close();
    }
    return embs;
}

void SimpleSSDEngineImpl::DeleteEmbeddings(std::vector<uint64_t> keys) {
    for (uint64_t key : keys) {
        auto it = key2Pos.find(key);
        fileInfoMap[it->second.fileID].validKeys.erase(key);
        key2Pos.erase(it);
    }
}

void SimpleSSDEngineImpl::CreateNewFile() {
    fileInfoMap[++fileCnt] = {0, {}};
    if (curFile.is_open()) {
        curFile.close();
    }
    std::string fileName = saveDir + "/emb-" + std::to_string(fileCnt);
    curFile.open(fileName, std::ios::binary | std::ios::out);
}

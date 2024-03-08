//
// Created by 86185 on 2024/3/9.
//

#include "write_buffer_ssd_engine_impl.h"
#include <filesystem>
#include <iostream>
#include <numeric>

namespace fs = std::filesystem;

WriteBufferSSDEngineImpl::WriteBufferSSDEngineImpl(uint64_t embSize, const std::string &saveDir) : embSize(embSize),
                                                                                                   saveDir(saveDir) {
    fs::path savePath(saveDir);
    if (fs::exists(savePath)) {
        fs::remove_all(savePath);
    }
    fs::create_directories(savePath);
    write_buffer = (char *) malloc(BUFFER_SIZE);
}

WriteBufferSSDEngineImpl::~WriteBufferSSDEngineImpl() {
    FlushWriteBuffer();
    if (curFile.is_open()) {
        curFile.close();
    }
    free(write_buffer);
}

void WriteBufferSSDEngineImpl::InsertEmbeddings(std::vector<uint64_t> keys, std::vector<float *> addrs) {
    uint64_t memSize = embSize * sizeof(float);
    for (uint64_t i = 0; i < keys.size(); i++) {
        if (key2Pos.count(keys[i])) {
            auto embPos = key2Pos[keys[i]];
            fileInfoMap[embPos.fileID].validKeys.erase(keys[i]);
        }
        if (fileInfoMap.empty() || buffer_off + memSize > BUFFER_SIZE) {
            FlushWriteBuffer();
        }

        memcpy(write_buffer + buffer_off, addrs[i], memSize);
        key2Pos[keys[i]] = {false, 0, buffer_off};
        keysInBuffer.push_back(keys[i]);

        buffer_off += memSize;
    }
}

std::vector<std::vector<float>> WriteBufferSSDEngineImpl::FetchEmbeddings(std::vector<uint64_t> keys) {
    uint64_t memSize = embSize * sizeof(float);
    std::vector<std::vector<float>> embs;
    for (uint64_t key :keys) {
        if (key2Pos.find(key) == key2Pos.end()) {
            throw std::runtime_error("FetchEmbeddings failed. key not in file");
        }
        auto embPos = key2Pos[key];
        std::vector<float> emb(embSize);
        if (embPos.flushed) {
            std::string fileName = saveDir + "/emb-" + std::to_string(embPos.fileID);
            std::fstream file = std::fstream(fileName, std::ios::binary | std::ios::in);
            file.seekg(embPos.offset);
            file.read(reinterpret_cast<char *>(emb.data()), memSize);
            file.close();
        } else {
            memcpy(emb.data(), write_buffer + embPos.offset, memSize);
        }
        embs.push_back(emb);
    }
    return embs;
}

void WriteBufferSSDEngineImpl::DeleteEmbeddings(std::vector<uint64_t> keys) {
    for (uint64_t key : keys) {
        auto it = key2Pos.find(key);
        if (it->second.flushed) {
            fileInfoMap[it->second.fileID].validKeys.erase(key);
        }
        key2Pos.erase(it);
    }
}

void WriteBufferSSDEngineImpl::CreateNewFile() {
    fileInfoMap[++fileCnt] = {{}};
    if (curFile.is_open()) {
        curFile.close();
    }
    std::string fileName = saveDir + "/emb-" + std::to_string(fileCnt);
    curFile.open(fileName, std::ios::binary | std::ios::out);
}

void WriteBufferSSDEngineImpl::FlushWriteBuffer() {
    CreateNewFile();
    std::cout << "FlushWriteBuffering ..." << std::endl;
    auto &curFileInfo = fileInfoMap[fileCnt];
    curFile.write(write_buffer, BUFFER_SIZE);
    for (uint64_t key : keysInBuffer) {
        if (!key2Pos.count(key)) {
            continue;
        }
        auto &pos = key2Pos[key];
        pos.flushed = true;
        pos.fileID = fileCnt;
        curFileInfo.validKeys.insert(key);
    }
    keysInBuffer.clear();
    buffer_off = 0;
}
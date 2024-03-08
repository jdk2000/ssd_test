//
// Created by 86185 on 2024/3/8.
//

#ifndef SSDENGINE_SSD_ENGINE_H
#define SSDENGINE_SSD_ENGINE_H


#include <cstdint>
#include <string>
#include <utility>
#include <vector>

class SSDEngine {
public:
    virtual void InsertEmbeddings(std::vector<uint64_t> keys, std::vector<float *> addrs) = 0;

    virtual std::vector<std::vector<float>> FetchEmbeddings(std::vector<uint64_t> keys) = 0;

    virtual void DeleteEmbeddings(std::vector<uint64_t> keys) = 0;

private:
    uint64_t embSize{};
    std::string saveDir;
};


#endif //SSDENGINE_SSD_ENGINE_H

#include <iostream>
#include <chrono>
#include <random>
#include "simple_ssd_engine_impl.h"
#include "write_buffer_ssd_engine_impl.h"

// Function to generate a random embedding vector
std::vector<float> generateRandomEmbedding(size_t embSize) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dis(-1.0f, 1.0f);

    std::vector<float> embedding(embSize);
    for (float &val : embedding) {
        val = dis(gen);
    }
    return embedding;
}

std::vector<uint64_t> getRandomKeys(const std::unordered_set<uint64_t> &keySet, size_t numKeys) {
    std::vector<uint64_t> keys;
    keys.reserve(numKeys);
    std::vector<uint64_t> temp(keySet.begin(), keySet.end());
    if (temp.size() < numKeys) {
        std::cerr << "Error: Not enough keys in the set to extract " << numKeys << " keys." << std::endl;
        return keys;
    }
    std::random_device rd;
    std::mt19937 g(rd());
    while (keys.size() < numKeys) {
        std::uniform_int_distribution<size_t> distr(0, temp.size() - 1);
        size_t idx = distr(g);
        keys.push_back(temp[idx]);
        // 防止重复提取相同的键
        std::swap(temp[idx], temp[temp.size() - 1]);
        temp.pop_back();
    }
    return keys;
}


const uint64_t embSize = 240 * 4;

void BenchMark(SSDEngine *ssdEngine) {
    std::vector<float *> insertEmbeddings(5000);
    std::vector<std::vector<float>> embeddings;
    for (uint64_t i = 0; i < 5000; ++i) {
        embeddings.push_back(generateRandomEmbedding(embSize));
        insertEmbeddings[i] = embeddings.back().data();
    }

    std::unordered_set<uint64_t> validKeys;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(0, 1e8);

    std::vector<double> insertionDurations;
    std::vector<double> fetchDurations;

    for (int i = 0; i < 50; ++i) {
        std::vector<uint64_t> insertKeys(5000);
        for (uint64_t i = 0; i < 5000; ++i) {
            insertKeys[i] = dis(gen);
        }
        validKeys.insert(insertKeys.begin(), insertKeys.end());
        // Insert embeddings
        auto start = std::chrono::high_resolution_clock::now();
        ssdEngine->InsertEmbeddings(insertKeys, insertEmbeddings);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        insertionDurations.push_back(duration);

        std::cout << "Insertion time: " << duration << " ms" << std::endl;
        std::vector<uint64_t> fetchKeys = getRandomKeys(validKeys, 5000);

        // Fetch embeddings
        start = std::chrono::high_resolution_clock::now();
        auto fetchedEmbeddings = ssdEngine->FetchEmbeddings(fetchKeys);
        end = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        fetchDurations.push_back(duration);
        std::cout << "Fetch time: " << duration << " ms" << std::endl;
    }

    // 计算插入和获取的平均时间
    double avgInsertionTime = std::accumulate(insertionDurations.begin(), insertionDurations.end(), 0.0) / insertionDurations.size();
    double avgFetchTime = std::accumulate(fetchDurations.begin(), fetchDurations.end(), 0.0) / fetchDurations.size();
    // 输出平均时间
    std::cout << "Average insertion time: " << avgInsertionTime << " ms" << std::endl;
    std::cout << "Average fetch time: " << avgFetchTime << " ms" << std::endl;

}

int main() {
    const std::string saveDir = "E:/ssd_path";

    auto *simpleSsdEngine = new SimpleSSDEngineImpl(embSize, saveDir);
    std::cout << "===============BenchMark SimpleSSDEngineImpl================" << std::endl;
    BenchMark(simpleSsdEngine);
    delete simpleSsdEngine;

    const std::string saveDir1 = "E:/ssd_path1";
    auto *writeBufferSsdEngine = new WriteBufferSSDEngineImpl(embSize, saveDir1);
    std::cout << "===============BenchMark WriteBufferSSDEngineImpl================" << std::endl;
    BenchMark(writeBufferSsdEngine);
    delete writeBufferSsdEngine;

    return 0;
}
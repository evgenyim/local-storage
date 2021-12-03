//
// Created by evgeny on 20.11.2021.
//

#ifndef LOCAL_STORAGE_STORAGE_H
#define LOCAL_STORAGE_STORAGE_H

#include <unistd.h>
#include <fcntl.h>
#include <cstdio>
#include <fstream>


class PersistentStorage {
private:
    std::unordered_map<std::string, uint64_t> _storage;
    int fd;
    FILE *file;
    std::string filename = "data.log";

    bool write_to_disk(const std::string &key, uint64_t value) {
        std::string to_write = key + " " + std::to_string(value) + " ";
        int res = fprintf(file, "%s", to_write.data());
        if (res == to_write.size()) {
            fflush(file);
//            fsync(fd);
            return true;
        }
        return false;
    }

    void load_from_disk() {
        std::fstream f(filename);
        std::string key;
        uint64_t value;
        while (f >> key >> value)
            _storage[key] = value;
        f.clear();
        for (auto &it : _storage)
            f << it.first << " " << it.second << " ";
        f.close();
    }

public:
    PersistentStorage() {
        load_from_disk();
        file = fopen(filename.c_str(), "a");
        fd = fileno(file);
        std::cout << "fd" << fd << std::endl;
    }
    ~PersistentStorage() {
        fclose(file);
    }

    void put(const std::string &key, uint64_t value) {
        if (write_to_disk(key, value))
            _storage[key] = value;
    }

    uint64_t  *find(const std::string &key) {
        auto it = _storage.find(key);
        if (it != _storage.end())
            return &it->second;
        return nullptr;
    }
};



#endif //LOCAL_STORAGE_STORAGE_H

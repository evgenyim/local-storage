//
// Created by evgeny on 20.11.2021.
//

#ifndef LOCAL_STORAGE_STORAGE_H
#define LOCAL_STORAGE_STORAGE_H

#include <unistd.h>
#include <fcntl.h>
#include <cstdio>
#include <fstream>
#include <queue>


class PersistentStorage {
private:
    std::unordered_map<std::string, uint64_t> _storage;
    int fd;
    FILE *file;
    std::string filename = "data.log";
    std::queue<std::pair<std::string, uint64_t>> not_confirmed;
    std::mutex mutex;

    bool write_to_disk(const std::string &key, uint64_t value) {
        std::string to_write = key + " " + std::to_string(value) + " ";
        int res = fprintf(file, "%s", to_write.data());
        if (res == to_write.size()) {
            fflush(file);
            std::lock_guard<std::mutex> g(mutex);
            not_confirmed.push({key, value});
            return true;
        }
        return false;
    }

    void load_from_disk() {
        std::ifstream f(filename);
        std::string key;
        uint64_t value;
        while (f >> key >> value)
            _storage[key] = value;
        f.close();
        std::ofstream f_rewrite(filename);
        for (auto &it : _storage)
            f_rewrite << it.first << " " << it.second << " ";
        f_rewrite.close();
    }

public:
    PersistentStorage(std::string filename="data.log") {
        filename = filename;
        load_from_disk();
        file = fopen(filename.c_str(), "a");
        fd = fileno(file);
    }
    ~PersistentStorage() {
        fclose(file);
        std::ofstream f_rewrite(filename);
        for (auto &it : _storage)
            f_rewrite << it.first << " " << it.second << " ";
        f_rewrite.close();
    }

    void put(const std::string &key, uint64_t value) {
        write_to_disk(key, value);
    }

    uint64_t *find(const std::string &key) {
        std::lock_guard<std::mutex> g(mutex);
        auto it = _storage.find(key);
        if (it != _storage.end())
            return &it->second;
        return nullptr;
    }

    void sync() {
        fsync(fd);
        std::lock_guard<std::mutex> g(mutex);
        while(!not_confirmed.empty()) {
            _storage[not_confirmed.front().first] = not_confirmed.front().second;
            not_confirmed.pop();
        }
    }
};

class Storage {
public:
    Storage() {
        std::cout << "Loading from disk" << std::endl;
        load_from_disk();
        std::cout << "Ready" << std::endl;
    }

    ~Storage() {
        fsync(fd);
        fclose(file);
    }

    void put(const std::string &key, const std::string &value) {
        write_to_log(key, value);
    }

    bool get(const std::string &key, std::string *value) {
        uint64_t *offset = map.find(key);
        if (offset == nullptr)
            return false;
        *value = get_from_log(*offset);
        return true;
    }

    void sync() {
        map.sync();
        fsync(fd);
    }

private:
    PersistentStorage map;
    FILE *file = nullptr;
    int fd;
    int next_file_id = 0;
    std::string filename = "str_data_";
    int max_size = 1024 * 1024 * 32;

    void load_from_disk() {
        int file_id = -1;
        while (FILE *f = fopen((filename + std::to_string(++file_id)).c_str(), "r")) {
            fclose(f);
        }
        next_file_id = file_id;
        open_new_file();
        for (int i = 0; i < file_id; i++) {
            FILE *f = fopen((filename + std::to_string(i)).c_str(), "r");
            int size;
            int offset = 0;
            while (true) {
                int cur_offset = 0;
                size_t s = fread(&size, sizeof(uint64_t), 1, f);
                if (s == 0)
                    break;
                cur_offset += sizeof(uint64_t);
                std::string key(size, 0);
                fread(&key[0], sizeof(char), size, f);
                cur_offset += size;
                fread(&size, sizeof(uint64_t), 1, f);
                cur_offset += sizeof(uint64_t);
                std::string value(size, 0);
                fread(&value[0], sizeof(char), size, f);
                cur_offset += size;
                uint64_t *saved = map.find(key);
                if (saved == nullptr) {
//                    std::cerr << "Error while loading. Logs corrupted" << std::endl;
                    continue;
                }
                if (*saved == file_id * max_size + offset) {
                    write_to_log(key, value);
                    map.put(key, (next_file_id - 1) * max_size + offset);
                }
                offset += cur_offset;
                if (offset >= max_size)
                    break;
            }
        }
    }

    void write_to_log(const std::string &key, const std::string &value) {
        uint64_t key_size = key.size();
        uint64_t value_size = value.size();
        uint64_t offset = ftell(file);
        if (offset >= max_size) {
            open_new_file();
            offset = 0;
        }
        fwrite(&key_size, sizeof(uint64_t), 1, file);
        fwrite(key.c_str(), sizeof(char), key_size, file);
        fwrite(&value_size, sizeof(uint64_t), 1, file);
        fwrite(value.c_str(), sizeof(char), value_size, file);
        fflush(file);
        map.put(key, (next_file_id - 1) * max_size + offset);
    }

    void open_new_file() {
        if (file != nullptr)
            fclose(file);
        file = fopen((filename + std::to_string(next_file_id++)).c_str(), "w+b");
        fd = fileno(file);
    }

    std::string get_from_log(uint64_t offset) {
        uint64_t file_id = offset / max_size;
        uint64_t file_offset = offset % max_size;
        FILE *f = file;
        if (file_id + 1 != next_file_id)
            f = fopen((filename + std::to_string(file_id)).c_str(), "r");
        uint64_t key_size;
        fseek(f, file_offset, SEEK_SET);
        fread(&key_size, sizeof(uint64_t), 1, f);
        std::string key(key_size, 0);
        fread(&key[0], sizeof(char), key_size, f);
        uint64_t value_size;
        fread(&value_size, sizeof(uint64_t), 1, f);
        std::string value(value_size, 0);
        fread(&value[0], sizeof(char), value_size, f);
        fseek(f, 0, SEEK_END);
        if (file_id + 1 != next_file_id)
            fclose(f);
        return value;
    }

};

#endif //LOCAL_STORAGE_STORAGE_H

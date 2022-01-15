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
public:
    PersistentStorage(const std::string &filename="data.log") {
        this->filename = filename;
        load_from_disk();
        file = fopen(filename.c_str(), "a");
        fd = fileno(file);
    }
    ~PersistentStorage() {
        on_shutdown();
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
        if (fsync(fd) == -1)
            abort();
        std::lock_guard<std::mutex> g(mutex);
        while(!not_confirmed.empty()) {
            _storage[not_confirmed.front().first] = not_confirmed.front().second;
            not_confirmed.pop();
        }
    }

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

    void on_shutdown() {
        fclose(file);
        std::ofstream f_rewrite(filename);
        for (auto &it : _storage)
            f_rewrite << it.first << " " << it.second << " ";
        f_rewrite.close();
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
        on_shutdown();
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
        std::lock_guard<std::mutex> g(mutex);
        if (fsync(fd) == -1)
            abort();
    }


private:
    PersistentStorage map;
    FILE *file = nullptr;
    int fd;
    int first_file_id = 0, next_file_id = 0;
    std::string filename = "str_data_";
    std::string config_filename = "config";
    uint64_t max_size = 1024 * 1024 * 64;
    std::mutex mutex;

    void load_from_disk() {
        load_config();
        open_new_file();
        int first_new_file_id = next_file_id;
        for (int i = first_file_id; i < first_new_file_id; i++) {
            FILE *f = fopen((filename + std::to_string(i)).c_str(), "r");
            int offset = 0;
            std::string key, value;
            while (true) {
                int cur_offset = 0;
                auto p = get_key_value(f);
                if (p.first.empty())
                    break;
                std::tie(key, value) = p;

                cur_offset += 2 * sizeof(uint64_t) + key.size() + value.size();
                uint64_t *saved_offset = map.find(key);
                if (saved_offset == nullptr) {
                    continue;
                }
                if (*saved_offset == i * max_size + offset) {
                    write_to_log(key, value);
                }
                offset += cur_offset;
                if (offset >= max_size)
                    break;
            }
            fclose(f);
            if (i + 1 < first_new_file_id) {
                std::remove((filename + std::to_string(i)).c_str());
            }
        }
        first_file_id = first_new_file_id - 1;
        save_config();
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
//        std::cout << next_file_id - 1 << ", " << max_size << ", " << offset << std::endl;
        map.put(key, (next_file_id - 1) * max_size + offset);
    }

    void open_new_file() {
        if (file != nullptr)
            fclose(file);
        file = fopen((filename + std::to_string(next_file_id++)).c_str(), "w+b");
        std::lock_guard<std::mutex> g(mutex);
        fd = fileno(file);
        save_config();
    }

    std::string get_from_log(uint64_t offset) {
        uint64_t file_id = offset / max_size;
        uint64_t file_offset = offset % max_size;
        FILE *f = file;
        if (file_id + 1 != next_file_id)
            f = fopen((filename + std::to_string(file_id)).c_str(), "r");
        fseek(f, file_offset, SEEK_SET);
        std::string key, value;
        std::tie(key, value) = get_key_value(f);
        fseek(f, 0, SEEK_END);
        if (file_id + 1 != next_file_id)
            fclose(f);
        return value;
    }

    std::pair<std::string , std::string> get_key_value(FILE *f) {
        size_t read_bytes;
        uint64_t key_size;
        read_bytes = fread(&key_size, sizeof(uint64_t), 1, f);
        if (read_bytes == 0)
            return std::make_pair(std::string(), std::string());

        std::string key(key_size, 0);
        fread(&key[0], sizeof(char), key_size, f);
        uint64_t value_size;
        fread(&value_size, sizeof(uint64_t), 1, f);
        std::string value(value_size, 0);
        fread(&value[0], sizeof(char), value_size, f);
        return std::make_pair(key, value);
    }

    void load_config() {
        std::ifstream f(config_filename);
        if (!(f >> first_file_id) || !(f >> next_file_id)) {
            f.close();
            first_file_id = next_file_id = 0;
            save_config();
        }
    }

    void save_config() {
        std::ofstream f(config_filename);
        f << first_file_id << "\n" << next_file_id << "\n";
    }

    void on_shutdown() {
        std::lock_guard<std::mutex> g(mutex);
        if (fsync(fd) == -1)
            abort();
        fclose(file);
    }

};

#endif //LOCAL_STORAGE_STORAGE_H

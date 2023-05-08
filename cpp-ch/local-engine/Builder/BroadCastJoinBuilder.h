#pragma once
#include <Shuffle/ShuffleReader.h>
#include <Storages/StorageJoinFromReadBuffer.h>

namespace local_engine
{
template <typename T>
class StorageJoinWrapper{
public:
    explicit StorageJoinWrapper()= default;;

    void build(
        const std::string & key,
        jobject input,
        size_t io_buffer_size,
        const DB::Names & key_names_,
        DB::JoinKind kind_,
        DB::JoinStrictness strictness_,
        const DB::ColumnsDescription & columns_);

    std::shared_ptr<T> get() const { return storage_join; }

private:
    std::shared_ptr<T> storage_join;
    std::mutex build_lock_mutex;

};


class BroadCastJoinBuilder
{
public:
    static std::shared_ptr<StorageJoinWrapper<StorageJoinFromReadBuffer>> buildJoin(
        const std::string & key,
        jobject input,
        size_t io_buffer_size,
        const DB::Names & key_names_,
        DB::JoinKind kind_,
        DB::JoinStrictness strictness_,
        const DB::ColumnsDescription & columns_);

    static std::shared_ptr<StorageJoinWrapper<StorageJoinFromReadBuffer>> buildJoin(
        const std::string & key,
        jobject input,
        size_t io_buffer_size,
        const std::string & join_keys,
        const std::string & join_type,
        const std::string & named_struct);

    static void cleanBuildHashTable(const std::string & hash_table_id, jlong instance);


    static std::shared_ptr<StorageJoinFromReadBuffer> getJoin(const std::string & key);

    static void clean();

private:
    static std::unordered_map<std::string, std::shared_ptr<StorageJoinWrapper<StorageJoinFromReadBuffer>>> storage_join_map;
    static std::mutex join_lock_mutex;
};


}

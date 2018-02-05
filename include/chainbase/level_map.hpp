#pragma once

#include <fstream>

#include <leveldb/cache.h>
#include <leveldb/comparator.h>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include <chainbase/exception.hpp>
#include <chainbase/upgrade_leveldb.hpp>

#include <boost/filesystem.hpp>
#include <boost/optional.hpp>
#include <array>
#include "datastream_back.hpp"

namespace chainbase {
    namespace db {

        namespace ldb = leveldb;

        /**
         *  @brief implements a high-level API on top of Level DB that stores items using fc::raw / reflection
         */
        template<typename Key, typename Value>
        class level_map {
        public:
            void open(const boost::filesystem::path &dir, bool create = true, size_t cache_size = 0) {
                try {
                    assert(!is_open());//, "Database is already open!");

                    ldb::Options opts;
                    opts.comparator = &_comparer;
                    opts.create_if_missing = create;
                    opts.max_open_files = 64;
                    opts.compression = leveldb::kNoCompression;

                    if (cache_size > 0) {
                        opts.write_buffer_size = cache_size / 4; // up to two write buffers may be held in memory simultaneously
                        _cache.reset(leveldb::NewLRUCache(cache_size / 2));
                        opts.block_cache = _cache.get();
                    }

                    if (ldb::kMajorVersion > 1 || (leveldb::kMajorVersion == 1 && leveldb::kMinorVersion >= 16)) {
                        // LevelDB versions before 1.16 consider short writes to be corruption. Only trigger error
                        // on corruption in later versions.
                        opts.paranoid_checks = true;
                    }

                    _read_options.verify_checksums = true;
                    _iter_options.verify_checksums = true;
                    _iter_options.fill_cache = false;
                    _sync_options.sync = true;

                    // Given path must exist to succeed toNativeAnsiPath
                    boost::filesystem::create_directories(dir);
                    std::string ldbPath = dir.generic_string();

                    ldb::DB *ndb = nullptr;
                    const auto ntrxstat = ldb::DB::Open(opts, ldbPath.c_str(), &ndb);
                    if (!ntrxstat.ok()) {
                        //elog("Failure opening database: ${db}\nStatus: ${msg}", ("db", dir)("msg", ntrxstat.ToString()));
                        //FC_THROW_EXCEPTION(level_map_open_failure, "Failure opening database: ${db}\nStatus: ${msg}", ("db", dir)("msg", ntrxstat.ToString()));
                        throw level_map_failure();
                    }
                    _db.reset(ndb);

                    try_upgrade_db(dir, ndb, "std::vector<char>"/*typename Value::name*/, sizeof(Value));
                } catch (...){

                }
                //FC_CAPTURE_AND_RETHROW((dir)(create)(cache_size))
            }

            bool is_open() const {
                return !!_db;
            }

            void close() {
                _db.reset();
                _cache.reset();
            }

            boost::optional<Value> fetch_optional(const Key &k) const {
                try {
                    assert(is_open());//, "Database is not open!");

                    auto itr = find(k);
                    if (itr.valid())
                        return itr.value();
                    return boost::optional<Value>();
                }catch (...){

                }
                //FC_RETHROW_EXCEPTIONS(warn, "")
            }

            Value fetch(const Key &k) {
                try {
                    assert(is_open());//, "Database is not open!");

                    std::vector<char> kslice = k.pack();
                    ldb::Slice ks(kslice.data(), kslice.size());
                    std::string value;
                    auto status = _db->Get(_read_options, ks, &value);
                    if (status.IsNotFound()) {
                        //FC_THROW_EXCEPTION(fc::key_not_found_exception, "unable to find key ${key}", ("key", k));
                        throw key_not_found_exception();
                    }
                    if (!status.ok()) {
                        throw level_map_failure();//, "database error: ${msg}", ("msg", status.ToString()));
                    }
                    datastream<const char *> ds(value.c_str(), value.size());
                    Value tmp;
                    fc::raw::unpack(ds, tmp);
                    return tmp;
                } catch (...){

                }
                //FC_RETHROW_EXCEPTIONS(warn, "failure fetching key ${key}", ("key", k));
            }

            class iterator {
            public:
                iterator() {}

                bool valid() const {
                    return _it && _it->Valid();
                }

                Key key() const {
                    Key tmp_key;
                    datastream<const char *> ds2(_it->key().data(), _it->key().size());
                    fc::raw::unpack(ds2, tmp_key);
                    return tmp_key;
                }

                Value value() const {
                    Value tmp_val;
                    datastream<const char *> ds(_it->value().data(), _it->value().size());
                    fc::raw::unpack(ds, tmp_val);
                    return tmp_val;
                }

                iterator &operator++() {
                    _it->Next();
                    return *this;
                }

                iterator &operator--() {
                    _it->Prev();
                    return *this;
                }

            protected:
                friend class level_map;

                iterator(ldb::Iterator *it) : _it(it) {}

                std::shared_ptr<ldb::Iterator> _it;
            };

            iterator begin() const {
                try {
                    assert(is_open());//, "Database is not open!");

                    iterator itr(_db->NewIterator(_iter_options));
                    itr._it->SeekToFirst();

                    if (itr._it->status().IsNotFound()) {
                        throw key_not_found_exception();
                        //FC_THROW_EXCEPTION(fc::key_not_found_exception, "");
                    }
                    if (!itr._it->status().ok()) {
                        throw level_map_failure();
                        //FC_THROW_EXCEPTION(level_map_failure, "database error: ${msg}", ("msg", itr._it->status().ToString()));
                    }

                    if (itr.valid()) {
                        return itr;
                    }
                    return iterator();
                } catch (...){

                }
                //FC_RETHROW_EXCEPTIONS(warn, "error seeking to first")
            }

            iterator find(const Key &key) const {
                try {
                    assert(is_open());//, "Database is not open!");

                    ldb::Slice key_slice;

                    /** avoid dynamic memory allocation at this step if possible, most
                     * keys should be relatively small in size and not require dynamic
                     * memory allocation to seralize the key.
                     */
                    std::array<char, 256 + sizeof(Key)> stack_buffer;

                    size_t pack_size = key.pack_size();
                    if (pack_size <= stack_buffer.size()) {
                        datastream<char *> ds(stack_buffer.data, stack_buffer.size());
                        fc::raw::pack(ds, key);
                        key_slice = ldb::Slice(stack_buffer.data, pack_size);
                    } else {
                        auto kslice = key.pack();
                        key_slice = ldb::Slice(kslice.data(), kslice.size());
                    }

                    iterator itr(_db->NewIterator(_iter_options));
                    itr._it->Seek(key_slice);
                    if (itr.valid() && itr.key() == key) {
                        return itr;
                    }
                    return iterator();
                } catch (...){

                }
                //FC_RETHROW_EXCEPTIONS(warn, "error finding ${key}", ("key", key))
            }

            iterator lower_bound(const Key &key) const {
                try {
                    assert(is_open());//, "Database is not open!");

                    std::vector<char> kslice = key.pack();
                    ldb::Slice key_slice(kslice.data(), kslice.size());

                    iterator itr(_db->NewIterator(_iter_options));
                    itr._it->Seek(key_slice);
                    return itr;
                } catch (...){

                }
                //FC_RETHROW_EXCEPTIONS(warn, "error finding ${key}", ("key", key))
            }

            iterator last() const {
                try {
                    assert(is_open());//, "Database is not open!");

                    iterator itr(_db->NewIterator(_iter_options));
                    itr._it->SeekToLast();
                    return itr;
                }catch (...){

                }
                //FC_RETHROW_EXCEPTIONS(warn, "error finding last")
            }

            bool last(Key &k) {
                try {
                    assert(is_open());//, "Database is not open!");

                    std::unique_ptr<ldb::Iterator> it(_db->NewIterator(_iter_options));
                    assert(it != nullptr);
                    it->SeekToLast();
                    if (!it->Valid()) {
                        return false;
                    }
                    datastream<const char *> ds2(it->key().data(), it->key().size());
                    fc::raw::unpack(ds2, k);
                    return true;
                } catch (...){

                }
                //FC_RETHROW_EXCEPTIONS(warn, "error reading last item from database");
            }

            bool last(Key &k, Value &v) {
                try {
                    assert(is_open());//, "Database is not open!");

                    std::unique_ptr<ldb::Iterator> it(_db->NewIterator(_iter_options));
                    assert(it != nullptr);
                    it->SeekToLast();
                    if (!it->Valid()) {
                        return false;
                    }
                    datastream<const char *> ds(it->value().data(), it->value().size());
                    fc::raw::unpack(ds, v);

                    datastream<const char *> ds2(it->key().data(), it->key().size());
                    fc::raw::unpack(ds2, k);
                    return true;
                } catch (...){

                }
                //FC_RETHROW_EXCEPTIONS(warn, "error reading last item from database");
            }

            /** this class allows batched, atomic database writes.
             *  usage:
             *  {
             *    write_batch batch = _db.create_batch();
             *    batch.store(key1, value1);
             *    batch.store(key2, value2);
             *  }
             *  when the batch goes out of scope, the operations are commited to the database
             */
            class write_batch {
            private:
                leveldb::WriteBatch _batch;
                level_map *_map = nullptr;
                leveldb::WriteOptions _write_options;

                friend class level_map;

                write_batch(level_map *map, bool sync = false) : _map(map) {
                    _write_options.sync = sync;
                }

            public:
                ~write_batch() {
                    try {
                        commit();
                    }
                    catch (const fc::canceled_exception &) {
                        throw;
                    }
                    catch (const fc::exception &) {
                        // we're in a destructor, nothing we can do...
                    }
                }

                void commit() {
                    try {
                        assert(_map->is_open());//, "Database is not open!");

                        ldb::Status status = _map->_db->Write(_write_options, &_batch);
                        if (!status.ok())
                            throw level_map_failure();
                            //FC_THROW_EXCEPTION(level_map_failure, "database error while applying batch: ${msg}", ("msg", status.ToString()));
                        _batch.Clear();
                    } catch (...){

                    }
                    //FC_RETHROW_EXCEPTIONS(warn, "error applying batch");
                }

                void abort() {
                    _batch.Clear();
                }

                void store(const Key &k, const Value &v) {
                    std::vector<char> kslice = k.pack();
                    ldb::Slice ks(kslice.data(), kslice.size());

                    auto vec = v.pack();
                    ldb::Slice vs(vec.data(), vec.size());

                    _batch.Put(ks, vs);
                }

                void remove(const Key &k) {
                    std::vector<char> kslice = k.pack();
                    ldb::Slice ks(kslice.data(), kslice.size());
                    _batch.Delete(ks);
                }
            };

            write_batch create_batch(bool sync = false) {
                assert(is_open());//, "Database is not open!");
                return write_batch(this, sync);
            }

            void store(const Key &k, const Value &v, bool sync = false) {
                try {
                    assert(is_open());//, "Database is not open!");

                    std::vector<char> kslice = k.pack();
                    ldb::Slice ks(kslice.data(), kslice.size());

                    //auto vec = v.pack();
                    ldb::Slice vs(v.data(), v.size());

                    auto status = _db->Put(sync ? _sync_options : _write_options, ks, vs);
                    if (!status.ok()) {
                        throw level_map_failure();
                        //FC_THROW_EXCEPTION(level_map_failure, "database error: ${msg}", ("msg", status.ToString()));
                    }
                }catch (...){

                }
                //FC_RETHROW_EXCEPTIONS(warn, "error storing ${key} = ${value}", ("key", k)("value", v));
            }

            void remove(const Key &k, bool sync = false) {
                try {
                    assert(is_open());//, "Database is not open!");

                    std::vector<char> kslice = k.pack();
                    ldb::Slice ks(kslice.data(), kslice.size());
                    auto status = _db->Delete(sync ? _sync_options : _write_options, ks);
                    if (!status.ok()) {
                        throw level_map_failure();
                        //FC_THROW_EXCEPTION(level_map_failure, "database error: ${msg}", ("msg", status.ToString()));
                    }
                } catch (...){

                }
                //FC_RETHROW_EXCEPTIONS(warn, "error removing ${key}", ("key", k));
            }

            void export_to_json(const boost::filesystem::path &path) const {
                throw std::exception();
                /*
                try {
                    assert(is_open());//, "Database is not open!");
                    assert(!boost::filesystem::exists(path));

                    std::ofstream fs(path.string());
                    fs.write("[\n", 2);

                    auto iter = begin();
                    while (iter.valid()) {
                        auto str = fc::json::to_pretty_string(std::make_pair(iter.key(), iter.value()));
                        if ((++iter).valid()) str += ",";
                        str += "\n";
                        fs.write(str.c_str(), str.size());
                    }

                    fs.write("]", 1);
                } catch (...){

                }
                //FC_CAPTURE_AND_RETHROW((path))
                 */
            }

            // note: this loops through all the items in the database, so it's not exactly fast.  it's intended for debugging, nothing else.
            size_t size() const {
                assert(is_open());//, "Database is not open!");

                iterator it = begin();
                size_t count = 0;
                while (it.valid()) {
                    ++count;
                    ++it;
                }
                return count;
            }

        private:
            class key_compare : public leveldb::Comparator {
            public:
                int Compare(const leveldb::Slice &a, const leveldb::Slice &b) const {
                    Key ak, bk;
                    datastream<const char *> dsa(a.data(), a.size());
                    fc::raw::unpack(dsa, ak);
                    datastream<const char *> dsb(b.data(), b.size());
                    fc::raw::unpack(dsb, bk);

                    if (ak < bk) return -1;
                    if (ak == bk) return 0;
                    return 1;
                }

                const char *Name() const { return "key_compare"; }

                void FindShortestSeparator(std::string *, const leveldb::Slice &) const {}

                void FindShortSuccessor(std::string *) const {};
            };

            std::unique_ptr<leveldb::DB> _db;
            std::unique_ptr<leveldb::Cache> _cache;
            key_compare _comparer;

            ldb::ReadOptions _read_options;
            ldb::ReadOptions _iter_options;
            ldb::WriteOptions _write_options;
            ldb::WriteOptions _sync_options;
        };

    }
} // bts::db
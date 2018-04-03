#include <chainbase/chainbase.hpp>
#include <boost/array.hpp>

#include <iostream>

namespace chainbase {


    struct environment_check {
        environment_check() {
            memset(&compiler_version, 0, sizeof(compiler_version));
            memcpy(&compiler_version, __VERSION__, std::min<size_t>(strlen(__VERSION__), 256));
#ifndef NDEBUG
            debug = true;
#endif
#ifdef __APPLE__
            apple = true;
#endif
#ifdef WIN32
            windows = true;
#endif
        }

        friend bool operator==(const environment_check &a, const environment_check &b) {
            return std::make_tuple(a.compiler_version, a.debug, a.apple, a.windows)
                   ==
                   std::make_tuple(b.compiler_version, b.debug, b.apple, b.windows);
        }

        boost::array<char, 256> compiler_version;
        bool debug = false;
        bool apple = false;
        bool windows = false;
    };

    void database::open(const boost::filesystem::path &dir, uint32_t flags, size_t shared_file_size) {

        bool write = flags & database::read_write;

        if (!boost::filesystem::exists(dir)) {
            if (!write)
                BOOST_THROW_EXCEPTION(std::runtime_error(
                        "database file not found at " + dir.native()));
        }

        boost::filesystem::create_directories(dir);
        if (_data_dir != dir) {
            close();
        }

        _data_dir = dir;
        auto abs_path = boost::filesystem::absolute(dir / "shared_memory.bin");

        if (boost::filesystem::exists(abs_path)) {
            if (write) {
                _file_size = boost::filesystem::file_size(abs_path);
                if (shared_file_size > _file_size) {
                    if (!boost::interprocess::managed_mapped_file::grow(
                            abs_path.generic_string().c_str(), shared_file_size - _file_size)
                    ) {
                        BOOST_THROW_EXCEPTION(std::runtime_error("could not grow database file to requested size."));
                    }
                    _file_size = shared_file_size;
                }

                _segment.reset(new boost::interprocess::managed_mapped_file(boost::interprocess::open_only,
                        abs_path.generic_string().c_str()
                ));
            } else {
                _segment.reset(new boost::interprocess::managed_mapped_file(boost::interprocess::open_read_only,
                        abs_path.generic_string().c_str()
                ));
                _read_only = true;
                _file_size = shared_file_size;
            }

            auto env = _segment->find<environment_check>("environment");
            if (!env.first || !(*env.first == environment_check())) {
                BOOST_THROW_EXCEPTION(std::runtime_error("database created by a different compiler, build, or operating system"));
            }
        } else {
            _segment.reset(new boost::interprocess::managed_mapped_file(boost::interprocess::create_only,
                    abs_path.generic_string().c_str(), shared_file_size
            ));
            _segment->find_or_construct<environment_check>("environment")();
            _file_size = shared_file_size;
        }

        abs_path = boost::filesystem::absolute(dir / "shared_memory.meta");

        if (boost::filesystem::exists(abs_path)) {
            _meta.reset(new boost::interprocess::managed_mapped_file(boost::interprocess::open_only, abs_path.generic_string().c_str()
            ));
        } else {
            _meta.reset(new boost::interprocess::managed_mapped_file(boost::interprocess::create_only,
                    abs_path.generic_string().c_str(),
                    sizeof(read_write_mutex_manager) * 2
            ));
        }

        if (write) {
            _flock = boost::interprocess::file_lock(abs_path.generic_string().c_str());
            if (!_flock.try_lock())
                BOOST_THROW_EXCEPTION(std::runtime_error("could not gain write access to the shared memory file"));
        }
    }

    void database::flush() {
        if (_segment) {
            _segment->flush();
        }
        if (_meta) {
            _meta->flush();
        }
    }

    void database::close() {
        _segment.reset();
        _meta.reset();
        _data_dir = boost::filesystem::path();
    }

    void database::wipe(const boost::filesystem::path &dir) {
        _segment.reset();
        _meta.reset();
        boost::filesystem::remove_all(dir / "shared_memory.bin");
        boost::filesystem::remove_all(dir / "shared_memory.meta");
        _data_dir = boost::filesystem::path();
        _index_list.clear();
        _index_map.clear();
        _index_types.clear();
    }

    void database::resize(size_t new_shared_file_size) {
        if (_undo_session_count) {
            BOOST_THROW_EXCEPTION(std::runtime_error("Cannot resize shared memory file while undo session is active"));
        }

        _segment.reset();
        _meta.reset();

        open(_data_dir, database::read_write, new_shared_file_size);

        _index_list.clear();
        _index_map.clear();

        for (auto &index_type: _index_types) {
            index_type->add_index(*this);
        }
    }

    void database::set_require_locking(bool enable_require_locking) {
#ifdef CHAINBASE_CHECK_LOCKING
        _enable_require_locking = enable_require_locking;
#endif
    }

#ifdef CHAINBASE_CHECK_LOCKING

    void database::require_lock_fail(const char *method, const char *lock_type, const char *tname) const {
        std::string err_msg =
                "database::" + std::string(method) + " require_" + std::string(lock_type) + "_lock() failed on type " +
                std::string(tname);
        std::cerr << err_msg << std::endl;
        BOOST_THROW_EXCEPTION(std::runtime_error( err_msg ));
    }

#endif

    void database::undo() {
        for (auto &item : _index_list) {
            item->undo();
        }
    }

    void database::squash() {
        for (auto &item : _index_list) {
            item->squash();
        }
    }

    void database::commit(int64_t revision) {
        for (auto &item : _index_list) {
            item->commit(revision);
        }
    }

    void database::undo_all() {
        for (auto &item : _index_list) {
            item->undo_all();
        }
    }

    database::session database::start_undo_session() {
        std::vector<boost::interprocess::unique_ptr<abstract_session>> sub_sessions;
        sub_sessions.reserve(_index_list.size());
        for (auto &item : _index_list) {
            sub_sessions.push_back(item->start_undo_session());
        }
        return session(std::move(sub_sessions), _undo_session_count);
    }

    void database::read_wait_micro(uint64_t value) {
        _read_wait_micro = value;
    }

    uint64_t database::read_wait_micro() const {
        return _read_wait_micro;
    }

    void database::max_read_wait_retries(uint32_t value) {
        _max_read_wait_retries = value;
    }

    uint32_t database::max_read_wait_retries() const {
        return _max_read_wait_retries;
    };

    void database::write_wait_micro(uint64_t value) {
        _write_wait_micro = value;
    }

    uint64_t database::write_wait_micro() const {
        return _write_wait_micro;
    }

    void database::max_write_wait_retries(uint32_t value) {
        _max_write_wait_retries = value;
    }

    uint32_t database::max_write_wait_retries() const {
        return _max_write_wait_retries;
    }

}  // namespace chainbase



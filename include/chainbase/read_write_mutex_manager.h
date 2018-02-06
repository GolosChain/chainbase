#pragma once

#include <boost/thread.hpp>
#ifndef CHAINBASE_NUM_RW_LOCKS
#define CHAINBASE_NUM_RW_LOCKS 10
#endif

using read_write_mutex = boost::shared_mutex;
using read_lock        = boost::shared_lock<read_write_mutex>;
using write_lock       = boost::unique_lock<read_write_mutex>;


     /**
     * The code we want to implement is this:
     *
     * ++target; try { ... } finally { --target }
     *
     * In C++ the only way to implement finally is to create a class
     * with a destructor, so that's what we do here.
     */
class int_incrementer {
public:
    int_incrementer(int32_t &target) : _target(target) {
        ++_target;
    }

    ~int_incrementer() {
        --_target;
    }

    int32_t get() const {
        return _target;
    }

private:
    int32_t &_target;
};


class read_write_mutex_manager {
public:
    read_write_mutex_manager() {
        _current_lock = 0;
    }

    ~read_write_mutex_manager() {
    }

    void next_lock() {
        _current_lock++;
        new(&_locks[_current_lock % CHAINBASE_NUM_RW_LOCKS]) read_write_mutex();
    }

    read_write_mutex &current_lock() {
        return _locks[_current_lock % CHAINBASE_NUM_RW_LOCKS];
    }

    uint32_t current_lock_num() {
        return _current_lock;
    }

private:
    std::array<read_write_mutex, CHAINBASE_NUM_RW_LOCKS> _locks;
    std::atomic<uint32_t> _current_lock;
};
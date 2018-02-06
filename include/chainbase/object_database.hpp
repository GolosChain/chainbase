#pragma once

#include <cassert>

#include <map>
#include <vector>
#include <iostream>

#include <chainbase/object.hpp>
#include <chainbase/index.hpp>
#include <chainbase/undo_database.hpp>
#include <chainbase/read_write_mutex_manager.h>


#ifdef CHAINBASE_CHECK_LOCKING
#define CHAINBASE_REQUIRE_READ_LOCK(m, t) require_read_lock(m, typeid(t).name())
#define CHAINBASE_REQUIRE_WRITE_LOCK(m, t) require_write_lock(m, typeid(t).name())
#else
#define CHAINBASE_REQUIRE_READ_LOCK(m, t)
#define CHAINBASE_REQUIRE_WRITE_LOCK(m, t)
#endif

namespace chainbase {
    namespace db {
        using std::vector;
        using std::shared_ptr;
        using std::unique_ptr;

        /**
         *   @class object_database
         *   @brief maintains a set of indexed objects that can be modified with multi-level rollback support
         */
        class object_database {
        public:

#ifdef CHAINBASE_CHECK_LOCKING

            void database::require_lock_fail(const char *method, const char *lock_type, const char *tname) const {
            std::string err_msg = "database::" + std::string(method) + " require_" + std::string(lock_type) + "_lock() failed on type " + std::string(tname);
            std::cerr << err_msg << std::endl;
            BOOST_THROW_EXCEPTION(std::runtime_error( err_msg ));
        }

            void require_read_lock(const char *method, const char *tname) const {
                if (BOOST_UNLIKELY(_enable_require_locking & _read_only & (_read_lock_count <= 0))) {
                    require_lock_fail(method, "read", tname);
                }
            }

            void require_write_lock(const char *method, const char *tname) {
                if (BOOST_UNLIKELY(_enable_require_locking & (_write_lock_count <= 0))) {
                    require_lock_fail(method, "write", tname);
                }
            }

#endif


            template<typename Lambda>
            auto with_read_lock(Lambda &&callback, uint64_t wait_micro = 1000000) const -> decltype((*(Lambda * )nullptr)()) {
                read_lock lock(_rw_manager->current_lock(), boost::defer_lock_t());
#ifdef CHAINBASE_CHECK_LOCKING
                BOOST_ATTRIBUTE_UNUSED
            int_incrementer ii(_read_lock_count);
#endif

                if (!wait_micro) {
                    lock.lock();
                } else {

                    if (!lock.timed_lock(boost::posix_time::microsec_clock::universal_time() + boost::posix_time::microseconds(wait_micro)))
                        BOOST_THROW_EXCEPTION(std::runtime_error("unable to acquire lock"));
                }

                return callback();
            }

            template<typename Lambda>
            auto with_write_lock(Lambda &&callback, uint64_t wait_micro = 1000000) -> decltype((*(Lambda * )nullptr)()) {
                if (_read_only)
                    BOOST_THROW_EXCEPTION(std::logic_error("cannot acquire write lock on read-only process"));

                write_lock lock(_rw_manager->current_lock(), boost::defer_lock_t());
#ifdef CHAINBASE_CHECK_LOCKING
                BOOST_ATTRIBUTE_UNUSED
            int_incrementer ii(_write_lock_count);
#endif

                if (!wait_micro) {
                    lock.lock();
                } else {
                    while (!lock.timed_lock(boost::posix_time::microsec_clock::universal_time() + boost::posix_time::microseconds(wait_micro))) {
                        _rw_manager->next_lock();
                        std::cerr << "Lock timeout, moving to lock " << _rw_manager->current_lock_num() << std::endl;
                        lock = write_lock(_rw_manager->current_lock(), boost::defer_lock_t());
                    }
                }

                return callback();
            }

            object_database();

            ~object_database();

            void reset_indexes();

            void open(const boost::filesystem::path &data_dir);

            /**
             * Saves the complete state of the object_database to disk, this could take a while
             */
            void flush();

            void wipe(const boost::filesystem::path &data_dir); // remove from disk

            void close();

            template<typename T, typename F>
            const T &create(F &&constructor) {
                auto &idx = get_mutable_index<T>();
                return static_cast<const T &>( idx.create([&](object &o) {
                    assert(dynamic_cast<T *>(&o));
                    constructor(static_cast<T &>(o));
                }));
            }

            ///These methods are used to retrieve indexes on the object_database. All public index accessors are const-access only.
            /// @{
            template<typename IndexType>
            const IndexType &get_index_type() const {
                static_assert(std::is_base_of<index, IndexType>::value, "Type must be an index type");
                return static_cast<const IndexType &>( get_index(IndexType::object_type::space_id,
                                                                 IndexType::object_type::type_id));
            }

            template<typename T>
            const index &get_index() const { return get_index(T::space_id, T::type_id); }

            const index &get_index(uint8_t space_id, uint8_t type_id) const;

            const index &get_index(object_id_type id) const { return get_index(id.space(), id.type()); }
            /// @}

            const object &get_object(object_id_type id) const;

            const object *find_object(object_id_type id) const;

            /// These methods are mutators of the object_database. You must use these methods to make changes to the object_database,
            /// in order to maintain proper undo history.
            ///@{

            const object &insert(object &&obj);

            void remove(const object &obj) { get_mutable_index(obj.id).remove(obj); }

            template<typename T, typename Lambda>
            void modify(const T &obj, const Lambda &m) {
                get_mutable_index(obj.id).modify(obj, m);
            }

            ///@}

            template<typename T>
            const T &get(object_id_type id) const {
                const object &obj = get_object(id);
                assert(nullptr != dynamic_cast<const T *>(&obj));
                return static_cast<const T &>(obj);
            }

            template<typename T>
            const T *find(object_id_type id) const {
                const object *obj = find_object(id);
                assert(!obj || nullptr != dynamic_cast<const T *>(obj));
                return static_cast<const T *>(obj);
            }

            template<uint8_t SpaceID, uint8_t TypeID, typename T>
            const T *find(object_id<SpaceID, TypeID, T> id) const { return find<T>(id); }

            template<uint8_t SpaceID, uint8_t TypeID, typename T>
            const T &get(object_id<SpaceID, TypeID, T> id) const { return get<T>(id); }

            template<typename IndexType>
            const IndexType *add_index() {
                typedef typename IndexType::object_type ObjectType;
                if (_index[ObjectType::space_id].size() <= ObjectType::type_id)
                    _index[ObjectType::space_id].resize(255);
                assert(!_index[ObjectType::space_id][ObjectType::type_id]);
                unique_ptr<index> indexptr(new IndexType(*this));
                _index[ObjectType::space_id][ObjectType::type_id] = std::move(indexptr);
                return static_cast<const IndexType *>(_index[ObjectType::space_id][ObjectType::type_id].get());
            }

            void pop_undo();

            boost::filesystem::path get_data_dir() const;

            /** public for testing purposes only... should be private in practice. */
            undo_database _undo_db;
        protected:
            template<typename IndexType>
            IndexType &get_mutable_index_type() {
                static_assert(std::is_base_of<index, IndexType>::value, "Type must be an index type");
                return static_cast<IndexType &>( get_mutable_index(IndexType::object_type::space_id, IndexType::object_type::type_id));
            }

            template<typename T>
            index &get_mutable_index() { return get_mutable_index(T::space_id, T::type_id); }

            index &get_mutable_index(object_id_type id) { return get_mutable_index(id.space(), id.type()); }

            index &get_mutable_index(uint8_t space_id, uint8_t type_id);

        private:

            friend class base_primary_index;

            friend class undo_database;

            void save_undo(const object &obj);

            void save_undo_add(const object &obj);

            void save_undo_remove(const object &obj);

            boost::filesystem::path _data_dir;
            vector<vector<unique_ptr<index> > > _index;
            db_type _object_id_to_object;

            read_write_mutex_manager *_rw_manager = nullptr;
            bool _read_only = false;
            int32_t _read_lock_count = 0;
            int32_t _write_lock_count = 0;
            bool _enable_require_locking = false;
        };

    }
} // chainbase::db



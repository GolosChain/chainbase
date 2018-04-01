#pragma once

#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/containers/set.hpp>
#include <boost/interprocess/containers/flat_map.hpp>
#include <boost/interprocess/containers/deque.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/smart_ptr/shared_ptr.hpp>
#include <boost/interprocess/smart_ptr/unique_ptr.hpp>
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/sync/interprocess_sharable_mutex.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include <boost/interprocess/sync/file_lock.hpp>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/key_extractors.hpp>

#include <boost/chrono.hpp>
#include <boost/config.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <boost/throw_exception.hpp>

#include <array>
#include <atomic>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <typeindex>
#include <typeinfo>

#ifndef CHAINBASE_NUM_RW_LOCKS
#define CHAINBASE_NUM_RW_LOCKS 10
#endif

#ifdef CHAINBASE_CHECK_LOCKING
#define CHAINBASE_REQUIRE_READ_LOCK(m, t) require_read_lock(m, typeid(t).name())
#define CHAINBASE_REQUIRE_WRITE_LOCK(m, t) require_write_lock(m, typeid(t).name())
#else
#define CHAINBASE_REQUIRE_READ_LOCK(m, t)
#define CHAINBASE_REQUIRE_WRITE_LOCK(m, t)
#endif

namespace chainbase {

    template<typename T> using allocator = boost::interprocess::allocator<T,
            boost::interprocess::managed_mapped_file::segment_manager>;

    typedef boost::interprocess::basic_string<char, std::char_traits<char>, allocator<char>> shared_string;

    template<typename T> using shared_vector = std::vector<T, allocator<T>>;

    struct strcmp_less {
        bool operator()(const shared_string &a, const shared_string &b) const {
            return less(a.c_str(), b.c_str());
        }

        bool operator()(const shared_string &a, const std::string &b) const {
            return less(a.c_str(), b.c_str());
        }

        bool operator()(const std::string &a, const shared_string &b) const {
            return less(a.c_str(), b.c_str());
        }

    private:
        inline bool less(const char *a, const char *b) const {
            return std::strcmp(a, b) < 0;
        }
    };

    typedef boost::shared_mutex read_write_mutex;
    typedef boost::shared_lock<read_write_mutex> read_lock;
    typedef boost::unique_lock<read_write_mutex> write_lock;

    /**
     *  Object ID type that includes the type of the object it references
     */
    template<typename T>
    class object_id {
    public:
        object_id(int64_t i = 0) : _id(i) {
        }

        object_id &operator++() {
            ++_id;
            return *this;
        }

        friend bool operator<(const object_id &a, const object_id &b) {
            return a._id < b._id;
        }

        friend bool operator>(const object_id &a, const object_id &b) {
            return a._id > b._id;
        }

        friend bool operator==(const object_id &a, const object_id &b) {
            return a._id == b._id;
        }

        friend bool operator!=(const object_id &a, const object_id &b) {
            return a._id != b._id;
        }

        int64_t _id = 0;
    };

    /**
     *  @defgroup objects Objects
     *
     *  The object is the fundamental building block of the database and
     *  is the level upon which undo/redo operations are performed.  Objects
     *  are used to track data and their relationships and provide an effecient
     *  means to find and update information.
     *
     *  Objects are assigned a unique and sequential object ID by the database within
     *  the id_space defined in the object.
     *
     *  All objects must be serializable via FC_REFLECT() and their content must be
     *  faithfully restored.   Additionally all objects must be copy-constructable and
     *  assignable in a relatively efficient manner.  In general this means that objects
     *  should only refer to other objects by ID and avoid expensive operations when
     *  they are copied, especially if they are modified frequently.
     *
     *  Additionally all objects may be annotated by plugins which wish to maintain
     *  additional information to an object.  There can be at most one annotation
     *  per id_space for each object.   An example of an annotation would be tracking
     *  extra data not required by validation such as the name and description of
     *  a user asset.  By carefully organizing how information is organized and
     *  tracked systems can minimize the workload to only that which is necessary
     *  to perform their function.
     *
     *
     *  @class object
     *  @ingroup objects
     *  @brief Base for all database objects
     *  @tparam TypeNumber TypeNumber must be unique for each object
     *  @tparam Derived Stored object type
     *  @tparam VersionNumber Stored object version number
     *
     *  Typical type_id space usage for 0xDEADBEEF for little-endian systems is:
     *  0xEF - Unused
     *  0xBE - Objects version identifier
     *  0xAD - Objects space identifier. For example plugins need to define object type IDs such that they do not conflict globally. If each plugin uses the upper 8 bits as a space identifier, with 0 being for chain, then the lower 8 bits are free for each plugin to define as they see fit. @file tags_plugin.hpp:41
     *  0xDE - Object identifier.
     *
     *  @note Do not use multiple inheritance with object because the code assumes
     *  a static_cast will work between object and derived types.
     */

    template<uint32_t TypeNumber, typename Derived, uint32_t VersionNumber = 1>
    class object {
    public:
        typedef uint16_t type_number_type;

        typedef object_id<Derived> id_type;

        static const uint32_t type_id = VersionNumber - 1 ? (VersionNumber << 16) + TypeNumber : TypeNumber;
        static const uint32_t version_number = VersionNumber;
    };

    /** this class is ment to be specified to enable lookup of index type by object type using
     * the SET_INDEX_TYPE macro.
     **/
    template<typename T>
    struct get_index_type {
    };

    /**
     *  This macro must be used at global scope and OBJECT_TYPE and INDEX_TYPE must be fully qualified
     */
#define CHAINBASE_SET_INDEX_TYPE(OBJECT_TYPE, INDEX_TYPE)  \
   namespace chainbase { template<> struct get_index_type<OBJECT_TYPE> { typedef INDEX_TYPE type; }; }

#define CHAINBASE_DEFAULT_CONSTRUCTOR(OBJECT_TYPE) \
   template<typename Constructor, typename Allocator> \
   OBJECT_TYPE( Constructor&& c, Allocator&&  ) { c(*this); }

    template<typename value_type>
    class undo_state {
    public:
        typedef typename value_type::id_type id_type;
        typedef allocator<std::pair<const id_type, value_type>> id_value_allocator_type;
        typedef allocator<id_type> id_allocator_type;

        template<typename T>
        undo_state(allocator<T> al)
                :old_values(id_value_allocator_type(al.get_segment_manager())),
                removed_values(id_value_allocator_type(al.get_segment_manager())),
                new_ids(id_allocator_type(al.get_segment_manager())) {
        }

        typedef boost::interprocess::map<id_type, value_type, std::less<id_type>,
                id_value_allocator_type> id_value_type_map;
        typedef boost::interprocess::set<id_type, std::less<id_type>, id_allocator_type> id_type_set;

        id_value_type_map old_values;
        id_value_type_map removed_values;
        id_type_set new_ids;
        id_type old_next_id = 0;
        int64_t revision = 0;
    };

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

//    template<typename MultiIndexType>
//    class secondary_index {
//    public:
//        typedef boost::interprocess::managed_mapped_file::segment_manager segment_manager_type;
//        typedef MultiIndexType index_type;
//        typedef typename index_type::value_type value_type;
//        typedef boost::interprocess::allocator<secondary_index<MultiIndexType>, segment_manager_type> allocator_type;
//        typedef boost::interprocess::deleter<secondary_index<MultiIndexType>, segment_manager_type> deleter_type;
//
//        virtual ~secondary_index() {
//        };
//
//        virtual void object_inserted(const value_type &obj) {
//        };
//
//        virtual void object_removed(const value_type &obj) {
//        };
//
//        virtual void about_to_modify(const value_type &before) {
//        };
//
//        virtual void object_modified(const value_type &after) {
//        };
//    };

    /**
     *  The value_type stored in the multiindex container must have a integer field with the name 'id'.  This will
     *  be the primary key and it will be assigned and managed by generic_index.
     *
     *  Additionally, the constructor for value_type must take an allocator
     */
    template<typename MultiIndexType>
    class generic_index {
    public:
        typedef boost::interprocess::managed_mapped_file::segment_manager segment_manager_type;
        typedef MultiIndexType index_type;
        typedef typename index_type::value_type value_type;
        typedef boost::interprocess::allocator<generic_index, segment_manager_type> allocator_type;
        typedef boost::interprocess::deleter<generic_index, segment_manager_type> deleter_type;
        typedef undo_state<value_type> undo_state_type;

        generic_index(allocator<value_type> a) : _stack(a), _indices(a),
                _size_of_value_type(sizeof(typename MultiIndexType::node_type)), _size_of_this(sizeof(*this))
//                  , _sindex(a)
        {
        }

        void validate() const {
            if (sizeof(typename MultiIndexType::node_type) != _size_of_value_type || sizeof(*this) != _size_of_this)
                BOOST_THROW_EXCEPTION(
                        std::runtime_error("content of memory does not match data expected by executable"));
        }

        /**
         * Construct a new element in the multi_index_container.
         * Set the ID to the next available ID, then increment _next_id and fire off on_create().
         */
        template<typename Constructor>
        const value_type &emplace(Constructor &&c) {
            auto new_id = _next_id;

            auto constructor = [&](value_type &v) {
                v.id = new_id;
                c(v);
            };

            auto insert_result = _indices.emplace(constructor, _indices.get_allocator());

            if (!insert_result.second) {
                BOOST_THROW_EXCEPTION(
                        std::logic_error("could not insert object, most likely a uniqueness constraint was violated"));
            }

            ++_next_id;
            on_create(*insert_result.first);

//            for (const auto &item : _sindex) {
//                item->object_inserted(*insert_result.first);
//            }

            return *insert_result.first;
        }

        template<typename Modifier>
        void modify(const value_type &obj, Modifier &&m) {
//            for (const auto &item : _sindex) {
//                item->about_to_modify(obj);
//            }

            on_modify(obj);

            auto ok = _indices.modify(_indices.iterator_to(obj), m);
            if (!ok)
                BOOST_THROW_EXCEPTION(
                        std::logic_error("Could not modify object, most likely a uniqueness constraint was violated"));

//            for (const auto &item : _sindex) {
//                item->object_modified(obj);
//            }
        }

        void remove(const value_type &obj) {
//            for (const auto &item : _sindex) {
//                item->object_removed(obj);
//            }
            on_remove(obj);
            _indices.erase(_indices.iterator_to(obj));
        }

        template<typename CompatibleKey>
        const value_type *find(CompatibleKey &&key) const {
            auto itr = _indices.find(std::forward<CompatibleKey>(key));
            if (itr != _indices.end()) {
                return &*itr;
            }
            return nullptr;
        }

        template<typename CompatibleKey>
        const value_type &get(CompatibleKey &&key) const {
            auto ptr = find(key);
            if (!ptr)
                BOOST_THROW_EXCEPTION(std::out_of_range("key not found"));
            return *ptr;
        }

        void inspect_objects(std::function<void(const value_type &)> inspector) const {
            for (const auto &ptr : _indices) {
                inspector(ptr);
            }
        }

        const index_type &indices() const {
            return _indices;
        }

        class session {
        public:
            session(session &&mv) : _index(mv._index), _apply(mv._apply) {
                mv._apply = false;
            }

            ~session() {
                if (_apply) {
                    _index.undo();
                }
            }

            /** leaves the UNDO state on the stack when session goes out of scope */
            void push() {
                _apply = false;
            }

            /** combines this session with the prior session */
            void squash() {
                if (_apply) {
                    _index.squash();
                }
                _apply = false;
            }

            void undo() {
                if (_apply) {
                    _index.undo();
                }
                _apply = false;
            }

            session &operator=(session &&mv) {
                if (this == &mv) {
                    return *this;
                }
                if (_apply) {
                    _index.undo();
                }
                _apply = mv._apply;
                mv._apply = false;
                return *this;
            }

            int64_t revision() const {
                return _revision;
            }

        private:
            friend class generic_index;

            session(generic_index &idx, int64_t revision) : _index(idx), _revision(revision) {
                if (revision == -1) {
                    _apply = false;
                }
            }

            generic_index &_index;
            bool _apply = true;
            int64_t _revision = 0;
        };

        session start_undo_session(bool enabled) {
            if (enabled) {
                _stack.emplace_back(_indices.get_allocator());
                _stack.back().old_next_id = _next_id;
                _stack.back().revision = ++_revision;
                return session(*this, _revision);
            } else {
                return session(*this, -1);
            }
        }

        const index_type &indicies() const {
            return _indices;
        }

        int64_t revision() const {
            return _revision;
        }


        /**
         *  Restores the state to how it was prior to the current session discarding all changes
         *  made between the last revision and the current revision.
         */
        void undo() {
            if (!enabled()) {
                return;
            }

            const auto &head = _stack.back();

            for (auto &item : head.old_values) {
                auto ok = _indices.modify(_indices.find(item.second.id), [&](value_type &v) {
                    v = std::move(item.second);
                });
                if (!ok)
                    BOOST_THROW_EXCEPTION(std::logic_error(
                            "Could not modify object, most likely a uniqueness constraint was violated"));
            }

            for (auto id : head.new_ids) {
                _indices.erase(_indices.find(id));
            }
            _next_id = head.old_next_id;

            for (auto &item : head.removed_values) {
                bool ok = _indices.emplace(std::move(item.second)).second;
                if (!ok)
                    BOOST_THROW_EXCEPTION(std::logic_error(
                            "Could not restore object, most likely a uniqueness constraint was violated"));
            }

            _stack.pop_back();
            --_revision;
        }

        /**
         *  This method works similar to git squash, it merges the change set from the two most
         *  recent revision numbers into one revision number (reducing the head revision number)
         *
         *  This method does not change the state of the index, only the state of the undo buffer.
         */
        void squash() {
            if (!enabled()) {
                return;
            }
            if (_stack.size() == 1) {
                _stack.pop_front();
                return;
            }

            auto &state = _stack.back();
            auto &prev_state = _stack[_stack.size() - 2];

            // An object's relationship to a state can be:
            // in new_ids            : new
            // in old_values (was=X) : upd(was=X)
            // in removed (was=X)    : del(was=X)
            // not in any of above   : nop
            //
            // When merging A=prev_state and B=state we have a 4x4 matrix of all possibilities:
            //
            //                   |--------------------- B ----------------------|
            //
            //                +------------+------------+------------+------------+
            //                | new        | upd(was=Y) | del(was=Y) | nop        |
            //   +------------+------------+------------+------------+------------+
            // / | new        | N/A        | new       A| nop       C| new       A|
            // | +------------+------------+------------+------------+------------+
            // | | upd(was=X) | N/A        | upd(was=X)A| del(was=X)C| upd(was=X)A|
            // A +------------+------------+------------+------------+------------+
            // | | del(was=X) | N/A        | N/A        | N/A        | del(was=X)A|
            // | +------------+------------+------------+------------+------------+
            // \ | nop        | new       B| upd(was=Y)B| del(was=Y)B| nop      AB|
            //   +------------+------------+------------+------------+------------+
            //
            // Each entry was composed by labelling what should occur in the given case.
            //
            // Type A means the composition of states contains the same entry as the first of the two merged states for that object.
            // Type B means the composition of states contains the same entry as the second of the two merged states for that object.
            // Type C means the composition of states contains an entry different from either of the merged states for that object.
            // Type N/A means the composition of states violates causal timing.
            // Type AB means both type A and type B simultaneously.
            //
            // The merge() operation is defined as modifying prev_state in-place to be the state object which represents the composition of
            // state A and B.
            //
            // Type A (and AB) can be implemented as a no-op; prev_state already contains the correct value for the merged state.
            // Type B (and AB) can be implemented by copying from state to prev_state.
            // Type C needs special case-by-case logic.
            // Type N/A can be ignored or assert(false) as it can only occur if prev_state and state have illegal values
            // (a serious logic error which should never happen).
            //

            // We can only be outside type A/AB (the nop path) if B is not nop, so it suffices to iterate through B's three containers.

            for (const auto &item : state.old_values) {
                if (prev_state.new_ids.find(item.second.id) != prev_state.new_ids.end()) {
                    // new+upd -> new, type A
                    continue;
                }
                if (prev_state.old_values.find(item.second.id) != prev_state.old_values.end()) {
                    // upd(was=X) + upd(was=Y) -> upd(was=X), type A
                    continue;
                }
                // del+upd -> N/A
                assert(prev_state.removed_values.find(item.second.id) == prev_state.removed_values.end());
                // nop+upd(was=Y) -> upd(was=Y), type B
                prev_state.old_values.emplace(std::move(item));
            }

            // *+new, but we assume the N/A cases don't happen, leaving type B nop+new -> new
            for (auto id : state.new_ids) {
                prev_state.new_ids.insert(id);
            }

            // *+del
            for (auto &obj : state.removed_values) {
                if (prev_state.new_ids.find(obj.second.id) != prev_state.new_ids.end()) {
                    // new + del -> nop (type C)
                    prev_state.new_ids.erase(obj.second.id);
                    continue;
                }
                auto it = prev_state.old_values.find(obj.second.id);
                if (it != prev_state.old_values.end()) {
                    // upd(was=X) + del(was=Y) -> del(was=X)
                    prev_state.removed_values.emplace(std::move(*it));
                    prev_state.old_values.erase(obj.second.id);
                    continue;
                }
                // del + del -> N/A
                assert(prev_state.removed_values.find(obj.second.id) == prev_state.removed_values.end());
                // nop + del(was=Y) -> del(was=Y)
                prev_state.removed_values.emplace(std::move(obj)); //[obj.second->id] = std::move(obj.second);
            }

            _stack.pop_back();
            --_revision;
        }

        /**
         * Discards all undo history prior to revision
         */
        void commit(int64_t revision) {
            while (_stack.size() && _stack[0].revision <= revision) {
                _stack.pop_front();
            }
        }

        /**
         * Unwinds all undo states
         */
        void undo_all() {
            while (enabled()) {
                undo();
            }
        }

        void set_revision(uint64_t revision) {
            if (_stack.size() != 0)
                BOOST_THROW_EXCEPTION(std::logic_error("cannot set revision while there is an existing undo stack"));
            _revision = revision;
        }

        void remove_object(int64_t id) {
            const value_type *val = find(typename value_type::id_type(id));
            if (!val)
                BOOST_THROW_EXCEPTION(std::out_of_range(boost::lexical_cast<std::string>(id)));
            remove(*val);
        }

//        template<typename T>
//        typename boost::interprocess::vector<
//                boost::interprocess::shared_ptr<
//                        secondary_index<typename T::index_type>,
//                        typename secondary_index<typename T::index_type>::allocator_type,
//                        typename secondary_index<typename T::index_type>::deleter_type
//                >, allocator<
//                        boost::interprocess::shared_ptr<
//                                secondary_index<typename T::index_type>,
//                                typename secondary_index<typename T::index_type>::allocator_type,
//                                typename secondary_index<typename T::index_type>::deleter_type
//                        >
//                >
//        >::reference add_secondary_index() {
//            segment_manager_type *seg_m = _sindex.get_allocator().get_segment_manager();
//            _sindex.emplace_back(new secondary_index<typename T::index_type>(),
//                    typename secondary_index<typename T::index_type>::allocator_type(seg_m),
//                    typename secondary_index<typename T::index_type>::deleter_type(seg_m));
//            return _sindex.back();
//        }
//
//        template<typename T>
//        const T &get_secondary_index() const {
//            for (const auto &item : _sindex) {
//                const T *result = dynamic_cast<const T *>(item.get());
//                if (result != nullptr) {
//                    return *result;
//                }
//            }
//            BOOST_THROW_EXCEPTION(std::logic_error("invalid index type"));
//        }

    private:
        bool enabled() const {
            return _stack.size();
        }

        void on_modify(const value_type &v) {
            if (!enabled()) {
                return;
            }

            auto &head = _stack.back();

            if (head.new_ids.find(v.id) != head.new_ids.end()) {
                return;
            }

            auto itr = head.old_values.find(v.id);
            if (itr != head.old_values.end()) {
                return;
            }

            head.old_values.emplace(std::pair<typename value_type::id_type, const value_type &>(v.id, v));
        }

        void on_remove(const value_type &v) {
            if (!enabled()) {
                return;
            }

            auto &head = _stack.back();
            if (head.new_ids.count(v.id)) {
                head.new_ids.erase(v.id);
                return;
            }

            auto itr = head.old_values.find(v.id);
            if (itr != head.old_values.end()) {
                head.removed_values.emplace(std::move(*itr));
                head.old_values.erase(v.id);
                return;
            }

            if (head.removed_values.count(v.id)) {
                return;
            }

            head.removed_values.emplace(std::pair<typename value_type::id_type, const value_type &>(v.id, v));
        }

        void on_create(const value_type &v) {
            if (!enabled()) {
                return;
            }
            auto &head = _stack.back();

            head.new_ids.insert(v.id);
        }

        boost::interprocess::deque<undo_state_type, allocator<undo_state_type>> _stack;

        /**
         *  Each new session increments the revision, a squash will decrement the revision by combining
         *  the two most recent revisions into one revision.
         *
         *  Commit will discard all revisions prior to the committed revision.
         */

        int64_t _revision = 0;
        typename value_type::id_type _next_id = 0;
        index_type _indices;
        uint32_t _size_of_value_type = 0;
        uint32_t _size_of_this = 0;

//        boost::interprocess::vector<
//                boost::interprocess::shared_ptr<
//                        secondary_index<MultiIndexType>,
//                        typename secondary_index<MultiIndexType>::allocator_type,
//                        typename secondary_index<MultiIndexType>::deleter_type
//                >, allocator<
//                        boost::interprocess::shared_ptr<
//                                secondary_index<MultiIndexType>,
//                                typename secondary_index<MultiIndexType>::allocator_type,
//                                typename secondary_index<MultiIndexType>::deleter_type
//                        >
//                >
//        > _sindex;
    };

    class abstract_session {
    public:
        virtual ~abstract_session() {
        };

        virtual void push()             = 0;

        virtual void squash()           = 0;

        virtual void undo()             = 0;

        virtual int64_t revision() const = 0;
    };

    template<typename SessionType>
    class session_impl : public abstract_session {
    public:
        session_impl(SessionType &&s) : _session(std::move(s)) {
        }

        virtual void push() override {
            _session.push();
        }

        virtual void squash() override {
            _session.squash();
        }

        virtual void undo() override {
            _session.undo();
        }

        virtual int64_t revision() const override {
            return _session.revision();
        }

    private:
        SessionType _session;
    };

    class abstract_index {
    public:
        abstract_index(void *i) : _idx_ptr(i) {
        }

        virtual ~abstract_index() {
        }

        virtual void set_revision(uint64_t revision) = 0;

        virtual boost::interprocess::unique_ptr<abstract_session> start_undo_session(bool enabled) = 0;

        virtual int64_t revision() const = 0;

        virtual void undo() const = 0;

        virtual void squash() const = 0;

        virtual void commit(int64_t revision) const = 0;

        virtual void undo_all() const = 0;

        virtual uint32_t type_id() const = 0;

        virtual void remove_object(int64_t id) = 0;

        void *get() const {
            return _idx_ptr;
        }

    private:
        void *_idx_ptr;
    };

    template<typename BaseIndex>
    class index_impl : public abstract_index {
    public:
        index_impl(BaseIndex &base) : abstract_index(&base), _base(base) {
        }

        virtual boost::interprocess::unique_ptr<abstract_session> start_undo_session(bool enabled) override {
            return boost::interprocess::unique_ptr<abstract_session>(
                    new session_impl<typename BaseIndex::session>(_base.start_undo_session(enabled)));
        }

        virtual void set_revision(uint64_t revision) override {
            _base.set_revision(revision);
        }

        virtual int64_t revision() const override {
            return _base.revision();
        }

        virtual void undo() const override {
            _base.undo();
        }

        virtual void squash() const override {
            _base.squash();
        }

        virtual void commit(int64_t revision) const override {
            _base.commit(revision);
        }

        virtual void undo_all() const override {
            _base.undo_all();
        }

        virtual uint32_t type_id() const override {
            return BaseIndex::value_type::type_id;
        }

        virtual void remove_object(int64_t id) override {
            return _base.remove_object(id);
        }

    private:
        BaseIndex &_base;
    };

    template<typename IndexType>
    class index : public index_impl<IndexType> {
    public:
        index(IndexType &i) : index_impl<IndexType>(i) {
        }
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


    /**
     *  This class
     */
    class database {
    public:
        enum open_flags {
            read_only = 0, read_write = 1
        };

        void open(const boost::filesystem::path &dir, uint32_t write = read_only, uint64_t shared_file_size = 0);

        void close();

        void flush();

        void wipe(const boost::filesystem::path &dir);

        void set_require_locking(bool enable_require_locking);

#ifdef CHAINBASE_CHECK_LOCKING

        void require_lock_fail(const char *method, const char *lock_type, const char *tname) const;

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

        struct session {
        public:
            session(session &&s) : _index_sessions(std::move(s._index_sessions)), _revision(s._revision) {
            }

            session(std::vector<boost::interprocess::unique_ptr<abstract_session>> &&s) : _index_sessions(
                    std::move(s)) {
                if (_index_sessions.size()) {
                    _revision = _index_sessions[0]->revision();
                }
            }

            ~session() {
                undo();
            }

            void push() {
                for (auto &i : _index_sessions) {
                    i->push();
                }
                _index_sessions.clear();
            }

            void squash() {
                for (auto &i : _index_sessions) {
                    i->squash();
                }
                _index_sessions.clear();
            }

            void undo() {
                for (auto &i : _index_sessions) {
                    i->undo();
                }
                _index_sessions.clear();
            }

            int64_t revision() const {
                return _revision;
            }

        private:
            friend class database;

            session() {
            }

            std::vector<boost::interprocess::unique_ptr<abstract_session>> _index_sessions;
            int64_t _revision = -1;
        };

        session start_undo_session(bool enabled);

        int64_t revision() const {
            if (_index_list.size() == 0) {
                return -1;
            }
            return _index_list[0]->revision();
        }

        void undo();

        void squash();

        void commit(int64_t revision);

        void undo_all();


        void set_revision(uint64_t revision) {
            CHAINBASE_REQUIRE_WRITE_LOCK("set_revision", uint64_t);
            for (auto i : _index_list) {
                i->set_revision(revision);
            }
        }


        template<typename MultiIndexType>
        generic_index<MultiIndexType> *add_index() {
            const uint16_t type_id = generic_index<MultiIndexType>::value_type::type_id;
            typedef generic_index <MultiIndexType> index_type;
            typedef typename index_type::allocator_type index_alloc;

            std::string type_name = boost::core::demangle(typeid(typename index_type::value_type).name());

            if (!(_index_map.size() <= type_id || _index_map[type_id] == nullptr)) {
                BOOST_THROW_EXCEPTION(std::logic_error(type_name + "::type_id is already in use"));
            }

            index_type *idx_ptr = nullptr;
            if (!_read_only) {
                idx_ptr = _segment->find_or_construct<index_type>(type_name.c_str())(
                        index_alloc(_segment->get_segment_manager()));
            } else {
                idx_ptr = _segment->find<index_type>(type_name.c_str()).first;
                if (!idx_ptr)
                    BOOST_THROW_EXCEPTION(std::runtime_error(
                                                  "unable to find index for " + type_name + " in read only database"));
            }

            idx_ptr->validate();

            if (type_id >= _index_map.size()) {
                _index_map.resize(type_id + 1);
            }

            auto new_index = new index <index_type>(*idx_ptr);
            _index_map[type_id].reset(new_index);
            _index_list.push_back(new_index);

            return idx_ptr;
        }

        auto get_segment_manager() -> decltype(((boost::interprocess::managed_mapped_file *) nullptr)->get_segment_manager()) {
            return _segment->get_segment_manager();
        }

        size_t get_free_memory() const {
            return _segment->get_segment_manager()->get_free_memory();
        }

        template<typename MultiIndexType>
        bool has_index()const {
            CHAINBASE_REQUIRE_READ_LOCK("get_index", typename MultiIndexType::value_type);
            typedef generic_index<MultiIndexType> index_type;
            return _index_map.size() > index_type::value_type::type_id && _index_map[index_type::value_type::type_id];
        }

        template<typename MultiIndexType>
        const generic_index<MultiIndexType> &get_index() const {
            CHAINBASE_REQUIRE_READ_LOCK("get_index", typename MultiIndexType::value_type);
            typedef generic_index <MultiIndexType> index_type;
            typedef index_type *index_type_ptr;
            assert(_index_map.size() > index_type::value_type::type_id);
            assert(_index_map[index_type::value_type::type_id]);
            return *index_type_ptr(_index_map[index_type::value_type::type_id]->get());
        }

        template<typename MultiIndexType, typename ByIndex>
        auto get_index() const -> decltype(((generic_index<MultiIndexType> *) (nullptr))->indicies().template get<
                ByIndex>()) {
            CHAINBASE_REQUIRE_READ_LOCK("get_index", typename MultiIndexType::value_type);
            typedef generic_index <MultiIndexType> index_type;
            typedef index_type *index_type_ptr;
            assert(_index_map.size() > index_type::value_type::type_id);
            assert(_index_map[index_type::value_type::type_id]);
            return index_type_ptr(_index_map[index_type::value_type::type_id]->get())->indicies().template get<
                    ByIndex>();
        }

        template<typename MultiIndexType>
        generic_index<MultiIndexType> &get_mutable_index() {
            CHAINBASE_REQUIRE_WRITE_LOCK("get_mutable_index", typename MultiIndexType::value_type);
            typedef generic_index <MultiIndexType> index_type;
            typedef index_type *index_type_ptr;
            assert(_index_map.size() > index_type::value_type::type_id);
            assert(_index_map[index_type::value_type::type_id]);
            return *index_type_ptr(_index_map[index_type::value_type::type_id]->get());
        }

        template<typename ObjectType, typename IndexedByType, typename CompatibleKey>
        const ObjectType *find(CompatibleKey &&key) const {
            CHAINBASE_REQUIRE_READ_LOCK("find", ObjectType);
            typedef typename get_index_type<ObjectType>::type index_type;
            const auto &idx = get_index<index_type>().indicies().template get<IndexedByType>();
            auto itr = idx.find(std::forward<CompatibleKey>(key));
            if (itr == idx.end()) {
                return nullptr;
            }
            return &*itr;
        }

        template<typename ObjectType>
        const ObjectType *find(object_id<ObjectType> key = object_id<ObjectType>()) const {
            CHAINBASE_REQUIRE_READ_LOCK("find", ObjectType);
            typedef typename get_index_type<ObjectType>::type index_type;
            const auto &idx = get_index<index_type>().indices();
            auto itr = idx.find(key);
            if (itr == idx.end()) {
                return nullptr;
            }
            return &*itr;
        }

        template<typename ObjectType, typename IndexedByType, typename CompatibleKey>
        const ObjectType &get(CompatibleKey &&key) const {
            CHAINBASE_REQUIRE_READ_LOCK("get", ObjectType);
            auto obj = find<ObjectType, IndexedByType>(std::forward<CompatibleKey>(key));
            if (!obj)
                BOOST_THROW_EXCEPTION(std::out_of_range("unknown key"));
            return *obj;
        }

        template<typename ObjectType>
        const ObjectType &get(const object_id<ObjectType> &key = object_id<ObjectType>()) const {
            CHAINBASE_REQUIRE_READ_LOCK("get", ObjectType);
            auto obj = find<ObjectType>(key);
            if (!obj)
                BOOST_THROW_EXCEPTION(std::out_of_range("unknown key"));
            return *obj;
        }

        template<typename ObjectType, typename Modifier>
        void modify(const ObjectType &obj, Modifier &&m) {
            CHAINBASE_REQUIRE_WRITE_LOCK("modify", ObjectType);
            typedef typename get_index_type<ObjectType>::type index_type;
            get_mutable_index<index_type>().modify(obj, m);
        }

        template<typename ObjectType>
        void remove(const ObjectType &obj) {
            CHAINBASE_REQUIRE_WRITE_LOCK("remove", ObjectType);
            typedef typename get_index_type<ObjectType>::type index_type;
            return get_mutable_index<index_type>().remove(obj);
        }

        template<typename ObjectType, typename Constructor>
        const ObjectType &create(Constructor &&con) {
            CHAINBASE_REQUIRE_WRITE_LOCK("create", ObjectType);
            typedef typename get_index_type<ObjectType>::type index_type;
            return get_mutable_index<index_type>().emplace(std::forward<Constructor>(con));
        }

        template<typename Lambda>
        auto with_read_lock(Lambda &&callback) -> decltype((*(Lambda * )nullptr)()) {
            read_lock lock(_mutex, boost::defer_lock_t());
#ifdef CHAINBASE_CHECK_LOCKING
            BOOST_ATTRIBUTE_UNUSED
            int_incrementer ii(_read_lock_count);
#endif

            if (!_read_wait_micro || !_max_read_wait_retries) {
                lock.lock();
            } else {
                auto lock_time = [&] {
                    return
                        boost::posix_time::microsec_clock::universal_time() +
                        boost::posix_time::microseconds(_read_wait_micro);
                };

                for (uint32_t retry = 0; ; ++retry) {
                    if (lock.timed_lock(lock_time())) {
                        break;
                    }

                    if (retry >= _max_read_wait_retries) {
                        std::cerr << "No more retries for read lock" << std::endl;
                        BOOST_THROW_EXCEPTION(std::runtime_error("Unable to acquire READ lock"));
                    } else {
                        std::cerr << "Read lock timeout" << std::endl;
                    }
                }
            }

            return callback();
        }

        template<typename Lambda>
        auto with_write_lock(
            uint64_t write_wait_micro,
            uint32_t max_write_wait_retries,
            Lambda &&callback
        ) -> decltype((*(Lambda * )nullptr)()) {
            if (_read_only)
                BOOST_THROW_EXCEPTION(std::logic_error("cannot acquire write lock on read-only process"));

            write_lock lock(_mutex, boost::defer_lock_t());
#ifdef CHAINBASE_CHECK_LOCKING
            BOOST_ATTRIBUTE_UNUSED
            int_incrementer ii(_write_lock_count);
#endif

            if (!write_wait_micro || !max_write_wait_retries) {
                lock.lock();
            } else {
                auto lock_time = [&] {
                    return
                        boost::posix_time::microsec_clock::universal_time() +
                        boost::posix_time::microseconds(write_wait_micro);
                };

                for (uint32_t retry = 0; ; ++retry) {
                    if (lock.timed_lock(lock_time())) {
                        break;
                    }

                    if (retry >= max_write_wait_retries) {
                        std::cerr << "FATAL write lock timeout!!!" << std::endl;
                        BOOST_THROW_EXCEPTION(std::runtime_error("Unable to acquire WRITE lock"));
                    } else {
                        std::cerr << "Write lock timeout" << std::endl;
                    }
                }
            }

            return callback();
        }

        template<typename Lambda>
        auto with_weak_write_lock(Lambda &&callback) -> decltype((*(Lambda * )nullptr)()) {
            return with_write_lock(_write_wait_micro, _max_write_wait_retries, std::forward<Lambda>(callback));
        }

        template<typename Lambda>
        auto with_strong_write_lock(Lambda &&callback) -> decltype((*(Lambda * )nullptr)()) {
            return with_write_lock(uint64_t(1000000), uint32_t(100000), std::forward<Lambda>(callback));
        }

        void read_wait_micro(uint64_t value);
        uint64_t read_wait_micro() const;

        void max_read_wait_retries(uint32_t value);
        uint32_t max_read_wait_retries() const;

        void write_wait_micro(uint64_t value);
        uint64_t write_wait_micro() const;

        void max_write_wait_retries(uint32_t value);
        uint32_t max_write_wait_retries() const;

    private:
        boost::interprocess::unique_ptr<boost::interprocess::managed_mapped_file> _segment;
        boost::interprocess::unique_ptr<boost::interprocess::managed_mapped_file> _meta;
        read_write_mutex _mutex;
        bool _read_only = false;
        boost::interprocess::file_lock _flock;

        /**
         * This is a sparse list of known indicies kept to accelerate creation of undo sessions
         */
        std::vector<abstract_index *> _index_list;

        /**
         * This is a full map (size 2^16) of all possible index designed for constant time lookup
         */
        std::vector<boost::interprocess::unique_ptr<abstract_index>> _index_map;

        boost::filesystem::path _data_dir;

        int32_t _read_lock_count = 0;
        int32_t _write_lock_count = 0;
        bool _enable_require_locking = false;

        uint64_t _read_wait_micro = 1000000;
        uint32_t _max_read_wait_retries = 5;

        uint64_t _write_wait_micro = 1000000;
        uint32_t _max_write_wait_retries = 10000;
    };

    template<typename Object, typename... Args> using shared_multi_index_container = boost::multi_index_container<
            Object, Args..., chainbase::allocator<Object>>;
}  // namepsace chainbase


#pragma once

#include <chainbase/object.hpp>
#include <chainbase/index.hpp>
#include <chainbase/undo_database.hpp>

#include <map>
#include <cassert>
#include <vector>

namespace chainbase {
    namespace db {

        /**
         *   @class object_database
         *   @brief maintains a set of indexed objects that can be modified with multi-level rollback support
         */
        class object_database {
        public:
            object_database();

            ~object_database();

            void reset_indexes() {
                _index.clear();
                _index.resize(255);
            }

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

            const object &insert(object &&obj) { return get_mutable_index(obj.id).insert(std::move(obj)); }

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

            boost::filesystem::path get_data_dir() const { return _data_dir; }

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
            std::vector<std::vector<unique_ptr<index> > > _index;
            shared_ptr<level_map<object_id_type, vector<char> >> _object_id_to_object;
        };

    }
} // chainbase::db



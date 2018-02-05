#pragma once

#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <memory>

#include <chainbase/object.hpp>


namespace chainbase {
    namespace db {

        using std::unordered_map;
        using std::unordered_set;
        using std::unique_ptr;

        class object_database;

        struct undo_state {
            unordered_map<object_id_type, unique_ptr < object> > old_values;
            unordered_map<object_id_type, object_id_type> old_index_next_ids;
            unordered_set<object_id_type> new_ids;
            unordered_map<object_id_type, unique_ptr < object> > removed;
        };


        /**
         * @class undo_database
         * @brief tracks changes to the state and allows changes to be undone
         *
         */
        class undo_database {
        public:
            undo_database(object_database &db) : _db(db) {}

            class session {
            public:
                session(session &&mv)
                        : _db(mv._db), _apply_undo(mv._apply_undo) {
                    mv._apply_undo = false;
                }

                ~session() {
                    try {
                        if (_apply_undo)_db.undo();
                    } catch(...){ //(const fc::exception &e) {
                        //elog("${e}", ("e", e.to_detail_string()));
                        throw; // maybe crash..
                    }
                }

                void commit() {
                    _apply_undo = false;
                    _db.commit();
                }

                void undo() {
                    if (_apply_undo) _db.undo();
                    _apply_undo = false;
                }

                void merge() {
                    if (_apply_undo) _db.merge();
                    _apply_undo = false;
                }

                session &operator=(session &&mv) {
                    if (this == &mv) return *this;
                    if (_apply_undo) _db.undo();
                    _apply_undo = mv._apply_undo;
                    mv._apply_undo = false;
                    return *this;
                }

            private:
                friend class undo_database;

                session(undo_database &db) : _db(db) {}

                undo_database &_db;
                bool _apply_undo = true;
            };

            void disable();

            void enable();

            session start_undo_session();

            /**
             * This should be called just after an object is created
             */
            void on_create(const object &obj);

            /**
             * This should be called just before an object is modified
             *
             * If it's a new object as of this undo state, its pre-modification value is not stored, because prior to this
             * undo state, it did not exist. Any modifications in this undo state are irrelevant, as the object will simply
             * be removed if we undo.
             */
            void on_modify(const object &obj);

            /**
             * This should be called just before an object is removed.
             *
             * If it's a new object as of this undo state, its pre-removal value is not stored, because prior to this undo
             * state, it did not exist. Now that it's been removed, it doesn't exist again, so nothing has happened.
             * Instead, remove it from the list of newly created objects (which must be deleted if we undo), as we don't
             * want to re-delete it if this state is undone.
             */
            void on_remove(const object &obj);

            /**
             *  Removes the last committed session,
             *  note... this is dangerous if there are
             *  active sessions... thus active sessions should
             *  track
             */
            void pop_commit();

            std::size_t size() const { return _stack.size(); }

            void set_max_size(size_t new_max_size) { _max_size = new_max_size; }

            size_t max_size() const { return _max_size; }

            const undo_state &head() const;

        private:
            void undo();

            void merge();

            void commit();

            uint32_t _active_sessions = 0;
            bool _disabled = true;
            std::deque<undo_state> _stack;
            object_database &_db;
            size_t _max_size = 256;
        };

    }
} // bts::db

#pragma once
#include <chainbase/object.hpp>
#include <chainbase/level_map.hpp>
#include <fc/interprocess/file_mapping.hpp>
#include <fc/io/raw.hpp>
#include <fc/io/json.hpp>
#include <fc/crypto/sha256.hpp>
#include <fstream>

namespace chainbase { namespace db {
        class object_database;

        /**
         * @class index_observer
         * @brief used to get callbacks when objects change
         */
        class index_observer
        {
        public:
            virtual ~index_observer(){}
            /** called just after the object is added */
            virtual void on_add( const object& obj ){}
            /** called just before obj is removed */
            virtual void on_remove( const object& obj ){}
            /** called just after obj is modified with new value*/
            virtual void on_modify( const object& obj ){}
        };

        /**
         *  @class index
         *  @brief abstract base class for accessing objects indexed in various ways.
         *
         *  All indexes assume that there exists an object ID space that will grow
         *  for ever in a seqential manner.  These IDs are used to identify the
         *  index, type, and instance of the object.
         *
         *  Items in an index can only be modified via a call to modify and
         *  all references to objects outside of that callback are const references.
         *
         *  Most implementations will probably be some form of boost::multi_index_container
         *  which means that they can covnert a reference to an object to an iterator.  When
         *  at all possible save a pointer/reference to your objects rather than constantly
         *  looking them up by ID.
         */
        class index
        {
        public:
            virtual ~index(){}

            virtual uint8_t object_space_id()const = 0;
            virtual uint8_t object_type_id()const = 0;

            virtual object_id_type get_next_id()const = 0;
            virtual void           use_next_id() = 0;
            virtual void           set_next_id( object_id_type id ) = 0;

            virtual const object&  load( const std::vector<char>& data ) = 0;
            /**
             *  Polymorphically insert by moving an object into the index.
             *  this should throw if the object is already in the database.
             */
            virtual const object& insert( object&& obj ) = 0;

            /**
             * Builds a new object and assigns it the next available ID and then
             * initializes it with constructor and lastly inserts it into the index.
             */
            virtual const object&  create( const std::function<void(object&)>& constructor ) = 0;

            /**
             *  Opens the index loading objects from a level_db database
             */
            virtual void open( const shared_ptr<graphene::db::level_map<object_id_type, vector<char> >>& db ){}

            /** @return the object with id or nullptr if not found */
            virtual const object*      find( object_id_type id )const = 0;

            /**
             * This version will automatically check for nullptr and throw an exception if the
             * object ID could not be found.
             */
            const object&              get( object_id_type id )const
            {
                auto maybe_found = find( id );
                FC_ASSERT( maybe_found != nullptr, "Unable to find Object", ("id",id) );
                return *maybe_found;
            }

            virtual void               modify( const object& obj, const std::function<void(object&)>& ) = 0;
            virtual void               remove( const object& obj ) = 0;

            /**
             *   When forming your lambda to modify obj, it is natural to have Object& be the signature, but
             *   that is not compatible with the type erasue required by the virtual method.  This method
             *   provides a helper to wrap the lambda in a form compatible with the virtual modify call.
             *   @note Lambda should have the signature:  void(Object&)
             */
            template<typename Object, typename Lambda>
            void modify( const Object& obj, const Lambda& l ) {
                modify( static_cast<const object&>(obj), std::function<void(object&)>( [&]( object& o ){ l( static_cast<Object&>(o) ); } ) );
            }

            virtual void               inspect_all_objects(std::function<void(const object&)> inspector)const = 0;
            virtual void               add_observer( const shared_ptr<index_observer>& ) = 0;

        };

        /**
         *   Defines the common implementation
         */
        class base_primary_index
        {
        public:
            base_primary_index( object_database& db ):_db(db){}

            /** called just before obj is modified */
            void save_undo( const object& obj );

            /** called just after the object is added */
            void on_add( const object& obj );

            /** called just before obj is removed */
            void on_remove( const object& obj );

            /** called just after obj is modified */
            void on_modify( const object& obj );

        protected:
            vector< shared_ptr<index_observer> > _observers;

        private:
            object_database& _db;
        };

        /**
         * @class primary_index
         * @brief  Wraps a derived index to intercept calls to create, modify, and remove so that
         *  callbacks may be fired and undo state saved.
         *
         *  @see http://en.wikipedia.org/wiki/Curiously_recurring_template_pattern
         */
        template<typename DerivedIndex>
        class primary_index  : public DerivedIndex, public base_primary_index {
        public:
            typedef typename DerivedIndex::object_type object_type;

            primary_index( object_database& db )
                    :base_primary_index(db),_next_id(object_type::space_id,object_type::type_id,0) {}

            virtual uint8_t object_space_id()const override
            { return object_type::space_id; }

            virtual uint8_t object_type_id()const override
            { return object_type::type_id; }

            virtual object_id_type get_next_id()const               { return _next_id;    }
            virtual void           use_next_id()                    { ++_next_id.number;  }
            virtual void           set_next_id( object_id_type id ) { _next_id = id;      }

            virtual const object&  load( const std::vector<char>& data ) {
                return DerivedIndex::insert( fc::raw::unpack<object_type>( data ) );
            }

            virtual void open( const shared_ptr<graphene::db::level_map<object_id_type, vector<char> >>& db ){
                auto first = object_id_type( DerivedIndex::object_type::space_id, DerivedIndex::object_type::type_id, 0 );
                auto last = object_id_type( DerivedIndex::object_type::space_id, DerivedIndex::object_type::type_id+1, 0 );
                auto itr = db->lower_bound( first );
                while( itr.valid() && itr.key() < last ) {
                    load( itr.value() );
                    ++itr;
                }
            }
            virtual const object&  create(const std::function<void(object&)>& constructor ) {
                const auto& result = DerivedIndex::create( constructor );
                on_add( result );
                return result;
            }

            virtual void  remove( const object& obj ) override {
                on_remove(obj);
                DerivedIndex::remove(obj);
            }

            virtual void modify( const object& obj, const std::function<void(object&)>& m )override {
                save_undo( obj );
                DerivedIndex::modify( obj, m );
                on_modify( obj );
            }

            virtual void add_observer( const shared_ptr<index_observer>& o ) override {
                _observers.emplace_back( o );
            }

        private:
            object_id_type _next_id;
        };

} } // chainbase::db

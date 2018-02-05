#pragma once

#include <memory>
#include <cstdint>
#include <vector>
#define BTS_DB_MAX_INSTANCE_ID  (uint64_t(-1)>>16)

namespace chainbase {
    namespace db {
        using std::shared_ptr;
        using std::unique_ptr;
        using std::vector;

        struct object_id_type {
            object_id_type(uint8_t s, uint8_t t, uint64_t i) {
                assert(i >> 48 == 0);//"instance overflow"
                //FC_ASSERT(i >> 48 == 0, "instance overflow", ("instance", i));
                number = (uint64_t(s) << 56) | (uint64_t(t) << 48) | i;
            }

            object_id_type() { number = 0; }

            uint8_t space() const { return number >> 56; }

            uint8_t type() const { return number >> 48 & 0x00ff; }

            uint16_t space_type() const { return number >> 48; }

            uint64_t instance() const { return number & BTS_DB_MAX_INSTANCE_ID; }

            bool is_null() const { return number == 0; }

            operator uint64_t() const { return number; }

            friend bool operator==(const object_id_type &a, const object_id_type &b) {
                return a.number == b.number;
            }

            object_id_type &operator++(int) {
                ++number;
                return *this;
            }

            object_id_type &operator++() {
                ++number;
                return *this;
            }

            friend size_t hash_value(object_id_type v) { return std::hash<uint64_t>()(v.number); }

            friend bool operator<(const object_id_type &a, const object_id_type &b) {
                return a.number < b.number;
            }

            std::vector<char> pack() const {
                //TODO: big problem
                return std::vector<char>();//std::to_string(space()) + "." + std::to_string(type()) + "." + std::to_string(instance());

            }

            uint64_t number;
        };

        class object;

        class object_database;

        template<uint8_t SpaceID, uint8_t TypeID, typename T = object>
        struct object_id {
            typedef T type;
            static const uint8_t space_id = SpaceID;
            static const uint8_t type_id = TypeID;

            object_id() {}

            object_id(uint32_t i) : instance(i) {}

            object_id(uint64_t i) : instance(i) {
                //FC_ASSERT((i >> 48) == 0);
                assert((i >> 48) == 0);
            }

            object_id(object_id_type id) : instance(id.instance()) {
            }

            operator object_id_type() const { return object_id_type(SpaceID, TypeID, instance); }

            operator uint64_t() const { return object_id_type(*this).number; }

            template<typename DB>
            const T &operator()(const DB &db) const { return db.get(*this); }

            friend bool operator==(const object_id &a, const object_id &b) {
                return a.instance == b.instance;
            }

            friend bool operator==(const object_id_type &a, const object_id &b) {
                return a == object_id_type(b);
            }

            friend bool operator==(const object_id &b, const object_id_type &a) {
                return a == object_id_type(b);
            }

            friend bool operator<(const object_id &a, const object_id &b) {
                return a.instance < b.instance;
            }

            friend size_t hash_value(object_id v) { return std::hash<uint64_t>()(v.instance); }

            uint32_t instance;
        };

    }
}


/*
// REFLECT object_id manually because it has 2 template params
namespace fc {
    template<uint8_t SpaceID, uint8_t TypeID, typename T>
    struct get_typename<chainbase::db::object_id < SpaceID, TypeID, T>> {
    static const char *name() {
        return typeid(get_typename).name();
        static std::string _str = string("bts::db::object_id<") + fc::to_string(SpaceID) + ":" + fc::to_string(TypeID) + ">";
        return _str.c_str();
    }
};

template<uint8_t SpaceID, uint8_t TypeID, typename T>
struct reflector<chainbase::db::object_id < SpaceID, TypeID, T> > {
typedef chainbase::db::object_id <SpaceID, TypeID, T> type;
typedef fc::true_type is_defined;
typedef fc::false_type is_enum;
enum member_count_enum {
    local_member_count = 1,
    total_member_count = 1
};

template<typename Visitor>
static inline void visit(const Visitor &visitor) {
    typedef decltype(((type *) nullptr)->instance) member_type;
    visitor.TEMPLATEoperator()<member_type, type, &type::instance>("instance");
}

};


inline void to_variant(const chainbase::db::object_id_type &var, fc::variant &vo) {
    vo = std::to_string(var.space()) + "." + std::to_string(var.type()) + "." + std::to_string(var.instance());
}

inline void from_variant(const fc::variant &var, chainbase::db::object_id_type &vo) {
    try {
        vo.number = 0;
        const auto &s = var.get_string();
        auto first_dot = s.find('.');
        auto second_dot = s.find('.', first_dot + 1);
        FC_ASSERT(first_dot != second_dot);
        FC_ASSERT(first_dot != 0 && first_dot != std::string::npos);
        vo.number = fc::to_uint64(s.substr(second_dot + 1));
        FC_ASSERT(vo.number <= BTS_DB_MAX_INSTANCE_ID);
        auto space_id = fc::to_uint64(s.substr(0, first_dot));
        FC_ASSERT(space_id <= 0xff);
        auto type_id = fc::to_uint64(s.substr(first_dot + 1, second_dot - first_dot - 1));
        FC_ASSERT(type_id <= 0xff);
        vo.number |= (space_id << 56) | (type_id << 48);
    }
    FC_CAPTURE_AND_RETHROW((var))
}

template<uint8_t SpaceID, uint8_t TypeID, typename T>
void to_variant(const chainbase::db::object_id <SpaceID, TypeID, T> &var, fc::variant &vo) {
    vo = std::to_string(SpaceID) + "." + std::to_string(TypeID) + "." + std::to_string(var.instance);
}

template<uint8_t SpaceID, uint8_t TypeID, typename T>
void from_variant(const fc::variant &var, chainbase::db::object_id <SpaceID, TypeID, T> &vo) {
    try {
        const auto &s = var.get_string();
        auto first_dot = s.find('.');
        auto second_dot = s.find('.', first_dot + 1);
        FC_ASSERT(first_dot != second_dot);
        FC_ASSERT(first_dot != 0 && first_dot != std::string::npos);
        FC_ASSERT(fc::to_uint64(s.substr(0, first_dot)) == SpaceID && fc::to_uint64(s.substr(first_dot + 1, second_dot - first_dot - 1)) == TypeID);
        vo.instance = fc::to_uint64(s.substr(second_dot + 1));
    }
    FC_CAPTURE_AND_RETHROW((var))
}

} // namespace fc
*/
namespace std {
    template<>
    struct hash<chainbase::db::object_id_type> {
        size_t operator()(const chainbase::db::object_id_type &x) const {
            return std::hash<uint64_t>()(x.number);
        }
    };
}
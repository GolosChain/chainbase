#pragma once

#include <memory>
#include <string>

namespace chainbase {
    namespace db {

        class peer;

        typedef std::shared_ptr<peer> peer_ptr;

        class peer_ram;

        typedef std::shared_ptr<peer_ram> peer_ram_ptr;

        using serialize_t = std::string ;
    }
} // namespace chainbase::db

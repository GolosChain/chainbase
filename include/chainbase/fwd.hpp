#pragma once
#include <memory>

namespace chainbase { namespace db {

     class peer;
     typedef std::shared_ptr<peer> peer_ptr;

     class peer_ram;
     typedef std::shared_ptr<peer_ram> peer_ram_ptr;

}} // namespace chainbase::db

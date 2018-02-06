#pragma once
namespace chainbase {
    namespace db {

        class level_map_failure : public  std::exception{
        };


        class level_map_open_failure : public  std::exception {
        };


        class level_pod_map_failure : public std::exception{

        };

        class level_pod_map_open_failure : public std::exception{

        };


        class key_not_found_exception: public std::exception{

        };


        class canceled_exception : public std::exception{

        };

    }
}
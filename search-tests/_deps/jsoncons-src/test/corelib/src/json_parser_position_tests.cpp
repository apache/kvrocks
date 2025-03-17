// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <map>

using namespace jsoncons;

namespace {

    class string_locator : public jsoncons::default_json_visitor
    {
        std::string path_;
        std::string from_;
        std::vector<std::string> current_;
        std::vector<std::size_t>& positions_;
        
        std::vector < std::pair<std::size_t,std::size_t>> arrayIndexes; //Position in current_, value
        std::vector<std::size_t> arrayObjects_;
        bool check = false;
        bool alreadyUpdated = false;
    public:
        using jsoncons::default_json_visitor::string_view_type;
        
        string_locator(const std::string& path,
                      const std::string& from, std::vector<std::size_t>& positions)
            : path_(path),
              from_(from),
              positions_(positions)
        {
        }

        std::string buildNormalizedPath(const std::vector<std::string>& iKeyList)
        {
            //Init
            std::string aNormalizedPath = "$";

            //For each key in the current stack
            for (auto& key : iKeyList)
            {
                aNormalizedPath += "[" + key + "]";
            }
            return aNormalizedPath;
        }

        bool custom_visit(const ser_context& context)
        {
            if (check)
            {
                arrayObjects_.push_back(current_.size());
            }
            check = false;
            std::string aNormPath;
            if (arrayObjects_.size() > 0 && arrayObjects_.back() == current_.size() && arrayIndexes.size() > 0)
            {
                auto& p = arrayIndexes.back();
                current_.at(p.first) = std::to_string(p.second);
                aNormPath = buildNormalizedPath(current_);
                p.second += 1;
            }
            else
            {
                aNormPath = buildNormalizedPath(current_);
            }
            //std::cout << aNormPath << '\n';
            if (path_ == aNormPath)
            {
                positions_.push_back(context.position());
            }
            alreadyUpdated = false;
            return true;
        }

        bool visit_begin_object(semantic_tag, const ser_context&, std::error_code&) override
        {
            //If we are in an array of objects and we are at the same depth (current_.size()) of the object 
            if (arrayObjects_.size() > 0 && arrayObjects_.back() == current_.size())
            {
                auto& p = arrayIndexes.back();
                p.second += 1;
                current_.at(p.first) = std::to_string(p.second);
                
            }else if (check)
            {
                //we have an array of objects
                //we save the size of the current stack in a vector
                //so when we are again at this size it means we need to update the index
                arrayObjects_.push_back(current_.size());
            }
            current_.emplace_back();
            return true;
        }

        bool visit_end_object(const ser_context&, std::error_code&) override
        {
            current_.pop_back();
            check = false;
            return true;
        }

        bool visit_key(const string_view_type& key, const ser_context&, std::error_code&) override
        {
            if (!current_.empty())
            {
                current_.back() = std::string("'")  + std::string(key) + std::string("'");
            }
            check = false;
            return true;
        }

        bool visit_begin_array(semantic_tag, const ser_context&, std::error_code&) override
        {
            current_.emplace_back(std::to_string(0));
            arrayIndexes.emplace_back(std::make_pair(current_.size()-1,0));
            check = true;
            return true;
        }

        bool visit_end_array(const ser_context&, std::error_code&) override
        {
            current_.pop_back();
            arrayIndexes.pop_back();
            check = false;
            arrayObjects_.pop_back();
            return true;
        }

        bool visit_string(const string_view_type&, jsoncons::semantic_tag, const jsoncons::ser_context& context, std::error_code&) override
        {
            return custom_visit(context);
        }

        bool visit_null(semantic_tag, const ser_context&, std::error_code&) override
        {
            return true;
        }

        bool visit_uint64(uint64_t, semantic_tag, const ser_context& context, std::error_code&) override
        {
            return custom_visit(context);
        }

        bool visit_int64(int64_t, semantic_tag, const ser_context& context, std::error_code&) override
        {
            return custom_visit(context);
        }

        bool visit_double(double, semantic_tag, const ser_context& context, std::error_code&) override
        {
            return custom_visit(context);
        }

        bool visit_bool(bool, semantic_tag, const ser_context& context, std::error_code&) override
        {
            return custom_visit(context);
        }
    };

    void update_in_place(std::string& input, const std::string& path, std::vector<std::size_t>& positions)
    {
        string_locator updater(path, "", positions);
        jsoncons::json_string_reader reader(input, updater);
        reader.read();
    }
}

TEST_CASE("json_parser position")
{
    SECTION("test 1")
    {
        std::string input1 = R"(
          {
            "Parent": {
                "Child": {
                    "Test": 4444333322221111,
                    "NegativeInt": -4444333322221111,
                    "Double" : 12345.6789,
                    "NegativeDouble" : -12345.6789
                }
            }
        }
      )";
        std::string input2 = R"(
          {
            "Parent": {
                "Child": {
                    "Test": "4444333322221111"
                }
            }
        }
      )";
        try
        {
            std::vector<std::size_t> positions;

            update_in_place(input1, "$['Parent']['Child']['Test']", positions);
            REQUIRE(positions.size() == 1);
            CHECK(input1.substr(positions.back(),16) == std::string("4444333322221111"));

            positions.clear();
            update_in_place(input2, "$['Parent']['Child']['Test']", positions);
            REQUIRE(positions.size() == 1);
            CHECK(input2.substr(positions.back(),18) == std::string("\"4444333322221111\""));

            positions.clear();
            update_in_place(input1, "$['Parent']['Child']['NegativeInt']", positions);
            REQUIRE(positions.size() == 1);
            CHECK(input1.substr(positions.back(),17) == std::string("-4444333322221111"));

            positions.clear();
            update_in_place(input1, "$['Parent']['Child']['Double']", positions);
            REQUIRE(positions.size() == 1);
            CHECK(input1.substr(positions.back(),10) == std::string("12345.6789"));

            positions.clear();
            update_in_place(input1, "$['Parent']['Child']['NegativeDouble']", positions);
            REQUIRE(positions.size() == 1);
            CHECK(input1.substr(positions.back(),11) == std::string("-12345.6789"));
        }
        catch (std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
    }
}


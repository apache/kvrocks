// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/utility/uri.hpp>
#include <iostream>
#include <catch/catch.hpp>

TEST_CASE("uri constructor tests")
{
    SECTION("from parts")
    {
        std::string scheme = "https";
        std::string userinfo = "!#$&'()*+,/:;=?@[]";
        std::string host = "www.example.com";
        std::string port = "10";
        std::string path = "!#$&'()*+,/:;=?@[]";
        std::string query = "!#$&'()*+,/:;=?@[]";
        std::string fragment = "!#$&'()*+,/:;=?@[]";
        
        jsoncons::uri uri{scheme,
            userinfo,
            host,
            port,
            path,
            query,
            fragment
        };
        
        CHECK("!%23$&'()*+,%2F:;=%3F%40%5B%5D" == uri.encoded_userinfo());
        CHECK("www.example.com" == uri.host());
        CHECK("10" == uri.port());
        CHECK("/!%23$&'()*+,/:;=%3F@%5B%5D" == uri.encoded_path());
        CHECK("!%23$&'()*+,/:;=?@[]" == uri.encoded_query());
        CHECK("!%23$&'()*+,/:;=?@[]" == uri.encoded_fragment());
        
        //std::cout << uri << "\n";
    }
}

TEST_CASE("uri tests (https://en.wikipedia.org/wiki/Uniform_Resource_Identifier)")
{
    SECTION("https://john.doe@www.example.com:123/forum/questions/?tag=networking&order=newest#top")
    {
        std::string s = "https://john.doe@www.example.com:123/forum/questions/?tag=networking&order=newest#top";

        jsoncons::uri uri(s); 

        //std::cout << uri.string() << "\n";

        CHECK(uri.scheme() == jsoncons::string_view("https"));
        CHECK(uri.encoded_authority() == jsoncons::string_view("john.doe@www.example.com:123"));
        CHECK(uri.userinfo() == jsoncons::string_view("john.doe"));
        CHECK(uri.host() == jsoncons::string_view("www.example.com"));
        CHECK(uri.port() == jsoncons::string_view("123"));
        CHECK(uri.path() == jsoncons::string_view("/forum/questions/"));
        CHECK(uri.encoded_query() == jsoncons::string_view("tag=networking&order=newest"));
        CHECK(uri.encoded_fragment() == jsoncons::string_view("top"));
        CHECK(uri.base().string() == "https://john.doe@www.example.com:123/forum/questions/");
        CHECK(uri.is_absolute());
    }
    SECTION("ldap://[2001:db8::7]/c=GB?objectClass?one")
    {
        std::string s = "ldap://[2001:db8::7]/c=GB?objectClass?one";

        jsoncons::uri uri(s); 

        CHECK(uri.scheme() == jsoncons::string_view("ldap"));
        CHECK(uri.encoded_authority() == jsoncons::string_view("2001:db8::7"));
        CHECK(uri.userinfo() == jsoncons::string_view(""));
        CHECK(uri.host() == jsoncons::string_view("2001:db8::7"));
        CHECK(uri.port() == jsoncons::string_view(""));
        CHECK(uri.encoded_path() == jsoncons::string_view("/c=GB"));
        CHECK(uri.encoded_query() == jsoncons::string_view("objectClass?one"));
        CHECK(uri.encoded_fragment() == jsoncons::string_view(""));
        CHECK(uri.is_absolute());
    }
    SECTION("mailto:John.Doe@example.com")
    {
        std::string s = "mailto:John.Doe@example.com";

        jsoncons::uri uri(s); 

        CHECK(uri.scheme() == jsoncons::string_view("mailto"));
        CHECK(uri.encoded_authority() == jsoncons::string_view(""));
        CHECK(uri.userinfo() == jsoncons::string_view(""));
        CHECK(uri.host() == jsoncons::string_view(""));
        CHECK(uri.port() == jsoncons::string_view(""));
        CHECK(uri.encoded_path() == jsoncons::string_view("John.Doe@example.com"));
        CHECK(uri.encoded_query() == jsoncons::string_view(""));
        CHECK(uri.encoded_fragment() == jsoncons::string_view(""));
        CHECK(uri.is_absolute());
    }
    SECTION("news:comp.infosystems.www.servers.unix")
    {
        std::string s = "news:comp.infosystems.www.servers.unix";

        jsoncons::uri uri(s); 

        CHECK(uri.scheme() == jsoncons::string_view("news"));
        CHECK(uri.encoded_authority() == jsoncons::string_view(""));
        CHECK(uri.userinfo() == jsoncons::string_view(""));
        CHECK(uri.host() == jsoncons::string_view(""));
        CHECK(uri.port() == jsoncons::string_view(""));
        CHECK(uri.encoded_path() == jsoncons::string_view("comp.infosystems.www.servers.unix"));
        CHECK(uri.encoded_query() == jsoncons::string_view(""));
        CHECK(uri.encoded_fragment() == jsoncons::string_view(""));
        CHECK(uri.is_absolute());
    }
    SECTION("tel:+1-816-555-1212")
    {
        std::string s = "tel:+1-816-555-1212";

        jsoncons::uri uri(s); 

        CHECK(uri.scheme() == jsoncons::string_view("tel"));
        CHECK(uri.encoded_authority() == jsoncons::string_view(""));
        CHECK(uri.userinfo() == jsoncons::string_view(""));
        CHECK(uri.host() == jsoncons::string_view(""));
        CHECK(uri.port() == jsoncons::string_view(""));
        CHECK(uri.encoded_path() == jsoncons::string_view("+1-816-555-1212"));
        CHECK(uri.encoded_query() == jsoncons::string_view(""));
        CHECK(uri.encoded_fragment() == jsoncons::string_view(""));
        CHECK(uri.is_absolute());
    }
    SECTION("telnet://192.0.2.16:80/")
    {
        std::string s = "telnet://192.0.2.16:80/";

        jsoncons::uri uri(s); 

        CHECK(uri.scheme() == jsoncons::string_view("telnet"));
        CHECK(uri.encoded_authority() == jsoncons::string_view("192.0.2.16:80"));
        CHECK(uri.userinfo() == jsoncons::string_view(""));
        CHECK(uri.host() == jsoncons::string_view("192.0.2.16"));
        CHECK(uri.port() == jsoncons::string_view("80"));
        CHECK(uri.encoded_path() == jsoncons::string_view("/"));
        CHECK(uri.encoded_query() == jsoncons::string_view(""));
        CHECK(uri.encoded_fragment() == jsoncons::string_view(""));
        CHECK(uri.is_absolute());
    }
    SECTION("urn:oasis:names:specification:docbook:dtd:xml:4.1.2")
    {
        std::string s = "urn:oasis:names:specification:docbook:dtd:xml:4.1.2";

        jsoncons::uri uri(s); 

        CHECK(uri.scheme() == jsoncons::string_view("urn"));
        CHECK(uri.encoded_authority() == jsoncons::string_view(""));
        CHECK(uri.userinfo() == jsoncons::string_view(""));
        CHECK(uri.host() == jsoncons::string_view(""));
        CHECK(uri.port() == jsoncons::string_view(""));
        CHECK(uri.encoded_path() == jsoncons::string_view("oasis:names:specification:docbook:dtd:xml:4.1.2"));
        CHECK(uri.encoded_query() == jsoncons::string_view(""));
        CHECK(uri.encoded_fragment() == jsoncons::string_view(""));
        CHECK(uri.is_absolute());
    }
    SECTION("urn:example:foo-bar-baz-qux?+CCResolve:cc=uk")
    {
        std::string s = "urn:example:foo-bar-baz-qux?+CCResolve:cc=uk";

        jsoncons::uri uri(s); 

        CHECK(uri.scheme() == jsoncons::string_view("urn"));
        CHECK(uri.encoded_authority() == jsoncons::string_view(""));
        CHECK(uri.userinfo() == jsoncons::string_view(""));
        CHECK(uri.host() == jsoncons::string_view(""));
        CHECK(uri.port() == jsoncons::string_view(""));
        CHECK(uri.encoded_path() == jsoncons::string_view("example:foo-bar-baz-qux"));
        CHECK(uri.encoded_query() == jsoncons::string_view("+CCResolve:cc=uk"));
        CHECK(uri.encoded_fragment() == jsoncons::string_view(""));
        CHECK(uri.is_absolute());
    }
}

TEST_CASE("uri fragment tests")
{
    SECTION("#/definitions/nonNegativeInteger")
    {
        std::string s = "#/definitions/nonNegativeInteger";

        jsoncons::uri uri(s); 

        CHECK(uri.scheme().empty());
        CHECK(uri.encoded_authority().empty());
        CHECK(uri.userinfo().empty());
        CHECK(uri.host().empty());
        CHECK(uri.port().empty());
        CHECK(uri.encoded_path().empty());
        CHECK(uri.encoded_query().empty());
        CHECK(uri.encoded_fragment() == jsoncons::string_view("/definitions/nonNegativeInteger"));
        CHECK(!uri.is_absolute());
    }
}

TEST_CASE("uri base tests")
{
    SECTION("http://json-schema.org/draft-07/schema#")
    {
        std::string s = "http://json-schema.org/draft-07/schema#";

        jsoncons::uri uri(s); 

        CHECK(uri.scheme() == jsoncons::string_view("http"));
        CHECK(uri.encoded_authority() == jsoncons::string_view("json-schema.org"));
        CHECK(uri.userinfo().empty());
        CHECK(uri.host() == jsoncons::string_view("json-schema.org"));
        CHECK(uri.port().empty());
        CHECK(uri.encoded_path() == jsoncons::string_view("/draft-07/schema"));
        CHECK(uri.encoded_query().empty());
        CHECK(uri.encoded_fragment().empty());
        CHECK(uri.is_absolute());
    }
    SECTION("folder/")
    {
        std::string s = "folder/";

        jsoncons::uri uri(s); 

        CHECK(uri.scheme().empty());
        CHECK(uri.encoded_authority().empty());
        CHECK(uri.userinfo().empty());
        CHECK(uri.host().empty());
        CHECK(uri.port().empty());
        CHECK(uri.encoded_path() == jsoncons::string_view("folder/"));
        CHECK(uri.encoded_query().empty());
        CHECK(uri.encoded_fragment().empty());
        CHECK(!uri.is_absolute());
    }
    SECTION("name.json#/definitions/orNull")
    {
        std::string s = "name.json#/definitions/orNull";

        jsoncons::uri uri(s); 

        CHECK(uri.scheme().empty());
        CHECK(uri.encoded_authority().empty());
        CHECK(uri.userinfo().empty());
        CHECK(uri.host().empty());
        CHECK(uri.port().empty());
        CHECK(uri.encoded_path() == jsoncons::string_view("name.json"));
        CHECK(uri.encoded_query().empty());
        CHECK(uri.encoded_fragment() == jsoncons::string_view("/definitions/orNull"));
        CHECK(!uri.is_absolute());
    }
}

TEST_CASE("uri resolve tests")
{
    /*SECTION("empty base")
    {
        jsoncons::uri base{ "" };
        jsoncons::uri rel{"dir1/other.schema.json"};
        jsoncons::uri uri = rel.resolve(base);
        CHECK(uri.base().string() == "dir1/other.schema.json");
        CHECK(uri.path() == "dir1/other.schema.json");
    }*/

    SECTION("base has no authority and no path")
    {
        jsoncons::uri base{ "https" };
        jsoncons::uri rel{ "dir1/other.schema.json" };
        jsoncons::uri uri = base.resolve(rel);
        CHECK(uri.base().string() == "dir1/other.schema.json");
        CHECK(uri.path() == "dir1/other.schema.json");
    }

    SECTION("base has authority and path")
    {
        jsoncons::uri base{ "https://root" };
        jsoncons::uri rel{"dir1/other.schema.json"};
        jsoncons::uri uri = base.resolve(rel);
        CHECK(uri.base().string() == "https://root/dir1/other.schema.json");
        CHECK(uri.path() == "/dir1/other.schema.json");
    }    
    SECTION("folder/")
    {
        jsoncons::uri base_uri("http://localhost:1234/scope_change_defs2.json"); 
        jsoncons::uri relative_uri("folder/");
        
        jsoncons::uri uri =  base_uri.resolve(relative_uri);

        CHECK(uri.scheme() == jsoncons::string_view("http"));
        CHECK(uri.encoded_authority() == jsoncons::string_view("localhost:1234"));
        CHECK(uri.userinfo().empty());
        CHECK(uri.host() == jsoncons::string_view("localhost"));
        CHECK(uri.port() == jsoncons::string_view("1234"));
        CHECK(uri.encoded_path() == jsoncons::string_view("/folder/"));
        CHECK(uri.encoded_query().empty());
        CHECK(uri.encoded_fragment().empty());
        CHECK(uri.is_absolute());
    }

    SECTION("folderInteger.json")
    {
        jsoncons::uri base_uri("http://localhost:1234/folder/"); 
        jsoncons::uri relative_uri("folderInteger.json");

        jsoncons::uri uri =  base_uri.resolve(relative_uri);

        CHECK(uri.scheme() == jsoncons::string_view("http"));
        CHECK(uri.encoded_authority() == jsoncons::string_view("localhost:1234"));
        CHECK(uri.userinfo().empty());
        CHECK(uri.host() == jsoncons::string_view("localhost"));
        CHECK(uri.port() == jsoncons::string_view("1234"));
        CHECK(uri.encoded_path() == jsoncons::string_view("/folder/folderInteger.json"));
        CHECK(uri.encoded_query().empty());
        CHECK(uri.encoded_fragment().empty());
        CHECK(uri.is_absolute());
    }
}

TEST_CASE("uri part decode tests")
{
    SECTION("test 1")
    {
        std::string raw = "%7e";
        std::string expected = "~";

        std::string decoded = jsoncons::uri::decode_part(raw);
        CHECK(expected == decoded);
    }
    SECTION("test 2")
    {
        std::string raw = "%25";
        std::string expected = "%";

        std::string decoded = jsoncons::uri::decode_part(raw);
        CHECK(expected == decoded);
    }
    SECTION("test 3")
    {
        std::string raw = "foo%25bar%7ebaz";
        std::string expected = "foo%bar~baz";

        std::string decoded = jsoncons::uri::decode_part(raw);
        CHECK(expected == decoded);
    }
}

TEST_CASE("uri part encode tests")
{
    SECTION("test 1")
    {
        std::string part = "/@_-!.~'()*azAZ09,;:$&+=%3F%ae";
        std::string expected = part;

        std::string encoded;
        jsoncons::uri::encode_path(part, encoded);
        CHECK(expected == encoded);
    }
    
    SECTION("test 2")
    {
        std::string part = "%?/[]@,;:$&+=";
        std::string expected = "%25%3F/%5B%5D@,;:$&+=";

        std::string encoded;
        jsoncons::uri::encode_path(part, encoded);
        CHECK(expected == encoded);
    }
}

TEST_CASE("uri part encode illegal characters tests")
{
    SECTION("test 1")
    {
        std::string part = "_-!.~'()*azAZ09?/[]@,;:$&+=%3F%ae";
        std::string expected = part;

        std::string encoded;
        jsoncons::uri::encode_illegal_characters(part, encoded);
        CHECK(expected == encoded);
    }
}

TEST_CASE("uri constructors")
{
    SECTION("test 1")
    {
        jsoncons::uri x{"http://localhost:4242/draft2019-09/recursiveRef6/base.json"};

        jsoncons::uri y{ x, jsoncons::uri_fragment_part, "/anyOf" };

        jsoncons::uri expected{"http://localhost:4242/draft2019-09/recursiveRef6/base.json#/anyOf"};

        CHECK(expected == y);       
    }
}


TEST_CASE("uri parsing tests")
{
    SECTION("an invalid URI with spaces")
    {
        std::string str = "http://shouldfail.com";
        
        std::error_code ec;
        jsoncons::uri id = jsoncons::uri::parse(str, ec);
        
        /*std::cout << "authority: [" << id.encoded_authority() << "]\n";
        std::cout << "host: [" << id.host() << "]\n";
        std::cout << "port: [" << id.port() << "]\n";
        std::cout << "path: [" << id.encoded_path() << "]\n";
        */

        //CHECK(ec);       
    }
}

TEST_CASE("cpp-netlib uri tests")
{
    SECTION("test_empty_path")
    {
        std::error_code ec;
        jsoncons::uri uri = jsoncons::uri::parse("http://123.34.23.56", ec);
        REQUIRE_FALSE(ec);
        CHECK(uri.encoded_path().empty());
    }
    SECTION("test_empty_path_with_query")
    {
        std::error_code ec;
        jsoncons::uri uri = jsoncons::uri::parse("http://123.34.23.56?query", ec);
        REQUIRE_FALSE(ec);
        CHECK(uri.encoded_path().empty());
    }
    SECTION("test_empty_path_with_fragment")
    {
        std::error_code ec;
        jsoncons::uri uri = jsoncons::uri::parse("http://123.34.23.56#fragment", ec);
        REQUIRE_FALSE(ec);
        CHECK(uri.encoded_path().empty());
    }
    SECTION("test_single_slash")
    {
        std::error_code ec;
        jsoncons::uri uri = jsoncons::uri::parse("http://123.34.23.56/", ec);
        REQUIRE_FALSE(ec);
        CHECK("/" == uri.encoded_path());
    }
    SECTION("test_single_slash_with_query")
    {
        std::error_code ec;
        jsoncons::uri uri = jsoncons::uri::parse("http://123.34.23.56/?query", ec);
        REQUIRE_FALSE(ec);
        CHECK("/" == uri.encoded_path());
    }
    SECTION("test_single_slash_with_fragment")
    {
        std::error_code ec;
        jsoncons::uri uri = jsoncons::uri::parse("http://123.34.23.56/#fragment", ec);
        REQUIRE_FALSE(ec);
        CHECK("/" == uri.encoded_path());
    }
    SECTION("test_double_slash_empty_path_empty_everything")
    {
        std::error_code ec;
        jsoncons::uri uri = jsoncons::uri::parse("file://", ec);
        REQUIRE(ec == jsoncons::uri_errc::invalid_uri);
        CHECK(uri.encoded_path().empty());
    }
    SECTION("test_triple_slash_empty_everything")
    {
        std::error_code ec;
        jsoncons::uri uri = jsoncons::uri::parse("file:///", ec);
        REQUIRE_FALSE(ec);
        CHECK("/" == uri.encoded_path());
    }
    SECTION("test_triple_slash_with_path_name")
    {
        std::error_code ec;
        jsoncons::uri uri = jsoncons::uri::parse("file:///path", ec);
        REQUIRE_FALSE(ec);
        CHECK("/path" == uri.encoded_path());
    }
    SECTION("test_rootless_1")
    {
        std::error_code ec;
        jsoncons::uri uri = jsoncons::uri::parse("mailto:john.doe@example.com", ec);
        REQUIRE_FALSE(ec);
        CHECK("john.doe@example.com" == uri.encoded_path());
    }
    SECTION("test_invalid_characters_in_path")
    {
        std::error_code ec;
        jsoncons::uri uri = jsoncons::uri::parse("mailto:jo%hn.doe@example.com", ec);
        REQUIRE(ec);
        REQUIRE(jsoncons::uri_errc::invalid_character_in_path == ec);
        //std::cout << ec.message() << "\n";
    }
    SECTION("test_invalid_percent_encoded_characters_in_path_1")
    {
        std::error_code ec;
        jsoncons::uri uri = jsoncons::uri::parse("mailto:john.doe@example%G0.com", ec);
        REQUIRE(ec);
        REQUIRE(jsoncons::uri_errc::invalid_character_in_path == ec);
        //std::cout << ec.message() << "\n";
    }
    SECTION("test_invalid_percent_encoded_characters_in_path_2")
    {
        std::error_code ec;
        jsoncons::uri uri = jsoncons::uri::parse("mailto:john.doe@example%0G.com", ec);
        REQUIRE(ec);
        REQUIRE(jsoncons::uri_errc::invalid_character_in_path == ec);
        //std::cout << ec.message() << "\n";
    }
}

TEST_CASE("cpp-netib uri resolve tests")
{
    jsoncons::uri base_uri{"http://a/b/c/d;p?q"};

    SECTION("is_absolute_uri__returns_other")
    {
        jsoncons::uri reference{"https://www.example.com/"};
        auto uri = base_uri.resolve(reference);
        CHECK("https://www.example.com/" == uri.string());
    }
    SECTION("base_has_empty_path__path_is_ref_path_1")
    {
        jsoncons::uri reference{"g"};
        jsoncons::uri base{"http://a/"};
        auto uri = base.resolve(reference);
        CHECK("http://a/g" == uri.string());
    }

    SECTION("base_has_empty_path__path_is_ref_path_2")
    {
        jsoncons::uri reference{"g/x/y?q=1#s"};
        jsoncons::uri base{"http://a/"};
        auto uri = base.resolve(reference);
        CHECK(uri.encoded_query() == jsoncons::string_view("q=1"));
        CHECK("http://a/g/x/y?q=1#s" == uri.string());
    }

    SECTION("remove_dot_segments1")
    {
        jsoncons::uri reference{"./g"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g" == uri.string());
    }

    SECTION("base_has_path__path_is_merged_1")
    {
        jsoncons::uri reference{"g/"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g/" == uri.string());
    }
    SECTION("base_has_path__path_is_merged_2")
    {
        jsoncons::uri reference{"g"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g" == uri.string());
    }
    SECTION("path_starts_with_slash__path_is_ref_path")
    {
        jsoncons::uri reference{"/g"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/g" == uri.string());
    }
    SECTION("path_starts_with_slash_with_query_fragment__path_is_ref_path")
    {
        jsoncons::uri reference{ "/g/x?y=z#s" };
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/g/x?y=z#s" == uri.string());
    }
    SECTION("path_is_empty_but_has_query__returns_base_with_ref_query")
    {
        jsoncons::uri reference{ "?y=z" };
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/d;p?y=z" == uri.string());
    }
    SECTION("path_is_empty_but_has_query_base_no_query__returns_base_with_ref_query")
    {
        jsoncons::uri reference{ "?y=z" };
        jsoncons::uri base{"http://a/b/c/d"};
        auto uri = base.resolve(reference);
        CHECK("http://a/b/c/d?y=z" == uri.string());
    }
    SECTION("merge_path_with_query")
    {
        jsoncons::uri reference{ "g?y=z" };
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g?y=z" == uri.string());
    }
    SECTION("append_fragment")
    {
        jsoncons::uri reference{ "#s" };
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/d;p?q#s" == uri.string());
    }
    SECTION("merge_paths_with_fragment")
    {
        jsoncons::uri reference{ "g#s" };
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g#s" == uri.string());
    }
    SECTION("merge_paths_with_query_and_fragment")
    {
        jsoncons::uri reference{ "g?y=z#s" };
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g?y=z#s" == uri.string());
    }
    SECTION("merge_paths_with_semicolon_1")
    {
        jsoncons::uri reference{ ";x" };
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/;x" == uri.string());
    }
    SECTION("merge_paths_with_semicolon_2")
    {
        jsoncons::uri reference{ "g;x" };
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g;x" == uri.string());
    }
    SECTION("merge_paths_with_semicolon_3")
    {
        jsoncons::uri reference{ "g;x?y=z#s" };
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g;x?y=z#s" == uri.string());
    }
    SECTION("abnormal_example_1")
    {
        jsoncons::uri reference{"../../../g"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/g" == uri.string());
    }
    SECTION("abnormal_example_2")
    {
        jsoncons::uri reference{"../../../../g"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/g" == uri.string());
    }
    SECTION("abnormal_example_3")
    {
        jsoncons::uri reference{"/./g"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/g" == uri.string());
    }
    SECTION("abnormal_example_4")
    {
        jsoncons::uri reference{"/../g"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/g" == uri.string());
    }
    SECTION("abnormal_example_5")
    {
        jsoncons::uri reference{"g."};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g." == uri.string());
    }
    SECTION("abnormal_example_6")
    {
        jsoncons::uri reference{".g"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/.g" == uri.string());
    }
    SECTION("abnormal_example_7")
    {
        jsoncons::uri reference{"g.."};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g.." == uri.string());
    }
    SECTION("abnormal_example_8")
    {
        jsoncons::uri reference{"..g"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/g" == uri.string());
    }
    SECTION("abnormal_example_9")
    {
        jsoncons::uri reference{"./../g"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/g" == uri.string());
    }
    SECTION("abnormal_example_10")
    {
        jsoncons::uri reference{"./g/."};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g/" == uri.string());
    }
    SECTION("abnormal_example_11")
    {
        jsoncons::uri reference{"g/./h"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g/h" == uri.string());
    }
    SECTION("abnormal_example_12")
    {
        jsoncons::uri reference{"g/../h"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/h" == uri.string());
    }
    SECTION("abnormal_example_13")
    {
        jsoncons::uri reference{"g;x=1/./y"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g;x=1/y" == uri.string());
    }
    SECTION("abnormal_example_14")
    {
        jsoncons::uri reference{"g;x=1/../y"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/y" == uri.string());
    }
    SECTION("abnormal_example_15")
    {
        jsoncons::uri reference{"g?y/./x"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g?y/./x" == uri.string());
    }
    SECTION("abnormal_example_16")
    {
        jsoncons::uri reference{"g?y/../x"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g?y/../x" == uri.string());
    }
    SECTION("abnormal_example_17")
    {
        jsoncons::uri reference{"g#s/./x"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g#s/./x" == uri.string());
    }
    SECTION("abnormal_example_18")
    {
        jsoncons::uri reference{"g#s/../x"};
        auto uri = base_uri.resolve(reference);
        CHECK("http://a/b/c/g#s/../x" == uri.string());
    }
}


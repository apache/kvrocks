#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <cstdlib> //std::system
#include <jsoncons/json.hpp>
#include <scoped_allocator>

using shmem_allocator = boost::interprocess::allocator<int,
    boost::interprocess::managed_shared_memory::segment_manager>;

using cust_allocator = std::scoped_allocator_adaptor<shmem_allocator>;

struct boost_sorted_policy 
{
    template <typename KeyT,typename Json>
    using object = jsoncons::sorted_json_object<KeyT,Json,boost::interprocess::vector>;

    template <typename Json>
    using array = jsoncons::json_array<Json,boost::interprocess::vector>;

    template <typename CharT,typename CharTraits,typename Allocator>
    using member_key = boost::interprocess::basic_string<CharT, CharTraits, Allocator>;
};

using cust_json = jsoncons::basic_json<char,boost_sorted_policy, cust_allocator>;

int main(int argc, char *argv[])
{
   typedef std::pair<double, int> MyType;

   if (argc == 1) // Parent process
   {  
      //Remove shared memory on construction and destruction
      struct shm_remove
      {
         shm_remove() { boost::interprocess::shared_memory_object::remove("MySharedMemory"); }
         ~shm_remove() noexcept { boost::interprocess::shared_memory_object::remove("MySharedMemory"); }
      } remover;

      //Construct managed shared memory
      boost::interprocess::managed_shared_memory segment(boost::interprocess::create_only, 
                                                         "MySharedMemory", 65536);

      //Initialize shared memory STL-compatible allocator
      const shmem_allocator alloc(segment.get_segment_manager());

      // Create json value with all dynamic allocations in shared memory

      cust_json* j = segment.construct<cust_json>("MyJson")(jsoncons::json_array_arg, alloc);
      j->push_back(10);

      cust_json o(jsoncons::json_object_arg, alloc);
      o.try_emplace("category", "reference");
      o.try_emplace("author", "Nigel Rees");
      o.insert_or_assign("title", "Sayings of the Century");
      o.insert_or_assign("price", 8.95);

      j->push_back(o);

      cust_json a(jsoncons::json_array_arg, 2,  cust_json(jsoncons::json_object_arg, alloc), 
          jsoncons::semantic_tag::none, alloc);
      a[0]["first"] = 1;

      j->push_back(a);

      std::pair<cust_json*, boost::interprocess::managed_shared_memory::size_type> res;
      res = segment.find<cust_json>("MyJson");

      std::cout << "Parent process:\n";
      std::cout << pretty_print(*(res.first)) << "\n\n";

      //Launch child process
      std::string s(argv[0]); s += " child ";
      if (0 != std::system(s.c_str()))
         return 1;

      //Check child has destroyed all objects
      if (segment.find<MyType>("MyJson").first)
         return 1;
   }
   else // Child process
   {
      //Open managed shared memory
      boost::interprocess::managed_shared_memory segment(boost::interprocess::open_only, 
          "MySharedMemory");

      std::pair<cust_json*, boost::interprocess::managed_shared_memory::size_type> res;
      res = segment.find<cust_json>("MyJson");

      if (res.first != nullptr)
      {
          std::cout << "Child process:\n";
          std::cout << pretty_print(*(res.first)) << "\n";
      }
      else
      {
          std::cout << "Result is null\n";
      }

      //We're done, delete all the objects
      segment.destroy<cust_json>("MyJson");
   }
   return 0;
}


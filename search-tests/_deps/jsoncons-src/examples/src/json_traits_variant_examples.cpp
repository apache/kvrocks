// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <cassert>
#include <string>
#include <vector>
#include <list>
#include <iomanip>
#include <jsoncons/json.hpp>

#if defined(JSONCONS_HAS_STD_VARIANT)

namespace { 
namespace ns {

    enum class Color {yellow, red, green, blue};

    inline
    std::ostream& operator<<(std::ostream& os, Color val)
    {
        switch (val)
        {
            case Color::yellow: os << "yellow"; break;
            case Color::red: os << "red"; break;
            case Color::green: os << "green"; break;
            case Color::blue: os << "blue"; break;
        }
        return os;
    }

    class Fruit 
    {
    private:
        JSONCONS_TYPE_TRAITS_FRIEND
        std::string name_;
        Color color_;
    public:
        friend std::ostream& operator<<(std::ostream& os, const Fruit& val)
        {
            os << "name: " << val.name_ << ", color: " << val.color_ << "\n";
            return os;
        }
    };

    class Fabric 
    {
    private:
        JSONCONS_TYPE_TRAITS_FRIEND
        int size_;
        std::string material_;
    public:
        friend std::ostream& operator<<(std::ostream& os, const Fabric& val)
        {
            os << "size: " << val.size_ << ", material: " << val.material_ << "\n";
            return os;
        }
    };

    class Basket 
    {
    private:
        JSONCONS_TYPE_TRAITS_FRIEND
        std::string owner_;
        std::vector<std::variant<Fruit, Fabric>> items_;

    public:
        std::string owner() const
        {
            return owner_;
        }

        std::vector<std::variant<Fruit, Fabric>> items() const
        {
            return items_;
        }
    };

    class Rectangle
    {
        double height_;
        double width_;
    public:
        Rectangle(double height, double width)
            : height_(height), width_(width)
        {
        }

        double height() const
        {
            return height_;
        }

        double width() const
        {
            return width_;
        }

        double area() const
        {
            return height_ * width_;
        }
    };

    class Triangle
    { 
        double height_;
        double width_;

    public:
        Triangle(double height, double width)
            : height_(height), width_(width)
        {
        }

        double height() const
        {
            return height_;
        }

        double width() const
        {
            return width_;
        }

        double area() const
        {
            return (height_ * width_)/2.0;
        }
    };                 

    class Circle
    { 
        double radius_;

    public:
        Circle(double radius)
            : radius_(radius)
        {
        }

        double radius() const
        {
            return radius_;
        }

        double area() const
        {
            constexpr double pi = 3.14159265358979323846;
            return pi*radius_*radius_;
        }
    };                 

    inline constexpr auto rectangle_marker = [](double) noexcept {return "rectangle"; };
    inline constexpr auto triangle_marker = [](double) noexcept {return "triangle";};
    inline constexpr auto circle_marker = [](double) noexcept {return "circle";};

} // ns
} // namespace

JSONCONS_ENUM_NAME_TRAITS(ns::Color, (yellow, "YELLOW"), (red, "RED"), (green, "GREEN"), (blue, "BLUE"))

JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Fruit,
                                (name_, "name"),
                                (color_, "color"))
JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Fabric,
                                (size_, "size"),
                                (material_, "material"))
JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Basket,
                                (owner_, "owner"),
                                (items_, "items"))

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Rectangle,
    (height,"type",JSONCONS_RDONLY,
     [](const std::string& type) noexcept{return type == "rectangle";},
     ns::rectangle_marker),
    (height, "height"),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Triangle,
    (height,"type", JSONCONS_RDONLY, 
     [](const std::string& type) noexcept {return type == "triangle";},
     ns::triangle_marker),
    (height, "height"),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Circle,
    (radius,"type", JSONCONS_RDONLY, 
     [](const std::string& type) noexcept {return type == "circle";},
     ns::circle_marker),
    (radius, "radius")
)

void variant_example()
{
    std::string input = R"(
{
  "owner": "Rodrigo",
  "items": [
    {
      "name": "banana",
      "color": "YELLOW"
    },
    {
      "size": 40,
      "material": "wool"
    },
    {
      "name": "apple",
      "color": "RED"
    },
    {
      "size": 40,
      "material": "cotton"
    }
  ]
}
    )";

    ns::Basket basket = jsoncons::decode_json<ns::Basket>(input);
    std::cout << basket.owner() << "\n\n";

    std::cout << "(1)\n";
    for (const auto& var : basket.items()) 
    {
        std::visit([](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, ns::Fruit>)
                std::cout << "Fruit " << arg << '\n';
            else if constexpr (std::is_same_v<T, ns::Fabric>)
                std::cout << "Fabric " << arg << '\n';
        }, var);
    }

    std::string output;
    jsoncons::encode_json(basket, output, jsoncons::indenting::indent);
    std::cout << "(2)\n" << output << "\n\n";
}

void variant_example2()
{
    using variant_type  = std::variant<int, double, bool, std::string, ns::Color>;

    std::vector<variant_type> vars = {100, 10.1, false, std::string("Hello World"), ns::Color::yellow};

    std::string buffer;
    jsoncons::encode_json(vars, buffer, jsoncons::indenting::indent);

    std::cout << "(1)\n" << buffer << "\n\n";

    auto vars2 = jsoncons::decode_json<std::vector<variant_type>>(buffer);

    auto visitor = [](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, int>)
                std::cout << "int " << arg << '\n';
            else if constexpr (std::is_same_v<T, double>)
                std::cout << "double " << arg << '\n';
            else if constexpr (std::is_same_v<T, bool>)
                std::cout << "bool " << arg << '\n';
            else if constexpr (std::is_same_v<T, std::string>)
                std::cout << "std::string " << arg << '\n';
            else if constexpr (std::is_same_v<T, ns::Color>)
                std::cout << "ns::Color " << arg << '\n';
        };

    std::cout << "(2)\n";
    for (const auto& item : vars2)
    {
        std::visit(visitor, item);
    }
    std::cout << "\n";
}

void variant_example3()
{
    using variant_type  = std::variant<int, double, bool, ns::Color, std::string>;

    std::vector<variant_type> vars = {100, 10.1, false, std::string("Hello World"), ns::Color::yellow};

    std::string buffer;
    jsoncons::encode_json(vars, buffer, jsoncons::indenting::indent);

    std::cout << "(1)\n" << buffer << "\n\n";

    auto vars2 = jsoncons::decode_json<std::vector<variant_type>>(buffer);

    auto visitor = [](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, int>)
                std::cout << "int " << arg << '\n';
            else if constexpr (std::is_same_v<T, double>)
                std::cout << "double " << arg << '\n';
            else if constexpr (std::is_same_v<T, bool>)
                std::cout << "bool " << arg << '\n';
            else if constexpr (std::is_same_v<T, std::string>)
                std::cout << "std::string " << arg << '\n';
            else if constexpr (std::is_same_v<T, ns::Color>)
                std::cout << "ns::Color " << arg << '\n';
        };

    std::cout << "(2)\n";
    for (const auto& item : vars2)
    {
        std::visit(visitor, item);
    }
    std::cout << "\n";
}

void variant_example4()
{
    using variant_type = std::variant<std::nullptr_t, int, double, bool, std::string>;
    
    std::vector<variant_type> v = {nullptr, 10, 5.1, true, std::string("Hello World")}; 

    std::string buffer;
    jsoncons::encode_json_pretty(v, buffer);
    std::cout << "(1)\n" << buffer << "\n\n";

    auto v2 = jsoncons::decode_json<std::vector<variant_type>>(buffer);

    auto visitor = [](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, std::nullptr_t>)
                std::cout << "nullptr " << arg << '\n';
            else if constexpr (std::is_same_v<T, int>)
                std::cout << "int " << arg << '\n';
            else if constexpr (std::is_same_v<T, double>)
                std::cout << "double " << arg << '\n';
            else if constexpr (std::is_same_v<T, bool>)
                std::cout << "bool " << arg << '\n';
            else if constexpr (std::is_same_v<T, std::string>)
                std::cout << "std::string " << arg << '\n';
        };

    std::cout << "(2)\n";
    for (const auto& item : v2)
    {
        std::visit(visitor, item);
    }
}

void distinguish_by_type()
{
    using shapes_t = std::variant<ns::Rectangle,ns::Triangle,ns::Circle>;

    std::string input = R"(
[
    {"type" : "rectangle", "width" : 2.0, "height" : 1.5 },
    {"type" : "triangle", "width" : 4.0, "height" : 2.0 },
    {"type" : "circle", "radius" : 1.0 }
]
    )";

    auto shapes = jsoncons::decode_json<std::vector<shapes_t>>(input);

    auto visitor = [](auto&& shape) {
        using T = std::decay_t<decltype(shape)>;
        if constexpr (std::is_same_v<T, ns::Rectangle>)
            std::cout << "rectangle area: " << shape.area() << '\n';
        else if constexpr (std::is_same_v<T, ns::Triangle>)
            std::cout << "triangle area: " << shape.area() << '\n';
        else if constexpr (std::is_same_v<T, ns::Circle>)
            std::cout << "circle area: " << shape.area() << '\n';
    };

    std::cout << "(1)\n";
    for (const auto& shape : shapes)
    {
        std::visit(visitor, shape);
    }

    std::string output;
    jsoncons::encode_json(shapes, output, jsoncons::indenting::indent);
    std::cout << "\n(2)\n" << output << "\n";
}

#endif // defined(JSONCONS_HAS_STD_VARIANT)

int main()
{
    std::cout << "\njson traits variant examples\n\n";

#if defined(JSONCONS_HAS_STD_VARIANT)
    variant_example();
    variant_example4();
    variant_example2();
    variant_example3();
    distinguish_by_type();
#endif

    std::cout << '\n';
}


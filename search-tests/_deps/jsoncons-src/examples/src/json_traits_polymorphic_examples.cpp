// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <cassert>
#include <string>
#include <vector>
#include <list>
#include <iomanip>
#include <jsoncons/json.hpp>

namespace { 
namespace ns {

    class Shape
    {
    public:
        virtual ~Shape() = default;
        virtual double area() const = 0;
    };
      
    class Rectangle : public Shape
    {
        double height_;
        double width_;
    public:
        Rectangle(double height, double width)
            : height_(height), width_(width)
        {
        }

        const std::string& type() const
        {
            static const std::string type_ = "rectangle"; 
            return type_;
        }

        double height() const
        {
            return height_;
        }

        double width() const
        {
            return width_;
        }

        double area() const override
        {
            return height_ * width_;
        }
    };

    class Triangle : public Shape
    { 
        double height_;
        double width_;

    public:
        Triangle(double height, double width)
            : height_(height), width_(width)
        {
        }

        const std::string& type() const
        {
            static const std::string type_ = "triangle"; 
            return type_;
        }

        double height() const
        {
            return height_;
        }

        double width() const
        {
            return width_;
        }

        double area() const override
        {
            return (height_ * width_)/2.0;
        }
    };                 

    class Circle : public Shape
    { 
        double radius_;

    public:
        Circle(double radius)
            : radius_(radius)
        {
        }

        const std::string& type() const
        {
            static const std::string type_ = "circle"; 
            return type_;
        }

        double radius() const
        {
            return radius_;
        }

        double area() const override
        {
            constexpr double pi = 3.14159265358979323846;
            return pi*radius_*radius_;
        }
    };                 

} // namespace ns
} // namespace

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Rectangle,
    (type,"type",JSONCONS_RDONLY,[](const std::string& type) noexcept{return type == "rectangle";}),
    (height, "height"),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Triangle,
    (type,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "triangle";}),
    (height, "height"),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Circle,
    (type,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "circle";}),
    (radius, "radius")
)

JSONCONS_POLYMORPHIC_TRAITS(ns::Shape,ns::Rectangle,ns::Triangle,ns::Circle)

namespace 
{
    void distinguish_by_type_example()
    {
        std::string input = R"(
    [
        {"type" : "rectangle", "width" : 2.0, "height" : 1.5 },
        {"type" : "triangle", "width" : 4.0, "height" : 2.0 },
        {"type" : "circle", "radius" : 1.0 }
    ]
        )";

        auto shapes = jsoncons::decode_json<std::vector<std::unique_ptr<ns::Shape>>>(input);

        std::cout << "(1)\n";
        for (const auto& shape : shapes)
        {
            std::cout << typeid(*shape.get()).name() << " area: " << shape->area() << "\n";
        }

        std::string output;

        jsoncons::encode_json(shapes, output, jsoncons::indenting::indent);
        std::cout << "\n(2)\n" << output << "\n";
    }
}

int main()
{
    std::cout << "\njson traits polymorphic examples\n\n";

    distinguish_by_type_example();

    std::cout << '\n';
}


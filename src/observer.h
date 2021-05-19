#pragma once

#include <vector>
#include <iostream>
#include <algorithm>
#include <functional>
#include <unordered_map>
#include <typeindex>

class Observable;

class Observer {
 public:
  class Event {
   public:
    virtual ~Event() {}
  };
  virtual ~Observer() {}
  virtual void OnNotify(Observable const& subject, Observer::Event const& event) = 0;
};

using ObserverEvent = Observer::Event;

class Observable {
 public:
  virtual ~Observable() {}
  // void RegisterObserver(Observer& observer) {
  //   if (std::find(std::begin(observers), std::end(observers), &observer) == std::end(observers)) {
  //     observers.push_back(&observer);
  //   }
  // }
  void RegisterObserver(Observer* observer) {
    if (std::find(std::begin(observers), std::end(observers), observer) == std::end(observers)) {
      observers.push_back(observer);
    }
  }
  void UnregisterObserver(Observer* observer) {
    observers.erase(std::remove(std::begin(observers), std::end(observers), observer), std::end(observers));
  }
  void NotifyObservers(ObserverEvent const& event) {
    for (Observer* observer : observers) {
      observer->OnNotify(*this, event);
    }
  }
  int ObserverCount() { return observers.size(); }

 private:
  std::vector<Observer*> observers;
};

class EventHandler : public Observer {
 public:
  void OnNotify(Observable const& subject, ObserverEvent const& event) override {
    auto find = handlers.find(std::type_index(typeid(event)));
    if (find != handlers.end()) {
      find->second(subject, event);
    }
  }
  template<typename T, typename std::enable_if<std::is_base_of<ObserverEvent, T>::value>::type* = nullptr>
  void registerEventHandler(std::function<void(Observable const&, ObserverEvent const&)> handler) {
    handlers[std::type_index(typeid(T))] = handler;
  }

 private:
  std::unordered_map<std::type_index, std::function<void(Observable const&, ObserverEvent const&)>> handlers;
};

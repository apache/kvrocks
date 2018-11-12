/**
 *   tcpkit --  toolkit to analyze tcp packet
 *   Copyright (C) 2018  @git-hulk
 *
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 **/

#include "server.h"

Server::Server(Engine::Storage *storage, int port, int workers) {
  listen_port_ = port;
  for (int i = 0; i < workers; i++) {
    auto worker = new Worker(storage, static_cast<uint32_t>(port), nullptr);
    workers_.emplace_back(new WorkerThread(worker));
  }
}

void Server::Start() {
  for (const auto worker : workers_) {
    worker->Start();
  }
}

void Server::Stop() {
  for (const auto worker : workers_) {
    worker->Stop();
  }
}

void Server::Join() {
  for (const auto worker : workers_) {
    worker->Join();
  }
}
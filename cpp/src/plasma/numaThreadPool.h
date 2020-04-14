// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// this file is copied from https://github.com/progschj/ThreadPool

#ifndef NUMA_THREAD_POOL_H
#define NUMA_THREAD_POOL_H

#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

#include <numa.h>

namespace plasma {

class numaThreadPool {
public:
  numaThreadPool(int, size_t, std::vector<int>&);
  template <class F, class... Args>
  auto enqueue(F &&f, Args &&... args)
      -> std::future<typename std::result_of<F(Args...)>::type>;
  static void getNumaNodeCpu(int node, std::vector<int> &cpus);
  ~numaThreadPool();

private:
  // need to keep track of threads so we can join them
  std::vector<std::thread> workers;
  // the task queue
  std::queue<std::function<void()>> tasks;

  // synchronization
  std::mutex queue_mutex;
  std::condition_variable condition;
  bool stop;

  // numa
  int numaNode;
  // std::vector<int> cpus;
  int threads;
};

// the constructor just launches some amount of workers
inline numaThreadPool::numaThreadPool(int numaNode_, size_t threads_, std::vector<int>& cpus) : stop(false) {
  numaNode = numaNode_;
  // getNumaNodeCpu(numaNode, cpus);
  threads = (threads_ < cpus.size()) ? threads_ : cpus.size();
  std::cout << "a thread pool with " << threads << " threads" << std::endl;
  for (int i = 0; i < threads; ++i)
    workers.emplace_back([&, i] {
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(cpus[i], &cpuset);
      int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
      if(rc != 0 )
        std::cout<<"initial thread affinity failed!"<<std::endl;
      for (;;) {
        std::function<void()> task;

        {
          std::unique_lock<std::mutex> lock(this->queue_mutex);
          this->condition.wait(
              lock, [this] { return this->stop || !this->tasks.empty(); });
          if (this->stop && this->tasks.empty())
            return;
          task = std::move(this->tasks.front());
          this->tasks.pop();
        }

        task();
      }
    });
}

// add new work item to the pool
template <class F, class... Args>
auto numaThreadPool::enqueue(F &&f, Args &&... args)
    -> std::future<typename std::result_of<F(Args...)>::type> {
  using return_type = typename std::result_of<F(Args...)>::type;

  auto task = std::make_shared<std::packaged_task<return_type()>>(
      std::bind(std::forward<F>(f), std::forward<Args>(args)...));

  std::future<return_type> res = task->get_future();
  {
    std::unique_lock<std::mutex> lock(queue_mutex);

    // don't allow enqueueing after stopping the pool
    if (stop)
      throw std::runtime_error("enqueue on stopped numaThreadPool");

    tasks.emplace([task]() { (*task)(); });
  }
  condition.notify_one();
  return res;
}

// the destructor joins all threads
inline numaThreadPool::~numaThreadPool() {
  {
    std::unique_lock<std::mutex> lock(queue_mutex);
    stop = true;
  }
  condition.notify_all();
  for (std::thread &worker : workers)
    worker.join();
}

void numaThreadPool::getNumaNodeCpu(int node, std::vector<int> &cpus) {
  int i, err;
  struct bitmask *cpumask;

  cpumask = numa_allocate_cpumask();
  err = numa_node_to_cpus(node, cpumask);
  if (err >= 0) {
    for (i = 0; i < (int)cpumask->size; i++)
      if (numa_bitmask_isbitset(cpumask, i))
        cpus.push_back(i);
  }
}


} //namespace plasma
#endif

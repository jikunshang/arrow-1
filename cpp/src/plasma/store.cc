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

// PLASMA STORE: This is a simple object store server process
//
// It accepts incoming client connections on a unix domain socket
// (name passed in via the -s option of the executable) and uses a
// single thread to serve the clients. Each client establishes a
// connection and can create objects, wait for objects and seal
// objects through that connection.
//
// It keeps a hash table that maps object_ids (which are 20 byte long,
// just enough to store and SHA1 hash) to memory mapped files.

#include "plasma/store.h"

#include <getopt.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <cinttypes>
#ifdef __linux__
#include <sys/statvfs.h>
#endif
#include <cstring>
#include <ctime>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "plasma/common.h"
#include "plasma/malloc.h"
#include "plasma/plasma_allocator.h"
#include "plasma/plasma_generated.h"
#include "plasma/protocol.h"

#ifdef PLASMA_CUDA
#include "arrow/gpu/cuda_api.h"

using arrow::cuda::CudaBuffer;
using arrow::cuda::CudaContext;
using arrow::cuda::CudaDeviceManager;
#endif

using arrow::util::ArrowLog;
using arrow::util::ArrowLogLevel;

namespace plasma {

using flatbuf::MessageType;

void SetMallocGranularity(int value);

struct GetRequest {
  GetRequest(asio::io_context& io_context,
             const std::shared_ptr<ClientConnection>& client,
             const std::vector<ObjectID>& object_ids)
      : client(client),
        object_ids(object_ids.begin(), object_ids.end()),
        objects(object_ids.size()),
        num_satisfied(0),
        timer_(io_context) {
    std::unordered_set<ObjectID> unique_ids(object_ids.begin(), object_ids.end());
    num_objects_to_wait_for = unique_ids.size();
  }

  void ReturnFromGet() {
    // Figure out how many file descriptors we need to send.
    std::unordered_set<int> fds_to_send;
    std::vector<int> store_fds;
    std::vector<int64_t> mmap_sizes;
    for (const auto& object_id : object_ids) {
      PlasmaObject& object = objects[object_id];
      int fd = object.store_fd;
      if (object.data_size != -1 && fds_to_send.count(fd) == 0 && fd != -1) {
        fds_to_send.insert(fd);
        store_fds.push_back(fd);
        mmap_sizes.push_back(GetMmapSize(fd));
      }
    }

    // Send the get reply to the client.
    Status s = SendGetReply(client, &object_ids[0], objects, object_ids.size(), store_fds,
                            mmap_sizes);
    // If we successfully sent the get reply message to the client, then also send
    // the file descriptors.
    if (s.ok()) {
      // Send all of the file descriptors for the present objects.
      for (int store_fd : store_fds) {
        auto status = client->SendFd(store_fd);
        if (!status.ok()) {
          // TODO(suquark): Should we close the client here?
          ARROW_LOG(ERROR) << "Failed to send a mmap fd to client";
        }
      }
    }
  }

  void AsyncWait(int64_t timeout_ms, std::function<void(const error_code&)> on_timeout) {
    // Set an expiry time relative to now.
    timer_.expires_from_now(std::chrono::milliseconds(timeout_ms));
    timer_.async_wait(on_timeout);
  }

  void CancelTimer() { timer_.cancel(); }

  /// The client that called get.
  std::shared_ptr<ClientConnection> client;
  /// The object IDs involved in this request. This is used in the reply.
  std::vector<ObjectID> object_ids;
  /// The object information for the objects in this request. This is used in
  /// the reply.
  std::unordered_map<ObjectID, PlasmaObject> objects;
  /// The minimum number of objects to wait for in this request.
  int64_t num_objects_to_wait_for;
  /// The number of object requests in this wait request that are already
  /// satisfied.
  int64_t num_satisfied;

 private:
  /// The timer that will time out and cause this wait to return to
  /// the client if it hasn't already returned.
  asio::steady_timer timer_;
};

PlasmaStore::PlasmaStore(asio::io_context& io_context, std::string directory,
                         bool hugepages_enabled, const std::string& stream_name,
                         std::shared_ptr<ExternalStore> external_store)
    : eviction_policy_(&store_info_, PlasmaAllocator::GetFootprintLimit()),
      external_store_(external_store),
      io_context_(io_context),
      stream_name_(stream_name),
      acceptor_(io::CreateLocalAcceptor(io_context, stream_name)),
      stream_(io_context) {
  if (external_store_) {
    auto status = external_store_->RegisterEvictionPolicy(&eviction_policy_);
    if (!status.ok()) {
      ARROW_LOG(ERROR) << "RegisterEvictionPolicy failed" ;
    }
  }
  store_info_.directory = directory;
  store_info_.hugepages_enabled = hugepages_enabled;
  store_info_.objects.reserve(200000); //FIXME: try to use a thread safe hashmap
#ifdef PLASMA_CUDA
  auto maybe_manager = CudaDeviceManager::Instance();
  DCHECK_OK(maybe_manager.status());
  manager_ = *maybe_manager;
#endif
  // Start listening for clients.
  DoAccept();
}

// TODO(pcm): Get rid of this destructor by using RAII to clean up data.
PlasmaStore::~PlasmaStore() {}

const PlasmaStoreInfo* PlasmaStore::GetPlasmaStoreInfo() { return &store_info_; }

// If this client is not already using the object, add the client to the
// object's list of clients, otherwise do nothing.
void PlasmaStore::AddToClientObjectIds(const ObjectID& object_id, ObjectTableEntry* entry,
                                       const std::shared_ptr<ClientConnection>& client) {
  // Check if this client is already using the object.
  if (client->ObjectIDExists(object_id)) {
    return;
  }
  client->object_ids.insert(object_id);
}

// Allocate memory
uint8_t* PlasmaStore::AllocateMemory(size_t size, bool evict_if_full, int* fd,
                                     int64_t* map_size, ptrdiff_t* offset,
                                     const std::shared_ptr<ClientConnection>& client,
                                     bool is_create) {
  if (evict_if_full) {
    ;
  }

  // Try to evict objects until there is enough space.
  uint8_t* pointer = nullptr;
  int waitFlag = 0;
  auto tic = std::chrono::steady_clock::now();
  while (true) {
    // Allocate space for the new object. We use memalign instead of malloc
    // in order to align the allocated region to a 64-byte boundary. This is not
    // strictly necessary, but it is an optimization that could speed up the
    // computation of a hash of the data (see compute_object_hash_parallel in
    // plasma_client.cc). Note that even though this pointer is 64-byte aligned,
    // it is not guaranteed that the corresponding pointer in the client will be
    // 64-byte aligned, but in practice it often will be.
    pointer = reinterpret_cast<uint8_t*>(PlasmaAllocator::Memalign(kBlockSize, size));
    if (pointer || !evict_if_full) {
      // If we manage to allocate the memory, return the pointer. If we cannot
      // allocate the space, but we are also not allowed to evict anything to
      // make more space, return an error to the client.
      break;
    }
    waitFlag++;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    if (waitFlag > 500) break;
  }
  if (waitFlag) {
    if (waitFlag > 500)
      ARROW_LOG(WARNING) << "allocate failed!!!  current allocated size is "
                         << PlasmaAllocator::Allocated();
    auto toc = std::chrono::steady_clock::now();
    std::chrono::duration<double> time_ = toc - tic;
    ARROW_LOG(DEBUG) << "wait free space takes " << time_.count() * 1000 << " ms";
  }

  if (pointer != nullptr) {
    GetMallocMapinfo(pointer, fd, map_size, offset);
  }

  return pointer;
}

#ifdef PLASMA_CUDA
Status PlasmaStore::AllocateCudaMemory(
    int device_num, int64_t size, uint8_t** out_pointer,
    std::shared_ptr<CudaIpcMemHandle>* out_ipc_handle) {
  DCHECK_NE(device_num, 0);
  ARROW_ASSIGN_OR_RAISE(auto context, manager_->GetContext(device_num - 1));
  ARROW_ASSIGN_OR_RAISE(auto cuda_buffer, context->Allocate(static_cast<int64_t>(size)));
  *out_pointer = reinterpret_cast<uint8_t*>(cuda_buffer->address());
  // The IPC handle will keep the buffer memory alive
  return cuda_buffer->ExportForIpc().Value(out_ipc_handle);
}

Status PlasmaStore::FreeCudaMemory(int device_num, int64_t size, uint8_t* pointer) {
  ARROW_ASSIGN_OR_RAISE(auto context, manager_->GetContext(device_num - 1));
  RETURN_NOT_OK(context->Free(pointer, size));
  return Status::OK();
}
#endif

// Create a new object buffer in the hash table.
PlasmaError PlasmaStore::CreateObject(const ObjectID& object_id, bool evict_if_full,
                                      int64_t data_size, int64_t metadata_size,
                                      int device_num,
                                      const std::shared_ptr<ClientConnection>& client,
                                      PlasmaObject* result) {
  ARROW_LOG(DEBUG) << "creating object " << object_id.hex();

  auto entry = GetObjectTableEntry(&store_info_, object_id);
  if (entry != nullptr) {
    // There is already an object with the same ID in the Plasma Store, so
    // ignore this request.
    return PlasmaError::ObjectExists;
  }
  auto ptr = std::unique_ptr<ObjectTableEntry>(new ObjectTableEntry());
  {
    std::lock_guard<std::mutex> lock_guard(entry_mtx);
    entry = store_info_.objects.emplace(object_id, std::move(ptr)).first->second.get();
    ARROW_LOG(DEBUG) << "Table size is " << store_info_.objects.size();
  }
  entry->data_size = data_size;
  entry->metadata_size = metadata_size;

  int fd = -1;
  int64_t map_size = 0;
  ptrdiff_t offset = 0;
  uint8_t* pointer = nullptr;
  auto total_size = data_size + metadata_size;

  if (device_num == 0) {
    pointer =
        AllocateMemory(total_size, evict_if_full, &fd, &map_size, &offset, client, true);

    if (!pointer) {
      ARROW_LOG(ERROR) << "Not enough memory to create the object " << object_id.hex()
                       << ", data_size=" << data_size
                       << ", metadata_size=" << metadata_size
                       << ", will send a reply of PlasmaError::OutOfMemory";
      if (!external_store_) {
        std::vector<ObjectID> objects_to_evict;
        bool success = eviction_policy_.RequireSpace(total_size, &objects_to_evict);
        if (!success) {
          ARROW_LOG(ERROR) << "RequireSpace failed" ;
        }
        EvictObjects(objects_to_evict);
      }
      pointer = AllocateMemory(total_size, evict_if_full, &fd, &map_size, &offset, client,
                               true);
      if (!pointer) return PlasmaError::OutOfMemory;
    }
  } else {
#ifdef PLASMA_CUDA
    auto st = AllocateCudaMemory(device_num, total_size, &pointer, &entry->ipc_handle);
    if (!st.ok()) {
      ARROW_LOG(ERROR) << "Failed to allocate CUDA memory: " << st.ToString();
      return PlasmaError::OutOfMemory;
    }
    result->ipc_handle = entry->ipc_handle;
#else
    ARROW_LOG(ERROR) << "device_num != 0 but CUDA not enabled";
    return PlasmaError::OutOfMemory;
#endif
  }

  entry->pointer = pointer;
  // TODO(pcm): Set the other fields.
  entry->fd = fd;
  entry->map_size = map_size;
  entry->offset = offset;
  entry->state = ObjectState::PLASMA_CREATED;
  entry->device_num = device_num;
  entry->create_time = std::time(nullptr);
  entry->construct_duration = -1;
  entry->ref_count = 0;

  result->store_fd = fd;
  result->data_offset = offset;
  result->metadata_offset = offset + data_size;
  result->data_size = data_size;
  result->metadata_size = metadata_size;
  result->device_num = device_num;

  // AddToClientObjectIds(object_id, store_info_.objects[object_id].get(), client);
  AddToClientObjectIds(object_id, nullptr, client);
  IncreaseObjectRefCount(object_id, entry);

  return PlasmaError::OK;
}

void PlasmaObject_init(PlasmaObject* object, ObjectTableEntry* entry) {
  DCHECK(object != nullptr);
  DCHECK(entry != nullptr);
  DCHECK(entry->state == ObjectState::PLASMA_SEALED);
#ifdef PLASMA_CUDA
  if (entry->device_num != 0) {
    object->ipc_handle = entry->ipc_handle;
  }
#endif
  object->store_fd = entry->fd;
  object->data_offset = entry->offset;
  object->metadata_offset = entry->offset + entry->data_size;
  object->data_size = entry->data_size;
  object->metadata_size = entry->metadata_size;
  object->device_num = entry->device_num;
}

void PlasmaStore::RemoveGetRequest(GetRequest* get_request) {
  // Remove the get request from each of the relevant object_get_requests hash
  // tables if it is present there. It should only be present there if the get
  // request timed out or if it was issued by a client that has disconnected.
  for (ObjectID& object_id : get_request->object_ids) {
    auto object_request_iter = object_get_requests_.find(object_id);
    if (object_request_iter != object_get_requests_.end()) {
      auto& get_requests = object_request_iter->second;
      // Erase get_req from the vector.
      auto it = std::find(get_requests.begin(), get_requests.end(), get_request);
      if (it != get_requests.end()) {
        get_requests.erase(it);
        // If the vector is empty, remove the object ID from the map.
        if (get_requests.empty()) {
          object_get_requests_.erase(object_request_iter);
        }
      }
    }
  }
  // Remove the get request.
  get_request->CancelTimer();
  delete get_request;
}

void PlasmaStore::RemoveGetRequestsForClient(
    const std::shared_ptr<ClientConnection>& client) {
  std::unordered_set<GetRequest*> get_requests_to_remove;
  for (auto const& pair : object_get_requests_) {
    for (GetRequest* get_request : pair.second) {
      if (get_request->client == client) {
        get_requests_to_remove.insert(get_request);
      }
    }
  }

  // It shouldn't be possible for a given client to be in the middle of multiple get
  // requests.
  ARROW_CHECK(get_requests_to_remove.size() <= 1);
  for (GetRequest* get_request : get_requests_to_remove) {
    RemoveGetRequest(get_request);
  }
}

void PlasmaStore::ReturnFromGet(GetRequest* get_req) {
  get_req->ReturnFromGet();
  // Remove the get request from each of the relevant object_get_requests hash
  // tables if it is present there. It should only be present there if the get
  // request timed out.
  RemoveGetRequest(get_req);
}

void PlasmaStore::UpdateObjectGetRequests(const ObjectID& object_id) {
  auto it = object_get_requests_.find(object_id);
  // If there are no get requests involving this object, then return.
  if (it == object_get_requests_.end()) {
    return;
  }

  auto& get_requests = it->second;

  // After finishing the loop below, get_requests and it will have been
  // invalidated by the removal of object_id from object_get_requests_.
  size_t index = 0;
  size_t num_requests = get_requests.size();
  for (size_t i = 0; i < num_requests; ++i) {
    auto get_req = get_requests[index];
    auto entry = GetObjectTableEntry(&store_info_, object_id);
    ARROW_CHECK(entry != nullptr);

    PlasmaObject_init(&get_req->objects[object_id], entry);
    get_req->num_satisfied += 1;
    // Record the fact that this client will be using this object and will
    // be responsible for releasing this object.
    AddToClientObjectIds(object_id, entry, get_req->client);  // TODO: need?

    // If this get request is done, reply to the client.
    if (get_req->num_satisfied == get_req->num_objects_to_wait_for) {
      ReturnFromGet(get_req);
    } else {
      // The call to ReturnFromGet will remove the current element in the
      // array, so we only increment the counter in the else branch.
      index += 1;
    }
  }

  // No get requests should be waiting for this object anymore. The object ID
  // may have been removed from the object_get_requests_ by ReturnFromGet, but
  // if the get request has not returned yet, then remove the object ID from the
  // map here.
  it = object_get_requests_.find(object_id);
  if (it != object_get_requests_.end()) {
    object_get_requests_.erase(object_id);
  }
}

Status PlasmaStore::ProcessGetRequest(const std::shared_ptr<ClientConnection>& client,
                                      const std::vector<ObjectID>& object_ids,
                                      int64_t timeout_ms) {
  // Create a get request for this object.
  auto get_req = new GetRequest(io_context_, client, object_ids);
  std::vector<ObjectID> evicted_ids;
  std::vector<ObjectTableEntry*> evicted_entries;
  for (auto object_id : object_ids) {
    // Check if this object is already present locally. If so, record that the
    // object is being used and mark it as accounted for.
    auto entry = GetObjectTableEntry(&store_info_, object_id);
    if (entry && entry->state == ObjectState::PLASMA_SEALED) {
      // Update the get request to take into account the present object.
      PlasmaObject_init(&get_req->objects[object_id], entry);
      get_req->num_satisfied += 1;
      // If necessary, record that this client is using this object. In the case
      // where entry == NULL, this will be called from SealObject.
    } else if (entry && entry->state == ObjectState::PLASMA_EVICTED) {
      // TODO: what if an object did not call contain?(user ensure) or called contain, but
      // evicted again?() we called Conatins earlier, so backend thread is pre-fetch
      // object.
      auto tic = std::chrono::steady_clock::now();
      int retry_time = 0;
      while (entry->state == ObjectState::PLASMA_EVICTED) {
        if (retry_time > 500) {  // TODO: What if this is a large object?
          ARROW_LOG(WARNING) << "prefetch object " << object_id.hex() << " failed!!!";
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        retry_time++;
      }
      auto toc = std::chrono::steady_clock::now();
      std::chrono::duration<double> time_ = toc - tic;
      ARROW_LOG(DEBUG) << "wait object ready takes " << time_.count() * 1000 << " ms";
      if (retry_time > 500) {
        get_req->objects[object_id].data_size = -1;
      } else
        PlasmaObject_init(&get_req->objects[object_id], entry);
      get_req->num_satisfied += 1;
    } else {
      // Add a placeholder plasma object to the get request to indicate that the
      // object is not present. This will be parsed by the client. We set the
      // data size to -1 to indicate that the object is not present.
      get_req->objects[object_id].data_size = -1;
      // Add the get request to the relevant data structures.
      object_get_requests_[object_id].push_back(get_req);
    }
  }

  // If all of the objects are present already or if the timeout is 0, return to
  // the client.
  if (get_req->num_satisfied == get_req->num_objects_to_wait_for || timeout_ms == 0) {
    ReturnFromGet(get_req);
  } else if (timeout_ms != -1) {
    // Set a timer that will cause the get request to return to the client. Note
    // that a timeout of -1 is used to indicate that no timer should be set.
    get_req->AsyncWait(timeout_ms, [this, get_req](const error_code& ec) {
      if (ec != asio::error::operation_aborted) {
        // Timer was not cancelled, take necessary action.
        ReturnFromGet(get_req);
      }
    });
  }
  return Status::OK();
}

void PlasmaStore::EraseFromObjectTable(const ObjectID& object_id) {
  std::lock_guard<std::mutex> lock_guard(entry_mtx);
  auto& object = store_info_.objects[object_id];
  if (object->ref_count == 0) {
    auto buff_size = object->data_size + object->metadata_size;
    if (object->device_num == 0) {
      if (object->pointer) {
        PlasmaAllocator::Free(object->pointer, buff_size);
        object->pointer = nullptr;
      }
    } else {
#ifdef PLASMA_CUDA
      ARROW_CHECK_OK(FreeCudaMemory(object->device_num, buff_size, object->pointer));
#endif
    }
    store_info_.objects.erase(object_id);
  } else {
    ARROW_LOG(DEBUG) << "try to erase an object not released!";
  }
}

void PlasmaStore::PushNotifications(
    std::vector<flatbuf::ObjectInfoT>& object_notifications) {
  return;
}

void PlasmaStore::ReleaseObject(const ObjectID& object_id,
                                const std::shared_ptr<ClientConnection>& client) {
  // Remove the client from the object's array of clients.
  // ARROW_CHECK(client->RemoveObjectIDIfExists(object_id));
  client->RemoveObjectIDIfExists(object_id);
  auto entry = GetObjectTableEntry(&store_info_, object_id);
  if (entry == nullptr) {
    ARROW_LOG(WARNING) << "try to release an object not exist in object table!!! "
                       << object_id.hex();
  }
  ARROW_CHECK(entry != nullptr);
  DecreaseObjectRefCount(object_id, entry);
  eviction_policy_.AddObject(object_id, entry->data_size + entry->metadata_size);
}

// Check if an object is present.
ObjectStatus PlasmaStore::ContainsObject(
    const ObjectID& object_id, const std::shared_ptr<ClientConnection>& client) {
  auto entry = GetObjectTableEntry(&store_info_, object_id);
  ObjectStatus status = ObjectStatus::OBJECT_NOT_FOUND;

  if (!entry) return status;

  {
    entry->mtx.lock();
    if (entry->state == ObjectState::PLASMA_EVICTED) {
      if (!external_store_) {
        status = ObjectStatus::OBJECT_NOT_FOUND;
      } else if (!external_store_->Exist(object_id).ok()) {
        ARROW_LOG(WARNING) << "erase from object table " << object_id.hex();
        EraseFromObjectTable(object_id);
        status = ObjectStatus::OBJECT_NOT_FOUND;
      } else {
        // ARROW_LOG(DEBUG) << "pre fetch object "<<object_id.hex();
        AddToClientObjectIds(object_id, entry, client);
        IncreaseObjectRefCount(object_id, entry);
        status = ObjectStatus::OBJECT_FOUND;
        auto status = external_store_->Get(object_id, entry);
      }
    } else if (entry->state == ObjectState::PLASMA_SEALED) {
      AddToClientObjectIds(object_id, entry, client);
      IncreaseObjectRefCount(object_id, entry);
      status = ObjectStatus::OBJECT_FOUND;
    }
    entry->mtx.unlock();
  }
  return status;
}

// Seal an object that has been created in the hash table.
void PlasmaStore::SealObjects(const std::vector<ObjectID>& object_ids,
                              const std::vector<std::string>& digests) {
  std::vector<flatbuf::ObjectInfoT> infos;
  for (size_t i = 0; i < object_ids.size(); ++i) {
    flatbuf::ObjectInfoT object_info;
    auto entry = GetObjectTableEntry(&store_info_, object_ids[i]);
    if (entry == nullptr) {
      ARROW_LOG(WARNING) << "try to seal an object not exist in object table!!! "
                         << object_ids[i].hex();
    }
    ARROW_CHECK(entry != nullptr);
    // ARROW_CHECK(entry->state == ObjectState::PLASMA_CREATED);
    if (entry->state != ObjectState::PLASMA_CREATED)
      ARROW_LOG(WARNING) << "This object state is not PLASMA_CREATED!!!";
    // Set the state of object to SEALED.
    entry->state = ObjectState::PLASMA_SEALED;
    // Set the object digest.
    std::memcpy(&entry->digest[0], digests[i].c_str(), kDigestSize);
    // Set object construction duration.
    entry->construct_duration = std::time(nullptr) - entry->create_time;

    object_info.object_id = object_ids[i].binary();
    object_info.data_size = entry->data_size;
    object_info.metadata_size = entry->metadata_size;
    object_info.digest = digests[i];
    infos.push_back(object_info);
  }

  PushNotifications(infos);

  for (size_t i = 0; i < object_ids.size(); ++i) {
    UpdateObjectGetRequests(object_ids[i]);
  }
}

int PlasmaStore::AbortObject(const ObjectID& object_id,
                             const std::shared_ptr<ClientConnection>& client) {
  auto entry = GetObjectTableEntry(&store_info_, object_id);
  ARROW_CHECK(entry != nullptr) << "To abort an object it must be in the object table.";
  ARROW_CHECK(entry->state != ObjectState::PLASMA_SEALED)
      << "To abort an object it must not have been sealed.";
  auto it = client->object_ids.find(object_id);
  if (it == client->object_ids.end()) {
    // If the client requesting the abort is not the creator, do not
    // perform the abort.
    return 0;
  } else {
    // The client requesting the abort is the creator. Free the object.
    EraseFromObjectTable(object_id);
    client->object_ids.erase(it);
    return 1;
  }
}

PlasmaError PlasmaStore::DeleteObject(ObjectID& object_id) {
  auto entry = GetObjectTableEntry(&store_info_, object_id);
  // TODO(rkn): This should probably not fail, but should instead throw an
  // error. Maybe we should also support deleting objects that have been
  // created but not sealed.
  if (entry == nullptr) {
    // To delete an object it must be in the object table.
    return PlasmaError::ObjectNotFound;
  }

  if (entry->state != ObjectState::PLASMA_SEALED) {
    // To delete an object it must have been sealed.
    // Put it into deletion cache, it will be deleted later.
    deletion_cache_.emplace(object_id);
    return PlasmaError::ObjectNotSealed;
  }

  if (entry->ref_count != 0) {
    // To delete an object, there must be no clients currently using it.
    // Put it into deletion cache, it will be deleted later.
    deletion_cache_.emplace(object_id);
    return PlasmaError::ObjectInUse;
  }

  eviction_policy_.RemoveObject(object_id);
  EraseFromObjectTable(object_id);
  // Inform all subscribers that the object has been deleted.
  // PushObjectDeletionNotification(object_id);
  return PlasmaError::OK;
}

void PlasmaStore::EvictObjects(const std::vector<ObjectID>& object_ids) {
  if (object_ids.size() == 0) {
    return;
  }
  if (external_store_)
    ARROW_LOG(WARNING) << "should not be called!!!";
  else {
    for (auto object_id : object_ids) {
      EraseFromObjectTable(object_id);
      eviction_policy_.RemoveObject(object_id);
    }
  }
}

void PlasmaStore::IncreaseObjectRefCount(const ObjectID& object_id,
                                         ObjectTableEntry* entry) {
  // Increase reference count.
  entry->ref_count++;
}

void PlasmaStore::DecreaseObjectRefCount(const ObjectID& object_id,
                                         ObjectTableEntry* entry) {
  // Decrease reference count.
  entry->ref_count--;
}

void PlasmaStore::PushObjectReadyNotification(const ObjectID& object_id,
                                              const ObjectTableEntry& entry) {
  // for (const auto& client : notification_clients_) {
  //   client->SendObjectReadyAsync(object_id, entry);
  // }
}

void PushObjectDeletionNotification(const ObjectID& object_id) {
  // for (const auto& client : notification_clients_) {
  //   client->SendObjectDeletionAsync(object_id);
  // }
}

// Subscribe to notifications about sealed objects.
void PlasmaStore::SubscribeToUpdates(const std::shared_ptr<ClientConnection>& client) {
  ARROW_LOG(DEBUG) << "subscribing to updates on fd " << client->GetNativeHandle();
  if (notification_clients_.count(client) > 0) {
    // This client has already subscribed. Return.
    return;
  }

  // Add this client to the notification set, which is needed for this client to receive
  // notifications.
  notification_clients_.insert(client);

}

void PlasmaStore::UpdateMetrics(PlasmaMetrics* metrics) {
  metrics->share_mem_total = PlasmaAllocator::GetFootprintLimit();
  metrics->share_mem_used = PlasmaAllocator::Allocated();
  int64_t external_total = 0;
  int64_t external_used = 0;
  if (external_store_) {
    external_store_->Metrics(&external_total, &external_used);
  }
  metrics->external_total = external_total;
  metrics->external_used = external_used;
}

void PlasmaStore::DoAccept() {
  // TODO(suquark): Use shared_from_this() here ?
  acceptor_.async_accept(stream_, [this](const error_code& ec) { HandleAccept(ec); });
}

void PlasmaStore::HandleAccept(const error_code& error) {
  if (!error) {
    io::MessageHandler message_handler = [this](std::shared_ptr<ClientConnection> client,
                                                int64_t message_type, int64_t length,
                                                const uint8_t* message) {
      Status s = ProcessClientMessage(client, message_type, length, message);
      if (!s.ok()) {
        ARROW_LOG(ERROR) << "[PlasmaStore] Failed to process the event"
                         << "(type=" << message_type << "): " << s << ", "
                         << "fd = " << client->GetNativeHandle();
      }
    };
    // Accept a new local client and dispatch it to the store.
    auto new_connection = ClientConnection::Create(std::move(stream_), message_handler);
    // Insert the client before processing messages.
    connected_clients_.insert(new_connection);
    // Process our new connection.
    new_connection->ProcessMessages();
  }
  // We're ready to accept another client.
  DoAccept();
}

void PlasmaStore::ReleaseClientResources(
    const std::shared_ptr<ClientConnection>& client) {
  // Release all the objects that the client was using.
  std::unordered_map<ObjectID, ObjectTableEntry*> sealed_objects;
  for (const auto& object_id : client->object_ids) {
    auto it = store_info_.objects.find(object_id);
    if (it == store_info_.objects.end()) {
      continue;
    }

    if (it->second->state == ObjectState::PLASMA_SEALED) {
      // Add sealed objects to a temporary list of object IDs. Do not perform
      // the remove here, since it potentially modifies the object_ids table.
      sealed_objects[it->first] = it->second.get();
    } else {
      // Abort unsealed object.
      // Don't call AbortObject() because client->object_ids would be modified.
      EraseFromObjectTable(object_id);
    }
  }

  /// Remove all of the client's GetRequests.
  RemoveGetRequestsForClient(client);

  for (const auto& entry : sealed_objects) {
    // The object ID must exist in client's record.
    client->RemoveObjectID(entry.first);
    DecreaseObjectRefCount(entry.first, entry.second);
  }
}

void PlasmaStore::ProcessDisconnectClient(
    const std::shared_ptr<ClientConnection>& client) {
  if (!client->IsOpen()) {
    ARROW_LOG(ERROR) << "Received disconnection request from a disconnected client.";
    return;
  }
  // Close the client.
  ARROW_LOG(INFO) << "Disconnecting client on fd " << client->GetNativeHandle();
  client->Close();

  // Remove the client from the connection set.
  auto it = connected_clients_.find(client);
  if (it == connected_clients_.end()) {
    ARROW_LOG(FATAL) << "[PlasmaStore] (on DisconnectClient) Unexpected error: The "
                     << "client to disconnect is not in the connected clients list.";
    return;
  }
  connected_clients_.erase(it);
  // Remove the client from the notification set.
  if (notification_clients_.count(client) > 0) {
    notification_clients_.erase(client);
  }

  // Release resources.
  ReleaseClientResources(client);
}

void PlasmaStore::OnKill() {
  ARROW_LOG(INFO) << "Total process time is " << process_total_time.count() * 1000;
}

Status PlasmaStore::ProcessClientMessage(const std::shared_ptr<ClientConnection>& client,
                                         int64_t message_type, int64_t message_size,
                                         const uint8_t* message_data) {
  auto message_type_value = static_cast<MessageType>(message_type);
  ObjectID object_id;

  auto tic = std::chrono::steady_clock::now();
  // Process the different types of requests.
  switch (message_type_value) {
    case MessageType::PlasmaCreateRequest: {
      bool evict_if_full;
      int64_t data_size;
      int64_t metadata_size;
      int device_num;
      RETURN_NOT_OK(ReadCreateRequest(message_data, message_size, &object_id,
                                      &evict_if_full, &data_size, &metadata_size,
                                      &device_num));
      PlasmaObject object = {};
      PlasmaError error_code = CreateObject(object_id, evict_if_full, data_size,
                                            metadata_size, device_num, client, &object);
      int64_t mmap_size = 0;
      if (error_code == PlasmaError::OK && device_num == 0) {
        mmap_size = GetMmapSize(object.store_fd);
      }
      RETURN_NOT_OK(SendCreateReply(client, object_id, &object, error_code, mmap_size));
      // Only send the file descriptor if it hasn't been sent (see analogous
      // logic in GetStoreFd in client.cc). Similar in ReturnFromGet.
      if (error_code == PlasmaError::OK && device_num == 0) {
        auto status = client->SendFd(object.store_fd);
        if (!status.ok()) {
          // TODO(suquark): Should we close the client here?
          ARROW_LOG(ERROR) << "[PlasmaStore] (on CreateRequest) Failed to send a mmap fd"
                           << " to the client.";
        }
      }
    } break;
    case MessageType::PlasmaCreateAndSealRequest: {
      bool evict_if_full;
      std::string data;
      std::string metadata;
      std::string digest;
      digest.reserve(kDigestSize);
      RETURN_NOT_OK(ReadCreateAndSealRequest(message_data, message_size, &object_id,
                                             &evict_if_full, &data, &metadata, &digest));
      PlasmaObject object = {};
      // CreateAndSeal currently only supports device_num = 0, which corresponds
      // to the host.
      int device_num = 0;
      PlasmaError error_code = CreateObject(object_id, evict_if_full, data.size(),
                                            metadata.size(), device_num, client, &object);

      // If the object was successfully created, fill out the object data and seal it.
      if (error_code == PlasmaError::OK) {
        auto entry = GetObjectTableEntry(&store_info_, object_id);
        if (entry == nullptr) {
          ARROW_LOG(WARNING) << "try to seal an object not exist in object table!!! "
                             << object_id.hex();
        }
        ARROW_CHECK(entry != nullptr);
        // Write the inlined data and metadata into the allocated object.
        std::memcpy(entry->pointer, data.data(), data.size());
        std::memcpy(entry->pointer + data.size(), metadata.data(), metadata.size());
        SealObjects({object_id}, {digest});
        // Remove the client from the object's array of clients because the
        // object is not being used by any client. The client was added to the
        // object's array of clients in CreateObject. This is analogous to the
        // Release call that happens in the client's Seal method.
        ARROW_CHECK(client->RemoveObjectIDIfExists(object_id));
        DecreaseObjectRefCount(object_id, entry);
      }

      auto status = SendCreateAndSealReply(client, error_code);
      if (!status.ok()) {
        ARROW_LOG(ERROR) << "SendCreateAndSealReply failed" ;
      }
      // Reply to the client.
      // HANDLE_SIGPIPE(SendCreateAndSealReply(client, error_code), client->fd);
    } break;
    // PlasmaCreateAndSealBatchRequest is not supported due to using raw socket
    // case fb::MessageType::PlasmaCreateAndSealBatchRequest: {
    //   bool evict_if_full;
    //   std::vector<ObjectID> object_ids;
    //   std::vector<std::string> data;
    //   std::vector<std::string> metadata;
    //   std::vector<std::string> digests;

    //   RETURN_NOT_OK(ReadCreateAndSealBatchRequest(
    //       input, input_size, &object_ids, &evict_if_full, &data, &metadata, &digests));

    //   // CreateAndSeal currently only supports device_num = 0, which corresponds
    //   // to the host.
    //   int device_num = 0;
    //   size_t i = 0;
    //   PlasmaError error_code = PlasmaError::OK;
    //   for (i = 0; i < object_ids.size(); i++) {
    //     error_code = CreateObject(object_ids[i], evict_if_full, data[i].size(),
    //                               metadata[i].size(), device_num, client, &object);
    //     if (error_code != PlasmaError::OK) {
    //       break;
    //     }
    //   }

    //   // if OK, seal all the objects,
    //   // if error, abort the previous i objects immediately
    //   if (error_code == PlasmaError::OK) {
    //     for (i = 0; i < object_ids.size(); i++) {
    //       auto entry = GetObjectTableEntry(&store_info_, object_ids[i]);
    //       ARROW_CHECK(entry != nullptr);
    //       // Write the inlined data and metadata into the allocated object.
    //       std::memcpy(entry->pointer, data[i].data(), data[i].size());
    //       std::memcpy(entry->pointer + data[i].size(), metadata[i].data(),
    //                   metadata[i].size());
    //     }

    //     SealObjects(object_ids, digests);
    //     // Remove the client from the object's array of clients because the
    //     // object is not being used by any client. The client was added to the
    //     // object's array of clients in CreateObject. This is analogous to the
    //     // Release call that happens in the client's Seal method.
    //     for (i = 0; i < object_ids.size(); i++) {
    //       auto entry = GetObjectTableEntry(&store_info_, object_ids[i]);
    //       ARROW_CHECK(RemoveFromClientObjectIds(object_ids[i], entry, client) == 1);
    //     }
    //   } else {
    //     for (size_t j = 0; j < i; j++) {
    //       AbortObject(object_ids[j], client);
    //     }
    //   }

    //   HANDLE_SIGPIPE(SendCreateAndSealBatchReply(client->fd, error_code), client->fd);
    // } break;
    case MessageType::PlasmaAbortRequest: {
      RETURN_NOT_OK(ReadAbortRequest(message_data, message_size, &object_id));
      ARROW_CHECK(AbortObject(object_id, client) == 1) << "To abort an object, the only "
                                                          "client currently using it "
                                                          "must be the creator.";
      // HANDLE_SIGPIPE(SendAbortReply(client->fd, object_id), client->fd);
      RETURN_NOT_OK(SendAbortReply(client, object_id));
    } break;
    case MessageType::PlasmaGetRequest: {
      std::vector<ObjectID> object_ids;
      int64_t timeout_ms;
      RETURN_NOT_OK(ReadGetRequest(message_data, message_size, object_ids, &timeout_ms));
      RETURN_NOT_OK(ProcessGetRequest(client, object_ids, timeout_ms));
    } break;
    case MessageType::PlasmaReleaseRequest: {
      RETURN_NOT_OK(ReadReleaseRequest(message_data, message_size, &object_id));
      ReleaseObject(object_id, client);
    } break;
    case MessageType::PlasmaDeleteRequest: {
      std::vector<ObjectID> object_ids;
      std::vector<PlasmaError> error_codes;
      RETURN_NOT_OK(ReadDeleteRequest(message_data, message_size, &object_ids));
      error_codes.reserve(object_ids.size());
      for (auto& object_id : object_ids) {
        error_codes.push_back(DeleteObject(object_id));
      }
      RETURN_NOT_OK(SendDeleteReply(client, object_ids, error_codes));
    } break;
    case MessageType::PlasmaContainsRequest: {
      RETURN_NOT_OK(ReadContainsRequest(message_data, message_size, &object_id));
      auto has_object = (ContainsObject(object_id, client) == ObjectStatus::OBJECT_FOUND);
      RETURN_NOT_OK(SendContainsReply(client, object_id, has_object));
    } break;
    case MessageType::PlasmaListRequest: {
      RETURN_NOT_OK(ReadListRequest(message_data, message_size));
      RETURN_NOT_OK(SendListReply(client, store_info_.objects));
    } break;
    case MessageType::PlasmaSealRequest: {
      std::string digest;
      digest.reserve(kDigestSize);
      RETURN_NOT_OK(ReadSealRequest(message_data, message_size, &object_id, &digest));
      SealObjects({object_id}, {digest});
      auto status = SendSealReply(client, object_id, PlasmaError::OK);
      if (!status.ok()) {
        ARROW_LOG(ERROR) << "SendSealReply failed" ;
      }
      // HANDLE_SIGPIPE(SendSealReply(client->fd, object_id, PlasmaError::OK),
      // client->fd);
    } break;
    case MessageType::PlasmaEvictRequest: {
      // This code path should only be used for testing.
      int64_t num_bytes;
      RETURN_NOT_OK(ReadEvictRequest(message_data, message_size, &num_bytes));
      std::vector<ObjectID> objects_to_evict;
      int64_t num_bytes_evicted =
          eviction_policy_.ChooseObjectsToEvict(num_bytes, &objects_to_evict);
      EvictObjects(objects_to_evict);
      RETURN_NOT_OK(SendEvictReply(client, num_bytes_evicted));
    } break;
    case MessageType::PlasmaSubscribeRequest:
      SubscribeToUpdates(client);
      break;
    case MessageType::PlasmaConnectRequest: {
      RETURN_NOT_OK(SendConnectReply(client, PlasmaAllocator::GetFootprintLimit()));
    } break;
    case MessageType::PlasmaMetricsRequest: {
      RETURN_NOT_OK(ReadMetricsRequest(message_data, message_size));
      PlasmaMetrics metrics;
      UpdateMetrics(&metrics);
      RETURN_NOT_OK(SendMetricsReply(client, &metrics));
    } break;
    case MessageType::PlasmaDisconnectClient:
      ARROW_LOG(DEBUG) << "Disconnecting client on fd " << client->GetNativeHandle();
      ProcessDisconnectClient(client);
      return Status::OK();  // Stop listening for more messages.
    default:
      // This code should be unreachable.
      ARROW_CHECK(0);
  }
  auto toc = std::chrono::steady_clock::now();
  process_total_time += (toc - tic);
  // Listen for more messages.
  client->ProcessMessages();
  return Status::OK();
}

class PlasmaStoreRunner {
 public:
  PlasmaStoreRunner() {}

  void Start(const std::string& stream_name, std::string directory,
             bool hugepages_enabled, std::shared_ptr<ExternalStore> external_store,
             int thread_num = 1) {
    signal_set_.async_wait([this](std::error_code ec, int signal) {
      if (signal == SIGTERM) {
        ARROW_LOG(INFO) << "SIGTERM Signal received, closing Plasma Server...";
        Stop();
      }
      if (signal == SIGKILL) {
        store_->OnKill();
      }
    });
    // Create the event loop.
    store_.reset(new PlasmaStore(io_context_, directory, hugepages_enabled, stream_name,
                                 external_store));
    plasma_config = store_->GetPlasmaStoreInfo();
    // We are using a single memory-mapped file by mallocing and freeing a single
    // large amount of space up front. According to the documentation,
    // dlmalloc might need up to 128*sizeof(size_t) bytes for internal
    // bookkeeping.
    void* pointer = PlasmaAllocator::Memalign(
        kBlockSize, PlasmaAllocator::GetFootprintLimit() - 256 * sizeof(size_t));
    ARROW_CHECK(pointer != nullptr);
    // This will unmap the file, but the next one created will be as large
    // as this one (this is an implementation detail of dlmalloc).

    plasma::PlasmaAllocator::Free(
        pointer, PlasmaAllocator::GetFootprintLimit() - 256 * sizeof(size_t));

    std::vector<std::thread> threads(thread_num - 1);
    ARROW_LOG(DEBUG) << "will start " << thread_num << " threads for server";
    for (unsigned long i = 0; i < threads.size(); i++) {
      threads[i] = std::thread([&]() { io_context_.run(); });
    }
    io_context_.run();
  }

  void Stop() { io_context_.stop(); }

  void Shutdown() {
    io_context_.stop();
    store_ = nullptr;
  }

 private:
  asio::io_context io_context_;
  asio::signal_set signal_set_{io_context_, SIGTERM};
  // Ignore SIGPIPE signals. If we don't do this, then when we attempt to write
  // to a client that has already died, the store could die.
  asio::signal_set signal_ign_{io_context_, SIGPIPE};
  std::unique_ptr<PlasmaStore> store_;
};

static std::unique_ptr<PlasmaStoreRunner> g_runner = nullptr;

}  // namespace plasma

int main(int argc, char* argv[]) {
  ArrowLog::StartArrowLog(argv[0], ArrowLogLevel::ARROW_DEBUG);
  ArrowLog::InstallFailureSignalHandler();
  char* stream_name = nullptr;
  // Directory where plasma memory mapped files are stored.
  std::string plasma_directory;
  std::string external_store_endpoint;
  bool hugepages_enabled = false;
  int64_t system_memory = -1;
  int thread_num = 1;
  int c;
  while ((c = getopt(argc, argv, "s:m:d:e:t:h")) != -1) {
    switch (c) {
      case 'd':
        plasma_directory = std::string(optarg);
        break;
      case 'e':
        external_store_endpoint = std::string(optarg);
        break;
      case 'h':
        hugepages_enabled = true;
        break;
      case 's':
        stream_name = optarg;
        break;
      case 't': {
        thread_num = atoi(optarg);
        break;
      }
      case 'm': {
        char extra;
        int scanned = sscanf(optarg, "%" SCNd64 "%c", &system_memory, &extra);
        ARROW_CHECK(scanned == 1);
        // Set system memory capacity
        plasma::PlasmaAllocator::SetFootprintLimit(static_cast<size_t>(system_memory));
        ARROW_LOG(INFO) << "Allowing the Plasma store to use up to "
                        << static_cast<double>(system_memory) / 1000000000
                        << "GB of memory.";
        break;
      }
      default:
        exit(-1);
    }
  }
  // Sanity check command line options.
  if (!stream_name) {
    ARROW_LOG(FATAL) << "please specify socket for incoming connections with -s switch";
  }
  if (system_memory == -1) {
    ARROW_LOG(FATAL) << "please specify the amount of system memory with -m switch";
  }
  if (hugepages_enabled && plasma_directory.empty()) {
    ARROW_LOG(FATAL) << "if you want to use hugepages, please specify path to huge pages "
                        "filesystem with -d";
  }
  if (plasma_directory.empty()) {
#ifdef __linux__
    plasma_directory = "/dev/shm";
#else
    plasma_directory = "/tmp";
#endif
  }
  ARROW_LOG(INFO) << "Starting object store with directory " << plasma_directory
                  << " and huge page support "
                  << (hugepages_enabled ? "enabled" : "disabled");
#ifdef __linux__
  if (!hugepages_enabled) {
    // On Linux, check that the amount of memory available in /dev/shm is large
    // enough to accommodate the request. If it isn't, then fail.
    int shm_fd = open(plasma_directory.c_str(), O_RDONLY);
    struct statvfs shm_vfs_stats;
    fstatvfs(shm_fd, &shm_vfs_stats);
    // The value shm_vfs_stats.f_bsize is the block size, and the value
    // shm_vfs_stats.f_bavail is the number of available blocks.
    int64_t shm_mem_avail = shm_vfs_stats.f_bsize * shm_vfs_stats.f_bavail;
    close(shm_fd);
    // Keep some safety margin for allocator fragmentation.
    shm_mem_avail = 9 * shm_mem_avail / 10;
    if (system_memory > shm_mem_avail) {
      ARROW_LOG(WARNING)
          << "System memory request exceeds memory available in " << plasma_directory
          << ". The request is for " << system_memory
          << " bytes, and the amount available is " << shm_mem_avail
          << " bytes. You may be able to free up space by deleting files in "
             "/dev/shm. If you are inside a Docker container, you may need to "
             "pass an argument with the flag '--shm-size' to 'docker run'.";
      system_memory = shm_mem_avail;
    }
  } else {
    plasma::SetMallocGranularity(1024 * 1024 * 1024);  // 1 GB
  }
#endif
  // Get external store
  std::shared_ptr<plasma::ExternalStore> external_store{nullptr};
  if (!external_store_endpoint.empty()) {
    std::string name;
    ARROW_CHECK_OK(
        plasma::ExternalStores::ExtractStoreName(external_store_endpoint, &name));
    external_store = plasma::ExternalStores::GetStore(name);
    if (external_store == nullptr) {
      ARROW_LOG(FATAL) << "No such external store \"" << name << "\"";
      return -1;
    }
    ARROW_CHECK_OK(external_store->Connect(external_store_endpoint));
  }
  ARROW_LOG(DEBUG) << "starting server listening on " << stream_name;
  plasma::g_runner.reset(new plasma::PlasmaStoreRunner());
  plasma::g_runner->Start(stream_name, plasma_directory, hugepages_enabled,
                          external_store, thread_num);
  plasma::g_runner->Shutdown();
  plasma::g_runner = nullptr;

  ArrowLog::UninstallSignalAction();
  ArrowLog::ShutDownArrowLog();
  return 0;
}

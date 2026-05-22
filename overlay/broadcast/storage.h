/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include <memory>

#include "overlay/broadcast/wire.h"
#include "td/fec/fec.h"
#include "td/utils/Status.h"
#include "td/utils/buffer.h"

namespace ton {
namespace overlay {

// Body fragmentation contract: pure transformer, body⇄pieces. Composed into a Profile
// alongside Wire and Algorithm.
class BroadcastStorage {
 public:
  virtual ~BroadcastStorage() = default;

  // Receiver-side decoder (assemble pieces into a body).
  virtual td::Result<std::unique_ptr<td::fec::Decoder>> for_receiving(const BroadcastInfo &info) const = 0;

  // Source-side encoder (split body into pieces).
  virtual std::unique_ptr<td::fec::Encoder> for_sourcing(const BroadcastInfo &info, td::BufferSlice body) const = 0;
};

// Storage singletons — each profile picks one.
const std::shared_ptr<const BroadcastStorage> &whole_storage();
const std::shared_ptr<const BroadcastStorage> &raptorq_storage();
const std::shared_ptr<const BroadcastStorage> &rlnc_storage();

}  // namespace overlay
}  // namespace ton

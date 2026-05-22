/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/

#include "overlay/broadcast/profile.h"
#include "overlay/broadcast/storage.h"
#include "overlay/broadcast/wire.h"

namespace ton {
namespace overlay {

namespace {

constexpr td::uint32 kPushPullTtlSeconds = 60;
constexpr td::uint32 kTwostepTtlSeconds = 25;

broadcast::BroadcastSessionConfig push_pull_session(const OverlayBroadcastOptions &opts) {
  return {.initial_peer_limit = opts.known_peer_limit, .ttl_seconds = kPushPullTtlSeconds};
}

broadcast::BroadcastSessionConfig twostep_session() {
  return {.initial_peer_limit = 0, .ttl_seconds = kTwostepTtlSeconds};
}

}  // namespace

broadcast::AlgorithmFamily bind_twostep_push(broadcast::AlgorithmFamily f) {
  f.wire = v2_wire();
  f.storage = whole_storage();
  f.session = twostep_session();
  return f;
}

broadcast::AlgorithmFamily bind_twostep_fec(broadcast::AlgorithmFamily f) {
  f.wire = v2_wire();
  f.storage = rlnc_storage();
  f.session = twostep_session();
  return f;
}

broadcast::AlgorithmFamily bind_push_pull_whole(broadcast::AlgorithmFamily f, const OverlayBroadcastOptions &opts) {
  f.wire = v2_wire();
  f.storage = whole_storage();
  f.session = push_pull_session(opts);
  return f;
}

broadcast::AlgorithmFamily bind_push_pull_rlnc(broadcast::AlgorithmFamily f, const OverlayBroadcastOptions &opts) {
  f.wire = v2_wire();
  f.storage = rlnc_storage();
  f.session = push_pull_session(opts);
  return f;
}

const std::shared_ptr<const BroadcastWire> &wire_for_mode(const BroadcastMode &) {
  return v2_wire();
}

}  // namespace overlay
}  // namespace ton

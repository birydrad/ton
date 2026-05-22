#pragma once

#include <functional>
#include <memory>
#include <string>

#include "overlay/broadcast/algorithm.h"
#include "td/utils/int_types.h"

namespace ton::overlay {
class BroadcastWire;
class BroadcastStorage;
}  // namespace ton::overlay

namespace ton::overlay::broadcast {

// How a session is paced and seeded — orthogonal to the algorithm's behaviour.
struct BroadcastSessionConfig {
  size_t initial_peer_limit = 0;
  td::uint32 ttl_seconds = 60;
};

// One registered algorithm family. Bundles everything the engine needs to run a broadcast of
// this mode: per-overlay shared state, per-broadcast algorithm factory, plus the mode-derived
// transport (wire codec, storage encoder/decoder, session policy).
//
// `shared` and `make_algorithm` are family-private — each engine instance (= overlay = bsim
// node) calls the factory to get a fresh family with a fresh Shared, so N nodes in one process
// don't share state. The closure captures a typed shared_ptr<ConcreteShared> next to `shared`
// so the algorithm sees its Shared statically without runtime downcasts.
struct AlgorithmFamily {
  std::string key;
  std::shared_ptr<algorithm::BroadcastShared> shared;  // never null; use algorithm::empty_shared() if stateless
  std::shared_ptr<const ::ton::overlay::BroadcastWire> wire;
  std::shared_ptr<const ::ton::overlay::BroadcastStorage> storage;
  BroadcastSessionConfig session;
  std::function<std::unique_ptr<algorithm::BroadcastAlgorithm>(algorithm::BroadcastInit)> make_algorithm;
};

}  // namespace ton::overlay::broadcast

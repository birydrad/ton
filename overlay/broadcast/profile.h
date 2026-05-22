/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include "overlay/broadcast/catalog.h"
#include "overlay/broadcast/overlay-broadcast-env.h"
#include "overlay/broadcast/wire.h"

namespace ton {
namespace overlay {

struct OverlayBroadcastOptions;

// Thin binders that stamp mode-derived transport (wire codec + storage + session policy) onto
// an algorithm-side AlgorithmFamily. Live in the heavy overlay lib because they pull in the
// wire/storage singletons; algorithm factories themselves stay hermetic.
broadcast::AlgorithmFamily bind_twostep_push(broadcast::AlgorithmFamily f);
broadcast::AlgorithmFamily bind_twostep_fec(broadcast::AlgorithmFamily f);
broadcast::AlgorithmFamily bind_push_pull_whole(broadcast::AlgorithmFamily f, const OverlayBroadcastOptions &opts);
broadcast::AlgorithmFamily bind_push_pull_rlnc(broadcast::AlgorithmFamily f, const OverlayBroadcastOptions &opts);

// Cheap wire lookup for the publish path (compute_broadcast_id) — pre-session, pre-family.
const std::shared_ptr<const BroadcastWire> &wire_for_mode(const BroadcastMode &mode);

}  // namespace overlay
}  // namespace ton

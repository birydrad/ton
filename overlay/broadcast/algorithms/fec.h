#pragma once

#include "overlay/broadcast/catalog.h"

namespace ton::overlay::broadcast {

// Multi-piece flood with RaptorQ semantics. Source emits `total_pieces` chunks at
// `emit_batch_size` per `emit_interval`; receivers deliver after collecting `required_pieces`
// distinct seqnos and then take over emission of unsent chunks. No scoring (random sampling),
// no PRUNE — feedback emitted purely for the overlay's peer-rotation layer.
AlgorithmFamily make_fec_family(algorithm::FecConfig fec);

}  // namespace ton::overlay::broadcast

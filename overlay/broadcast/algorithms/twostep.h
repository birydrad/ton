#pragma once

#include "overlay/broadcast/catalog.h"

namespace ton::overlay::broadcast {

// Two-step push: source delivers body (or one initial piece each in FEC mode) to persistent
// peers; those re-emit to everyone. No pull. `make_twostep_push_family` = whole-body mode;
// `make_twostep_fec_family` = piece mode.
AlgorithmFamily make_twostep_push_family();
AlgorithmFamily make_twostep_fec_family();

}  // namespace ton::overlay::broadcast

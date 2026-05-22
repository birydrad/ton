add_library(overlay-broadcast-support STATIC
  broadcast/rlnc-codec.cpp
  broadcast/algorithm.cpp
  broadcast/algorithms/eager-lazy.cpp
  broadcast/algorithms/fec.cpp
  broadcast/algorithms/pull-only.cpp
  broadcast/algorithms/sub-direct.cpp
  broadcast/algorithms/gossip-mask-fec.cpp
  broadcast/algorithms/leech.cpp
  broadcast/algorithms/optimum-p2p.cpp
  broadcast/algorithms/plumtree.cpp
  broadcast/algorithms/push.cpp
  broadcast/algorithms/twostep.cpp
  broadcast/score.cpp

  broadcast/rlnc-codec.h
  broadcast/algorithm.h
  broadcast/algorithms/peer-table.h
  broadcast/score.h
)

target_include_directories(overlay-broadcast-support PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
)
target_link_libraries(overlay-broadcast-support PUBLIC tdutils tdfec)

set(OVERLAY_SOURCE
  broadcast-fec.cpp
  broadcast/overlay-broadcast.cpp
  broadcast/profile.cpp
  broadcast/overlay-broadcast-session.cpp
  broadcast/storage.cpp
  broadcast/wire.cpp
  broadcast-simple.cpp
  broadcast-twostep.cpp
  overlay-manager.cpp
  overlay.cpp
  overlay-peers.cpp

  broadcast-fec.hpp
  broadcast/overlay-broadcast.h
  broadcast/overlay-broadcast-session.h
  broadcast/profile.h
  broadcast/storage.h
  broadcast/wire.h
  broadcast/algorithm.h
  broadcast/score.h
  broadcast-simple.hpp
  broadcast-twostep.hpp
  overlay-manager.h
  overlay.h
  overlay.hpp
  overlays.h
)

add_library(overlay STATIC ${OVERLAY_SOURCE})

target_include_directories(overlay PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/..
  ${OPENSSL_INCLUDE_DIR}
)
target_link_libraries(overlay PRIVATE overlay-broadcast-support tdutils tdactor adnl tl_api dht fec)

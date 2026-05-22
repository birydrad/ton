add_library(bsim STATIC bsim.cpp)
target_link_libraries(bsim PUBLIC overlay-broadcast-support tdutils)

add_library(bsim-catalogue STATIC catalogue.cpp)
target_link_libraries(bsim-catalogue PUBLIC bsim)

add_library(bsim-mainnet STATIC mainnet-loader.cpp)
target_link_libraries(bsim-mainnet PUBLIC bsim-catalogue)

add_executable(broadcast-bench bench-synthetic.cpp bench-mainnet.cpp)
target_link_libraries(broadcast-bench PRIVATE bsim-mainnet)

add_custom_target(run-broadcast-bench-quick
  COMMAND $<TARGET_FILE:broadcast-bench> --suite quick
  DEPENDS broadcast-bench
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_custom_target(run-broadcast-bench-smoke
  COMMAND $<TARGET_FILE:broadcast-bench> --suite smoke
  DEPENDS broadcast-bench
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_executable(test-bsim test-bsim.cpp)
target_link_libraries(test-bsim PRIVATE bsim td-test-main)
ton_test(test-bsim)

add_executable(test-overlay-broadcast-session test-overlay-broadcast-session.cpp)
target_include_directories(test-overlay-broadcast-session PRIVATE ${CMAKE_SOURCE_DIR}/crypto)
target_link_libraries(test-overlay-broadcast-session PRIVATE
  overlay
  overlay-broadcast-support
  tl_api
  tdactor
  tddb
  td-test-main
)
ton_test(test-overlay-broadcast-session)

add_executable(benchmark-broadcast-codec broadcast-codec-benchmark.cpp)
target_link_libraries(benchmark-broadcast-codec PRIVATE overlay-broadcast-support)

#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>

#include "overlay/broadcast/rlnc-codec.h"
#include "td/utils/benchmark.h"

namespace ton::broadcast_sim {
namespace {

using ::ton::overlay::broadcast::make_rlnc_decoder;
using ::ton::overlay::broadcast::make_rlnc_encoder;
using ::ton::overlay::broadcast::RlncDecoder;

td::BufferSlice make_body(size_t size) {
  std::string body(size, '\0');
  for (size_t i = 0; i < size; i++) {
    body[i] = static_cast<char>('a' + (i % 26));
  }
  return td::BufferSlice(body);
}

template <class F>
double measure_ms(td::uint32 iterations, F &&f) {
  auto started = std::chrono::steady_clock::now();
  for (td::uint32 i = 0; i < iterations; i++) {
    f(i);
  }
  return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started).count();
}

void print_row(const std::string &name, size_t body_size, td::uint32 shard_count, td::uint32 iterations,
               double elapsed_ms) {
  std::cout << std::left << std::setw(14) << name << std::right << std::setw(10) << body_size / 1024 << std::setw(8)
            << shard_count << std::setw(10) << iterations << std::setw(12) << elapsed_ms << std::setw(12)
            << elapsed_ms * 1000.0 / iterations << "\n";
}

void bench_codec(size_t body_size, td::uint32 target_shards, td::uint32 iterations) {
  auto body = make_body(body_size);
  auto encoder = make_rlnc_encoder(target_shards, body.clone());
  // Approximate shard_count: target_shards (RLNC uses ceil(body/payload) which equals target).
  auto shard_count = target_shards;

  auto encode_ms = measure_ms(iterations, [&](td::uint32 i) {
    auto shard = encoder->gen_symbol(i % (shard_count * 2));
    td::do_not_optimize_away(shard.data.size());
  });
  print_row("encode", body_size, shard_count, iterations, encode_ms);

  auto decode_ms = measure_ms(iterations, [&](td::uint32) {
    auto decoder = make_rlnc_decoder(target_shards, body.size());
    for (td::uint32 shard_id = 0; shard_id < shard_count; shard_id++) {
      decoder->add_symbol(encoder->gen_symbol(shard_id)).ensure();
    }
    auto decoded = decoder->try_decode(false).move_as_ok();
    td::do_not_optimize_away(decoded.data.size());
  });
  print_row("decode", body_size, shard_count, iterations, decode_ms);

  RlncDecoder partial(target_shards, body.size());
  for (td::uint32 shard_id = 0; shard_id < std::max<td::uint32>(1, shard_count / 2); shard_id++) {
    partial.add_symbol(encoder->gen_symbol(shard_id)).ensure();
  }
  auto recode_ms = measure_ms(iterations, [&](td::uint32 i) {
    auto shard = partial.gen_symbol(shard_count + i).move_as_ok();
    td::do_not_optimize_away(shard.data.size());
  });
  print_row("recode", body_size, shard_count, iterations, recode_ms);
}

int benchmark_main() {
  std::cout << std::fixed << std::setprecision(3);
  std::cout << std::left << std::setw(14) << "op" << std::right << std::setw(10) << "body_kb" << std::setw(8) << "k"
            << std::setw(10) << "iters" << std::setw(12) << "ms" << std::setw(12) << "us/op" << "\n";
  bench_codec(10 << 10, 8, 2000);
  bench_codec(50 << 10, 8, 1000);
  bench_codec(1 << 20, 8, 100);
  return 0;
}

}  // namespace
}  // namespace ton::broadcast_sim

int main() {
  return ton::broadcast_sim::benchmark_main();
}

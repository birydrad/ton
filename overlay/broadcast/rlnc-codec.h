#pragma once

#include <memory>
#include <vector>

#include "td/fec/algebra/MatrixGF256.h"
#include "td/fec/fec.h"
#include "td/utils/buffer.h"
#include "td/utils/int_types.h"

namespace ton::overlay::broadcast {

// SIMD-backed RLNC over GF(2^8). Each wire shard is:
//   coefficient_vector[shard_count] || coded_payload[shard_payload_size]
// where shard_payload_size = ⌈body_size / target_shard_count⌉.

class RlncEncoder final : public td::fec::Encoder {
 public:
  RlncEncoder(td::uint32 target_shard_count, td::BufferSlice body);
  td::fec::Symbol gen_symbol(td::uint32 shard_id) override;

 private:
  td::uint32 shard_payload_size_ = 0;
  td::uint32 shard_count_ = 0;
  size_t row_size_ = 0;
  td::MatrixGF256 source_rows_;
};

class RlncDecoder final : public td::fec::Decoder {
 public:
  RlncDecoder(td::uint32 target_shard_count, td::uint64 body_size);

  bool may_try_decode() const override;
  td::Status add_symbol(td::fec::Symbol symbol) override;
  td::Result<td::fec::DataWithEncoder> try_decode(bool need_encoder) override;

  // Produce a recoded shard as a random linear combination of the shards already accepted.
  // Used when a partial-rank node forwards on behalf of others. Returns Status::Error if no
  // shards accepted yet.
  td::Result<td::fec::Symbol> gen_symbol(td::uint32 shard_id) const override;

 private:
  td::uint32 reduced_pivot(td::Slice wire);

  td::uint64 body_size_ = 0;
  td::uint32 shard_payload_size_ = 0;
  td::uint32 shard_count_ = 0;
  size_t row_size_ = 0;
  td::uint32 rank_ = 0;
  std::vector<std::unique_ptr<td::MatrixGF256>> rows_;
  td::MatrixGF256 coefficients_;
  td::MatrixGF256 incoming_;
};

// Factories returning td::fec types (production callers don't need the concrete RLNC API).
std::unique_ptr<td::fec::Encoder> make_rlnc_encoder(td::uint32 target_shard_count, td::BufferSlice body);
std::unique_ptr<td::fec::Decoder> make_rlnc_decoder(td::uint32 target_shard_count, td::uint64 body_size);

}  // namespace ton::overlay::broadcast

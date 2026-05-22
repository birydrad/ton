#include <algorithm>
#include <limits>
#include <utility>

#include "rlnc-codec.h"

namespace ton::overlay::broadcast {
namespace {

td::uint32 ceil_div_to_uint32(td::uint64 value, td::uint32 divisor) {
  if (divisor == 0) {
    return 0;
  }
  auto result = value == 0 ? 0 : 1 + (value - 1) / divisor;
  return result > std::numeric_limits<td::uint32>::max() ? 0 : static_cast<td::uint32>(result);
}

struct RlncLayout {
  td::uint32 shard_payload_size = 0;
  td::uint32 shard_count = 0;
};

RlncLayout rlnc_layout(td::uint64 body_size, td::uint32 target_shard_count) {
  auto shard_payload_size = ceil_div_to_uint32(std::max<td::uint64>(1, body_size), target_shard_count);
  auto shard_count = ceil_div_to_uint32(body_size, shard_payload_size);
  return {.shard_payload_size = shard_payload_size, .shard_count = std::max<td::uint32>(1, shard_count)};
}

td::uint32 next_random(td::uint32 &state) {
  state ^= state << 13;
  state ^= state >> 17;
  state ^= state << 5;
  return state;
}

td::uint8 random_coefficient(td::uint32 &state) {
  return static_cast<td::uint8>(next_random(state) & 0xff);
}

td::MutableSlice rlnc_payload(td::MutableSlice row, td::uint32 shard_count) {
  return row.substr(shard_count);
}

td::uint32 rlnc_pivot(td::Slice row, td::uint32 shard_count) {
  for (td::uint32 i = 0; i < shard_count; i++) {
    if (row[i] != 0) {
      return i;
    }
  }
  return shard_count;
}

void add_random_rows(td::MatrixGF256 &row, const td::MatrixGF256 &basis, td::uint32 shard_count, td::uint32 shard_id) {
  td::uint32 state = shard_id * 1103515245u + 12345u;
  bool any = false;
  for (td::uint32 i = 0; i < shard_count; i++) {
    auto factor = random_coefficient(state);
    any = any || factor != 0;
    row.row_add_mul(0, basis.row(i), td::Octet(factor));
  }
  if (!any && shard_count != 0) {
    row.row_add(0, basis.row(shard_id % shard_count));
  }
}

}  // namespace

// === RlncEncoder ================================================================================

RlncEncoder::RlncEncoder(td::uint32 target_shard_count, td::BufferSlice body)
    : shard_payload_size_(rlnc_layout(body.size(), target_shard_count).shard_payload_size)
    , shard_count_(rlnc_layout(body.size(), target_shard_count).shard_count)
    , row_size_(static_cast<size_t>(shard_count_) + shard_payload_size_)
    , source_rows_(shard_count_, row_size_) {
  source_rows_.set_zero();
  for (td::uint32 i = 0; i < shard_count_; i++) {
    source_rows_.set(i, i, td::Octet(1));
    auto begin = static_cast<size_t>(i) * shard_payload_size_;
    auto end = std::min<size_t>(body.size(), begin + shard_payload_size_);
    rlnc_payload(source_rows_.row(i), shard_count_)
        .substr(0, end - begin)
        .copy_from(body.as_slice().substr(begin, end - begin));
  }
}

td::fec::Symbol RlncEncoder::gen_symbol(td::uint32 shard_id) {
  if (shard_id < shard_count_) {
    return {shard_id, td::BufferSlice(source_rows_.row(shard_id))};
  }
  td::MatrixGF256 row(1, row_size_);
  row.set_zero();
  add_random_rows(row, source_rows_, shard_count_, shard_id);
  return {shard_id, td::BufferSlice(row.row(0))};
}

// === RlncDecoder ================================================================================

RlncDecoder::RlncDecoder(td::uint32 target_shard_count, td::uint64 body_size)
    : body_size_(body_size)
    , shard_payload_size_(rlnc_layout(body_size, target_shard_count).shard_payload_size)
    , shard_count_(rlnc_layout(body_size, target_shard_count).shard_count)
    , row_size_(static_cast<size_t>(shard_count_) + shard_payload_size_)
    , rows_(shard_count_)
    , coefficients_(1, shard_count_)
    , incoming_(1, row_size_) {
  coefficients_.set_zero();
  incoming_.set_zero();
}

bool RlncDecoder::may_try_decode() const {
  return shard_count_ != 0 && rank_ == shard_count_;
}

td::Status RlncDecoder::add_symbol(td::fec::Symbol symbol) {
  if (shard_payload_size_ == 0 || shard_count_ == 0 ||
      symbol.data.size() != static_cast<size_t>(shard_count_ + shard_payload_size_)) {
    return td::Status::Error("invalid rlnc shard");
  }
  auto pivot = reduced_pivot(symbol.data.as_slice());
  if (pivot == shard_count_) {
    return td::Status::OK();  // linearly dependent — silently dropped
  }
  incoming_.set_zero();
  incoming_.row_set(0, symbol.data.as_slice());
  for (td::uint32 i = 0; i < shard_count_; i++) {
    if (rows_[i] != nullptr) {
      incoming_.row_add_mul(0, rows_[i]->row(0), td::Octet(incoming_.row(0)[i]));
    }
  }
  incoming_.row_multiply(0, td::Octet(incoming_.row(0)[pivot]).inverse());
  for (td::uint32 i = 0; i < shard_count_; i++) {
    if (rows_[i] != nullptr && rows_[i]->row(0)[pivot] != 0) {
      rows_[i]->row_add_mul(0, incoming_.row(0), td::Octet(rows_[i]->row(0)[pivot]));
    }
  }
  if (rows_[pivot] == nullptr) {
    rows_[pivot] = std::make_unique<td::MatrixGF256>(1, row_size_);
  }
  rows_[pivot]->row_set(0, incoming_.row(0));
  rank_++;
  return td::Status::OK();
}

td::Result<td::fec::DataWithEncoder> RlncDecoder::try_decode(bool) {
  if (!may_try_decode()) {
    return td::Status::Error("broadcast shards are incomplete");
  }
  td::BufferSlice body(static_cast<size_t>(body_size_));
  auto output = body.as_slice();
  size_t offset = 0;
  for (td::uint32 shard_id = 0; shard_id < shard_count_; shard_id++) {
    auto size = std::min<size_t>(shard_payload_size_, output.size() - offset);
    output.substr(offset, size).copy_from(rlnc_payload(rows_[shard_id]->row(0), shard_count_).substr(0, size));
    offset += size;
  }
  td::fec::DataWithEncoder result;
  result.data = std::move(body);
  return std::move(result);
}

td::Result<td::fec::Symbol> RlncDecoder::gen_symbol(td::uint32 shard_id) const {
  if (rank_ == 0) {
    return td::Status::Error("no broadcast shards to recode");
  }
  td::MatrixGF256 recoded(1, row_size_);
  recoded.set_zero();
  td::uint32 state = shard_id * 1103515245u + 12345u;
  td::uint32 first = shard_count_;
  bool any = false;
  for (td::uint32 i = 0; i < shard_count_; i++) {
    if (rows_[i] == nullptr) {
      continue;
    }
    first = std::min(first, i);
    auto factor = random_coefficient(state);
    any = any || factor != 0;
    recoded.row_add_mul(0, rows_[i]->row(0), td::Octet(factor));
  }
  if (!any && first != shard_count_) {
    recoded.row_add(0, rows_[first]->row(0));
  }
  if (rlnc_pivot(recoded.row(0), shard_count_) == shard_count_) {
    return td::Status::Error("failed to recode broadcast shard");
  }
  return td::fec::Symbol{shard_id, td::BufferSlice(recoded.row(0))};
}

td::uint32 RlncDecoder::reduced_pivot(td::Slice wire) {
  coefficients_.set_zero();
  coefficients_.row_set(0, wire.substr(0, shard_count_));
  for (td::uint32 i = 0; i < shard_count_; i++) {
    if (rows_[i] != nullptr) {
      coefficients_.row_add_mul(0, rows_[i]->row(0), td::Octet(coefficients_.row(0)[i]));
    }
  }
  return rlnc_pivot(coefficients_.row(0), shard_count_);
}

// === Factories ==================================================================================

std::unique_ptr<td::fec::Encoder> make_rlnc_encoder(td::uint32 target_shard_count, td::BufferSlice body) {
  return std::make_unique<RlncEncoder>(target_shard_count, std::move(body));
}

std::unique_ptr<td::fec::Decoder> make_rlnc_decoder(td::uint32 target_shard_count, td::uint64 body_size) {
  return std::make_unique<RlncDecoder>(target_shard_count, body_size);
}

}  // namespace ton::overlay::broadcast

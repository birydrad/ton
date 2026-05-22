/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/

#include "common/errorcode.h"
#include "overlay/broadcast/rlnc-codec.h"

#include "storage.h"

namespace ton {
namespace overlay {

namespace {

// === Codec adapters =============================================================================

class WholeFecDecoder final : public td::fec::Decoder {
 public:
  explicit WholeFecDecoder(td::uint32 expected_size) : expected_size_(expected_size) {
  }

  bool may_try_decode() const override {
    return received_;
  }

  td::Result<td::fec::DataWithEncoder> try_decode(bool) override {
    if (!received_) {
      return td::Status::Error(ErrorCode::notready, "whole payload has no body");
    }
    td::fec::DataWithEncoder result;
    result.data = body_.clone();
    return std::move(result);
  }

  td::Status add_symbol(td::fec::Symbol symbol) override {
    if (symbol.id != 0) {
      return td::Status::Error(ErrorCode::protoviolation, "whole payload accepts only symbol id 0");
    }
    if (received_) {
      return td::Status::OK();
    }
    if (symbol.data.size() != expected_size_) {
      return td::Status::Error(ErrorCode::protoviolation, "whole payload size mismatch");
    }
    body_ = std::move(symbol.data);
    received_ = true;
    return td::Status::OK();
  }

 private:
  td::uint32 expected_size_;
  td::BufferSlice body_;
  bool received_ = false;
};

class WholeFecEncoder final : public td::fec::Encoder {
 public:
  explicit WholeFecEncoder(td::BufferSlice body) : body_(std::move(body)) {
  }

  td::fec::Symbol gen_symbol(td::uint32 id) override {
    CHECK(id == 0);
    return {id, body_.clone()};
  }

 private:
  td::BufferSlice body_;
};

// === Storage impls ==============================================================================

class WholeStorage final : public BroadcastStorage {
 public:
  td::Result<std::unique_ptr<td::fec::Decoder>> for_receiving(const BroadcastInfo &info) const override {
    return std::make_unique<WholeFecDecoder>(info.common.data_size);
  }
  std::unique_ptr<td::fec::Encoder> for_sourcing(const BroadcastInfo &, td::BufferSlice body) const override {
    return std::make_unique<WholeFecEncoder>(std::move(body));
  }
};

class RaptorqStorage final : public BroadcastStorage {
 public:
  td::Result<std::unique_ptr<td::fec::Decoder>> for_receiving(const BroadcastInfo &info) const override {
    auto *fec = std::get_if<mode::TwostepFec>(&info.mode);
    CHECK(fec != nullptr);
    if (fec->part_size == 0) {
      return td::Status::Error(ErrorCode::protoviolation, "invalid FEC part size");
    }
    auto symbols_needed = (info.common.data_size + fec->part_size - 1) / fec->part_size;
    TRY_RESULT(decoder, td::fec::RaptorQDecoder::create({info.common.data_size, fec->part_size, symbols_needed}));
    return std::unique_ptr<td::fec::Decoder>(std::move(decoder));
  }
  std::unique_ptr<td::fec::Encoder> for_sourcing(const BroadcastInfo &info, td::BufferSlice body) const override {
    auto *fec = std::get_if<mode::TwostepFec>(&info.mode);
    CHECK(fec != nullptr);
    auto encoder = td::fec::RaptorQEncoder::create(std::move(body), fec->part_size);
    encoder->prepare_more_symbols();
    return encoder;
  }
};

class RlncStorage final : public BroadcastStorage {
 public:
  td::Result<std::unique_ptr<td::fec::Decoder>> for_receiving(const BroadcastInfo &info) const override {
    return broadcast::make_rlnc_decoder(mode_traits(info.mode).required_pieces, info.common.data_size);
  }
  std::unique_ptr<td::fec::Encoder> for_sourcing(const BroadcastInfo &info, td::BufferSlice body) const override {
    return broadcast::make_rlnc_encoder(mode_traits(info.mode).required_pieces, std::move(body));
  }
};

}  // namespace

// === Storage singletons =========================================================================

const std::shared_ptr<const BroadcastStorage> &whole_storage() {
  static const auto instance = std::shared_ptr<const BroadcastStorage>(std::make_shared<WholeStorage>());
  return instance;
}

const std::shared_ptr<const BroadcastStorage> &raptorq_storage() {
  static const auto instance = std::shared_ptr<const BroadcastStorage>(std::make_shared<RaptorqStorage>());
  return instance;
}

const std::shared_ptr<const BroadcastStorage> &rlnc_storage() {
  static const auto instance = std::shared_ptr<const BroadcastStorage>(std::make_shared<RlncStorage>());
  return instance;
}

}  // namespace overlay
}  // namespace ton

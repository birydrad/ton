/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    TON Blockchain Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with TON Blockchain Library.  If not, see <http://www.gnu.org/licenses/>.
*/
#pragma once

#include <set>

#include "adnl.h"

namespace ton::adnl {

class AdnlSenderEx : public AdnlSenderInterface {
 public:
  AdnlSenderEx() = default;
  explicit AdnlSenderEx(td::uint64 default_mtu) : default_mtu_(default_mtu) {
  }

  virtual void add_id(AdnlNodeIdShort local_id) = 0;

  // MTU for incoming messages in peer pair (local_id, peer_id) is max of:
  // - default mtu
  // - local id mtu of local_id
  // - max peer mtu of (local_id, peer_id)
  // MTU = 0 means that incoming connections from this peer are not accepted
  // Use PeersMtuGuard instead of calling add_peer_mtu/remove_peer_mtu directly
  void set_default_mtu(td::uint64 mtu);
  void set_local_id_mtu(AdnlNodeIdShort local_id, td::uint64 mtu);
  void add_peer_mtu(AdnlNodeIdShort local_id, AdnlNodeIdShort peer_id, td::uint64 mtu);
  void remove_peer_mtu(AdnlNodeIdShort local_id, AdnlNodeIdShort peer_id, td::uint64 mtu);

 protected:
  // Called after changing mtu through methods above
  // No local_id or peer_id means all local ids/peer ids
  // Use get_peer_mtu to get mtu value
  // If peer_id is present, local_id is guaranteed to be present
  virtual void on_mtu_updated(td::optional<AdnlNodeIdShort> local_id, td::optional<AdnlNodeIdShort> peer_id) = 0;

  td::uint64 get_peer_mtu(AdnlNodeIdShort local_id, AdnlNodeIdShort peer_id);

  td::uint64 get_peer_mtu_inner(AdnlNodeIdShort local_id, AdnlNodeIdShort peer_id);
  std::vector<std::pair<AdnlNodeIdShort, td::uint64>> get_local_id_peers_mtu(AdnlNodeIdShort local_id);
  td::uint64 get_local_id_mtu(AdnlNodeIdShort local_id);

 private:
  td::uint64 default_mtu_ = Adnl::get_mtu();

  struct LocalIdMtu {
    td::uint64 mtu = 0;
    std::map<AdnlNodeIdShort, std::multiset<td::uint64>> mtu_peers;
  };
  std::map<AdnlNodeIdShort, LocalIdMtu> mtu_local_ids_;
};

class PeersMtuGuard {
 public:
  PeersMtuGuard() = default;
  PeersMtuGuard(td::actor::ActorId<AdnlSenderEx> sender, AdnlNodeIdShort local_id, td::uint64 mtu)
      : local_id_(local_id), mtu_(mtu) {
    add_sender(std::move(sender));
  }
  PeersMtuGuard(std::vector<td::actor::ActorId<AdnlSenderEx>> senders, AdnlNodeIdShort local_id, td::uint64 mtu)
      : local_id_(local_id), mtu_(mtu) {
    for (auto &sender : senders) {
      add_sender(std::move(sender));
    }
  }
  PeersMtuGuard(td::actor::ActorId<AdnlSenderEx> sender, AdnlNodeIdShort local_id,
                std::vector<AdnlNodeIdShort> peer_ids, td::uint64 mtu)
      : PeersMtuGuard(std::move(sender), local_id, mtu) {
    for (const auto &peer_id : peer_ids) {
      set_peer(peer_id, true);
    }
  }
  PeersMtuGuard(const PeersMtuGuard &) = delete;
  PeersMtuGuard(PeersMtuGuard &&other) noexcept
      : senders_(std::move(other.senders_))
      , local_id_(other.local_id_)
      , peer_ids_(std::move(other.peer_ids_))
      , mtu_(other.mtu_) {
    other.senders_.clear();
  }
  ~PeersMtuGuard() {
    reset();
  }
  PeersMtuGuard &operator=(const PeersMtuGuard &other) = delete;
  PeersMtuGuard &operator=(PeersMtuGuard &&other) noexcept {
    if (this == &other) {
      return *this;
    }
    reset();
    senders_ = std::move(other.senders_);
    local_id_ = other.local_id_;
    peer_ids_ = std::move(other.peer_ids_);
    mtu_ = other.mtu_;
    other.senders_.clear();
    return *this;
  }

  void set_peer(AdnlNodeIdShort peer_id, bool enabled) {
    if (senders_.empty()) {
      return;
    }
    bool exists = peer_ids_.count(peer_id) > 0;
    if (enabled == exists) {
      return;
    }
    if (enabled) {
      peer_ids_.insert(peer_id);
      for (auto &sender : senders_) {
        td::actor::send_closure(sender, &AdnlSenderEx::add_peer_mtu, local_id_, peer_id, mtu_);
      }
    } else {
      peer_ids_.erase(peer_id);
      for (auto &sender : senders_) {
        td::actor::send_closure(sender, &AdnlSenderEx::remove_peer_mtu, local_id_, peer_id, mtu_);
      }
    }
  }

 private:
  std::vector<td::actor::ActorId<AdnlSenderEx>> senders_;
  AdnlNodeIdShort local_id_;
  std::set<AdnlNodeIdShort> peer_ids_;
  td::uint64 mtu_ = 0;

  void add_sender(td::actor::ActorId<AdnlSenderEx> sender) {
    if (sender.empty()) {
      return;
    }
    for (const auto &existing : senders_) {
      if (existing == sender) {
        return;
      }
    }
    senders_.push_back(std::move(sender));
  }

  void reset() {
    if (senders_.empty()) {
      return;
    }
    for (const AdnlNodeIdShort peer_id : peer_ids_) {
      for (auto &sender : senders_) {
        td::actor::send_closure(sender, &AdnlSenderEx::remove_peer_mtu, local_id_, peer_id, mtu_);
      }
    }
    peer_ids_.clear();
    senders_.clear();
  }
};

}  // namespace ton::adnl

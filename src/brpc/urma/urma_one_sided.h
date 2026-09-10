// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BRPC_URMA_ONE_SIDED_H
#define BRPC_URMA_ONE_SIDED_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <vector>

#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "butil/macros.h"

#if BRPC_WITH_URMA

namespace brpc {
namespace urma {

// ============================================================================
// One-sided operation types and wire structures.
//
// The URMA transport can operate in three IO modes (controlled by
// --urma_io_mode):
//   0 = SEND_ONLY  (default, backward-compatible two-sided SEND/RECV)
//   1 = WRITE_ONLY (all sizes use WRITE_IN_BAND)
//   2 = HYBRID     (small IO <= threshold uses WRITE_IN_BAND,
//                   large IO uses PRE_WRITE + READ)
//
// Small IO path (WRITE_IN_BAND, 1 RTT):
//   Sender writes payload directly into peer's pre-registered recv_buf via
//   URMA_OPC_WRITE_IMM. The 64-bit immediate data carries the opcode and
//   the buffer offset. The receiver copies the payload from recv_buf into
//   _socket->_read_buf, then sends a WRITE_IN_BAND_ACK to release the
//   sender's send_buf slot.
//
// Large IO path (PRE_WRITE + READ, 2 RTT, zero-copy):
//   Sender builds a control message listing its IOBuf block addresses,
//   sends it via WRITE_IMM into peer's recv_buf. The receiver issues
//   URMA_OPC_READ WRs to pull data from the sender's pool buffers into
//   local pool buffers, then sends POST_WRITE to release the sender's
//   send_buf slot and saved IOBuf block references.
// ============================================================================

// IO opcode encoded in the low 4 bits of the 64-bit immediate data.
enum UrmaIoOpcode : uint8_t {
    URMA_IO_SEND            = 0,  // (unused in one-sided path, reserved)
    URMA_IO_PRE_WRITE       = 1,  // control message: "I have data to be READ"
    URMA_IO_POST_WRITE      = 2,  // ack: "READ done, you can release send_buf"
    URMA_IO_WRITE_IN_BAND   = 3,  // small IO: data written inline into recv_buf
    URMA_IO_WRITE_IN_BAND_ACK = 4,  // ack: "data consumed, you can release send_buf"
};

// 64-bit immediate data carried by WRITE_IMM WRs.
// Layout: [opcode:4][buffer_offset:16][reserved:44]
// buffer_offset is in units of 1024 bytes (the allocation granularity).
union UrmaWriteImmData {
    uint64_t data{0};
    struct {
        uint64_t opcode       : 4;
        uint64_t buffer_offset : 16;  // offset / 1024 in the peer's buffer
        uint64_t reserved     : 44;
    } io;
};
static_assert(sizeof(UrmaWriteImmData) == sizeof(uint64_t),
              "UrmaWriteImmData must be 64 bits");

// Control message header (16 bytes, 4-byte aligned). Placed at the start
// of every one-sided buffer slot.
struct UrmaMessageHead {
    uint32_t magic;          // URMA_CTRL_MAGIC
    uint32_t message_size;   // total allocated size (head + payload), bytes
    uint32_t data_count;     // WRITE_IN_BAND: payload byte count
                             // PRE_WRITE: number of PageBufferInMessage entries
    uint32_t flags;          // reserved for future use
    uint64_t request_id;     // sender-side request ID for matching
};
static_assert(sizeof(UrmaMessageHead) == 24, "UrmaMessageHead must be 24 bytes");

constexpr uint32_t URMA_CTRL_MAGIC = 0x55524D31;  // "URM1"

// Per-block entry in a PRE_WRITE control message. Describes one IOBuf
// block on the sender side that the receiver should READ from.
struct PageBufferInMessage {
    uint64_t addr;           // sender-side virtual address of the block
    uint32_t size;           // block size in bytes
    uint32_t seg_token_id;   // sender's pool segment token id (unused; pool
                             // seg is already imported via handshake)
};

// Allocation granularity for send_buf / recv_buf (bytes).
constexpr uint32_t URMA_ONE_SIDED_ALLOC_UNIT = 1024;

// Bitmap-based ring buffer allocator for send_buf / recv_buf.
// ACK/POST_WRITE messages may release slots out of order, so a simple
// FIFO ring is insufficient. Uses 1024-byte granularity.
// Thread-safe (internal mutex), since PollCq (completion handler) and
// KeepWrite (send path) may run on different threads in polling mode.
class UrmaRingBuf {
public:
    UrmaRingBuf() = default;
    ~UrmaRingBuf() = default;

    // Initialize the allocator for a buffer of @capacity bytes.
    // @capacity must be a multiple of URMA_ONE_SIDED_ALLOC_UNIT.
    void Init(uint32_t capacity);

    // Try to allocate @size bytes (rounded up to allocation units).
    // On success, sets @offset to the byte offset within the buffer and
    // returns true. On failure (not enough contiguous space), returns false.
    bool Allocate(uint32_t size, uint32_t* offset);

    // Release a previously allocated region starting at @offset with @size
    // bytes (same size passed to Allocate).
    void Release(uint32_t offset, uint32_t size);

    // Total free space in bytes.
    uint32_t Available() const;

    // Total capacity in bytes.
    uint32_t Capacity() const { return _capacity; }

    DISALLOW_COPY_AND_ASSIGN(UrmaRingBuf);

private:
    mutable std::mutex _mutex;
    uint32_t _capacity{0};       // total bytes
    uint32_t _unit_count{0};     // capacity / ALLOC_UNIT
    uint32_t _free_units{0};     // currently free units
    // Bitmap: 1 = allocated, 0 = free. Indexed by unit number.
    std::vector<uint8_t> _bitmap;

    // Round up to allocation units.
    uint32_t UnitsForSize(uint32_t size) const {
        return (size + URMA_ONE_SIDED_ALLOC_UNIT - 1) /
               URMA_ONE_SIDED_ALLOC_UNIT;
    }
};

// ---- UrmaRingBuf inline implementations ----

inline void UrmaRingBuf::Init(uint32_t capacity) {
    std::lock_guard<std::mutex> lock(_mutex);
    _capacity = capacity;
    _unit_count = capacity / URMA_ONE_SIDED_ALLOC_UNIT;
    _free_units = _unit_count;
    _bitmap.assign(_unit_count, 0);
}

inline bool UrmaRingBuf::Allocate(uint32_t size, uint32_t* offset) {
    const uint32_t units = UnitsForSize(size);
    if (units == 0 || units > _unit_count) {
        return false;
    }
    std::lock_guard<std::mutex> lock(_mutex);
    if (units > _free_units) {
        return false;
    }
    // First-fit search for contiguous free units.
    uint32_t start = 0;
    uint32_t consecutive = 0;
    for (uint32_t i = 0; i < _unit_count; ++i) {
        if (_bitmap[i] == 0) {
            if (consecutive == 0) {
                start = i;
            }
            ++consecutive;
            if (consecutive >= units) {
                for (uint32_t j = start; j < start + units; ++j) {
                    _bitmap[j] = 1;
                }
                _free_units -= units;
                *offset = start * URMA_ONE_SIDED_ALLOC_UNIT;
                return true;
            }
        } else {
            consecutive = 0;
        }
    }
    return false;
}

inline void UrmaRingBuf::Release(uint32_t offset, uint32_t size) {
    const uint32_t units = UnitsForSize(size);
    const uint32_t start = offset / URMA_ONE_SIDED_ALLOC_UNIT;
    std::lock_guard<std::mutex> lock(_mutex);
    for (uint32_t i = start; i < start + units && i < _unit_count; ++i) {
        if (_bitmap[i] == 1) {
            _bitmap[i] = 0;
            ++_free_units;
        }
    }
}

inline uint32_t UrmaRingBuf::Available() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _free_units * URMA_ONE_SIDED_ALLOC_UNIT;
}

// Receiver-side slot state for the large IO (PRE_WRITE + READ) path.
// Each PRE_WRITE control message consumes one RxSlot. The slot tracks
// the in-flight READ WRs and buffers the received data until all READs
// complete.
struct UrmaRxSlot {
    enum State : uint8_t {
        IDLE       = 0,
        READING    = 1,
        DATA_READY = 2,
    };
    butil::atomic<State> state{IDLE};
    uint64_t write_imm{0};   // the imm_data from the PRE_WRITE (for ACK)
    uint64_t request_id{0};  // sender's request ID
    uint32_t total_bytes{0}; // total bytes to receive across all READs
    uint32_t received_bytes{0};
    // READ target buffers: {local_addr, size} pairs. Data is copied from
    // these into _socket->_read_buf after all READs complete.
    std::vector<std::pair<void*, size_t>> read_targets;
    // Local pool buffers allocated as READ destinations (need cleanup).
    std::vector<void*> local_bufs;

    UrmaRxSlot() = default;
    void Reset() {
        state.store(IDLE, butil::memory_order_relaxed);
        write_imm = 0;
        request_id = 0;
        total_bytes = 0;
        received_bytes = 0;
        read_targets.clear();
        local_bufs.clear();
    }
};

// Sender-side context for an in-flight one-sided message. Tracks the
// send_buf offset/size so the SEND completion or POST_WRITE ACK can
// release the buffer. For large IO, also holds IOBuf block references
// to keep the blocks alive until the receiver finishes READing them.
struct UrmaSendContext {
    uint64_t request_id{0};
    uint32_t send_buf_offset{0};  // byte offset in send_buf
    uint32_t send_buf_size{0};    // allocated size in send_buf
    uint8_t opcode{0};            // URMA_IO_WRITE_IN_BAND or URMA_IO_PRE_WRITE
    butil::IOBuf saved_blocks;    // large IO: holds IOBuf block refs

    UrmaSendContext() = default;
};

// user_ctx encoding for one-sided WRs. Since urma_cr_opcode_t has no
// READ/WRITE value, we encode the operation type into the 64-bit
// user_ctx field to distinguish completions in HandleCompletion.
enum OneSideSenderType : uint8_t {
    ONE_SIDE_NONE           = 0,  // 0 reserved for pure-ack (existing path)
    CTRL_DATA_REQUEST       = 1,  // WRITE_IN_BAND or PRE_WRITE send completion
    READ_DATA_REQUEST       = 2,  // READ completion (receiver side)
    CTRL_DATA_RESPONSE      = 3,  // WRITE_IN_BAND_ACK or POST_WRITE send completion
};

// Encode/decode helpers for user_ctx.
// Layout: [type:8][seq:56]
inline uint64_t EncodeUserCtx(OneSideSenderType type, uint64_t seq) {
    return (static_cast<uint64_t>(type) << 56) | (seq & 0x00FFFFFFFFFFFFFF);
}

inline OneSideSenderType DecodeUserCtxType(uint64_t ctx) {
    return static_cast<OneSideSenderType>((ctx >> 56) & 0xFF);
}

inline uint64_t DecodeUserCtxSeq(uint64_t ctx) {
    return ctx & 0x00FFFFFFFFFFFFFF;
}

// Size of the RX slot ring (number of concurrent large-IO receives).
constexpr uint32_t URMA_RX_RING_SIZE = 128;

// Magic value for request_id generation (simple counter).
// Using uint64 so wrap-around is practically impossible.

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA

#endif  // BRPC_URMA_ONE_SIDED_H

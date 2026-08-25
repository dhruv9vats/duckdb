//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/memory_usage_probe.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

//! Storage accounting events. Implementations must not throw.
class MemoryUsageProbe {
public:
	virtual ~MemoryUsageProbe() = default;

	virtual void BufferPoolSnapshot(MemoryTag tag, idx_t bytes) noexcept = 0;
	virtual void BufferPoolDelta(MemoryTag tag, int64_t bytes) noexcept = 0;
	virtual void BufferPoolLimit(idx_t bytes) noexcept = 0;

	virtual void TemporaryStorageDelta(MemoryTag tag, int64_t bytes) noexcept = 0;
	virtual void TemporaryStorageLimit(optional_idx bytes) noexcept = 0;
	virtual void TemporaryDirectoryDelta(int64_t bytes) noexcept = 0;
};

} // namespace duckdb

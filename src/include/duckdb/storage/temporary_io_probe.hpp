//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/temporary_io_probe.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/common/query_context.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

enum class TemporaryIoDirection : uint8_t { SPILL, RELOAD };

struct TemporaryIoInfo {
	QueryContext context;
	TemporaryIoDirection direction;
	uint64_t block_id;
	MemoryTag tag;
	uint64_t buffer_bytes;
};

//! One temporary-block I/O operation. Destruction records failure.
class TemporaryIoEvent {
public:
	virtual ~TemporaryIoEvent() = default;
	virtual void Complete(uint64_t storage_bytes) noexcept = 0;
};

//! Storage-level probe; telemetry implementations remain outside storage.
class TemporaryIoProbe {
public:
	virtual ~TemporaryIoProbe() = default;
	virtual unique_ptr<TemporaryIoEvent> Start(const TemporaryIoInfo &info) = 0;
};

} // namespace duckdb

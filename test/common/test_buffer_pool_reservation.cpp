#include "catch.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/storage/block_allocator.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/buffer/buffer_pool_reservation.hpp"
#include "duckdb/main/database.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

namespace {

class RecordingMemoryProbe final : public MemoryUsageProbe {
public:
	void BufferPoolSnapshot(MemoryTag tag, idx_t bytes) noexcept override {
		snapshots[uint8_t(tag)] = bytes;
		snapshot_count++;
	}

	void BufferPoolDelta(MemoryTag tag, int64_t bytes) noexcept override {
		deltas.emplace_back(tag, bytes);
	}

	void BufferPoolLimit(idx_t bytes) noexcept override {
		limits.push_back(bytes);
	}

	void TemporaryStorageDelta(MemoryTag, int64_t) noexcept override {
	}

	void TemporaryStorageLimit(optional_idx) noexcept override {
	}

	void TemporaryDirectoryDelta(int64_t) noexcept override {
	}

	array<idx_t, MEMORY_TAG_COUNT> snapshots {};
	duckdb::vector<std::pair<MemoryTag, int64_t>> deltas;
	duckdb::vector<idx_t> limits;
	idx_t snapshot_count = 0;
};

class TestBufferPool final : public BufferPool {
public:
	TestBufferPool(BlockAllocator &allocator, idx_t limit) : BufferPool(allocator, limit, false, 0) {
	}

	void RegisterProbe(const duckdb::shared_ptr<MemoryUsageProbe> &probe) {
		RegisterMemoryUsageProbe(probe);
	}

protected:
	// Keep limit outcomes independent from storage eviction.
	EvictionResult EvictBlocks(QueryContext, MemoryTag tag, idx_t extra_memory, idx_t memory_limit,
	                           duckdb::unique_ptr<FileBuffer> *) override {
		TempBufferPoolReservation reservation(tag, *this, extra_memory);
		if (GetUsedMemory(false) <= memory_limit) {
			return {true, std::move(reservation)};
		}

		reservation.Resize(0);
		return {false, std::move(reservation)};
	}
};

} // namespace

TEST_CASE("BufferPoolReservation move assignment releases old reservation", "[storage][buffer_pool]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &pool = DatabaseInstance::GetDatabase(context).GetBufferPool();

	auto baseline = pool.GetUsedMemory();

	BufferPoolReservation r1(MemoryTag::BASE_TABLE, pool);
	r1.Resize(1000);
	REQUIRE(pool.GetUsedMemory() == baseline + 1000);

	BufferPoolReservation r2(MemoryTag::BASE_TABLE, pool);
	r2.Resize(500);
	REQUIRE(pool.GetUsedMemory() == baseline + 1500);

	r1 = std::move(r2);
	REQUIRE(r1.size == 500);
	REQUIRE(r2.size == 0);
	REQUIRE(pool.GetUsedMemory() == baseline + 500);

	r1.Resize(0);
}

TEST_CASE("BufferPool memory probes preserve accounting order", "[storage][buffer_pool]") {
	static constexpr idx_t BLOCK_SIZE = 4096;
	static constexpr idx_t ALLOCATOR_SIZE = 1024 * 1024;
	static constexpr idx_t INITIAL_LIMIT = 128 * 1024;
	static constexpr idx_t UPDATED_LIMIT = 256 * 1024;
	static constexpr idx_t CHARGE = 64 * 1024;
	static constexpr idx_t FAILED_LIMIT = CHARGE - 1;

	Allocator allocator;
	BlockAllocator block_allocator(allocator, BLOCK_SIZE, ALLOCATOR_SIZE, ALLOCATOR_SIZE);
	TestBufferPool pool(block_allocator, INITIAL_LIMIT);
	auto probe = make_shared_ptr<RecordingMemoryProbe>();
	pool.RegisterProbe(probe);

	REQUIRE(probe->snapshot_count == MEMORY_TAG_COUNT);
	REQUIRE(probe->snapshots[uint8_t(MemoryTag::BASE_TABLE)] == 0);
	REQUIRE(probe->limits == duckdb::vector<idx_t> {INITIAL_LIMIT});

	pool.UpdateUsedMemory(MemoryTag::BASE_TABLE, CHARGE);
	REQUIRE(probe->deltas == duckdb::vector<std::pair<MemoryTag, int64_t>> {{MemoryTag::BASE_TABLE, CHARGE}});

	auto second_probe = make_shared_ptr<RecordingMemoryProbe>();
	pool.RegisterProbe(second_probe);
	REQUIRE(second_probe->snapshot_count == MEMORY_TAG_COUNT);
	REQUIRE(second_probe->snapshots[uint8_t(MemoryTag::BASE_TABLE)] == CHARGE);
	REQUIRE(second_probe->deltas.empty());

	pool.SetLimit(UPDATED_LIMIT, "");
	REQUIRE(probe->limits == duckdb::vector<idx_t> {INITIAL_LIMIT, UPDATED_LIMIT});
	REQUIRE(second_probe->limits == duckdb::vector<idx_t> {INITIAL_LIMIT, UPDATED_LIMIT});

	REQUIRE_THROWS(pool.SetLimit(FAILED_LIMIT, ""));
	REQUIRE(probe->limits == duckdb::vector<idx_t> {INITIAL_LIMIT, UPDATED_LIMIT});
	REQUIRE(second_probe->limits == duckdb::vector<idx_t> {INITIAL_LIMIT, UPDATED_LIMIT});

	pool.UpdateUsedMemory(MemoryTag::BASE_TABLE, -static_cast<int64_t>(CHARGE));
	REQUIRE(second_probe->deltas ==
	        duckdb::vector<std::pair<MemoryTag, int64_t>> {{MemoryTag::BASE_TABLE, -static_cast<int64_t>(CHARGE)}});
}

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/telemetry_context.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"

#include <cstdint>

namespace duckdb {

class ClientContext;
class DataChunk;
class Pipeline;
class PipelineExecutor;
class PipelineTask;
class PhysicalOperator;
class MemoryUsageProbe;
class TemporaryIoProbe;
struct DBConfig;
enum class TaskExecutionMode : uint8_t;

enum class TelemetryTaskOutcome : uint8_t { SUCCESS, FAILURE };
enum class TelemetryOperatorPhase : uint8_t { SOURCE, EXECUTE, FINAL_EXECUTE, SINK };

class TelemetryContext {
public:
	explicit TelemetryContext(DBConfig &config);
	~TelemetryContext();

	void Initialize(ClientContext &context);
	shared_ptr<TemporaryIoProbe> TempIoProbe();
	shared_ptr<duckdb::MemoryUsageProbe> MemoryUsageProbe();
	static void StartExecution(ClientContext &context, const PhysicalOperator &root);
#ifdef DUCKDB_QUENT_TELEMETRY
	static void PipelineTaskCreated(ClientContext &context, const PipelineTask &task, const Pipeline &pipeline);
	static void PipelineTaskExecutor(ClientContext &context, const PipelineTask &task,
	                                 const PipelineExecutor &executor);
	static void PipelineTaskRunning(ClientContext &context, const PipelineTask &task, TaskExecutionMode mode);
	static void PipelineTaskReady(ClientContext &context, const PipelineTask &task);
	static void PipelineTaskBlocked(ClientContext &context, const PipelineTask &task);
	static void PipelineTaskFinished(ClientContext &context, const PipelineTask &task, TelemetryTaskOutcome outcome);
	static void EmitChunkTransfer(ClientContext &context, const PipelineExecutor &executor,
	                              const PhysicalOperator &source, const PhysicalOperator &target,
	                              const DataChunk &chunk);
	static void OperatorInvocationStarted(ClientContext &context, const PipelineExecutor &executor,
	                                      const PhysicalOperator &op, TelemetryOperatorPhase phase,
	                                      optional_ptr<const DataChunk> input);
	static void OperatorInvocationFinished(ClientContext &context, const PipelineExecutor &executor,
	                                       const PhysicalOperator &op, optional_ptr<const DataChunk> output);
#else
	static void PipelineTaskCreated(ClientContext &, const PipelineTask &, const Pipeline &) {
	}
	static void PipelineTaskExecutor(ClientContext &, const PipelineTask &, const PipelineExecutor &) {
	}
	static void PipelineTaskRunning(ClientContext &, const PipelineTask &, TaskExecutionMode) {
	}
	static void PipelineTaskReady(ClientContext &, const PipelineTask &) {
	}
	static void PipelineTaskBlocked(ClientContext &, const PipelineTask &) {
	}
	static void PipelineTaskFinished(ClientContext &, const PipelineTask &, TelemetryTaskOutcome) {
	}
	static void EmitChunkTransfer(ClientContext &, const PipelineExecutor &, const PhysicalOperator &,
	                              const PhysicalOperator &, const DataChunk &) {
	}
	static void OperatorInvocationStarted(ClientContext &, const PipelineExecutor &, const PhysicalOperator &,
	                                      TelemetryOperatorPhase, optional_ptr<const DataChunk>) {
	}
	static void OperatorInvocationFinished(ClientContext &, const PipelineExecutor &, const PhysicalOperator &,
	                                       optional_ptr<const DataChunk>) {
	}
#endif

private:
	class Impl;
	shared_ptr<Impl> impl;
};

} // namespace duckdb

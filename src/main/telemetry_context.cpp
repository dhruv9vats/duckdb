#include "duckdb/main/telemetry_context.hpp"

#ifdef DUCKDB_QUENT_TELEMETRY

#include "duckdb-telemetry-bridge/gen/context.rs.h"
#include "duckdb-telemetry-bridge/gen/chunk_transfer.rs.h"
#include "duckdb-telemetry-bridge/gen/engine.rs.h"
#include "duckdb-telemetry-bridge/gen/operator.rs.h"
#include "duckdb-telemetry-bridge/gen/operator_invocation.rs.h"
#include "duckdb-telemetry-bridge/gen/pipeline_task.rs.h"
#include "duckdb-telemetry-bridge/gen/plan.rs.h"
#include "duckdb-telemetry-bridge/gen/port.rs.h"
#include "duckdb-telemetry-bridge/gen/query.rs.h"
#include "duckdb-telemetry-bridge/gen/query_group.rs.h"
#include "duckdb-telemetry-bridge/gen/execution_thread.rs.h"
#include "duckdb-telemetry-bridge/gen/task_queue.rs.h"
#include "duckdb-telemetry-bridge/gen/temporary_block_io.rs.h"
#include "duckdb-telemetry-bridge/gen/temporary_io_channel.rs.h"
#include "duckdb-telemetry-bridge/gen/uuid.rs.h"
#include "duckdb-telemetry-bridge/gen/worker.rs.h"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/temporary_io_probe.hpp"

namespace duckdb {

static constexpr const char *TELEMETRY_STATE_NAME = "quent_telemetry";
static constexpr const char *EXPORTER_ENV = "QUENT_EXPORTER";
static constexpr const char *OUTPUT_DIR_ENV = "QUENT_OUTPUT_DIR";
static constexpr const char *COLLECTOR_ADDRESS_ENV = "QUENT_COLLECTOR_ADDRESS";
static constexpr const char *DEFAULT_OUTPUT_DIR = "events";
static constexpr const char *DEFAULT_COLLECTOR_ADDRESS = "http://localhost:7836";
static constexpr const char *TEMP_SPILL_NAME = "temporary-spill";
static constexpr const char *TEMP_RELOAD_NAME = "temporary-reload";

enum class TelemetryIoOutcome : uint8_t { SUCCESS, FAILURE };

static const char *OperatorPhaseName(TelemetryOperatorPhase phase) {
	switch (phase) {
	case TelemetryOperatorPhase::SOURCE:
		return "source";
	case TelemetryOperatorPhase::EXECUTE:
		return "execute";
	case TelemetryOperatorPhase::FINAL_EXECUTE:
		return "final_execute";
	case TelemetryOperatorPhase::SINK:
		return "sink";
	}
	throw InternalException("Unknown telemetry operator phase");
}

static const char *IoDirectionName(TemporaryIoDirection direction) {
	switch (direction) {
	case TemporaryIoDirection::SPILL:
		return "spill";
	case TemporaryIoDirection::RELOAD:
		return "reload";
	}
	throw InternalException("Unknown temporary I/O direction");
}

class QuentTemporaryIoEvent final : public TemporaryIoEvent {
public:
	explicit QuentTemporaryIoEvent(rust::Box<quent::temporary_block_io::TemporaryBlockIoHandle> handle_p)
	    : handle(std::move(handle_p)) {
	}

	~QuentTemporaryIoEvent() override {
		try {
			Finish(TelemetryIoOutcome::FAILURE, 0);
		} catch (...) {
		}
	}

	void Complete(uint64_t storage_bytes) noexcept override {
		try {
			Finish(TelemetryIoOutcome::SUCCESS, storage_bytes);
		} catch (...) {
		}
	}

private:
	void Finish(TelemetryIoOutcome outcome, uint64_t storage_bytes) {
		if (finished) {
			return;
		}
		finished = true;

		quent::temporary_block_io::IoCompleted completed;
		completed.instance_name = "";
		completed.success = outcome == TelemetryIoOutcome::SUCCESS;
		completed.storage_bytes = storage_bytes;
		handle->io_completed(std::move(completed));
		handle->exit();
	}

private:
	rust::Box<quent::temporary_block_io::TemporaryBlockIoHandle> handle;
	bool finished = false;
};

struct OperatorIds {
	uuid::UUID operator_id;
	uuid::UUID output_port_id;
	bool output_declared;
};

struct PlanEdgeIds {
	reference<const PhysicalOperator> source;
	reference<const PhysicalOperator> target;
	uuid::UUID source_port_id;
	uuid::UUID target_port_id;
};

struct RuntimePlan {
	uuid::UUID plan_id;
	reference_map_t<const PhysicalOperator, OperatorIds> operators;
	vector<PlanEdgeIds> edges;

	bool FindOperator(const PhysicalOperator &op, uuid::UUID &operator_id) const {
		auto entry = operators.find(std::cref(op));
		if (entry == operators.end()) {
			return false;
		}
		operator_id = entry->second.operator_id;
		return true;
	}

	bool FindEdge(const PhysicalOperator &source, const PhysicalOperator &target, uuid::UUID &source_operator_id,
	              uuid::UUID &source_port_id, uuid::UUID &target_operator_id, uuid::UUID &target_port_id) const {
		for (auto &edge : edges) {
			if (&edge.source.get() == &source && &edge.target.get() == &target) {
				auto source_entry = operators.find(std::cref(source));
				auto target_entry = operators.find(std::cref(target));
				D_ASSERT(source_entry != operators.end());
				D_ASSERT(target_entry != operators.end());
				source_operator_id = source_entry->second.operator_id;
				source_port_id = edge.source_port_id;
				target_operator_id = target_entry->second.operator_id;
				target_port_id = edge.target_port_id;
				return true;
			}
		}
		return false;
	}
};

struct TaskTelemetry {
	explicit TaskTelemetry(rust::Box<quent::pipeline_task::PipelineTaskHandle> handle_p)
	    : handle(std::move(handle_p)), task_id(handle->uuid()) {
	}

	rust::Box<quent::pipeline_task::PipelineTaskHandle> handle;
	uuid::UUID task_id;
};

struct InvocationTelemetry {
	explicit InvocationTelemetry(rust::Box<quent::operator_invocation::OperatorInvocationHandle> handle_p,
	                             uuid::UUID task_id_p)
	    : handle(std::move(handle_p)), task_id(task_id_p) {
	}

	rust::Box<quent::operator_invocation::OperatorInvocationHandle> handle;
	uuid::UUID task_id;
};

struct IoAttribution {
	const_reference<PipelineExecutor> executor;
	uuid::UUID query_id;
	uuid::UUID plan_id;
	uuid::UUID task_id;
	uuid::UUID operator_id;
};

static thread_local vector<IoAttribution> active_io_attributions;

static void PushIoAttribution(const PipelineExecutor &executor, uuid::UUID query_id, uuid::UUID plan_id,
                              uuid::UUID task_id, uuid::UUID operator_id) {
	active_io_attributions.push_back({std::cref(executor), query_id, plan_id, task_id, operator_id});
}

static void RemoveIoAttribution(const PipelineExecutor &executor) {
	for (idx_t index = active_io_attributions.size(); index > 0; index--) {
		if (&active_io_attributions[index - 1].executor.get() != &executor) {
			continue;
		}

		active_io_attributions.erase(active_io_attributions.begin() + index - 1);
		return;
	}
}

static optional<IoAttribution> CurrentIoAttribution(uuid::UUID query_id) {
	for (idx_t index = active_io_attributions.size(); index > 0; index--) {
		auto &attribution = active_io_attributions[index - 1];
		if (attribution.query_id == query_id) {
			return attribution;
		}
	}
	return nullopt;
}

class PlanEmitter {
public:
	PlanEmitter(const quent::Context &context, RuntimePlan &runtime_plan_p, uuid::UUID query_id_p,
	            uuid::UUID worker_id_p)
	    : runtime_plan(runtime_plan_p), query_id(query_id_p), worker_id(worker_id_p),
	      operator_observer(quent::operator_::create_observer(context)),
	      port_observer(quent::port::create_observer(context)), plan_observer(quent::plan::create_observer(context)) {
		runtime_plan.plan_id = uuid::now_v7();
	}

	void Emit(const PhysicalOperator &root) {
		EmitOperator(root);

		quent::plan::Declaration declaration;
		declaration.parent.query_id = query_id;
		declaration.parent.plan_id = uuid::new_nil();
		declaration.instance_name = "physical";
		declaration.edges = std::move(edges);
		declaration.worker_id = worker_id;
		plan_observer->declaration(runtime_plan.plan_id, std::move(declaration));
	}

private:
	void EmitOperator(const PhysicalOperator &op) {
		auto entry = runtime_plan.operators.find(std::cref(op));
		if (entry != runtime_plan.operators.end()) {
			return;
		}

		OperatorIds ids {uuid::now_v7(), uuid::now_v7(), false};
		runtime_plan.operators.emplace(std::cref(op), ids);

		quent::operator_::Declaration declaration;
		declaration.plan_id = runtime_plan.plan_id;
		declaration.instance_name = op.GetName();
		declaration.type_name = PhysicalOperatorToString(op.type);
		operator_observer->declaration(ids.operator_id, std::move(declaration));

		if (op.type == PhysicalOperatorType::MERGE_INTO) {
			auto &merge = op.Cast<PhysicalMergeInto>();
			for (idx_t action_index = 0; action_index < merge.actions.size(); action_index++) {
				auto &action = merge.actions[action_index];
				if (!action->op) {
					continue;
				}
				EmitOperator(*action->op);
				auto action_entry = runtime_plan.operators.find(std::cref(*action->op));
				D_ASSERT(action_entry != runtime_plan.operators.end());
				AddEdge(op, *action->op, "action-in-" + std::to_string(action_index));
			}
		}

		auto children = op.GetChildren();
		for (idx_t child_index = 0; child_index < children.size(); child_index++) {
			auto &child = children[child_index].get();
			EmitOperator(child);
			auto input_name = children.size() == 1 ? string("in") : "in-" + std::to_string(child_index);
			AddEdge(child, op, input_name);
		}
	}

	void AddEdge(const PhysicalOperator &source, const PhysicalOperator &target, const string &target_name) {
		auto source_entry = runtime_plan.operators.find(std::cref(source));
		auto target_entry = runtime_plan.operators.find(std::cref(target));
		D_ASSERT(source_entry != runtime_plan.operators.end());
		D_ASSERT(target_entry != runtime_plan.operators.end());
		auto &source_ids = source_entry->second;
		if (!source_ids.output_declared) {
			DeclarePort(source_ids.output_port_id, source_ids.operator_id, "out");
			source_ids.output_declared = true;
		}

		auto input_port_id = uuid::now_v7();
		DeclarePort(input_port_id, target_entry->second.operator_id, target_name);
		quent::plan::Edges edge;
		edge.source = source_ids.output_port_id;
		edge.target = input_port_id;
		edges.push_back(std::move(edge));
		runtime_plan.edges.push_back({std::cref(source), std::cref(target), source_ids.output_port_id, input_port_id});
	}

	void DeclarePort(uuid::UUID id, uuid::UUID operator_id, const string &name) {
		quent::port::Declaration declaration;
		declaration.operator_id = operator_id;
		declaration.instance_name = name;
		port_observer->declaration(id, std::move(declaration));
	}

private:
	RuntimePlan &runtime_plan;
	uuid::UUID query_id;
	uuid::UUID worker_id;
	rust::Box<quent::operator_::OperatorObserver> operator_observer;
	rust::Box<quent::port::PortObserver> port_observer;
	rust::Box<quent::plan::PlanObserver> plan_observer;
	rust::Vec<quent::plan::Edges> edges;
};

static rust::Box<quent::ExporterOptions> CreateExporter(const string &name) {
	if (name == "ndjson") {
		auto output_dir = FileSystem::GetEnvVariable(OUTPUT_DIR_ENV);
		return quent::ExporterOptions::ndjson(output_dir.empty() ? DEFAULT_OUTPUT_DIR : output_dir);
	}
	if (name == "msgpack" || name == "messagepack") {
		auto output_dir = FileSystem::GetEnvVariable(OUTPUT_DIR_ENV);
		return quent::ExporterOptions::msgpack(output_dir.empty() ? DEFAULT_OUTPUT_DIR : output_dir);
	}
	if (name == "postcard") {
		auto output_dir = FileSystem::GetEnvVariable(OUTPUT_DIR_ENV);
		return quent::ExporterOptions::postcard(output_dir.empty() ? DEFAULT_OUTPUT_DIR : output_dir);
	}
	if (name == "collector") {
		auto address = FileSystem::GetEnvVariable(COLLECTOR_ADDRESS_ENV);
		return quent::ExporterOptions::collector(address.empty() ? DEFAULT_COLLECTOR_ADDRESS : address);
	}
	throw InvalidInputException("Unknown Quent exporter: %s", name);
}

class TelemetryContext::Impl {
public:
	class ClientState : public ClientContextState {
	public:
		explicit ClientState(shared_ptr<Impl> telemetry_p)
		    : telemetry(std::move(telemetry_p)), query_group_id(uuid::now_v7()) {
		}

		~ClientState() override {
			FinishQuery();
		}

		void QueryBegin(ClientContext &context) override {
			FinishQuery();
			if (!query_group_declared) {
				quent::query_group::Declaration declaration;
				if (context.GetConnectionId() == DConstants::INVALID_INDEX) {
					declaration.instance_name = "internal-connection";
				} else {
					declaration.instance_name = "connection-" + std::to_string(context.GetConnectionId());
				}
				declaration.engine_id = telemetry->engine_id;
				telemetry->query_group_observer->declaration(query_group_id, std::move(declaration));
				query_group_declared = true;
			}
			query_text = context.GetCurrentQuery();
		}

		void QueryEnd(ClientContext &, optional_ptr<ErrorData>) override {
			FinishQuery();
		}

		void StartExecution(const PhysicalOperator &root) {
			D_ASSERT(query_text);
			quent::query::Init init;
			init.instance_name = std::move(*query_text);
			init.query_group_id = query_group_id;
			query.emplace(quent::query::create(*telemetry->context, std::move(init)));
			query_text.reset();
			(*query)->planning();
			try {
				auto new_plan = make_uniq<RuntimePlan>();
				PlanEmitter(*telemetry->context, *new_plan, (*query)->uuid(), telemetry->worker_id).Emit(root);
				plan = std::move(new_plan);
			} catch (...) {
				(*query)->executing();
				return;
			}
			(*query)->executing();
		}

		void TaskCreated(const PipelineTask &task, const Pipeline &pipeline) {
			lock_guard<mutex> guard(lock);
			if (!query || !plan) {
				return;
			}

			quent::pipeline_task::Created created;
			created.instance_name = "pipeline-task-" + std::to_string(next_task_index);
			created.query_id = (*query)->uuid();
			created.plan_id = plan->plan_id;
			created.worker_id = telemetry->worker_id;
			created.task_index = next_task_index++;
			created.queue_resource_id = telemetry->TaskQueueId();
			created.queue_capacity_entries = 1;
			for (auto &op : pipeline.GetOperators()) {
				uuid::UUID operator_id;
				if (plan->FindOperator(op.get(), operator_id)) {
					created.operator_ids.push_back(operator_id);
				}
			}
			auto handle = quent::pipeline_task::create(*telemetry->context, std::move(created));
			tasks.emplace(std::cref(task), TaskTelemetry(std::move(handle)));
		}

		void TaskExecutor(const PipelineTask &task, const PipelineExecutor &executor) {
			lock_guard<mutex> guard(lock);
			auto entry = tasks.find(std::cref(task));
			if (entry != tasks.end()) {
				executors.erase(std::cref(executor));
				executors.emplace(std::cref(executor), entry->second.task_id);
			}
		}

		void TaskRunning(const PipelineTask &task, TaskExecutionMode mode) {
			lock_guard<mutex> guard(lock);
			auto entry = tasks.find(std::cref(task));
			if (entry == tasks.end()) {
				return;
			}
			auto execution_thread_id = telemetry->ExecutionThreadId();
			quent::pipeline_task::Running running;
			running.instance_name = "";
			running.mode = mode == TaskExecutionMode::PROCESS_PARTIAL ? "partial" : "all";
			running.cpu_id = TaskScheduler::GetEstimatedCPUId();
			running.execution_thread_resource_id = execution_thread_id;
			entry->second.handle->running(std::move(running));
		}

		void TaskReady(const PipelineTask &task) {
			lock_guard<mutex> guard(lock);
			auto entry = tasks.find(std::cref(task));
			if (entry != tasks.end()) {
				quent::pipeline_task::Ready ready;
				ready.queue_resource_id = telemetry->TaskQueueId();
				ready.queue_capacity_entries = 1;
				entry->second.handle->ready(std::move(ready));
			}
		}

		void TaskBlocked(const PipelineTask &task) {
			lock_guard<mutex> guard(lock);
			auto entry = tasks.find(std::cref(task));
			if (entry != tasks.end()) {
				entry->second.handle->blocked();
			}
		}

		void TaskFinished(const PipelineTask &task, TelemetryTaskOutcome outcome) {
			optional<rust::Box<quent::pipeline_task::PipelineTaskHandle>> task_handle;
			vector<rust::Box<quent::operator_invocation::OperatorInvocationHandle>> invocation_handles;
			{
				lock_guard<mutex> guard(lock);
				auto entry = tasks.find(std::cref(task));
				if (entry == tasks.end()) {
					return;
				}
				task_handle.emplace(std::move(entry->second.handle));
				auto task_id = entry->second.task_id;
				invocation_handles = TakeInvocations(task_id);
				EraseExecutors(task_id);
				tasks.erase(entry);
			}
			for (auto &invocation_handle : invocation_handles) {
				try {
					CompleteInvocation(std::move(invocation_handle), TelemetryTaskOutcome::FAILURE, nullptr);
				} catch (...) {
				}
			}
			quent::pipeline_task::Finalizing finalizing;
			finalizing.instance_name = "";
			finalizing.success = outcome == TelemetryTaskOutcome::SUCCESS;
			(*task_handle)->finalizing(std::move(finalizing));
			(*task_handle)->exit();
		}

		void InvocationStarted(const PipelineExecutor &executor, const PhysicalOperator &op,
		                       TelemetryOperatorPhase phase, optional_ptr<const DataChunk> input) {
			lock_guard<mutex> guard(lock);
			if (!query || !plan) {
				return;
			}

			uuid::UUID operator_id;
			if (!plan->FindOperator(op, operator_id)) {
				return;
			}
			auto execution_thread_id = telemetry->ExecutionThreadId();
			auto active = invocations.find(std::cref(executor));
			if (active != invocations.end()) {
				FinishInvocation(executor, TelemetryTaskOutcome::FAILURE, nullptr);
			}

			auto task_id = uuid::new_nil();
			auto task_entry = executors.find(std::cref(executor));
			if (task_entry != executors.end()) {
				task_id = task_entry->second;
			}

			quent::operator_invocation::InvocationCreated created;
			created.instance_name = "operator-invocation-" + std::to_string(next_invocation_index++);
			created.query_id = (*query)->uuid();
			created.plan_id = plan->plan_id;
			created.task_id = task_id;
			created.operator_id = operator_id;
			created.phase = OperatorPhaseName(phase);
			auto handle = quent::operator_invocation::create(*telemetry->context, std::move(created));

			quent::operator_invocation::InvocationRunning running;
			running.instance_name = "";
			running.input_rows = input ? input->size() : 0;
			running.input_logical_bytes = input ? input->GetDataSize() : 0;
			running.execution_thread_resource_id = execution_thread_id;
			handle->invocation_running(std::move(running));
			invocations.emplace(std::cref(executor), InvocationTelemetry(std::move(handle), task_id));
			PushIoAttribution(executor, (*query)->uuid(), plan->plan_id, task_id, operator_id);
		}

		void InvocationFinished(const PipelineExecutor &executor, const PhysicalOperator &,
		                        optional_ptr<const DataChunk> output) {
			RemoveIoAttribution(executor);
			optional<rust::Box<quent::operator_invocation::OperatorInvocationHandle>> handle;
			{
				lock_guard<mutex> guard(lock);
				auto entry = invocations.find(std::cref(executor));
				if (entry == invocations.end()) {
					return;
				}
				handle.emplace(std::move(entry->second.handle));
				invocations.erase(entry);
			}
			CompleteInvocation(std::move(*handle), TelemetryTaskOutcome::SUCCESS, output);
		}

		void EmitChunkTransfer(const PipelineExecutor &executor, const PhysicalOperator &source,
		                       const PhysicalOperator &target, const DataChunk &chunk) {
			uuid::UUID query_id;
			uuid::UUID task_id = uuid::new_nil();
			uuid::UUID source_operator_id;
			uuid::UUID source_port_id;
			uuid::UUID target_operator_id;
			uuid::UUID target_port_id;
			uint64_t transfer_index;
			{
				lock_guard<mutex> guard(lock);
				if (!query || !plan || chunk.size() == 0 ||
				    !plan->FindEdge(source, target, source_operator_id, source_port_id, target_operator_id,
				                    target_port_id)) {
					return;
				}
				query_id = (*query)->uuid();
				auto task_entry = executors.find(std::cref(executor));
				if (task_entry != executors.end()) {
					task_id = task_entry->second;
				}
				transfer_index = next_transfer_index++;
			}

			quent::chunk_transfer::Produced produced;
			produced.instance_name = "chunk-transfer-" + std::to_string(transfer_index);
			produced.query_id = query_id;
			produced.task_id = task_id;
			produced.source_operator_id = source_operator_id;
			produced.source_port_id = source_port_id;
			produced.target_operator_id = target_operator_id;
			produced.target_port_id = target_port_id;
			produced.rows = chunk.size();
			produced.logical_bytes = chunk.GetDataSize();
			auto handle = quent::chunk_transfer::create(*telemetry->context, std::move(produced));
			handle->published();
			handle->exit();
		}

		unique_ptr<TemporaryIoEvent> StartTempIo(const TemporaryIoInfo &info) {
			lock_guard<mutex> guard(lock);
			if (!query || !plan) {
				return nullptr;
			}

			auto query_id = (*query)->uuid();
			auto task_id = uuid::new_nil();
			auto operator_id = uuid::new_nil();
			auto attribution = CurrentIoAttribution(query_id);
			if (attribution && attribution->plan_id == plan->plan_id) {
				task_id = attribution->task_id;
				operator_id = attribution->operator_id;
			}

			quent::temporary_block_io::IoRequested requested;
			requested.instance_name = "temporary-block-io-" + std::to_string(next_temp_io_index++);
			requested.query_id = query_id;
			requested.plan_id = plan->plan_id;
			requested.task_id = task_id;
			requested.trigger_operator_id = operator_id;
			requested.block_id = info.block_id;
			requested.memory_tag = EnumUtil::ToString(info.tag);
			requested.direction = IoDirectionName(info.direction);
			auto handle = quent::temporary_block_io::create(*telemetry->context, std::move(requested));

			quent::temporary_block_io::IoActive active;
			active.channel_resource_id = telemetry->TemporaryIoChannelId(info.direction);
			active.channel_capacity_operations = 1;
			active.channel_capacity_buffer_bytes = info.buffer_bytes;
			handle->io_active(std::move(active));

			return make_uniq<QuentTemporaryIoEvent>(std::move(handle));
		}

	private:
		static void CompleteInvocation(rust::Box<quent::operator_invocation::OperatorInvocationHandle> handle,
		                               TelemetryTaskOutcome outcome, optional_ptr<const DataChunk> output) {
			quent::operator_invocation::InvocationCompleted completed;
			completed.instance_name = "";
			completed.success = outcome == TelemetryTaskOutcome::SUCCESS;
			completed.output_rows = output ? output->size() : 0;
			completed.output_logical_bytes = output ? output->GetDataSize() : 0;
			handle->invocation_completed(std::move(completed));
			handle->exit();
		}

		void FinishInvocation(const PipelineExecutor &executor, TelemetryTaskOutcome outcome,
		                      optional_ptr<const DataChunk> output) {
			RemoveIoAttribution(executor);
			auto entry = invocations.find(std::cref(executor));
			if (entry == invocations.end()) {
				return;
			}
			auto handle = std::move(entry->second.handle);
			invocations.erase(entry);
			CompleteInvocation(std::move(handle), outcome, output);
		}

		vector<rust::Box<quent::operator_invocation::OperatorInvocationHandle>> TakeInvocations(uuid::UUID task_id) {
			vector<rust::Box<quent::operator_invocation::OperatorInvocationHandle>> handles;
			for (auto entry = invocations.begin(); entry != invocations.end();) {
				if (entry->second.task_id != task_id) {
					entry++;
					continue;
				}
				RemoveIoAttribution(entry->first.get());
				handles.push_back(std::move(entry->second.handle));
				entry = invocations.erase(entry);
			}
			return handles;
		}

		void FinishInvocations() {
			vector<rust::Box<quent::operator_invocation::OperatorInvocationHandle>> handles;
			handles.reserve(invocations.size());
			for (auto &entry : invocations) {
				RemoveIoAttribution(entry.first.get());
				handles.push_back(std::move(entry.second.handle));
			}
			invocations.clear();
			for (auto &handle : handles) {
				try {
					CompleteInvocation(std::move(handle), TelemetryTaskOutcome::FAILURE, nullptr);
				} catch (...) {
				}
			}
		}

		void EraseExecutors(uuid::UUID task_id) {
			for (auto entry = executors.begin(); entry != executors.end();) {
				if (entry->second == task_id) {
					entry = executors.erase(entry);
				} else {
					entry++;
				}
			}
		}

		void FinishTasks() {
			FinishInvocations();
			vector<rust::Box<quent::pipeline_task::PipelineTaskHandle>> handles;
			handles.reserve(tasks.size());
			for (auto &entry : tasks) {
				handles.push_back(std::move(entry.second.handle));
			}
			tasks.clear();
			executors.clear();
			for (auto &handle : handles) {
				try {
					quent::pipeline_task::Finalizing finalizing;
					finalizing.instance_name = "";
					finalizing.success = false;
					handle->finalizing(std::move(finalizing));
					handle->exit();
				} catch (...) {
				}
			}
		}

		void FinishQuery() {
			lock_guard<mutex> guard(lock);
			query_text.reset();
			FinishTasks();
			plan.reset();
			if (!query) {
				return;
			}
			auto handle = std::move(*query);
			query.reset();
			try {
				handle->exit();
			} catch (...) {
			}
		}

	private:
		shared_ptr<Impl> telemetry;
		uuid::UUID query_group_id;
		optional<rust::Box<quent::query::QueryHandle>> query;
		unique_ptr<RuntimePlan> plan;
		reference_map_t<const PipelineTask, TaskTelemetry> tasks;
		reference_map_t<const PipelineExecutor, uuid::UUID> executors;
		reference_map_t<const PipelineExecutor, InvocationTelemetry> invocations;
		optional<string> query_text;
		mutex lock;
		uint64_t next_task_index = 0;
		uint64_t next_transfer_index = 0;
		uint64_t next_invocation_index = 0;
		uint64_t next_temp_io_index = 0;
		bool query_group_declared = false;
	};

	class TempIoProbe final : public TemporaryIoProbe {
	public:
		unique_ptr<TemporaryIoEvent> Start(const TemporaryIoInfo &info) override {
			auto context = info.context.GetClientContext();
			if (!context) {
				return nullptr;
			}

			auto state = context->registered_state->Get<ClientState>(TELEMETRY_STATE_NAME);
			if (!state) {
				return nullptr;
			}
			return state->StartTempIo(info);
		}
	};

	Impl(rust::Box<quent::ExporterOptions> exporter, const string &instance_name)
	    : engine_id(uuid::now_v7()), worker_id(uuid::now_v7()), context(quent::create_context(std::move(exporter))),
	      engine_observer(quent::engine::create_observer(*context)),
	      worker_observer(quent::worker::create_observer(*context)),
	      query_group_observer(quent::query_group::create_observer(*context)) {
		quent::engine::Init engine_init;
		engine_init.implementation.name = "DuckDB";
		engine_init.implementation.version = DuckDB::LibraryVersion();
		engine_init.instance_name = instance_name;
		engine_observer->init(engine_id, std::move(engine_init));

		quent::worker::Init worker_init;
		worker_init.parent_engine_id = engine_id;
		worker_init.instance_name = "local";
		worker_observer->init(worker_id, std::move(worker_init));

		quent::task_queue::Initializing queue_init;
		queue_init.instance_name = "runnable-pipeline-tasks";
		queue_init.parent_group_id = worker_id;
		task_queue.emplace(quent::task_queue::create(*context, std::move(queue_init)));
		quent::task_queue::Operating operating;
		operating.capacity_entries = NumericLimits<uint64_t>::Maximum();
		(*task_queue)->operating(std::move(operating));

		temp_spill_channel.emplace(CreateIoChannel(TEMP_SPILL_NAME));
		temp_reload_channel.emplace(CreateIoChannel(TEMP_RELOAD_NAME));
	}

	~Impl() {
		Exit();
	}

	void Exit() {
		if (exited) {
			return;
		}
		exited = true;
		{
			lock_guard<mutex> guard(resource_lock);
			for (auto &entry : execution_threads) {
				entry.second->finalizing();
				entry.second->exit();
			}
			execution_threads.clear();
			if (task_queue) {
				(*task_queue)->finalizing();
				(*task_queue)->exit();
				task_queue.reset();
			}
			auto exit_channel = [](auto &channel) {
				if (!channel) {
					return;
				}

				(*channel)->finalizing();
				(*channel)->exit();
				channel.reset();
			};
			exit_channel(temp_spill_channel);
			exit_channel(temp_reload_channel);
		}
		worker_observer->exit(worker_id);
		engine_observer->exit(engine_id);
	}

	uuid::UUID TaskQueueId() const {
		D_ASSERT(task_queue);
		return (*task_queue)->uuid();
	}

	uuid::UUID ExecutionThreadId() {
		auto thread_name = ThreadUtil::GetThreadIdString();
		lock_guard<mutex> guard(resource_lock);
		auto entry = execution_threads.find(thread_name);
		if (entry != execution_threads.end()) {
			return entry->second->uuid();
		}

		quent::execution_thread::Initializing initializing;
		initializing.instance_name = "thread-" + thread_name;
		initializing.parent_group_id = worker_id;
		auto handle = quent::execution_thread::create(*context, std::move(initializing));
		handle->operating();
		auto id = handle->uuid();
		execution_threads.emplace(std::move(thread_name), std::move(handle));
		return id;
	}

	uuid::UUID TemporaryIoChannelId(TemporaryIoDirection direction) const {
		switch (direction) {
		case TemporaryIoDirection::SPILL:
			D_ASSERT(temp_spill_channel);
			return (*temp_spill_channel)->uuid();
		case TemporaryIoDirection::RELOAD:
			D_ASSERT(temp_reload_channel);
			return (*temp_reload_channel)->uuid();
		}
		throw InternalException("Unknown temporary I/O direction");
	}

private:
	rust::Box<quent::temporary_io_channel::TemporaryIoChannelHandle> CreateIoChannel(const char *name) {
		quent::temporary_io_channel::Initializing initializing;
		initializing.instance_name = name;
		initializing.parent_group_id = worker_id;
		auto handle = quent::temporary_io_channel::create(*context, std::move(initializing));

		quent::temporary_io_channel::Operating operating;
		operating.capacity_operations = NumericLimits<uint64_t>::Maximum();
		operating.capacity_buffer_bytes = NumericLimits<uint64_t>::Maximum();
		handle->operating(std::move(operating));
		return handle;
	}

private:
	uuid::UUID engine_id;
	uuid::UUID worker_id;
	rust::Box<quent::Context> context;
	rust::Box<quent::engine::EngineObserver> engine_observer;
	rust::Box<quent::worker::WorkerObserver> worker_observer;
	rust::Box<quent::query_group::QueryGroupObserver> query_group_observer;
	optional<rust::Box<quent::task_queue::TaskQueueHandle>> task_queue;
	optional<rust::Box<quent::temporary_io_channel::TemporaryIoChannelHandle>> temp_spill_channel;
	optional<rust::Box<quent::temporary_io_channel::TemporaryIoChannelHandle>> temp_reload_channel;
	unordered_map<string, rust::Box<quent::execution_thread::ExecutionThreadHandle>> execution_threads;
	mutex resource_lock;
	bool exited = false;
};

TelemetryContext::TelemetryContext(DBConfig &config) {
	auto exporter = StringUtil::Lower(FileSystem::GetEnvVariable(EXPORTER_ENV));
	if (exporter.empty() || exporter == "none") {
		return;
	}
	auto instance_name = config.options.database_path.empty() ? string(":memory:") : config.options.database_path;
	impl = make_shared_ptr<Impl>(CreateExporter(exporter), instance_name);
}

TelemetryContext::~TelemetryContext() {
	if (impl) {
		impl->Exit();
	}
}

void TelemetryContext::Initialize(ClientContext &context) {
	if (impl) {
		context.registered_state->Insert(TELEMETRY_STATE_NAME, make_shared_ptr<Impl::ClientState>(impl));
	}
}

shared_ptr<TemporaryIoProbe> TelemetryContext::TempIoProbe() {
	if (!impl) {
		return nullptr;
	}
	return make_shared_ptr<Impl::TempIoProbe>();
}

void TelemetryContext::StartExecution(ClientContext &context, const PhysicalOperator &root) {
	auto state = context.registered_state->Get<Impl::ClientState>(TELEMETRY_STATE_NAME);
	if (state) {
		state->StartExecution(root);
	}
}

void TelemetryContext::PipelineTaskCreated(ClientContext &context, const PipelineTask &task, const Pipeline &pipeline) {
	try {
		auto state = context.registered_state->Get<Impl::ClientState>(TELEMETRY_STATE_NAME);
		if (state) {
			state->TaskCreated(task, pipeline);
		}
	} catch (...) {
	}
}

void TelemetryContext::PipelineTaskExecutor(ClientContext &context, const PipelineTask &task,
                                            const PipelineExecutor &executor) {
	try {
		auto state = context.registered_state->Get<Impl::ClientState>(TELEMETRY_STATE_NAME);
		if (state) {
			state->TaskExecutor(task, executor);
		}
	} catch (...) {
	}
}

void TelemetryContext::PipelineTaskRunning(ClientContext &context, const PipelineTask &task, TaskExecutionMode mode) {
	try {
		auto state = context.registered_state->Get<Impl::ClientState>(TELEMETRY_STATE_NAME);
		if (state) {
			state->TaskRunning(task, mode);
		}
	} catch (...) {
	}
}

void TelemetryContext::PipelineTaskReady(ClientContext &context, const PipelineTask &task) {
	try {
		auto state = context.registered_state->Get<Impl::ClientState>(TELEMETRY_STATE_NAME);
		if (state) {
			state->TaskReady(task);
		}
	} catch (...) {
	}
}

void TelemetryContext::PipelineTaskBlocked(ClientContext &context, const PipelineTask &task) {
	try {
		auto state = context.registered_state->Get<Impl::ClientState>(TELEMETRY_STATE_NAME);
		if (state) {
			state->TaskBlocked(task);
		}
	} catch (...) {
	}
}

void TelemetryContext::PipelineTaskFinished(ClientContext &context, const PipelineTask &task,
                                            TelemetryTaskOutcome outcome) {
	try {
		auto state = context.registered_state->Get<Impl::ClientState>(TELEMETRY_STATE_NAME);
		if (state) {
			state->TaskFinished(task, outcome);
		}
	} catch (...) {
	}
}

void TelemetryContext::EmitChunkTransfer(ClientContext &context, const PipelineExecutor &executor,
                                         const PhysicalOperator &source, const PhysicalOperator &target,
                                         const DataChunk &chunk) {
	try {
		auto state = context.registered_state->Get<Impl::ClientState>(TELEMETRY_STATE_NAME);
		if (state) {
			state->EmitChunkTransfer(executor, source, target, chunk);
		}
	} catch (...) {
	}
}

void TelemetryContext::OperatorInvocationStarted(ClientContext &context, const PipelineExecutor &executor,
                                                 const PhysicalOperator &op, TelemetryOperatorPhase phase,
                                                 optional_ptr<const DataChunk> input) {
	try {
		auto state = context.registered_state->Get<Impl::ClientState>(TELEMETRY_STATE_NAME);
		if (state) {
			state->InvocationStarted(executor, op, phase, input);
		}
	} catch (...) {
	}
}

void TelemetryContext::OperatorInvocationFinished(ClientContext &context, const PipelineExecutor &executor,
                                                  const PhysicalOperator &op, optional_ptr<const DataChunk> output) {
	try {
		auto state = context.registered_state->Get<Impl::ClientState>(TELEMETRY_STATE_NAME);
		if (state) {
			state->InvocationFinished(executor, op, output);
		}
	} catch (...) {
	}
}

} // namespace duckdb

#else

namespace duckdb {

TelemetryContext::TelemetryContext(DBConfig &) {
}

TelemetryContext::~TelemetryContext() {
}

void TelemetryContext::Initialize(ClientContext &) {
}

shared_ptr<TemporaryIoProbe> TelemetryContext::TempIoProbe() {
	return nullptr;
}

void TelemetryContext::StartExecution(ClientContext &, const PhysicalOperator &) {
}

} // namespace duckdb

#endif

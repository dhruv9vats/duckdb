#include "duckdb/main/telemetry_context.hpp"

#ifdef DUCKDB_QUENT_TELEMETRY

#include "duckdb-telemetry-bridge/gen/quent.hpp"

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
#include "duckdb/storage/memory_usage_probe.hpp"
#include "duckdb/storage/temporary_io_probe.hpp"

#include <variant>

namespace duckdb {

static constexpr const char *TELEMETRY_STATE_NAME = "quent_telemetry";
static constexpr const char *EXPORTER_ENV = "QUENT_EXPORTER";
static constexpr const char *OUTPUT_DIR_ENV = "QUENT_OUTPUT_DIR";
static constexpr const char *COLLECTOR_ADDRESS_ENV = "QUENT_COLLECTOR_ADDRESS";
static constexpr const char *DEFAULT_OUTPUT_DIR = "events";
static constexpr const char *DEFAULT_COLLECTOR_ADDRESS = "http://localhost:7836";
static constexpr const char *TEMP_SPILL_NAME = "temporary-spill";
static constexpr const char *TEMP_RELOAD_NAME = "temporary-reload";
static constexpr const char *BUFFER_POOL_MEMORY_NAME = "buffer-pool-memory";
static constexpr const char *TEMP_STORAGE_NAME = "temporary-storage";
static constexpr const char *TEMP_DIRECTORY_STORAGE_NAME = "temporary-directory-storage";
static constexpr const char *TEMP_DIRECTORY_ACCOUNT_NAME = "temporary-directory";
static constexpr const char *TEMP_DIRECTORY_ACCOUNT_TAG = "UNKNOWN";

enum class TelemetryIoOutcome : uint8_t { SUCCESS, FAILURE };
enum class MemoryTelemetryMode : uint8_t { ENABLED, DISABLED };

using QueryExecuting = quent::FsmHandle<quent::Query, quent::query_state::Executing>;
using TaskCreatedHandle = quent::FsmHandle<quent::PipelineTask, quent::pipeline_task_state::Created>;
using TaskRunningHandle = quent::FsmHandle<quent::PipelineTask, quent::pipeline_task_state::Running>;
using TaskReadyHandle = quent::FsmHandle<quent::PipelineTask, quent::pipeline_task_state::Ready>;
using TaskBlockedHandle = quent::FsmHandle<quent::PipelineTask, quent::pipeline_task_state::Blocked>;
using TaskFinalizing = quent::FsmHandle<quent::PipelineTask, quent::pipeline_task_state::Finalizing>;
using TaskState = std::variant<TaskCreatedHandle, TaskRunningHandle, TaskReadyHandle, TaskBlockedHandle>;
using InvocationRunning =
    quent::FsmHandle<quent::OperatorInvocation, quent::operator_invocation_state::InvocationRunning>;
using IoActive = quent::FsmHandle<quent::TemporaryBlockIo, quent::temporary_block_io_state::IoActive>;
using Accounted = quent::FsmHandle<quent::MemoryAccount, quent::memory_account_state::Accounted>;
using ThreadOperating = quent::FsmHandle<quent::ExecutionThread, quent::execution_thread_state::Operating>;
using QueueOperating = quent::FsmHandle<quent::TaskQueue, quent::task_queue_state::Operating>;
using IoChannelOperating = quent::FsmHandle<quent::TemporaryIoChannel, quent::temporary_io_channel_state::Operating>;
using BufferPoolOperating = quent::FsmHandle<quent::BufferPoolMemory, quent::buffer_pool_memory_state::Operating>;
using TempStorageOperating = quent::FsmHandle<quent::TemporaryStorage, quent::temporary_storage_state::Operating>;
using TempDirectoryOperating =
    quent::FsmHandle<quent::TemporaryDirectoryStorage, quent::temporary_directory_storage_state::Operating>;

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
	explicit QuentTemporaryIoEvent(IoActive handle_p) : handle(std::move(handle_p)) {
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
		completed.success = outcome == TelemetryIoOutcome::SUCCESS;
		completed.storage_bytes = storage_bytes;
		auto completed_handle = std::move(handle).io_completed(std::move(completed));
		std::move(completed_handle).exit();
	}

private:
	IoActive handle;
	bool finished = false;
};

struct OperatorIds {
	quent::Uuid operator_id;
	quent::Uuid output_port_id;
	bool output_declared;
};

struct PlanEdgeIds {
	reference<const PhysicalOperator> source;
	reference<const PhysicalOperator> target;
	quent::Uuid source_port_id;
	quent::Uuid target_port_id;
};

struct RuntimePlan {
	quent::Uuid plan_id;
	reference_map_t<const PhysicalOperator, OperatorIds> operators;
	vector<PlanEdgeIds> edges;
	unordered_map<const PhysicalOperator *, unordered_map<const PhysicalOperator *, idx_t>> edge_index;

	bool FindOperator(const PhysicalOperator &op, quent::Uuid &operator_id) const {
		auto entry = operators.find(std::cref(op));
		if (entry == operators.end()) {
			return false;
		}
		operator_id = entry->second.operator_id;
		return true;
	}

	bool FindEdge(const PhysicalOperator &source, const PhysicalOperator &target, quent::Uuid &source_operator_id,
	              quent::Uuid &source_port_id, quent::Uuid &target_operator_id, quent::Uuid &target_port_id) const {
		auto source_edges = edge_index.find(&source);
		if (source_edges == edge_index.end()) {
			return false;
		}
		auto edge_entry = source_edges->second.find(&target);
		if (edge_entry == source_edges->second.end()) {
			return false;
		}

		auto source_entry = operators.find(std::cref(source));
		auto target_entry = operators.find(std::cref(target));
		D_ASSERT(source_entry != operators.end());
		D_ASSERT(target_entry != operators.end());
		auto &edge = edges[edge_entry->second];
		source_operator_id = source_entry->second.operator_id;
		source_port_id = edge.source_port_id;
		target_operator_id = target_entry->second.operator_id;
		target_port_id = edge.target_port_id;
		return true;
	}
};

struct TaskTelemetry {
	explicit TaskTelemetry(TaskCreatedHandle handle_p) : task_id(handle_p.id().raw()), handle(std::move(handle_p)) {
	}

	quent::Uuid task_id;
	TaskState handle;
};

struct InvocationTelemetry {
	explicit InvocationTelemetry(InvocationRunning handle_p, std::optional<quent::Uuid> task_id_p,
	                             uint64_t generation_p)
	    : handle(std::move(handle_p)), task_id(task_id_p), generation(generation_p) {
	}

	InvocationRunning handle;
	std::optional<quent::Uuid> task_id;
	uint64_t generation;
};

struct MemoryAccountTelemetry {
	optional<Accounted> handle;
	uint64_t bytes = 0;
};

struct IoAttribution {
	const void *owner;
	const PipelineExecutor *executor;
	quent::Uuid query_id;
	quent::Uuid plan_id;
	std::optional<quent::Uuid> task_id;
	quent::Uuid operator_id;
	uint64_t generation;
};

static thread_local vector<IoAttribution> active_io_attributions;

static void PushIoAttribution(const void *owner, const PipelineExecutor &executor, quent::Uuid query_id,
                              quent::Uuid plan_id, std::optional<quent::Uuid> task_id, quent::Uuid operator_id,
                              uint64_t generation) {
	active_io_attributions.push_back({owner, &executor, query_id, plan_id, task_id, operator_id, generation});
}

static void RemoveIoAttribution(const void *owner, const PipelineExecutor &executor) {
	for (idx_t index = active_io_attributions.size(); index > 0; index--) {
		auto &attribution = active_io_attributions[index - 1];
		if (attribution.owner != owner || attribution.executor != &executor) {
			continue;
		}

		active_io_attributions.erase(active_io_attributions.begin() + index - 1);
		return;
	}
}

struct ExecutionThreadCache {
	const void *owner;
	quent::Uuid engine_id;
	quent::Uuid thread_id;
};

static thread_local vector<ExecutionThreadCache> execution_thread_cache;

class PlanEmitter {
public:
	PlanEmitter(RuntimePlan &runtime_plan_p, quent::Uuid query_id_p, quent::Uuid worker_id_p,
	            const std::shared_ptr<quent::operator_::OperatorObserver> &operator_observer_p,
	            const std::shared_ptr<quent::port::PortObserver> &port_observer_p,
	            const std::shared_ptr<quent::plan::PlanObserver> &plan_observer_p)
	    : runtime_plan(runtime_plan_p), query_id(query_id_p), worker_id(worker_id_p),
	      operator_observer(operator_observer_p), port_observer(port_observer_p), plan_observer(plan_observer_p) {
		runtime_plan.plan_id = quent::now_v7();
	}

	void Emit(const PhysicalOperator &root) {
		DiscoverOperator(root);
		DeclareOperators();
		EmitEdges(root);

		quent::plan::Declaration declaration {{quent::query::QueryId(query_id), std::nullopt},
		                                      "physical",
		                                      std::move(edges),
		                                      quent::worker::WorkerId(worker_id)};
		plan_observer->handle(quent::plan::PlanId(runtime_plan.plan_id)).declaration(std::move(declaration));
	}

private:
	void DiscoverOperator(const PhysicalOperator &op) {
		auto entry = runtime_plan.operators.find(std::cref(op));
		if (entry != runtime_plan.operators.end()) {
			return;
		}

		OperatorIds ids {quent::now_v7(), quent::now_v7(), false};
		runtime_plan.operators.emplace(std::cref(op), std::move(ids));

		if (op.type == PhysicalOperatorType::MERGE_INTO) {
			auto &merge = op.Cast<PhysicalMergeInto>();
			for (auto &action : merge.actions) {
				if (!action->op) {
					continue;
				}
				DiscoverOperator(*action->op);
			}
		}

		for (auto &child : op.GetChildren()) {
			DiscoverOperator(child.get());
		}
	}

	void DeclareOperators() {
		for (auto &entry : runtime_plan.operators) {
			quent::operator_::Declaration declaration {quent::plan::PlanId(runtime_plan.plan_id),
			                                           {},
			                                           entry.first.get().GetName(),
			                                           PhysicalOperatorToString(entry.first.get().type),
			                                           {}};
			operator_observer->handle(quent::operator_::OperatorId(entry.second.operator_id))
			    .declaration(std::move(declaration));
		}
	}

	void EmitEdges(const PhysicalOperator &op) {
		if (!expanded.insert(&op).second) {
			return;
		}

		if (op.type == PhysicalOperatorType::MERGE_INTO) {
			auto &merge = op.Cast<PhysicalMergeInto>();
			for (idx_t action_index = 0; action_index < merge.actions.size(); action_index++) {
				auto &action = merge.actions[action_index];
				if (!action->op) {
					continue;
				}
				AddEdge(op, *action->op, "action-in-" + std::to_string(action_index));
				EmitEdges(*action->op);
			}
		}

		auto children = op.GetChildren();
		for (idx_t child_index = 0; child_index < children.size(); child_index++) {
			auto &child = children[child_index].get();
			auto input_name = children.size() == 1 ? string("in") : "in-" + std::to_string(child_index);
			AddEdge(child, op, input_name);
			EmitEdges(child);
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

		auto input_port_id = quent::now_v7();
		DeclarePort(input_port_id, target_entry->second.operator_id, target_name);
		quent::records::Edge edge {quent::port::PortId(source_ids.output_port_id), quent::port::PortId(input_port_id)};
		edges.push_back(std::move(edge));
		runtime_plan.edges.push_back({std::cref(source), std::cref(target), source_ids.output_port_id, input_port_id});
		auto edge_index = runtime_plan.edges.size() - 1;
		runtime_plan.edge_index[&source].emplace(&target, edge_index);
	}

	void DeclarePort(quent::Uuid id, quent::Uuid operator_id, const string &name) {
		quent::port::Declaration declaration {quent::operator_::OperatorId(operator_id), name};
		port_observer->handle(quent::port::PortId(id)).declaration(std::move(declaration));
	}

private:
	RuntimePlan &runtime_plan;
	quent::Uuid query_id;
	quent::Uuid worker_id;
	std::shared_ptr<quent::operator_::OperatorObserver> operator_observer;
	std::shared_ptr<quent::port::PortObserver> port_observer;
	std::shared_ptr<quent::plan::PlanObserver> plan_observer;
	std::vector<quent::records::Edge> edges;
	unordered_set<const PhysicalOperator *> expanded;
};

static quent::Context CreateContext(const string &name) {
	if (name == "ndjson") {
		auto output_dir = FileSystem::GetEnvVariable(OUTPUT_DIR_ENV);
		return quent::Context::ndjson(output_dir.empty() ? DEFAULT_OUTPUT_DIR : output_dir);
	}
	if (name == "msgpack" || name == "messagepack") {
		auto output_dir = FileSystem::GetEnvVariable(OUTPUT_DIR_ENV);
		return quent::Context::msgpack(output_dir.empty() ? DEFAULT_OUTPUT_DIR : output_dir);
	}
	if (name == "postcard") {
		auto output_dir = FileSystem::GetEnvVariable(OUTPUT_DIR_ENV);
		return quent::Context::postcard(output_dir.empty() ? DEFAULT_OUTPUT_DIR : output_dir);
	}
	if (name == "collector") {
		auto address = FileSystem::GetEnvVariable(COLLECTOR_ADDRESS_ENV);
		return quent::Context::collector(address.empty() ? DEFAULT_COLLECTOR_ADDRESS : address);
	}
	throw InvalidInputException("Unknown Quent exporter: %s", name);
}

class TelemetryContext::Impl {
	friend class TelemetryContext;

public:
	class ClientState : public ClientContextState {
	public:
		explicit ClientState(shared_ptr<Impl> telemetry_p)
		    : telemetry(std::move(telemetry_p)), query_group_id(quent::now_v7()) {
		}

		~ClientState() override {
			try {
				FinishQuery();
			} catch (...) {
			}
		}

		void QueryBegin(ClientContext &context) override {
			try {
				FinishQuery();
				if (!query_group_declared) {
					string instance_name;
					if (context.GetConnectionId() == DConstants::INVALID_INDEX) {
						instance_name = "internal-connection";
					} else {
						instance_name = "connection-" + std::to_string(context.GetConnectionId());
					}
					quent::query_group::Declaration declaration {std::move(instance_name),
					                                             quent::engine::EngineId(telemetry->engine_id)};
					telemetry->query_group_observer->handle(quent::query_group::QueryGroupId(query_group_id))
					    .declaration(std::move(declaration));
					query_group_declared = true;
				}
				query_text = context.GetCurrentQuery();
			} catch (...) {
				query_text.reset();
			}
		}

		void QueryEnd(ClientContext &, optional_ptr<ErrorData>) override {
			try {
				FinishQuery();
			} catch (...) {
			}
		}

		void StartExecution(const PhysicalOperator &root) {
			if (!query_text) {
				return;
			}
			quent::query::Init init {std::move(*query_text), quent::query_group::QueryGroupId(query_group_id)};
			auto initialized = std::move(telemetry->query_observer->handle()).init(std::move(init));
			query_text.reset();
			auto planning = std::move(initialized).planning();
			auto query_id = planning.id().raw();
			try {
				auto new_plan = make_uniq<RuntimePlan>();
				PlanEmitter(*new_plan, query_id, telemetry->worker_id, telemetry->operator_observer,
				            telemetry->port_observer, telemetry->plan_observer)
				    .Emit(root);
				plan = std::move(new_plan);
			} catch (...) {
				query.emplace(std::move(planning).executing());
				return;
			}
			query.emplace(std::move(planning).executing());
		}

		void TaskCreated(const PipelineTask &task, const Pipeline &pipeline) {
			lock_guard<mutex> guard(lock);
			if (!query || !plan) {
				return;
			}

			vector<quent::operator_::OperatorId> operator_ids;
			for (auto &op : pipeline.GetOperators()) {
				quent::Uuid operator_id;
				if (plan->FindOperator(op.get(), operator_id)) {
					operator_ids.emplace_back(operator_id);
				}
			}
			auto task_index = next_task_index++;
			quent::pipeline_task::Created created {"pipeline-task-" + std::to_string(task_index),
			                                       query->id(),
			                                       quent::plan::PlanId(plan->plan_id),
			                                       quent::worker::WorkerId(telemetry->worker_id),
			                                       std::move(operator_ids),
			                                       task_index,
			                                       {quent::task_queue::TaskQueueId(telemetry->TaskQueueId()), {1}}};
			auto handle = std::move(telemetry->pipeline_task_observer->handle()).created(std::move(created));
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
			auto execution_thread_id = telemetry->ExecutionThreadId();
			auto cpu_id = TaskScheduler::GetEstimatedCPUId();

			lock_guard<mutex> guard(lock);
			auto entry = tasks.find(std::cref(task));
			if (entry == tasks.end()) {
				return;
			}
			quent::pipeline_task::Running running {
			    mode == TaskExecutionMode::PROCESS_PARTIAL ? "partial" : "all",
			    cpu_id,
			    {quent::execution_thread::ExecutionThreadId(execution_thread_id), {}}};
			SetTaskRunning(entry->second.handle, std::move(running));
		}

		void TaskReady(const PipelineTask &task) {
			lock_guard<mutex> guard(lock);
			auto entry = tasks.find(std::cref(task));
			if (entry != tasks.end()) {
				quent::pipeline_task::Ready ready {{quent::task_queue::TaskQueueId(telemetry->TaskQueueId()), {1}}};
				SetTaskReady(entry->second.handle, std::move(ready));
			}
		}

		void TaskBlocked(const PipelineTask &task) {
			lock_guard<mutex> guard(lock);
			auto entry = tasks.find(std::cref(task));
			if (entry != tasks.end()) {
				SetTaskBlocked(entry->second.handle);
			}
		}

		void TaskFinished(const PipelineTask &task, TelemetryTaskOutcome outcome) {
			optional<TaskState> task_handle;
			vector<InvocationRunning> invocation_handles;
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
			CompleteTask(std::move(*task_handle), outcome);
		}

		void InvocationStarted(const PipelineExecutor &executor, const PhysicalOperator &op,
		                       TelemetryOperatorPhase phase, optional_ptr<const DataChunk> input) {
			auto input_rows = input ? input->size() : 0;
			auto input_logical_bytes = input ? input->GetDataSize() : 0;
			optional<InvocationRunning> previous_handle;
			quent::Uuid query_id;
			quent::Uuid plan_id;
			quent::Uuid operator_id;
			std::optional<quent::Uuid> task_id;
			uint64_t invocation_index;
			{
				lock_guard<mutex> guard(lock);
				if (!query || !plan || !plan->FindOperator(op, operator_id)) {
					return;
				}

				query_id = query->id().raw();
				plan_id = plan->plan_id;
				invocation_index = next_invocation_index++;
				auto active = invocations.find(std::cref(executor));
				if (active != invocations.end()) {
					previous_handle.emplace(std::move(active->second.handle));
					invocations.erase(active);
				}

				auto task_entry = executors.find(std::cref(executor));
				if (task_entry != executors.end()) {
					task_id = task_entry->second;
				}
			}
			RemoveIoAttribution(this, executor);
			if (previous_handle) {
				try {
					CompleteInvocation(std::move(*previous_handle), TelemetryTaskOutcome::FAILURE, nullptr);
				} catch (...) {
				}
			}

			std::optional<quent::pipeline_task::PipelineTaskId> task_ref;
			if (task_id) {
				task_ref = quent::pipeline_task::PipelineTaskId(*task_id);
			}
			quent::operator_invocation::InvocationCreated created {"operator-invocation-" +
			                                                           std::to_string(invocation_index),
			                                                       quent::query::QueryId(query_id),
			                                                       quent::plan::PlanId(plan_id),
			                                                       std::move(task_ref),
			                                                       quent::operator_::OperatorId(operator_id),
			                                                       OperatorPhaseName(phase)};
			auto created_handle =
			    std::move(telemetry->operator_invocation_observer->handle()).invocation_created(std::move(created));

			auto execution_thread_id = telemetry->ExecutionThreadId();
			quent::operator_invocation::InvocationRunning running {
			    input_rows, input_logical_bytes, {quent::execution_thread::ExecutionThreadId(execution_thread_id), {}}};
			auto handle = std::move(created_handle).invocation_running(std::move(running));
			{
				lock_guard<mutex> guard(lock);
				bool task_active = !task_id;
				auto task_entry = executors.find(std::cref(executor));
				if (task_id && task_entry != executors.end()) {
					task_active = task_entry->second == *task_id;
				}
				if (query && plan && query->id().raw() == query_id && plan->plan_id == plan_id && task_active &&
				    invocations.find(std::cref(executor)) == invocations.end()) {
					invocations.emplace(std::cref(executor),
					                    InvocationTelemetry(std::move(handle), task_id, invocation_index));
					CurrentIoAttribution(query_id);
					PushIoAttribution(this, executor, query_id, plan_id, task_id, operator_id, invocation_index);
					return;
				}
			}
			CompleteInvocation(std::move(handle), TelemetryTaskOutcome::FAILURE, nullptr);
		}

		void InvocationFinished(const PipelineExecutor &executor, const PhysicalOperator &,
		                        optional_ptr<const DataChunk> output) {
			RemoveIoAttribution(this, executor);
			optional<InvocationRunning> handle;
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
			quent::Uuid query_id;
			std::optional<quent::Uuid> task_id;
			quent::Uuid source_operator_id;
			quent::Uuid source_port_id;
			quent::Uuid target_operator_id;
			quent::Uuid target_port_id;
			uint64_t transfer_index;
			{
				lock_guard<mutex> guard(lock);
				if (!query || !plan || chunk.size() == 0 ||
				    !plan->FindEdge(source, target, source_operator_id, source_port_id, target_operator_id,
				                    target_port_id)) {
					return;
				}
				query_id = query->id().raw();
				auto task_entry = executors.find(std::cref(executor));
				if (task_entry != executors.end()) {
					task_id = task_entry->second;
				}
				transfer_index = next_transfer_index++;
			}

			std::optional<quent::pipeline_task::PipelineTaskId> task_ref;
			if (task_id) {
				task_ref = quent::pipeline_task::PipelineTaskId(*task_id);
			}
			quent::chunk_transfer::Produced produced {"chunk-transfer-" + std::to_string(transfer_index),
			                                          quent::query::QueryId(query_id),
			                                          std::move(task_ref),
			                                          quent::operator_::OperatorId(source_operator_id),
			                                          quent::port::PortId(source_port_id),
			                                          quent::operator_::OperatorId(target_operator_id),
			                                          quent::port::PortId(target_port_id),
			                                          chunk.size(),
			                                          chunk.GetDataSize()};
			auto produced_handle =
			    std::move(telemetry->chunk_transfer_observer->handle()).produced(std::move(produced));
			auto published_handle = std::move(produced_handle).published();
			std::move(published_handle).exit();
		}

		unique_ptr<TemporaryIoEvent> StartTempIo(const TemporaryIoInfo &info) {
			lock_guard<mutex> guard(lock);
			if (!query || !plan) {
				return nullptr;
			}

			auto query_id = query->id().raw();
			std::optional<quent::Uuid> task_id;
			std::optional<quent::Uuid> operator_id;
			auto attribution = CurrentIoAttribution(query_id);
			if (attribution && attribution->plan_id == plan->plan_id) {
				task_id = attribution->task_id;
				operator_id = attribution->operator_id;
			}

			std::optional<quent::pipeline_task::PipelineTaskId> task_ref;
			if (task_id) {
				task_ref = quent::pipeline_task::PipelineTaskId(*task_id);
			}
			std::optional<quent::operator_::OperatorId> operator_ref;
			if (operator_id) {
				operator_ref = quent::operator_::OperatorId(*operator_id);
			}
			quent::temporary_block_io::IoRequested requested {"temporary-block-io-" +
			                                                      std::to_string(next_temp_io_index++),
			                                                  quent::query::QueryId(query_id),
			                                                  quent::plan::PlanId(plan->plan_id),
			                                                  std::move(task_ref),
			                                                  std::move(operator_ref),
			                                                  info.block_id,
			                                                  EnumUtil::ToString(info.tag),
			                                                  IoDirectionName(info.direction)};
			auto requested_handle =
			    std::move(telemetry->temporary_block_io_observer->handle()).io_requested(std::move(requested));

			quent::temporary_block_io::IoActive active {
			    {quent::temporary_io_channel::TemporaryIoChannelId(telemetry->TemporaryIoChannelId(info.direction)),
			     {1, info.buffer_bytes}}};
			auto handle = std::move(requested_handle).io_active(std::move(active));

			return make_uniq<QuentTemporaryIoEvent>(std::move(handle));
		}

	private:
		bool InvocationActive(const PipelineExecutor *executor, uint64_t generation) const {
			for (auto &entry : invocations) {
				if (&entry.first.get() == executor && entry.second.generation == generation) {
					return true;
				}
			}
			return false;
		}

		optional<IoAttribution> CurrentIoAttribution(quent::Uuid query_id) const {
			optional<IoAttribution> current;
			for (idx_t index = active_io_attributions.size(); index > 0;) {
				index--;
				auto attribution = active_io_attributions[index];
				if (attribution.owner != this) {
					continue;
				}
				if (attribution.query_id != query_id ||
				    !InvocationActive(attribution.executor, attribution.generation)) {
					active_io_attributions.erase(active_io_attributions.begin() + index);
					continue;
				}
				if (!current) {
					current = attribution;
				}
			}
			return current;
		}

		static void SetTaskRunning(TaskState &handle, quent::pipeline_task::Running running) {
			if (auto created = std::get_if<TaskCreatedHandle>(&handle)) {
				handle.emplace<TaskRunningHandle>(std::move(*created).running(std::move(running)));
				return;
			}
			if (auto ready = std::get_if<TaskReadyHandle>(&handle)) {
				handle.emplace<TaskRunningHandle>(std::move(*ready).running(std::move(running)));
				return;
			}
			if (auto blocked = std::get_if<TaskBlockedHandle>(&handle)) {
				handle.emplace<TaskRunningHandle>(std::move(*blocked).running(std::move(running)));
			}
		}

		static void SetTaskReady(TaskState &handle, quent::pipeline_task::Ready ready) {
			auto running = std::get_if<TaskRunningHandle>(&handle);
			if (!running) {
				return;
			}
			handle.emplace<TaskReadyHandle>(std::move(*running).ready(std::move(ready)));
		}

		static void SetTaskBlocked(TaskState &handle) {
			auto running = std::get_if<TaskRunningHandle>(&handle);
			if (!running) {
				return;
			}
			handle.emplace<TaskBlockedHandle>(std::move(*running).blocked());
		}

		static void CompleteTask(TaskState handle, TelemetryTaskOutcome outcome) {
			quent::pipeline_task::Finalizing finalizing;
			finalizing.success = outcome == TelemetryTaskOutcome::SUCCESS;
			auto finalizing_handle = std::visit(
			    [&finalizing](auto &current) { return std::move(current).finalizing(std::move(finalizing)); }, handle);
			std::move(finalizing_handle).exit();
		}

		static void CompleteInvocation(InvocationRunning handle, TelemetryTaskOutcome outcome,
		                               optional_ptr<const DataChunk> output) {
			quent::operator_invocation::InvocationCompleted completed;
			completed.success = outcome == TelemetryTaskOutcome::SUCCESS;
			completed.output_rows = output ? output->size() : 0;
			completed.output_logical_bytes = output ? output->GetDataSize() : 0;
			auto completed_handle = std::move(handle).invocation_completed(std::move(completed));
			std::move(completed_handle).exit();
		}

		vector<InvocationRunning> TakeInvocations(quent::Uuid task_id) {
			vector<InvocationRunning> handles;
			for (auto entry = invocations.begin(); entry != invocations.end();) {
				if (!entry->second.task_id || *entry->second.task_id != task_id) {
					entry++;
					continue;
				}
				RemoveIoAttribution(this, entry->first.get());
				handles.push_back(std::move(entry->second.handle));
				entry = invocations.erase(entry);
			}
			return handles;
		}

		void FinishInvocations() {
			vector<InvocationRunning> handles;
			handles.reserve(invocations.size());
			for (auto &entry : invocations) {
				RemoveIoAttribution(this, entry.first.get());
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

		void EraseExecutors(quent::Uuid task_id) {
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
			vector<TaskState> handles;
			handles.reserve(tasks.size());
			for (auto &entry : tasks) {
				handles.push_back(std::move(entry.second.handle));
			}
			tasks.clear();
			executors.clear();
			for (auto &handle : handles) {
				try {
					CompleteTask(std::move(handle), TelemetryTaskOutcome::FAILURE);
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
				std::move(handle).exit();
			} catch (...) {
			}
		}

	private:
		shared_ptr<Impl> telemetry;
		quent::Uuid query_group_id;
		optional<QueryExecuting> query;
		unique_ptr<RuntimePlan> plan;
		reference_map_t<const PipelineTask, TaskTelemetry> tasks;
		reference_map_t<const PipelineExecutor, quent::Uuid> executors;
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
			try {
				auto context = info.context.GetClientContext();
				if (!context) {
					return nullptr;
				}

				auto state = context->registered_state->Get<ClientState>(TELEMETRY_STATE_NAME);
				if (state) {
					return state->StartTempIo(info);
				}
			} catch (...) {
			}
			return nullptr;
		}
	};

	class MemoryProbe final : public MemoryUsageProbe {
	public:
		explicit MemoryProbe(const shared_ptr<Impl> &telemetry_p) : telemetry(telemetry_p) {
		}

		void BufferPoolSnapshot(MemoryTag tag, idx_t bytes) noexcept override {
			try {
				if (auto target = telemetry.lock()) {
					target->SetBufferPoolUsage(tag, bytes);
				}
			} catch (...) {
			}
		}

		void BufferPoolDelta(MemoryTag tag, int64_t bytes) noexcept override {
			try {
				if (auto target = telemetry.lock()) {
					target->ChangeBufferPoolUsage(tag, bytes);
				}
			} catch (...) {
			}
		}

		void BufferPoolLimit(idx_t bytes) noexcept override {
			try {
				if (auto target = telemetry.lock()) {
					target->ResizeBufferPool(bytes);
				}
			} catch (...) {
			}
		}

		void TemporaryStorageDelta(MemoryTag tag, int64_t bytes) noexcept override {
			try {
				if (auto target = telemetry.lock()) {
					target->ChangeTempStorage(tag, bytes);
				}
			} catch (...) {
			}
		}

		void TemporaryStorageLimit(optional_idx bytes) noexcept override {
			try {
				if (auto target = telemetry.lock()) {
					target->ResizeTempStorage(bytes);
				}
			} catch (...) {
			}
		}

		void TemporaryDirectoryDelta(int64_t bytes) noexcept override {
			try {
				if (auto target = telemetry.lock()) {
					target->ChangeTempDirectory(bytes);
				}
			} catch (...) {
			}
		}

	private:
		weak_ptr<Impl> telemetry;
	};

	Impl(quent::Context context_p, const string &instance_name, MemoryTelemetryMode memory_mode)
	    : engine_id(quent::now_v7()), worker_id(quent::now_v7()), context(std::move(context_p)),
	      engine_observer(context.engine_observer()), worker_observer(context.worker_observer()),
	      query_group_observer(context.query_group_observer()), plan_observer(context.plan_observer()),
	      operator_observer(context.operator_observer()), port_observer(context.port_observer()),
	      query_observer(context.query_observer()), pipeline_task_observer(context.pipeline_task_observer()),
	      chunk_transfer_observer(context.chunk_transfer_observer()),
	      operator_invocation_observer(context.operator_invocation_observer()),
	      temporary_block_io_observer(context.temporary_block_io_observer()),
	      memory_account_observer(context.memory_account_observer()),
	      execution_thread_observer(context.execution_thread_observer()),
	      task_queue_observer(context.task_queue_observer()),
	      temporary_io_channel_observer(context.temporary_io_channel_observer()),
	      buffer_pool_memory_observer(context.buffer_pool_memory_observer()),
	      temporary_storage_observer(context.temporary_storage_observer()),
	      temporary_directory_storage_observer(context.temporary_directory_storage_observer()),
	      engine_handle(engine_observer->handle(quent::engine::EngineId(engine_id))),
	      worker_handle(worker_observer->handle(quent::worker::WorkerId(worker_id))) {
		quent::engine::Init engine_init;
		engine_init.implementation.name = "DuckDB";
		engine_init.implementation.version = DuckDB::LibraryVersion();
		engine_init.instance_name = instance_name;
		engine_handle.init(std::move(engine_init));

		quent::worker::Init worker_init {quent::engine::EngineId(engine_id), "local"};
		worker_handle.init(std::move(worker_init));

		quent::task_queue::Initializing queue_init {"runnable-pipeline-tasks", quent::worker::WorkerId(worker_id)};
		auto queue_initializing = std::move(task_queue_observer->handle()).initializing(std::move(queue_init));
		task_queue.emplace(std::move(queue_initializing).operating());

		temp_spill_channel.emplace(CreateIoChannel(TEMP_SPILL_NAME));
		temp_reload_channel.emplace(CreateIoChannel(TEMP_RELOAD_NAME));
		if (memory_mode == MemoryTelemetryMode::ENABLED) {
			InitializeMemoryResources();
		}
	}

	~Impl() {
		Exit();
	}

	void Exit() noexcept {
		{
			lock_guard<mutex> guard(resource_lock);
			if (exited) {
				return;
			}
			exited = true;

			for (auto &entry : execution_threads) {
				try {
					auto finalizing = std::move(entry.second).finalizing();
					std::move(finalizing).exit();
				} catch (...) {
				}
			}
			execution_threads.clear();
			if (task_queue) {
				try {
					auto handle = std::move(*task_queue);
					task_queue.reset();
					auto finalizing = std::move(handle).finalizing();
					std::move(finalizing).exit();
				} catch (...) {
				}
				task_queue.reset();
			}
			auto exit_channel = [](auto &channel) {
				if (!channel) {
					return;
				}

				try {
					auto handle = std::move(*channel);
					channel.reset();
					auto finalizing = std::move(handle).finalizing();
					std::move(finalizing).exit();
				} catch (...) {
					channel.reset();
				}
			};
			exit_channel(temp_spill_channel);
			exit_channel(temp_reload_channel);
			ExitMemoryResources();
		}
		try {
			worker_handle.exit();
		} catch (...) {
		}
		try {
			engine_handle.exit();
		} catch (...) {
		}
	}

	quent::Uuid TaskQueueId() const {
		D_ASSERT(task_queue);
		return task_queue->id().raw();
	}

	quent::Uuid ExecutionThreadId() {
		for (auto &cached : execution_thread_cache) {
			if (cached.owner == this && cached.engine_id == engine_id) {
				return cached.thread_id;
			}
		}

		auto thread_name = ThreadUtil::GetThreadIdString();
		lock_guard<mutex> guard(resource_lock);
		auto entry = execution_threads.find(thread_name);
		if (entry != execution_threads.end()) {
			auto id = entry->second.id().raw();
			execution_thread_cache.push_back({this, engine_id, id});
			return id;
		}

		quent::execution_thread::Initializing initializing {"thread-" + thread_name,
		                                                    quent::worker::WorkerId(worker_id)};
		auto initializing_handle = std::move(execution_thread_observer->handle()).initializing(std::move(initializing));
		auto handle = std::move(initializing_handle).operating();
		auto id = handle.id().raw();
		execution_threads.emplace(std::move(thread_name), std::move(handle));
		execution_thread_cache.push_back({this, engine_id, id});
		return id;
	}

	quent::Uuid TemporaryIoChannelId(TemporaryIoDirection direction) const {
		switch (direction) {
		case TemporaryIoDirection::SPILL:
			D_ASSERT(temp_spill_channel);
			return temp_spill_channel->id().raw();
		case TemporaryIoDirection::RELOAD:
			D_ASSERT(temp_reload_channel);
			return temp_reload_channel->id().raw();
		}
		throw InternalException("Unknown temporary I/O direction");
	}

private:
	bool MemoryEnabled() const {
		return buffer_pool_memory.has_value();
	}

	IoChannelOperating CreateIoChannel(const char *name) {
		quent::temporary_io_channel::Initializing initializing {name, quent::worker::WorkerId(worker_id)};
		auto handle = std::move(temporary_io_channel_observer->handle()).initializing(std::move(initializing));
		return std::move(handle).operating();
	}

	void InitializeMemoryResources();
	void UpdateBufferAccount(MemoryTag tag);
	void UpdateTempAccount(MemoryTag tag);
	void UpdateDirectoryAccount();
	void ExitMemoryResources() noexcept;
	void SetBufferPoolUsage(MemoryTag tag, idx_t bytes);
	void ChangeBufferPoolUsage(MemoryTag tag, int64_t bytes);
	void ResizeBufferPool(idx_t bytes);
	void ChangeTempStorage(MemoryTag tag, int64_t bytes);
	void ResizeTempStorage(optional_idx bytes);
	void ChangeTempDirectory(int64_t bytes);
	static void ApplyDelta(uint64_t &current, int64_t delta) noexcept;

private:
	quent::Uuid engine_id;
	quent::Uuid worker_id;
	quent::Context context;
	std::shared_ptr<quent::engine::EngineObserver> engine_observer;
	std::shared_ptr<quent::worker::WorkerObserver> worker_observer;
	std::shared_ptr<quent::query_group::QueryGroupObserver> query_group_observer;
	std::shared_ptr<quent::plan::PlanObserver> plan_observer;
	std::shared_ptr<quent::operator_::OperatorObserver> operator_observer;
	std::shared_ptr<quent::port::PortObserver> port_observer;
	std::shared_ptr<quent::query::QueryObserver> query_observer;
	std::shared_ptr<quent::pipeline_task::PipelineTaskObserver> pipeline_task_observer;
	std::shared_ptr<quent::chunk_transfer::ChunkTransferObserver> chunk_transfer_observer;
	std::shared_ptr<quent::operator_invocation::OperatorInvocationObserver> operator_invocation_observer;
	std::shared_ptr<quent::temporary_block_io::TemporaryBlockIoObserver> temporary_block_io_observer;
	std::shared_ptr<quent::memory_account::MemoryAccountObserver> memory_account_observer;
	std::shared_ptr<quent::execution_thread::ExecutionThreadObserver> execution_thread_observer;
	std::shared_ptr<quent::task_queue::TaskQueueObserver> task_queue_observer;
	std::shared_ptr<quent::temporary_io_channel::TemporaryIoChannelObserver> temporary_io_channel_observer;
	std::shared_ptr<quent::buffer_pool_memory::BufferPoolMemoryObserver> buffer_pool_memory_observer;
	std::shared_ptr<quent::temporary_storage::TemporaryStorageObserver> temporary_storage_observer;
	std::shared_ptr<quent::temporary_directory_storage::TemporaryDirectoryStorageObserver>
	    temporary_directory_storage_observer;
	quent::Handle<quent::Engine> engine_handle;
	quent::Handle<quent::Worker> worker_handle;
	optional<QueueOperating> task_queue;
	optional<IoChannelOperating> temp_spill_channel;
	optional<IoChannelOperating> temp_reload_channel;
	optional<BufferPoolOperating> buffer_pool_memory;
	optional<TempStorageOperating> temporary_storage;
	optional<TempDirectoryOperating> temporary_directory_storage;
	array<MemoryAccountTelemetry, MEMORY_TAG_COUNT> buffer_pool_accounts;
	array<MemoryAccountTelemetry, MEMORY_TAG_COUNT> temporary_storage_accounts;
	MemoryAccountTelemetry temporary_directory_account;
	unordered_map<string, ThreadOperating> execution_threads;
	mutex resource_lock;
	bool exited = false;
};

void TelemetryContext::Impl::InitializeMemoryResources() {
	quent::buffer_pool_memory::Initializing buffer_pool_initializing {BUFFER_POOL_MEMORY_NAME,
	                                                                  quent::engine::EngineId(engine_id)};
	auto buffer_pool_handle =
	    std::move(buffer_pool_memory_observer->handle()).initializing(std::move(buffer_pool_initializing));
	quent::buffer_pool_memory::Operating buffer_pool_operating;
	buffer_pool_operating.limits.bytes = NumericLimits<uint64_t>::Maximum();
	buffer_pool_memory.emplace(std::move(buffer_pool_handle).operating(std::move(buffer_pool_operating)));

	quent::temporary_storage::Initializing temporary_storage_initializing {TEMP_STORAGE_NAME,
	                                                                       quent::engine::EngineId(engine_id)};
	auto temporary_storage_handle =
	    std::move(temporary_storage_observer->handle()).initializing(std::move(temporary_storage_initializing));
	quent::temporary_storage::Operating temporary_storage_operating;
	temporary_storage_operating.limits.bytes = NumericLimits<uint64_t>::Maximum();
	temporary_storage.emplace(std::move(temporary_storage_handle).operating(std::move(temporary_storage_operating)));

	quent::temporary_directory_storage::Initializing temporary_directory_initializing {
	    TEMP_DIRECTORY_STORAGE_NAME, quent::engine::EngineId(engine_id)};
	auto temporary_directory_handle = std::move(temporary_directory_storage_observer->handle())
	                                      .initializing(std::move(temporary_directory_initializing));
	quent::temporary_directory_storage::Operating temporary_directory_operating;
	temporary_directory_operating.limits.bytes = NumericLimits<uint64_t>::Maximum();
	temporary_directory_storage.emplace(
	    std::move(temporary_directory_handle).operating(std::move(temporary_directory_operating)));

	for (idx_t tag_index = 0; tag_index < MEMORY_TAG_COUNT; tag_index++) {
		auto tag = MemoryTag(tag_index);
		auto tag_name = EnumUtil::ToString(tag);

		quent::memory_account::AccountRegistered buffer_registered {string(BUFFER_POOL_MEMORY_NAME) + "-" + tag_name,
		                                                            quent::engine::EngineId(engine_id), tag_name};
		auto buffer_account =
		    std::move(memory_account_observer->handle()).account_registered(std::move(buffer_registered));
		quent::memory_account::Accounted buffer_accounted;
		buffer_accounted.buffer_pool = quent::refs::BufferPoolMemoryUsageRef {
		    quent::buffer_pool_memory::BufferPoolMemoryId(buffer_pool_memory->id().raw()), {0}};
		buffer_pool_accounts[tag_index].handle.emplace(
		    std::move(buffer_account).accounted(std::move(buffer_accounted)));

		quent::memory_account::AccountRegistered temporary_registered {string(TEMP_STORAGE_NAME) + "-" + tag_name,
		                                                               quent::engine::EngineId(engine_id), tag_name};
		auto temporary_account =
		    std::move(memory_account_observer->handle()).account_registered(std::move(temporary_registered));
		quent::memory_account::Accounted temporary_accounted;
		temporary_accounted.temporary_storage = quent::refs::TemporaryStorageUsageRef {
		    quent::temporary_storage::TemporaryStorageId(temporary_storage->id().raw()), {0}};
		temporary_storage_accounts[tag_index].handle.emplace(
		    std::move(temporary_account).accounted(std::move(temporary_accounted)));
	}

	quent::memory_account::AccountRegistered directory_registered {
	    TEMP_DIRECTORY_ACCOUNT_NAME, quent::engine::EngineId(engine_id), TEMP_DIRECTORY_ACCOUNT_TAG};
	auto directory_account =
	    std::move(memory_account_observer->handle()).account_registered(std::move(directory_registered));
	quent::memory_account::Accounted directory_accounted;
	directory_accounted.temporary_directory = quent::refs::TemporaryDirectoryStorageUsageRef {
	    quent::temporary_directory_storage::TemporaryDirectoryStorageId(temporary_directory_storage->id().raw()), {0}};
	temporary_directory_account.handle.emplace(std::move(directory_account).accounted(std::move(directory_accounted)));
}

void TelemetryContext::Impl::UpdateBufferAccount(MemoryTag tag) {
	auto &account = buffer_pool_accounts[uint8_t(tag)];
	D_ASSERT(account.handle);
	D_ASSERT(buffer_pool_memory);

	quent::memory_account::Accounted accounted;
	accounted.buffer_pool = quent::refs::BufferPoolMemoryUsageRef {
	    quent::buffer_pool_memory::BufferPoolMemoryId(buffer_pool_memory->id().raw()), {account.bytes}};
	auto handle = std::move(*account.handle);
	account.handle.reset();
	account.handle.emplace(std::move(handle).accounted(std::move(accounted)));
}

void TelemetryContext::Impl::UpdateTempAccount(MemoryTag tag) {
	auto &account = temporary_storage_accounts[uint8_t(tag)];
	D_ASSERT(account.handle);
	D_ASSERT(temporary_storage);

	quent::memory_account::Accounted accounted;
	accounted.temporary_storage = quent::refs::TemporaryStorageUsageRef {
	    quent::temporary_storage::TemporaryStorageId(temporary_storage->id().raw()), {account.bytes}};
	auto handle = std::move(*account.handle);
	account.handle.reset();
	account.handle.emplace(std::move(handle).accounted(std::move(accounted)));
}

void TelemetryContext::Impl::UpdateDirectoryAccount() {
	D_ASSERT(temporary_directory_account.handle);
	D_ASSERT(temporary_directory_storage);

	quent::memory_account::Accounted accounted;
	accounted.temporary_directory = quent::refs::TemporaryDirectoryStorageUsageRef {
	    quent::temporary_directory_storage::TemporaryDirectoryStorageId(temporary_directory_storage->id().raw()),
	    {temporary_directory_account.bytes}};
	auto handle = std::move(*temporary_directory_account.handle);
	temporary_directory_account.handle.reset();
	temporary_directory_account.handle.emplace(std::move(handle).accounted(std::move(accounted)));
}

void TelemetryContext::Impl::SetBufferPoolUsage(MemoryTag tag, idx_t bytes) {
	lock_guard<mutex> guard(resource_lock);
	if (exited) {
		return;
	}

	buffer_pool_accounts[uint8_t(tag)].bytes = bytes;
	UpdateBufferAccount(tag);
}

void TelemetryContext::Impl::ChangeBufferPoolUsage(MemoryTag tag, int64_t bytes) {
	lock_guard<mutex> guard(resource_lock);
	if (exited) {
		return;
	}

	auto &account = buffer_pool_accounts[uint8_t(tag)];
	ApplyDelta(account.bytes, bytes);
	UpdateBufferAccount(tag);
}

void TelemetryContext::Impl::ResizeBufferPool(idx_t bytes) {
	lock_guard<mutex> guard(resource_lock);
	if (exited || !buffer_pool_memory) {
		return;
	}

	auto handle = std::move(*buffer_pool_memory);
	buffer_pool_memory.reset();
	auto resizing = std::move(handle).resizing();
	quent::buffer_pool_memory::Operating operating;
	operating.limits.bytes = bytes;
	buffer_pool_memory.emplace(std::move(resizing).operating(std::move(operating)));
}

void TelemetryContext::Impl::ChangeTempStorage(MemoryTag tag, int64_t bytes) {
	lock_guard<mutex> guard(resource_lock);
	if (exited) {
		return;
	}

	auto &account = temporary_storage_accounts[uint8_t(tag)];
	ApplyDelta(account.bytes, bytes);
	UpdateTempAccount(tag);
}

void TelemetryContext::Impl::ResizeTempStorage(optional_idx bytes) {
	lock_guard<mutex> guard(resource_lock);
	if (exited || !temporary_storage || !temporary_directory_storage) {
		return;
	}

	auto capacity = bytes.IsValid() ? bytes.GetIndex() : NumericLimits<uint64_t>::Maximum();
	auto temporary_handle = std::move(*temporary_storage);
	temporary_storage.reset();
	auto temporary_resizing = std::move(temporary_handle).resizing();
	quent::temporary_storage::Operating temporary_operating;
	temporary_operating.limits.bytes = capacity;
	temporary_storage.emplace(std::move(temporary_resizing).operating(std::move(temporary_operating)));

	auto directory_handle = std::move(*temporary_directory_storage);
	temporary_directory_storage.reset();
	auto directory_resizing = std::move(directory_handle).resizing();
	quent::temporary_directory_storage::Operating directory_operating;
	directory_operating.limits.bytes = capacity;
	temporary_directory_storage.emplace(std::move(directory_resizing).operating(std::move(directory_operating)));
}

void TelemetryContext::Impl::ChangeTempDirectory(int64_t bytes) {
	lock_guard<mutex> guard(resource_lock);
	if (exited) {
		return;
	}

	ApplyDelta(temporary_directory_account.bytes, bytes);
	UpdateDirectoryAccount();
}

void TelemetryContext::Impl::ApplyDelta(uint64_t &current, int64_t delta) noexcept {
	if (delta >= 0) {
		current += static_cast<uint64_t>(delta);
		return;
	}

	auto decrease = static_cast<uint64_t>(-(delta + 1)) + 1;
	D_ASSERT(current >= decrease);
	current = current >= decrease ? current - decrease : 0;
}

void TelemetryContext::Impl::ExitMemoryResources() noexcept {
	auto exit_account = [](auto &account) {
		if (!account.handle) {
			return;
		}

		try {
			auto handle = std::move(*account.handle);
			account.handle.reset();
			std::move(handle).exit();
		} catch (...) {
			account.handle.reset();
		}
	};
	for (auto &account : buffer_pool_accounts) {
		exit_account(account);
	}
	for (auto &account : temporary_storage_accounts) {
		exit_account(account);
	}
	exit_account(temporary_directory_account);

	auto exit_resource = [](auto &resource) {
		if (!resource) {
			return;
		}

		try {
			auto handle = std::move(*resource);
			resource.reset();
			auto finalizing = std::move(handle).finalizing();
			std::move(finalizing).exit();
		} catch (...) {
			resource.reset();
		}
	};
	exit_resource(buffer_pool_memory);
	exit_resource(temporary_storage);
	exit_resource(temporary_directory_storage);
}

TelemetryContext::TelemetryContext(DBConfig &config) {
	auto exporter = StringUtil::Lower(FileSystem::GetEnvVariable(EXPORTER_ENV));
	if (exporter.empty() || exporter == "none") {
		return;
	}
	auto instance_name = config.options.database_path.empty() ? string(":memory:") : config.options.database_path;
	auto memory_mode = config.buffer_manager ? MemoryTelemetryMode::DISABLED : MemoryTelemetryMode::ENABLED;
	try {
		impl = make_shared_ptr<Impl>(CreateContext(exporter), instance_name, memory_mode);
	} catch (...) {
	}
}

TelemetryContext::~TelemetryContext() {
	if (impl) {
		impl->Exit();
	}
}

void TelemetryContext::Initialize(ClientContext &context) {
	try {
		if (impl) {
			context.registered_state->Insert(TELEMETRY_STATE_NAME, make_shared_ptr<Impl::ClientState>(impl));
		}
	} catch (...) {
	}
}

shared_ptr<TemporaryIoProbe> TelemetryContext::TempIoProbe() {
	if (!impl) {
		return nullptr;
	}
	return make_shared_ptr<Impl::TempIoProbe>();
}

shared_ptr<duckdb::MemoryUsageProbe> TelemetryContext::MemoryUsageProbe() {
	if (!impl || !impl->MemoryEnabled()) {
		return nullptr;
	}
	return make_shared_ptr<Impl::MemoryProbe>(impl);
}

void TelemetryContext::StartExecution(ClientContext &context, const PhysicalOperator &root) {
	try {
		auto state = context.registered_state->Get<Impl::ClientState>(TELEMETRY_STATE_NAME);
		if (state) {
			state->StartExecution(root);
		}
	} catch (...) {
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

shared_ptr<duckdb::MemoryUsageProbe> TelemetryContext::MemoryUsageProbe() {
	return nullptr;
}

void TelemetryContext::StartExecution(ClientContext &, const PhysicalOperator &) {
}

} // namespace duckdb

#endif

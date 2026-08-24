#include "duckdb/main/telemetry_context.hpp"

#ifdef DUCKDB_QUENT_TELEMETRY

#include "duckdb-telemetry-bridge/gen/context.rs.h"
#include "duckdb-telemetry-bridge/gen/engine.rs.h"
#include "duckdb-telemetry-bridge/gen/operator.rs.h"
#include "duckdb-telemetry-bridge/gen/plan.rs.h"
#include "duckdb-telemetry-bridge/gen/port.rs.h"
#include "duckdb-telemetry-bridge/gen/query.rs.h"
#include "duckdb-telemetry-bridge/gen/query_group.rs.h"
#include "duckdb-telemetry-bridge/gen/uuid.rs.h"
#include "duckdb-telemetry-bridge/gen/worker.rs.h"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

static constexpr const char *TELEMETRY_STATE_NAME = "quent_telemetry";
static constexpr const char *EXPORTER_ENV = "QUENT_EXPORTER";
static constexpr const char *OUTPUT_DIR_ENV = "QUENT_OUTPUT_DIR";
static constexpr const char *COLLECTOR_ADDRESS_ENV = "QUENT_COLLECTOR_ADDRESS";
static constexpr const char *DEFAULT_OUTPUT_DIR = "events";
static constexpr const char *DEFAULT_COLLECTOR_ADDRESS = "http://localhost:7836";

struct OperatorIds {
	uuid::UUID operator_id;
	uuid::UUID output_port_id;
	bool output_declared;
};

class PlanEmitter {
public:
	PlanEmitter(const quent::Context &context, uuid::UUID query_id_p, uuid::UUID worker_id_p)
	    : plan_id(uuid::now_v7()), query_id(query_id_p), worker_id(worker_id_p),
	      operator_observer(quent::operator_::create_observer(context)),
	      port_observer(quent::port::create_observer(context)), plan_observer(quent::plan::create_observer(context)) {
	}

	void Emit(const PhysicalOperator &root) {
		EmitOperator(root);

		quent::plan::Declaration declaration;
		declaration.parent.query_id = query_id;
		declaration.parent.plan_id = uuid::new_nil();
		declaration.instance_name = "physical";
		declaration.edges = std::move(edges);
		declaration.worker_id = worker_id;
		plan_observer->declaration(plan_id, std::move(declaration));
	}

private:
	void EmitOperator(const PhysicalOperator &op) {
		auto entry = operators.find(std::cref(op));
		if (entry != operators.end()) {
			return;
		}

		OperatorIds ids {uuid::now_v7(), uuid::now_v7(), false};
		operators.emplace(std::cref(op), ids);

		quent::operator_::Declaration declaration;
		declaration.plan_id = plan_id;
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
				auto action_entry = operators.find(std::cref(*action->op));
				D_ASSERT(action_entry != operators.end());
				AddEdge(op, action_entry->second.operator_id, "action-in-" + std::to_string(action_index));
			}
		}

		auto children = op.GetChildren();
		for (idx_t child_index = 0; child_index < children.size(); child_index++) {
			auto &child = children[child_index].get();
			EmitOperator(child);
			auto input_name = children.size() == 1 ? string("in") : "in-" + std::to_string(child_index);
			AddEdge(child, ids.operator_id, input_name);
		}
	}

	void AddEdge(const PhysicalOperator &source, uuid::UUID target_operator_id, const string &target_name) {
		auto source_entry = operators.find(std::cref(source));
		D_ASSERT(source_entry != operators.end());
		auto &source_ids = source_entry->second;
		if (!source_ids.output_declared) {
			DeclarePort(source_ids.output_port_id, source_ids.operator_id, "out");
			source_ids.output_declared = true;
		}

		auto input_port_id = uuid::now_v7();
		DeclarePort(input_port_id, target_operator_id, target_name);
		quent::plan::Edges edge;
		edge.source = source_ids.output_port_id;
		edge.target = input_port_id;
		edges.push_back(std::move(edge));
	}

	void DeclarePort(uuid::UUID id, uuid::UUID operator_id, const string &name) {
		quent::port::Declaration declaration;
		declaration.operator_id = operator_id;
		declaration.instance_name = name;
		port_observer->declaration(id, std::move(declaration));
	}

private:
	uuid::UUID plan_id;
	uuid::UUID query_id;
	uuid::UUID worker_id;
	rust::Box<quent::operator_::OperatorObserver> operator_observer;
	rust::Box<quent::port::PortObserver> port_observer;
	rust::Box<quent::plan::PlanObserver> plan_observer;
	reference_map_t<const PhysicalOperator, OperatorIds> operators;
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
				PlanEmitter(*telemetry->context, (*query)->uuid(), telemetry->worker_id).Emit(root);
			} catch (...) {
				(*query)->executing();
				return;
			}
			(*query)->executing();
		}

	private:
		void FinishQuery() {
			query_text.reset();
			if (!query) {
				return;
			}
			(*query)->exit();
			query.reset();
		}

	private:
		shared_ptr<Impl> telemetry;
		uuid::UUID query_group_id;
		optional<rust::Box<quent::query::QueryHandle>> query;
		optional<string> query_text;
		bool query_group_declared = false;
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
	}

	~Impl() {
		Exit();
	}

	void Exit() {
		if (exited) {
			return;
		}
		exited = true;
		worker_observer->exit(worker_id);
		engine_observer->exit(engine_id);
	}

private:
	uuid::UUID engine_id;
	uuid::UUID worker_id;
	rust::Box<quent::Context> context;
	rust::Box<quent::engine::EngineObserver> engine_observer;
	rust::Box<quent::worker::WorkerObserver> worker_observer;
	rust::Box<quent::query_group::QueryGroupObserver> query_group_observer;
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

void TelemetryContext::StartExecution(ClientContext &context, const PhysicalOperator &root) {
	auto state = context.registered_state->Get<Impl::ClientState>(TELEMETRY_STATE_NAME);
	if (state) {
		state->StartExecution(root);
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

void TelemetryContext::StartExecution(ClientContext &, const PhysicalOperator &) {
}

} // namespace duckdb

#endif

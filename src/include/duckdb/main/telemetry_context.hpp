//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/telemetry_context.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

class ClientContext;
class PhysicalOperator;
struct DBConfig;

class TelemetryContext {
public:
	explicit TelemetryContext(DBConfig &config);
	~TelemetryContext();

	void Initialize(ClientContext &context);
	static void StartExecution(ClientContext &context, const PhysicalOperator &root);

private:
	class Impl;
	shared_ptr<Impl> impl;
};

} // namespace duckdb

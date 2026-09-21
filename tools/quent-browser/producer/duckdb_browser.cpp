#include "duckdb.hpp"
#include "duckdb/main/telemetry_context.hpp"

#include <emscripten/emscripten.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

namespace {

constexpr int32_t STATUS_OK = 0;
constexpr int32_t STATUS_ERROR = 1;
constexpr int32_t DRAIN_EMPTY = 0;
constexpr int32_t DRAIN_READY = 1;
constexpr int32_t DRAIN_FAILED = 2;
constexpr uint32_t PROTOCOL_VERSION = 1;
constexpr const char *EXPORTER_ENV = "QUENT_EXPORTER";
constexpr const char *BROWSER_EXPORTER = "browser";
constexpr char HEX_DIGITS[] = "0123456789abcdef";

std::unique_ptr<duckdb::DuckDB> database;
std::unique_ptr<duckdb::Connection> connection;
duckdb::BrowserTelemetryBatch batch;
std::string result_json;
std::string query_ids_json;
std::string context_id_json;
std::string error_json;
const std::string manifest_json = "{\"protocol_version\":" + std::to_string(PROTOCOL_VERSION) +
                                  ",\"schema_hash\":\"" DUCKDB_BROWSER_SCHEMA_HASH
                                  "\",\"build_id\":\"" DUCKDB_BROWSER_BUILD_ID "\"}";
uint64_t previous_dropped = 0;

void AppendJsonString(std::string &output, const std::string &value) {
	output.push_back('"');
	for (const auto character : value) {
		switch (character) {
		case '"':
			output += "\\\"";
			break;
		case '\\':
			output += "\\\\";
			break;
		case '\b':
			output += "\\b";
			break;
		case '\f':
			output += "\\f";
			break;
		case '\n':
			output += "\\n";
			break;
		case '\r':
			output += "\\r";
			break;
		case '\t':
			output += "\\t";
			break;
		default:
			if (static_cast<unsigned char>(character) < 0x20) {
				const auto value = static_cast<unsigned char>(character);
				output += "\\u00";
				output.push_back(HEX_DIGITS[value >> 4]);
				output.push_back(HEX_DIGITS[value & 0x0f]);
			} else {
				output.push_back(character);
			}
		}
	}
	output.push_back('"');
}

void SetError(const std::string &message) {
	error_json = "{\"message\":";
	AppendJsonString(error_json, message);
	error_json.push_back('}');
}

int32_t Open() {
	if (setenv(EXPORTER_ENV, BROWSER_EXPORTER, 1) != 0) {
		SetError("Unable to configure browser telemetry");
		return STATUS_ERROR;
	}

	try {
		database = std::make_unique<duckdb::DuckDB>(nullptr);
		connection = std::make_unique<duckdb::Connection>(*database);
		batch = {};
		previous_dropped = 0;
		return STATUS_OK;
	} catch (const std::exception &error) {
		connection.reset();
		database.reset();
		SetError(error.what());
		return STATUS_ERROR;
	}
}

void SerializeResult(duckdb::MaterializedQueryResult &result, uint64_t row_limit) {
	const auto row_count = result.RowCount();
	const auto preview_count = std::min<duckdb::idx_t>(row_count, row_limit);
	result_json = "{\"columns\":[";
	for (duckdb::idx_t column = 0; column < result.ColumnCount(); column++) {
		if (column != 0) {
			result_json.push_back(',');
		}
		AppendJsonString(result_json, result.ColumnName(column).GetIdentifierName());
	}
	result_json += "],\"rows\":[";
	for (duckdb::idx_t row = 0; row < preview_count; row++) {
		if (row != 0) {
			result_json.push_back(',');
		}
		result_json.push_back('[');
		for (duckdb::idx_t column = 0; column < result.ColumnCount(); column++) {
			if (column != 0) {
				result_json.push_back(',');
			}
			auto value = result.GetValue(column, row);
			if (value.IsNull()) {
				result_json += "null";
			} else {
				AppendJsonString(result_json, value.ToString());
			}
		}
		result_json.push_back(']');
	}
	result_json += "],\"row_count\":" + std::to_string(row_count);
	result_json += ",\"truncated\":";
	result_json += preview_count < row_count ? "true}" : "false}";
}

duckdb::optional_ptr<duckdb::DatabaseInstance> Instance() {
	if (!database) {
		return nullptr;
	}
	return database->instance;
}

} // namespace

extern "C" {

EMSCRIPTEN_KEEPALIVE const char *quent_browser_manifest_json() {
	return manifest_json.c_str();
}

EMSCRIPTEN_KEEPALIVE int32_t quent_browser_open() {
	if (database) {
		SetError("DuckDB is already open");
		return STATUS_ERROR;
	}
	return Open();
}

EMSCRIPTEN_KEEPALIVE int32_t quent_browser_query(const char *sql, uint32_t row_limit) {
	if (!connection || !sql) {
		SetError("DuckDB is not open");
		return STATUS_ERROR;
	}

	try {
		if (!database->instance->BeginBrowserTelemetryRun()) {
			SetError("Previous telemetry capture was not drained");
			return STATUS_ERROR;
		}
		previous_dropped = 0;
		batch = {};
		auto result = connection->Query(sql);
		duckdb::optional_ptr<duckdb::QueryResult> selected;
		for (auto current = duckdb::optional_ptr<duckdb::QueryResult>(result.get()); current;
		     current = current->next.get()) {
			if (current->HasError()) {
				SetError(current->GetError());
				return STATUS_ERROR;
			}
			selected = current;
		}
		if (!selected || selected->GetResultType() != duckdb::QueryResultType::MATERIALIZED_RESULT) {
			SetError("DuckDB returned no materialized result");
			return STATUS_ERROR;
		}
		SerializeResult(selected->Cast<duckdb::MaterializedQueryResult>(), row_limit);
		return STATUS_OK;
	} catch (const std::exception &error) {
		SetError(error.what());
		return STATUS_ERROR;
	}
}

EMSCRIPTEN_KEEPALIVE const char *quent_browser_result_json() {
	return result_json.c_str();
}

EMSCRIPTEN_KEEPALIVE uint32_t quent_browser_drain(uint64_t max_bytes) {
	auto instance = Instance();
	if (!instance) {
		batch = {};
		return DRAIN_FAILED;
	}
	batch = instance->DrainBrowserEvents(max_bytes);
	if (batch.status == duckdb::BrowserTelemetryStatus::FAILED ||
	    batch.status == duckdb::BrowserTelemetryStatus::UNAVAILABLE) {
		return DRAIN_FAILED;
	}
	return batch.payload.empty() ? DRAIN_EMPTY : DRAIN_READY;
}

EMSCRIPTEN_KEEPALIVE uint32_t quent_browser_drain_status() {
	if (batch.status == duckdb::BrowserTelemetryStatus::FAILED ||
	    batch.status == duckdb::BrowserTelemetryStatus::UNAVAILABLE) {
		return DRAIN_FAILED;
	}
	return batch.payload.empty() ? DRAIN_EMPTY : DRAIN_READY;
}

EMSCRIPTEN_KEEPALIVE const uint8_t *quent_browser_drain_ptr() {
	return batch.payload.data();
}

EMSCRIPTEN_KEEPALIVE uint32_t quent_browser_drain_len() {
	return static_cast<uint32_t>(batch.payload.size());
}

EMSCRIPTEN_KEEPALIVE uint32_t quent_browser_drain_count() {
	return batch.event_count;
}

EMSCRIPTEN_KEEPALIVE uint64_t quent_browser_drain_min_ts() {
	return batch.min_timestamp;
}

EMSCRIPTEN_KEEPALIVE uint64_t quent_browser_drain_max_ts() {
	return batch.max_timestamp;
}

EMSCRIPTEN_KEEPALIVE uint64_t quent_browser_drain_dropped() {
	const auto delta = batch.dropped_events - std::min(batch.dropped_events, previous_dropped);
	previous_dropped = batch.dropped_events;
	return delta;
}

EMSCRIPTEN_KEEPALIVE uint64_t quent_browser_watermark() {
	auto instance = Instance();
	return instance ? instance->BrowserTelemetryWatermark() : 0;
}

EMSCRIPTEN_KEEPALIVE const char *quent_browser_query_ids_json() {
	auto instance = Instance();
	query_ids_json = "[";
	if (instance) {
		const auto ids = instance->BrowserTelemetryQueryIds();
		for (duckdb::idx_t index = 0; index < ids.size(); index++) {
			if (index != 0) {
				query_ids_json.push_back(',');
			}
			AppendJsonString(query_ids_json, ids[index]);
		}
	}
	query_ids_json.push_back(']');
	return query_ids_json.c_str();
}

EMSCRIPTEN_KEEPALIVE const char *quent_browser_context_id_json() {
	auto instance = Instance();
	context_id_json = "null";
	if (instance) {
		context_id_json.clear();
		AppendJsonString(context_id_json, instance->BrowserTelemetryContextId());
	}
	return context_id_json.c_str();
}

EMSCRIPTEN_KEEPALIVE const char *quent_browser_error_json() {
	return error_json.c_str();
}

EMSCRIPTEN_KEEPALIVE int32_t quent_browser_reset() {
	connection.reset();
	database.reset();
	result_json.clear();
	query_ids_json.clear();
	context_id_json.clear();
	error_json.clear();
	return Open();
}

} // extern "C"

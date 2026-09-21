export const EMPTY_REVISION = '0';

export type QuentApiMethod =
	| 'fetchQueryBundle'
	| 'fetchListEngines'
	| 'fetchEngineContexts'
	| 'fetchNvtxCatalog'
	| 'fetchNvtxViewport'
	| 'fetchListCoordinators'
	| 'fetchListQueries'
	| 'fetchSingleTimeline'
	| 'fetchBulkTimelines'
	| 'fetchEntityList'
	| 'fetchDataFlow';

export interface CaptureSnapshot {
	type: 'snapshot';
	revision: string;
	captureId?: string;
	engineId?: string;
	lastQueryId?: string;
}

export interface QuentConnectMessage {
	type: 'quent-connect';
}

export interface QuentReadyMessage {
	type: 'ready';
}

export interface QuentRpcRequest {
	type: 'rpc';
	id: string;
	revision: string;
	method: QuentApiMethod;
	args: unknown[];
}

export interface QuentRpcResult {
	type: 'rpc-result';
	id: string;
	revision: string;
	result: unknown;
}

export interface QuentRpcError {
	type: 'rpc-error';
	id: string;
	revision: string;
	error: string;
}

export type QuentPortMessage =
	| CaptureSnapshot
	| QuentReadyMessage
	| QuentRpcRequest
	| QuentRpcResult
	| QuentRpcError;

export function isSnapshotMessage(value: unknown): value is CaptureSnapshot {
	if (!value || typeof value !== 'object') {
		return false;
	}

	const message = value as Partial<CaptureSnapshot>;
	return message.type === 'snapshot' && typeof message.revision === 'string';
}

export function isRpcReply(value: unknown): value is QuentRpcResult | QuentRpcError {
	if (!value || typeof value !== 'object') {
		return false;
	}

	const message = value as Partial<QuentRpcResult | QuentRpcError>;
	return (
		(message.type === 'rpc-result' || message.type === 'rpc-error') &&
		typeof message.id === 'string' &&
		typeof message.revision === 'string'
	);
}

import { describe, expect, it } from 'vitest';
import { reloadRoute } from './reload-route';

const snapshot = {
	type: 'snapshot' as const,
	revision: 'r1',
	captureId: 'c1',
	engineId: 'engine 1',
	lastQueryId: 'query/1',
};

describe('reloadRoute', () => {
	it('preserves a matching full-UI tab and deep-link state', () => {
		expect(
			reloadRoute('#/profile/engine/engine%201/query/query%2F1/entities?s=view-state', snapshot),
		).toEqual({ tab: 'entities', search: { s: 'view-state' } });
	});

	it('rejects a route from another capture query', () => {
		expect(reloadRoute('#/profile/engine/engine%201/query/other/operators', snapshot)).toBeNull();
	});

	it('rejects malformed encodings', () => {
		expect(reloadRoute('#/profile/engine/%ZZ/query/query%2F1/timeline', snapshot)).toBeNull();
	});
});

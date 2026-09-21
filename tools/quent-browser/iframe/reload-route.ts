import type { CaptureSnapshot } from './protocol';

export type ProfileTab = 'timeline' | 'operators' | 'entities';

export interface ReloadRoute {
	tab: ProfileTab;
	search: { s?: string };
}

const PROFILE_ROUTE = /^#?\/profile\/engine\/([^/]+)\/query\/([^/]+)\/(timeline|operators|entities)(?:\?([^#]*))?$/;

export function reloadRoute(hash: string, snapshot: CaptureSnapshot): ReloadRoute | null {
	if (!snapshot.engineId || !snapshot.lastQueryId || !snapshot.captureId) {
		return null;
	}

	const match = PROFILE_ROUTE.exec(hash);
	if (!match) {
		return null;
	}

	try {
		if (decodeURIComponent(match[1]) !== snapshot.engineId) {
			return null;
		}
		if (decodeURIComponent(match[2]) !== snapshot.lastQueryId) {
			return null;
		}
	} catch {
		return null;
	}

	const state = new URLSearchParams(match[4]).get('s');
	return {
		tab: match[3] as ProfileTab,
		search: state === null ? {} : { s: state },
	};
}

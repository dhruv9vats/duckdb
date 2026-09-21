import { setApiClient } from '@quent/client';
import { QueryClientProvider } from '@tanstack/react-query';
import { createHashHistory, createRouter, RouterProvider } from '@tanstack/react-router';
import React from 'react';
import ReactDOM from 'react-dom/client';
import { queryClient } from '@/lib/queryClient';
import { routeTree } from '@/routeTree.gen';
import { EMPTY_REVISION, isSnapshotMessage, type QuentConnectMessage } from './protocol';
import { reloadRoute } from './reload-route';
import { IframeApiClient } from './rpc-client';
import './index.css';

const history = createHashHistory();
const router = createRouter({ routeTree, history });

declare module '@tanstack/react-router' {
	interface Register {
		router: typeof router;
	}
}

const rootElement = document.getElementById('root');
if (!rootElement) {
	throw new Error('Missing iframe root');
}

const root = ReactDOM.createRoot(rootElement);
let activeRevision = EMPTY_REVISION;
let snapshotChain = Promise.resolve();
let snapshotGeneration = 0;
let reloadHash = window.location.hash;
let receivedSnapshot = false;

function render(): void {
	root.render(
		<React.StrictMode>
			<QueryClientProvider key={activeRevision} client={queryClient}>
				<RouterProvider router={router} />
			</QueryClientProvider>
		</React.StrictMode>,
	);
}

function showError(error: unknown): void {
	const message = error instanceof Error ? error.message : String(error);
	root.render(<div role="alert">Could not switch query profile: {message}</div>);
}

async function applySnapshot(
	message: Parameters<IframeApiClient['setSnapshot']>[0],
	generation: number,
): Promise<void> {
	if (generation !== snapshotGeneration) {
		return;
	}

	await queryClient.cancelQueries();
	if (generation !== snapshotGeneration) {
		return;
	}
	queryClient.clear();

	const reload = reloadRoute(reloadHash, message);
	reloadHash = '';
	if (message.engineId && message.lastQueryId && message.captureId) {
		await router.navigate({
			to: `/profile/engine/$engineId/query/$queryId/${reload?.tab ?? 'timeline'}`,
			params: { engineId: message.engineId, queryId: message.lastQueryId },
			search: reload?.search ?? {},
			replace: true,
		});
	} else {
		await router.navigate({ to: '/profile', replace: true });
	}

	if (generation !== snapshotGeneration) {
		return;
	}
	await router.invalidate();
	if (generation !== snapshotGeneration) {
		return;
	}
	render();
}

function connect(port: MessagePort): void {
	const client = new IframeApiClient(port);
	setApiClient(client);
	window.addEventListener('pagehide', () => client.close(), { once: true });
	port.addEventListener('message', event => {
		if (!isSnapshotMessage(event.data)) {
			return;
		}

		const message = event.data;
		if (receivedSnapshot && message.revision === activeRevision) {
			return;
		}

		receivedSnapshot = true;
		client.setSnapshot(message);
		activeRevision = message.revision;
		const generation = ++snapshotGeneration;
		snapshotChain = snapshotChain
			.catch(() => undefined)
			.then(() => applySnapshot(message, generation))
			.catch(error => {
				if (generation === snapshotGeneration) {
					showError(error);
				}
			});
	});
	port.start();
	port.postMessage({ type: 'ready' });

	snapshotChain = router
		.navigate({ to: '/profile', replace: true })
		.then(render)
		.catch(showError);
}

function receivePort(event: MessageEvent): void {
	if (event.source !== window.parent || event.origin !== window.location.origin) {
		return;
	}
	if ((event.data as { type?: unknown } | null)?.type !== 'quent-port') {
		return;
	}
	const port = event.ports[0];
	if (!port) {
		return;
	}

	window.removeEventListener('message', receivePort);
	connect(port);
}

window.addEventListener('message', receivePort);
window.parent.postMessage({ type: 'quent-connect' } satisfies QuentConnectMessage, window.location.origin);

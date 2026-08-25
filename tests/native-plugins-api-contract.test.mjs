import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

async function installHarness() {
    const calls = [];
    globalThis.window = { __TAURITAVERN__: { api: {} } };
    const { installNativePluginsApi } = await import(pathToFileURL(path.join(
        REPO_ROOT,
        'src/tauri/main/api/native-plugins.js',
    )));
    installNativePluginsApi({
        safeInvoke: async (command, args) => {
            calls.push({ command, args });
            return { command, args };
        },
    });
    return { calls, api: globalThis.window.__TAURITAVERN__.api.nativePlugins };
}

test('api.nativePlugins exposes the native command ABI', async () => {
    const { calls, api } = await installHarness();
    await api.list();
    await api.call('character-library.helper', 'search', { query: 'Ada' });
    await api.deactivate('character-library.helper');

    assert.deepEqual(calls, [
        { command: 'list_native_plugins', args: undefined },
        {
            command: 'call_native_plugin',
            args: {
                dto: {
                    pluginId: 'character-library.helper',
                    operation: 'search',
                    input: { query: 'Ada' },
                },
            },
        },
        {
            command: 'deactivate_native_plugin',
            args: { dto: { pluginId: 'character-library.helper' } },
        },
    ]);
});

test('api.nativePlugins rejects empty or unsafe identifiers before invoking Rust', async () => {
    const { calls, api } = await installHarness();
    await assert.rejects(() => api.call('../escape', 'search'), /unsupported characters/);
    await assert.rejects(() => api.call('valid.plugin', ''), /operation is required/);
    assert.equal(calls.length, 0);
});

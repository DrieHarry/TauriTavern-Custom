// @ts-check

/**
 * @param {unknown} value
 * @param {string} label
 */
function requireIdentifier(value, label) {
    const resolved = String(value || '').trim();
    if (!resolved) {
        throw new Error(`${label} is required`);
    }
    if (!/^[A-Za-z0-9._-]{1,128}$/.test(resolved)) {
        throw new Error(`${label} contains unsupported characters`);
    }
    return resolved;
}

/**
 * @param {{ safeInvoke: (command: string, args?: any) => Promise<any> }} deps
 */
function createNativePluginsApi({ safeInvoke }) {
    async function list() {
        return safeInvoke('list_native_plugins');
    }

    async function call(pluginId, operation, input = null) {
        const resolvedPluginId = requireIdentifier(pluginId, 'pluginId');
        const resolvedOperation = requireIdentifier(operation, 'operation');
        return safeInvoke('call_native_plugin', {
            dto: {
                pluginId: resolvedPluginId,
                operation: resolvedOperation,
                input,
            },
        });
    }

    async function deactivate(pluginId) {
        const resolvedPluginId = requireIdentifier(pluginId, 'pluginId');
        return safeInvoke('deactivate_native_plugin', {
            dto: {
                pluginId: resolvedPluginId,
            },
        });
    }

    return { list, call, deactivate };
}

/**
 * @param {any} context
 */
export function installNativePluginsApi(context) {
    const hostWindow = /** @type {any} */ (window);
    const hostAbi = hostWindow.__TAURITAVERN__;
    if (!hostAbi || typeof hostAbi !== 'object') {
        throw new Error('Host ABI __TAURITAVERN__ is missing');
    }
    if (typeof context?.safeInvoke !== 'function') {
        throw new Error('Tauri main context safeInvoke is missing');
    }
    if (!hostAbi.api || typeof hostAbi.api !== 'object') {
        hostAbi.api = {};
    }
    hostAbi.api.nativePlugins = createNativePluginsApi({ safeInvoke: context.safeInvoke });
}

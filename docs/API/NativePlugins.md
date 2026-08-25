# Native Plugins API

`window.__TAURITAVERN__.api.nativePlugins` 是 TauriTavern 专属的原生插件入口。它不模拟 SillyTavern server plugin，也不提供 Express/Node 兼容层。

## Package

原生插件与它的 UI 扩展放在同一个已安装扩展目录中。目录根需要增加 `tauritavern-plugin.json`：

```json
{
  "schemaVersion": 1,
  "id": "example.helper",
  "name": "Example Helper",
  "version": "1.0.0",
  "entry": "native-plugin/index.js",
  "permissions": {
    "http": {
      "origins": ["https://api.example.com"]
    }
  }
}
```

`entry` 必须是扩展目录内的单文件、UTF-8、bundled ESM。运行时不提供 Node builtin、npm resolution、任意文件系统或动态 native library 加载。

入口模块必须导出：

```js
export async function handle(operation, input, host) {
  return { operation, input };
}
```

可选导出：

```js
export async function activate(host) {}
export async function deactivate(host) {}
```

## Frontend API

```js
await window.__TAURITAVERN__.ready;

const plugins = window.__TAURITAVERN__.api.nativePlugins;
const installed = await plugins.list();
const result = await plugins.call('example.helper', 'search', { query: 'Ada' });
await plugins.deactivate('example.helper');
```

- `list()`：列出有效的 local/global 原生插件；同一 id 时 local 优先。
- `call(pluginId, operation, input?)`：加载并激活插件，然后串行调用对应实例。实例状态会在后续调用中保留。
- `deactivate(pluginId)`：调用可选 `deactivate()` 并卸载内存实例。未加载时幂等成功。

## Plugin Host

`handle()`、`activate()`、`deactivate()` 收到冻结的 `host`：

```js
const response = await host.http({
  method: 'POST',
  url: 'https://api.example.com/items',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ query: 'Ada' }),
});

await host.storage.set('settings', { enabled: true });
const settings = await host.storage.get('settings');
await host.storage.delete('settings');
host.log('info', 'request complete');
```

- HTTP 只允许 manifest 声明的 exact origin，不跟随 redirect；text response 使用 `body`，非 UTF-8 response 使用 `bodyBase64`。
- storage 是 plugin-id 隔离的 JSON KV；key 只允许字母、数字、`.`、`_`、`-`。
- 单次调用、输入、结果、脚本、HTTP body、storage value 和已激活实例数都有固定上限。
- API 在 Windows 与 Android 使用同一个 Rust/QuickJS runtime，不启动 sidecar 或本地 server。

具体 TypeScript 类型见 `src/types.d.ts`。

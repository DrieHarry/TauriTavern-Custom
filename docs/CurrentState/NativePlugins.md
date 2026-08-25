# Native Plugins 当前状态

## 结论

TauriTavern 现在有一套纯 TauriTavern 的 native plugin runtime。它服务于需要后端能力的 TauriTavern 扩展，不兼容、也不解释 SillyTavern Node server plugins。

插件包与已安装 UI 扩展同目录，通过 `tauritavern-plugin.json` 发现；local/global 目录沿用现有扩展目录，local 同 id 覆盖 global。这样安装、更新、移动和 Android 数据目录语义仍只有一套。

## 链路

1. UI 调用 `window.__TAURITAVERN__.api.nativePlugins`。
2. Host ABI 调用 `list_native_plugins`、`call_native_plugin` 或 `deactivate_native_plugin`。
3. `NativePluginService` 校验 id、operation 与 payload budget。
4. `FileNativePluginRepository` 读取 manifest 与 bundled ESM，并拒绝目录逃逸。
5. `QuickJsNativePluginRuntime` 为每个 plugin id 建立持久、串行、受内存/栈/时间限制的实例。
6. 插件只能通过 manifest-scoped HTTP gateway、plugin-isolated JSON store 与结构化日志接触宿主。

## 明确支持

- Windows/Android 共用 runtime，不依赖 Node、sidecar、localhost server 或平台动态库。
- bundled ESM 的 `activate` / `handle` / `deactivate` 生命周期。
- exact-origin HTTP、无 redirect、文字与 base64 二进制 body。
- plugin-id 隔离 JSON KV。
- source revision 变化后重建 runtime；显式 deactivate 幂等。
- local/global discovery，local 优先。

## 明确不支持

- SillyTavern server plugin manifest、Express route、Node builtin、npm runtime resolution。
- 插件任意文件系统、进程、shell、socket 或动态 native library 访问。
- 多文件 runtime import；入口必须预先 bundle 成单个 ESM 文件。
- 插件自己的 HTTP server 或 `/api/plugins/*` 兼容路由。

## 持续开发约束

- 新能力先扩展 typed port 与 versioned Host ABI；不要把 Rust command 名直接当 public contract。
- 不要为兼容 Node plugin 扩大 QuickJS sandbox。
- HTTP permission 继续保持 exact origin 且 redirect disabled。
- storage path 必须由 plugin id/key 派生并保持原子 JSON 写入。
- runtime 必须有有界 input/output、内存、stack、execution time 与 instance count。
- 插件入口更改、HTTP/storage host 更改和 Host ABI 更改都需要 focused contract/regression test。

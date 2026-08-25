//! QuickJS 沙箱脚本引擎（`SkillScriptEngine` 的 adapter 实现）。

mod api;
mod engine;
mod kit;
mod runtime_module;

pub use engine::QuickJsScriptEngine;
pub use native_plugin::QuickJsNativePluginRuntime;

mod native_plugin;

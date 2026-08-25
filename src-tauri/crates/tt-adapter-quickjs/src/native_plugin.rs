use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rquickjs::function::{Async, Func};
use rquickjs::{AsyncContext, AsyncRuntime, Ctx, Function, Module, Object, Value as JsValue};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time::timeout;

use tt_contracts::native_plugin::NativePluginHttpRequest;
use tt_domain::errors::DomainError;
use tt_ports::native_plugin::{
    NativePluginDataStore, NativePluginHttpGateway, NativePluginPackage, NativePluginRuntime,
};

const EXECUTION_TIMEOUT: Duration = Duration::from_secs(30);
const MEMORY_LIMIT_BYTES: usize = 32 * 1024 * 1024;
const MAX_STACK_BYTES: usize = 256 * 1024;
const MAX_RESULT_BYTES: usize = 1024 * 1024;
const MAX_LOADED_PLUGINS: usize = 8;

const HOST_BOOTSTRAP: &str = r#"
(() => {
    const unwrap = (raw) => {
        const envelope = JSON.parse(raw);
        if (!envelope.ok) throw new Error(envelope.error || 'Native host operation failed');
        return envelope.value;
    };
    const storage = Object.freeze({
        get: async (key) => unwrap(await __ttNativeStorageGet(String(key))),
        set: async (key, value) => {
            const encoded = JSON.stringify(value);
            if (encoded === undefined) throw new Error('Native plugin storage value must be JSON-serializable');
            return unwrap(await __ttNativeStorageSet(String(key), encoded));
        },
        delete: async (key) => unwrap(await __ttNativeStorageDelete(String(key))),
    });
    globalThis.__TAURITAVERN_PLUGIN__ = Object.freeze({
        abiVersion: 1,
        http: async (request) => unwrap(await __ttNativeHttp(JSON.stringify(request))),
        storage,
        log: (level, message) => __ttNativeLog(String(level), String(message)),
    });
})();
"#;

pub struct QuickJsNativePluginRuntime {
    http: Arc<dyn NativePluginHttpGateway>,
    data: Arc<dyn NativePluginDataStore>,
    instances: Mutex<HashMap<String, Arc<PluginInstance>>>,
    creation_gate: Mutex<()>,
}

struct PluginInstance {
    revision: String,
    context: AsyncContext,
    _runtime: AsyncRuntime,
    deadline: Arc<StdMutex<Option<Instant>>>,
    gate: Mutex<()>,
}

impl QuickJsNativePluginRuntime {
    pub fn new(
        http: Arc<dyn NativePluginHttpGateway>,
        data: Arc<dyn NativePluginDataStore>,
    ) -> Self {
        Self {
            http,
            data,
            instances: Mutex::new(HashMap::new()),
            creation_gate: Mutex::new(()),
        }
    }

    async fn instance(
        &self,
        package: &NativePluginPackage,
    ) -> Result<Arc<PluginInstance>, DomainError> {
        if let Some(instance) = self
            .instances
            .lock()
            .await
            .get(&package.descriptor.id)
            .cloned()
            && instance.revision == package.revision
        {
            return Ok(instance);
        }

        let _creation_guard = self.creation_gate.lock().await;
        if let Some(instance) = self
            .instances
            .lock()
            .await
            .get(&package.descriptor.id)
            .cloned()
            && instance.revision == package.revision
        {
            return Ok(instance);
        }
        let mut instances = self.instances.lock().await;
        instances.remove(&package.descriptor.id);
        if instances.len() >= MAX_LOADED_PLUGINS {
            return Err(DomainError::Conflict(format!(
                "At most {MAX_LOADED_PLUGINS} native plugins may be active; deactivate one before loading another"
            )));
        }
        drop(instances);

        let instance =
            Arc::new(create_instance(package, self.http.clone(), self.data.clone()).await?);
        self.instances
            .lock()
            .await
            .insert(package.descriptor.id.clone(), instance.clone());
        Ok(instance)
    }

    async fn evict_if_current(&self, plugin_id: &str, instance: &Arc<PluginInstance>) {
        let mut instances = self.instances.lock().await;
        if instances
            .get(plugin_id)
            .is_some_and(|current| Arc::ptr_eq(current, instance))
        {
            instances.remove(plugin_id);
        }
    }
}

#[async_trait]
impl NativePluginRuntime for QuickJsNativePluginRuntime {
    async fn call(
        &self,
        package: NativePluginPackage,
        operation: &str,
        input: Value,
    ) -> Result<Value, DomainError> {
        let instance = self.instance(&package).await?;
        let _guard = instance.gate.lock().await;
        set_deadline(&instance.deadline);
        let outcome = timeout(
            EXECUTION_TIMEOUT,
            call_instance(&instance.context, operation, input),
        )
        .await;
        clear_deadline(&instance.deadline);

        match outcome {
            Ok(result) => result,
            Err(_) => {
                self.evict_if_current(&package.descriptor.id, &instance)
                    .await;
                Err(DomainError::Transient(format!(
                    "Native plugin `{}` operation `{operation}` timed out after {} seconds",
                    package.descriptor.id,
                    EXECUTION_TIMEOUT.as_secs()
                )))
            }
        }
    }

    async fn deactivate(&self, plugin_id: &str) -> Result<(), DomainError> {
        let Some(instance) = self.instances.lock().await.remove(plugin_id) else {
            return Ok(());
        };
        let _guard = instance.gate.lock().await;
        set_deadline(&instance.deadline);
        let outcome = timeout(EXECUTION_TIMEOUT, deactivate_instance(&instance.context)).await;
        clear_deadline(&instance.deadline);
        match outcome {
            Ok(result) => result,
            Err(_) => Err(DomainError::Transient(format!(
                "Native plugin `{plugin_id}` deactivation timed out"
            ))),
        }
    }
}

async fn create_instance(
    package: &NativePluginPackage,
    http: Arc<dyn NativePluginHttpGateway>,
    data: Arc<dyn NativePluginDataStore>,
) -> Result<PluginInstance, DomainError> {
    let runtime = AsyncRuntime::new().map_err(runtime_error)?;
    runtime.set_memory_limit(MEMORY_LIMIT_BYTES).await;
    runtime.set_max_stack_size(MAX_STACK_BYTES).await;
    let deadline = Arc::new(StdMutex::new(Some(Instant::now() + EXECUTION_TIMEOUT)));
    let interrupt_deadline = deadline.clone();
    runtime
        .set_interrupt_handler(Some(Box::new(move || {
            interrupt_deadline
                .lock()
                .ok()
                .and_then(|deadline| *deadline)
                .is_some_and(|deadline| Instant::now() >= deadline)
        })))
        .await;
    let context = AsyncContext::full(&runtime).await.map_err(runtime_error)?;

    let plugin_id = package.descriptor.id.clone();
    let allowed_origins = package.manifest.permissions.http.origins.clone();
    let entry_source = package.entry_source.clone();
    let module_name = format!("tauritavern-native:{plugin_id}");
    let setup_result = timeout(
        EXECUTION_TIMEOUT,
        context.async_with(async |ctx| {
            let result = async {
                install_host_functions(&ctx, plugin_id.clone(), allowed_origins, http, data)?;
                ctx.eval::<(), _>(HOST_BOOTSTRAP)?;
                let declared = Module::declare(ctx.clone(), module_name, entry_source)?;
                let (module, evaluated) = declared.eval()?;
                evaluated.into_future::<JsValue>().await?;

                let handle = module.get::<_, Function>("handle").map_err(|_| {
                    rquickjs::Exception::throw_message(
                        &ctx,
                        "native plugin must export a `handle(operation, input, host)` function",
                    )
                })?;
                ctx.globals().set("__ttNativeHandle", handle)?;
                if let Ok(activate) = module.get::<_, Function>("activate") {
                    ctx.globals().set("__ttNativeActivate", activate.clone())?;
                    let host = ctx.globals().get::<_, Object>("__TAURITAVERN_PLUGIN__")?;
                    let returned = activate.call::<_, JsValue>((host,))?;
                    resolve_js_value(returned).await?;
                }
                if let Ok(deactivate) = module.get::<_, Function>("deactivate") {
                    ctx.globals().set("__ttNativeDeactivate", deactivate)?;
                }
                Ok::<(), rquickjs::Error>(())
            }
            .await;
            result.map_err(|error| format_js_error(&ctx, &error))
        }),
    )
    .await;

    match setup_result {
        Ok(Ok(())) => {
            clear_deadline(&deadline);
            Ok(PluginInstance {
                revision: package.revision.clone(),
                context,
                _runtime: runtime,
                deadline,
                gate: Mutex::new(()),
            })
        }
        Ok(Err(detail)) => Err(DomainError::InvalidData(format!(
            "Failed to activate native plugin `{}`: {error}",
            package.descriptor.id,
            error = detail,
        ))),
        Err(_) => Err(DomainError::Transient(format!(
            "Native plugin `{}` activation timed out",
            package.descriptor.id
        ))),
    }
}

fn install_host_functions(
    ctx: &Ctx<'_>,
    plugin_id: String,
    allowed_origins: Vec<String>,
    http: Arc<dyn NativePluginHttpGateway>,
    data: Arc<dyn NativePluginDataStore>,
) -> rquickjs::Result<()> {
    let http_gateway = http.clone();
    ctx.globals().set(
        "__ttNativeHttp",
        Func::from(Async(move |request_json: String| {
            let http = http_gateway.clone();
            let origins = allowed_origins.clone();
            async move {
                let result = match serde_json::from_str::<NativePluginHttpRequest>(&request_json) {
                    Ok(request) => http.send(&origins, request).await.and_then(to_json_value),
                    Err(error) => Err(DomainError::InvalidData(format!(
                        "Invalid native plugin HTTP request: {error}"
                    ))),
                };
                host_envelope(result)
            }
        })),
    )?;

    let get_store = data.clone();
    let get_plugin_id = plugin_id.clone();
    ctx.globals().set(
        "__ttNativeStorageGet",
        Func::from(Async(move |key: String| {
            let store = get_store.clone();
            let plugin_id = get_plugin_id.clone();
            async move { host_envelope(store.get(&plugin_id, &key).await) }
        })),
    )?;

    let set_store = data.clone();
    let set_plugin_id = plugin_id.clone();
    ctx.globals().set(
        "__ttNativeStorageSet",
        Func::from(Async(move |key: String, value_json: String| {
            let store = set_store.clone();
            let plugin_id = set_plugin_id.clone();
            async move {
                let result = match serde_json::from_str(&value_json) {
                    Ok(value) => store
                        .set(&plugin_id, &key, value)
                        .await
                        .map(|()| Value::Null),
                    Err(error) => Err(DomainError::InvalidData(format!(
                        "Invalid native plugin storage value: {error}"
                    ))),
                };
                host_envelope(result)
            }
        })),
    )?;

    let delete_plugin_id = plugin_id.clone();
    ctx.globals().set(
        "__ttNativeStorageDelete",
        Func::from(Async(move |key: String| {
            let store = data.clone();
            let plugin_id = delete_plugin_id.clone();
            async move { host_envelope(store.delete(&plugin_id, &key).await.map(|()| Value::Null)) }
        })),
    )?;

    ctx.globals().set(
        "__ttNativeLog",
        Func::from(move |level: String, message: String| match level.as_str() {
            "error" => tracing::error!(plugin_id = %plugin_id, "{message}"),
            "warn" => tracing::warn!(plugin_id = %plugin_id, "{message}"),
            "debug" => tracing::debug!(plugin_id = %plugin_id, "{message}"),
            _ => tracing::info!(plugin_id = %plugin_id, "{message}"),
        }),
    )?;
    Ok(())
}

async fn call_instance(
    context: &AsyncContext,
    operation: &str,
    input: Value,
) -> Result<Value, DomainError> {
    let operation = operation.to_string();
    let input_json = serde_json::to_string(&input)
        .map_err(|error| DomainError::InvalidData(error.to_string()))?;
    let result = context
        .async_with(async |ctx| {
            let result = async {
                let handler = ctx.globals().get::<_, Function>("__ttNativeHandle")?;
                let host = ctx.globals().get::<_, Object>("__TAURITAVERN_PLUGIN__")?;
                let js_input = ctx.json_parse(input_json)?;
                let returned = handler.call::<_, JsValue>((operation, js_input, host))?;
                let value = resolve_js_value(returned).await?;
                let text = ctx
                    .json_stringify(value)?
                    .ok_or_else(|| {
                        rquickjs::Exception::throw_message(
                            &ctx,
                            "native plugin result must be JSON-serializable",
                        )
                    })?
                    .to_string()?;
                Ok::<String, rquickjs::Error>(text)
            }
            .await;
            result.map_err(|error| format_js_error(&ctx, &error))
        })
        .await
        .map_err(|error| DomainError::InvalidData(format!("Native plugin call failed: {error}")))?;
    if result.len() > MAX_RESULT_BYTES {
        return Err(DomainError::InvalidData(format!(
            "Native plugin result is {} bytes, exceeding the {MAX_RESULT_BYTES}-byte limit",
            result.len()
        )));
    }
    serde_json::from_str(&result).map_err(|error| {
        DomainError::InvalidData(format!("Native plugin returned invalid JSON: {error}"))
    })
}

async fn deactivate_instance(context: &AsyncContext) -> Result<(), DomainError> {
    context
        .async_with(async |ctx| {
            let result = async {
                let Ok(deactivate) = ctx.globals().get::<_, Function>("__ttNativeDeactivate")
                else {
                    return Ok::<(), rquickjs::Error>(());
                };
                let host = ctx.globals().get::<_, Object>("__TAURITAVERN_PLUGIN__")?;
                let returned = deactivate.call::<_, JsValue>((host,))?;
                resolve_js_value(returned).await?;
                Ok(())
            }
            .await;
            result.map_err(|error| format_js_error(&ctx, &error))
        })
        .await
        .map_err(|error| {
            DomainError::InvalidData(format!("Native plugin deactivation failed: {error}"))
        })
}

async fn resolve_js_value<'js>(value: JsValue<'js>) -> rquickjs::Result<JsValue<'js>> {
    if let Some(promise) = value.clone().into_promise() {
        promise.into_future::<JsValue>().await
    } else {
        Ok(value)
    }
}

fn to_json_value<T: Serialize>(value: T) -> Result<Value, DomainError> {
    serde_json::to_value(value).map_err(|error| {
        DomainError::InternalError(format!("Failed to encode host value: {error}"))
    })
}

fn host_envelope<T: Serialize>(result: Result<T, DomainError>) -> String {
    match result {
        Ok(value) => serde_json::json!({ "ok": true, "value": value }).to_string(),
        Err(error) => serde_json::json!({ "ok": false, "error": error.to_string() }).to_string(),
    }
}

fn runtime_error(error: rquickjs::Error) -> DomainError {
    DomainError::InternalError(format!("QuickJS native plugin runtime failure: {error}"))
}

fn format_js_error(ctx: &Ctx<'_>, error: &rquickjs::Error) -> String {
    if !matches!(error, rquickjs::Error::Exception) {
        return error.to_string();
    }
    let Some(exception) = ctx.catch().into_object() else {
        return "unknown JavaScript exception".to_string();
    };
    let message = exception
        .get::<_, JsValue>("message")
        .ok()
        .and_then(|value| value.as_string().map(|string| string.to_string()))
        .and_then(Result::ok);
    let stack = exception
        .get::<_, JsValue>("stack")
        .ok()
        .and_then(|value| value.as_string().map(|string| string.to_string()))
        .and_then(Result::ok);
    match (message, stack) {
        (Some(message), Some(stack)) => format!("{message}\n{stack}"),
        (Some(message), None) => message,
        (None, Some(stack)) => stack,
        (None, None) => "JavaScript exception without message".to_string(),
    }
}

fn set_deadline(deadline: &StdMutex<Option<Instant>>) {
    if let Ok(mut deadline) = deadline.lock() {
        *deadline = Some(Instant::now() + EXECUTION_TIMEOUT);
    }
}

fn clear_deadline(deadline: &StdMutex<Option<Instant>>) {
    if let Ok(mut deadline) = deadline.lock() {
        *deadline = None;
    }
}

#[cfg(test)]
mod tests;

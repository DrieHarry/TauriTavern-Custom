use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{Value, json};

use tt_contracts::native_plugin::{
    NativePluginDescriptor, NativePluginHttpRequest, NativePluginHttpResponse,
    NativePluginManifest, NativePluginPermissions, NativePluginScope,
};
use tt_domain::errors::DomainError;
use tt_ports::native_plugin::{
    NativePluginDataStore, NativePluginHttpGateway, NativePluginPackage, NativePluginRuntime,
};

use super::QuickJsNativePluginRuntime;

#[derive(Default)]
struct MemoryStore(Mutex<BTreeMap<(String, String), Value>>);

#[async_trait]
impl NativePluginDataStore for MemoryStore {
    async fn get(&self, plugin_id: &str, key: &str) -> Result<Option<Value>, DomainError> {
        Ok(self
            .0
            .lock()
            .expect("store lock")
            .get(&(plugin_id.to_string(), key.to_string()))
            .cloned())
    }

    async fn set(&self, plugin_id: &str, key: &str, value: Value) -> Result<(), DomainError> {
        self.0
            .lock()
            .expect("store lock")
            .insert((plugin_id.to_string(), key.to_string()), value);
        Ok(())
    }

    async fn delete(&self, plugin_id: &str, key: &str) -> Result<(), DomainError> {
        self.0
            .lock()
            .expect("store lock")
            .remove(&(plugin_id.to_string(), key.to_string()));
        Ok(())
    }
}

struct StubHttp;

#[async_trait]
impl NativePluginHttpGateway for StubHttp {
    async fn send(
        &self,
        _allowed_origins: &[String],
        request: NativePluginHttpRequest,
    ) -> Result<NativePluginHttpResponse, DomainError> {
        Ok(NativePluginHttpResponse {
            status: 200,
            headers: BTreeMap::new(),
            body: Some(format!("echo:{}", request.url)),
            body_base64: None,
        })
    }
}

fn package(source: &str, revision: &str) -> NativePluginPackage {
    let manifest = NativePluginManifest {
        schema_version: 1,
        id: "test.native".to_string(),
        name: "Test Native".to_string(),
        version: "1.0.0".to_string(),
        entry: "native.js".to_string(),
        permissions: NativePluginPermissions::default(),
    };
    NativePluginPackage {
        descriptor: NativePluginDescriptor {
            id: manifest.id.clone(),
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            extension_name: "test-extension".to_string(),
            scope: NativePluginScope::Local,
            permissions: manifest.permissions.clone(),
        },
        manifest,
        entry_source: source.to_string(),
        revision: revision.to_string(),
    }
}

#[tokio::test]
async fn keeps_runtime_state_and_uses_isolated_host_storage() {
    let runtime =
        QuickJsNativePluginRuntime::new(Arc::new(StubHttp), Arc::new(MemoryStore::default()));
    let plugin = package(
        r#"
            let calls = 0;
            export async function handle(operation, input, host) {
                calls += 1;
                if (operation === 'save') await host.storage.set('last', input);
                return { calls, saved: await host.storage.get('last') };
            }
        "#,
        "revision-one",
    );

    let first = runtime
        .call(plugin.clone(), "save", json!({ "name": "Ada" }))
        .await
        .expect("first call");
    let second = runtime
        .call(plugin, "read", Value::Null)
        .await
        .expect("second call");

    assert_eq!(first["calls"], 1);
    assert_eq!(second["calls"], 2);
    assert_eq!(second["saved"], json!({ "name": "Ada" }));
}

#[tokio::test]
async fn rejects_plugin_without_handle_export() {
    let runtime =
        QuickJsNativePluginRuntime::new(Arc::new(StubHttp), Arc::new(MemoryStore::default()));
    let error = runtime
        .call(
            package("export const value = 1;", "bad"),
            "read",
            Value::Null,
        )
        .await
        .expect_err("missing handle must fail");
    assert!(error.to_string().contains("handle"));
}

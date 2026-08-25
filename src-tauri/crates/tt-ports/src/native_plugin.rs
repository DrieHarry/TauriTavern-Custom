use async_trait::async_trait;
use serde_json::Value;

use tt_contracts::native_plugin::{
    NativePluginDescriptor, NativePluginHttpRequest, NativePluginHttpResponse, NativePluginManifest,
};
use tt_domain::errors::DomainError;

#[derive(Debug, Clone)]
pub struct NativePluginPackage {
    pub descriptor: NativePluginDescriptor,
    pub manifest: NativePluginManifest,
    pub entry_source: String,
    pub revision: String,
}

#[async_trait]
pub trait NativePluginPackageRepository: Send + Sync {
    async fn list(&self) -> Result<Vec<NativePluginPackage>, DomainError>;

    async fn find(&self, plugin_id: &str) -> Result<NativePluginPackage, DomainError>;
}

#[async_trait]
pub trait NativePluginDataStore: Send + Sync {
    async fn get(&self, plugin_id: &str, key: &str) -> Result<Option<Value>, DomainError>;

    async fn set(&self, plugin_id: &str, key: &str, value: Value) -> Result<(), DomainError>;

    async fn delete(&self, plugin_id: &str, key: &str) -> Result<(), DomainError>;
}

#[async_trait]
pub trait NativePluginHttpGateway: Send + Sync {
    async fn send(
        &self,
        allowed_origins: &[String],
        request: NativePluginHttpRequest,
    ) -> Result<NativePluginHttpResponse, DomainError>;
}

#[async_trait]
pub trait NativePluginRuntime: Send + Sync {
    async fn call(
        &self,
        package: NativePluginPackage,
        operation: &str,
        input: Value,
    ) -> Result<Value, DomainError>;

    async fn deactivate(&self, plugin_id: &str) -> Result<(), DomainError>;
}

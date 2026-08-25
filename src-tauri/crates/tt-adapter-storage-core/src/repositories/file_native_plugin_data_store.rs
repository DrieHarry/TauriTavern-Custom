use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde_json::Value;
use tokio::fs;

use tt_domain::errors::DomainError;
use tt_ports::native_plugin::NativePluginDataStore;

use crate::file_system::{read_json_file, write_json_file};

const MAX_VALUE_BYTES: usize = 1024 * 1024;

pub struct FileNativePluginDataStore {
    root: PathBuf,
}

impl FileNativePluginDataStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn value_path(&self, plugin_id: &str, key: &str) -> Result<PathBuf, DomainError> {
        validate_component(plugin_id, "plugin id")?;
        validate_component(key, "storage key")?;
        Ok(self.root.join(plugin_id).join(format!("{key}.json")))
    }
}

#[async_trait]
impl NativePluginDataStore for FileNativePluginDataStore {
    async fn get(&self, plugin_id: &str, key: &str) -> Result<Option<Value>, DomainError> {
        let path = self.value_path(plugin_id, key)?;
        match read_json_file(&path).await {
            Ok(value) => Ok(Some(value)),
            Err(DomainError::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn set(&self, plugin_id: &str, key: &str, value: Value) -> Result<(), DomainError> {
        let path = self.value_path(plugin_id, key)?;
        let encoded = serde_json::to_vec(&value)
            .map_err(|error| DomainError::InvalidData(error.to_string()))?;
        if encoded.len() > MAX_VALUE_BYTES {
            return Err(DomainError::InvalidData(format!(
                "Native plugin storage value is {} bytes, exceeding the {MAX_VALUE_BYTES}-byte limit",
                encoded.len()
            )));
        }
        write_json_file(&path, &value).await
    }

    async fn delete(&self, plugin_id: &str, key: &str) -> Result<(), DomainError> {
        let path = self.value_path(plugin_id, key)?;
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io_error("delete native plugin storage value", &path, error)),
        }
    }
}

fn validate_component(value: &str, label: &str) -> Result<(), DomainError> {
    if !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        Ok(())
    } else {
        Err(DomainError::InvalidData(format!(
            "Native plugin {label} contains unsupported characters"
        )))
    }
}

fn io_error(action: &str, path: &Path, error: std::io::Error) -> DomainError {
    DomainError::InternalError(format!("Failed to {action} {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::json;
    use tt_ports::native_plugin::NativePluginDataStore;

    use super::FileNativePluginDataStore;

    #[tokio::test]
    async fn stores_json_in_plugin_isolated_paths() {
        let root = std::env::temp_dir().join(format!(
            "tauritavern-native-plugin-store-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let store = FileNativePluginDataStore::new(root.clone());

        store
            .set("plugin.one", "settings", json!({ "enabled": true }))
            .await
            .expect("store value");
        assert_eq!(
            store
                .get("plugin.one", "settings")
                .await
                .expect("get value"),
            Some(json!({ "enabled": true }))
        );
        assert_eq!(
            store
                .get("plugin.two", "settings")
                .await
                .expect("get other"),
            None
        );
        assert!(store.set("../escape", "settings", json!(1)).await.is_err());

        store
            .delete("plugin.one", "settings")
            .await
            .expect("delete value");
        assert_eq!(
            store
                .get("plugin.one", "settings")
                .await
                .expect("get deleted"),
            None
        );
        tokio::fs::remove_dir_all(root)
            .await
            .expect("clean temp root");
    }
}

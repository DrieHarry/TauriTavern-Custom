use std::sync::Arc;

use serde_json::Value;

use tt_contracts::native_plugin::NativePluginDescriptor;
use tt_ports::native_plugin::{NativePluginPackageRepository, NativePluginRuntime};

use crate::errors::ApplicationError;

const MAX_CALL_INPUT_BYTES: usize = 1024 * 1024;

pub struct NativePluginService {
    packages: Arc<dyn NativePluginPackageRepository>,
    runtime: Arc<dyn NativePluginRuntime>,
}

impl NativePluginService {
    pub fn new(
        packages: Arc<dyn NativePluginPackageRepository>,
        runtime: Arc<dyn NativePluginRuntime>,
    ) -> Self {
        Self { packages, runtime }
    }

    pub async fn list(&self) -> Result<Vec<NativePluginDescriptor>, ApplicationError> {
        let mut descriptors = self
            .packages
            .list()
            .await?
            .into_iter()
            .map(|package| package.descriptor)
            .collect::<Vec<_>>();
        descriptors.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(descriptors)
    }

    pub async fn call(
        &self,
        plugin_id: &str,
        operation: &str,
        input: Value,
    ) -> Result<Value, ApplicationError> {
        validate_identifier(plugin_id, "plugin id")?;
        validate_identifier(operation, "operation")?;

        let input_bytes = serde_json::to_vec(&input)
            .map_err(|error| ApplicationError::ValidationError(error.to_string()))?
            .len();
        if input_bytes > MAX_CALL_INPUT_BYTES {
            return Err(ApplicationError::ValidationError(format!(
                "native plugin input is {input_bytes} bytes, exceeding the {MAX_CALL_INPUT_BYTES}-byte limit"
            )));
        }

        let package = self.packages.find(plugin_id).await?;
        self.runtime
            .call(package, operation, input)
            .await
            .map_err(Into::into)
    }

    pub async fn deactivate(&self, plugin_id: &str) -> Result<(), ApplicationError> {
        validate_identifier(plugin_id, "plugin id")?;
        self.runtime.deactivate(plugin_id).await.map_err(Into::into)
    }
}

fn validate_identifier(value: &str, label: &str) -> Result<(), ApplicationError> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(ApplicationError::ValidationError(format!(
            "{label} must contain only ASCII letters, digits, dots, underscores, or hyphens and be at most 128 characters"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::validate_identifier;

    #[test]
    fn identifiers_are_path_safe() {
        assert!(validate_identifier("character-library.lookup", "operation").is_ok());
        assert!(validate_identifier("../escape", "operation").is_err());
        assert!(validate_identifier("contains space", "operation").is_err());
    }
}

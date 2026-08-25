use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::fs;

use tt_contracts::native_plugin::{
    NATIVE_PLUGIN_SCHEMA_VERSION, NativePluginDescriptor, NativePluginManifest, NativePluginScope,
};
use tt_domain::errors::DomainError;
use tt_ports::native_plugin::{NativePluginPackage, NativePluginPackageRepository};

const MANIFEST_FILE_NAME: &str = "tauritavern-plugin.json";
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const MAX_ENTRY_BYTES: u64 = 2 * 1024 * 1024;

pub struct FileNativePluginRepository {
    local_extensions: PathBuf,
    global_extensions: PathBuf,
}

impl FileNativePluginRepository {
    pub fn new(local_extensions: PathBuf, global_extensions: PathBuf) -> Self {
        Self {
            local_extensions,
            global_extensions,
        }
    }

    async fn discover(&self) -> Result<Vec<NativePluginPackage>, DomainError> {
        let mut packages = BTreeMap::<String, NativePluginPackage>::new();
        self.scan_root(
            &self.global_extensions,
            NativePluginScope::Global,
            &mut packages,
        )
        .await?;
        self.scan_root(
            &self.local_extensions,
            NativePluginScope::Local,
            &mut packages,
        )
        .await?;
        Ok(packages.into_values().collect())
    }

    async fn scan_root(
        &self,
        root: &Path,
        scope: NativePluginScope,
        packages: &mut BTreeMap<String, NativePluginPackage>,
    ) -> Result<(), DomainError> {
        let root_metadata = match fs::metadata(root).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(io_error("read native plugin root", root, error)),
        };
        if !root_metadata.is_dir() {
            return Err(DomainError::InvalidData(format!(
                "Native plugin root is not a directory: {}",
                root.display()
            )));
        }

        let canonical_root = fs::canonicalize(root)
            .await
            .map_err(|error| io_error("canonicalize native plugin root", root, error))?;
        let mut directory = fs::read_dir(root)
            .await
            .map_err(|error| io_error("scan native plugin root", root, error))?;
        let mut extension_paths = Vec::new();
        while let Some(entry) = directory
            .next_entry()
            .await
            .map_err(|error| io_error("scan native plugin root", root, error))?
        {
            let file_type = entry
                .file_type()
                .await
                .map_err(|error| io_error("inspect extension directory", &entry.path(), error))?;
            if file_type.is_dir() {
                extension_paths.push(entry.path());
            }
        }
        extension_paths.sort();

        for extension_path in extension_paths {
            let manifest_path = extension_path.join(MANIFEST_FILE_NAME);
            if fs::metadata(&manifest_path).await.is_err() {
                continue;
            }
            match read_package(&canonical_root, &extension_path, &manifest_path, scope).await {
                Ok(package) => {
                    if let Some(existing) = packages.get(&package.descriptor.id)
                        && existing.descriptor.scope == package.descriptor.scope
                    {
                        tracing::warn!(
                            plugin_id = %package.descriptor.id,
                            kept_extension = %existing.descriptor.extension_name,
                            ignored_extension = %package.descriptor.extension_name,
                            "Ignoring duplicate native plugin id in the same extension scope"
                        );
                        continue;
                    }
                    packages.insert(package.descriptor.id.clone(), package);
                }
                Err(error) => {
                    tracing::warn!(
                        manifest = %manifest_path.display(),
                        %error,
                        "Ignoring invalid TauriTavern native plugin package"
                    );
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl NativePluginPackageRepository for FileNativePluginRepository {
    async fn list(&self) -> Result<Vec<NativePluginPackage>, DomainError> {
        self.discover().await
    }

    async fn find(&self, plugin_id: &str) -> Result<NativePluginPackage, DomainError> {
        self.discover()
            .await?
            .into_iter()
            .find(|package| package.descriptor.id == plugin_id)
            .ok_or_else(|| DomainError::NotFound(format!("Native plugin `{plugin_id}`")))
    }
}

async fn read_package(
    canonical_root: &Path,
    extension_path: &Path,
    manifest_path: &Path,
    scope: NativePluginScope,
) -> Result<NativePluginPackage, DomainError> {
    let canonical_extension = fs::canonicalize(extension_path)
        .await
        .map_err(|error| io_error("canonicalize extension directory", extension_path, error))?;
    if !canonical_extension.starts_with(canonical_root) {
        return Err(DomainError::InvalidData(
            "Extension directory resolves outside its installation root".to_string(),
        ));
    }

    let manifest_bytes = read_bounded_file(manifest_path, MAX_MANIFEST_BYTES).await?;
    let manifest: NativePluginManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|error| {
            DomainError::InvalidData(format!("Invalid {MANIFEST_FILE_NAME}: {error}"))
        })?;
    validate_manifest(&manifest)?;

    let entry_path = canonical_extension.join(&manifest.entry);
    let canonical_entry = fs::canonicalize(&entry_path)
        .await
        .map_err(|error| io_error("resolve native plugin entry", &entry_path, error))?;
    if !canonical_entry.starts_with(&canonical_extension) {
        return Err(DomainError::InvalidData(
            "Native plugin entry resolves outside its extension directory".to_string(),
        ));
    }
    let entry_bytes = read_bounded_file(&canonical_entry, MAX_ENTRY_BYTES).await?;
    let entry_source = String::from_utf8(entry_bytes.clone()).map_err(|_| {
        DomainError::InvalidData("Native plugin entry must be UTF-8 JavaScript".to_string())
    })?;

    let extension_name = extension_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| DomainError::InvalidData("Invalid extension directory name".to_string()))?
        .to_string();
    let descriptor = NativePluginDescriptor {
        id: manifest.id.clone(),
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        extension_name,
        scope,
        permissions: manifest.permissions.clone(),
    };

    let mut hasher = Sha256::new();
    hasher.update(&manifest_bytes);
    hasher.update(&entry_bytes);
    let revision = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    Ok(NativePluginPackage {
        descriptor,
        manifest,
        entry_source,
        revision,
    })
}

fn validate_manifest(manifest: &NativePluginManifest) -> Result<(), DomainError> {
    if manifest.schema_version != NATIVE_PLUGIN_SCHEMA_VERSION {
        return Err(DomainError::InvalidData(format!(
            "Unsupported native plugin schemaVersion {}; expected {}",
            manifest.schema_version, NATIVE_PLUGIN_SCHEMA_VERSION
        )));
    }
    validate_identifier(&manifest.id, "id")?;
    if manifest.name.trim().is_empty() || manifest.name.len() > 160 {
        return Err(DomainError::InvalidData(
            "Native plugin name must be 1-160 characters".to_string(),
        ));
    }
    if manifest.version.trim().is_empty() || manifest.version.len() > 64 {
        return Err(DomainError::InvalidData(
            "Native plugin version must be 1-64 characters".to_string(),
        ));
    }
    let entry = Path::new(&manifest.entry);
    if manifest.entry.trim().is_empty() || entry.is_absolute() {
        return Err(DomainError::InvalidData(
            "Native plugin entry must be a relative path".to_string(),
        ));
    }
    Ok(())
}

fn validate_identifier(value: &str, label: &str) -> Result<(), DomainError> {
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

async fn read_bounded_file(path: &Path, limit: u64) -> Result<Vec<u8>, DomainError> {
    let metadata = fs::metadata(path)
        .await
        .map_err(|error| io_error("inspect native plugin file", path, error))?;
    if !metadata.is_file() {
        return Err(DomainError::InvalidData(format!(
            "Native plugin path is not a file: {}",
            path.display()
        )));
    }
    if metadata.len() > limit {
        return Err(DomainError::InvalidData(format!(
            "Native plugin file {} is {} bytes, exceeding the {limit}-byte limit",
            path.display(),
            metadata.len()
        )));
    }
    fs::read(path)
        .await
        .map_err(|error| io_error("read native plugin file", path, error))
}

fn io_error(action: &str, path: &Path, error: std::io::Error) -> DomainError {
    DomainError::InternalError(format!("Failed to {action} {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use tokio::fs;
    use tt_ports::native_plugin::NativePluginPackageRepository;

    use super::FileNativePluginRepository;

    fn temp_root() -> PathBuf {
        std::env::temp_dir().join(format!(
            "tauritavern-native-plugin-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ))
    }

    #[tokio::test]
    async fn discovers_bundled_plugin_and_local_scope_wins() {
        let root = temp_root();
        let local = root.join("local");
        let global = root.join("global");
        for (scope_root, version) in [(&global, "1.0.0"), (&local, "2.0.0")] {
            let extension = scope_root.join("character-library");
            fs::create_dir_all(&extension)
                .await
                .expect("create extension");
            fs::write(
                extension.join("tauritavern-plugin.json"),
                format!(
                    r#"{{"schemaVersion":1,"id":"character-library.helper","name":"Helper","version":"{version}","entry":"native.js"}}"#
                ),
            )
            .await
            .expect("write manifest");
            fs::write(
                extension.join("native.js"),
                "export function handle() { return 1; }",
            )
            .await
            .expect("write entry");
        }

        let repository = FileNativePluginRepository::new(local, global);
        let packages = repository.list().await.expect("discover plugins");
        assert_eq!(packages.len(), 1);
        assert_eq!(packages[0].manifest.version, "2.0.0");
        assert!(packages[0].revision.len() == 64);

        fs::remove_dir_all(root).await.expect("clean temp root");
    }

    #[tokio::test]
    async fn ignores_entry_that_escapes_extension_directory() {
        let root = temp_root();
        let local = root.join("local");
        let global = root.join("global");
        let extension = local.join("bad");
        fs::create_dir_all(&extension)
            .await
            .expect("create extension");
        fs::write(root.join("outside.js"), "export function handle() {}")
            .await
            .expect("write outside entry");
        fs::write(
            extension.join("tauritavern-plugin.json"),
            r#"{"schemaVersion":1,"id":"bad.plugin","name":"Bad","version":"1","entry":"../../outside.js"}"#,
        )
        .await
        .expect("write manifest");

        let repository = FileNativePluginRepository::new(local, global);
        assert!(
            repository
                .list()
                .await
                .expect("discover plugins")
                .is_empty()
        );

        fs::remove_dir_all(root).await.expect("clean temp root");
    }
}

use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use base64::Engine;
use chrono::{DateTime, Utc};
use reqwest::header::{
    ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};
use tt_domain::errors::DomainError;
use tt_ports::repositories::codex_auth_repository::{CodexAuthImportOutcome, CodexAuthRepository};
use uuid::Uuid;

pub const CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
pub const CODEX_REFRESH_URL: &str = "https://auth.openai.com/oauth/token";
pub const CODEX_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const DEFAULT_CLIENT_VERSION: &str = "0.145.0";
const REFRESH_AFTER_SECS: i64 = 8 * 24 * 60 * 60; // 8 days
const MAX_AUTH_FILE_BYTES: u64 = 1024 * 1024;
const IMPORT_STAGING_FILE_PREFIX: &str = "codex-auth-import-";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodexAuthTokens {
    #[serde(default)]
    pub access_token: Option<String>,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub id_token: Option<String>,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodexAuth {
    #[serde(default)]
    pub tokens: Option<CodexAuthTokens>,
    #[serde(default)]
    pub access_token: Option<String>,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub id_token: Option<String>,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub last_refresh: Option<String>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

impl CodexAuth {
    pub fn get_access_token(&self) -> Option<&str> {
        self.tokens
            .as_ref()
            .and_then(|t| t.access_token.as_deref())
            .or(self.access_token.as_deref())
    }

    pub fn get_refresh_token(&self) -> Option<&str> {
        self.tokens
            .as_ref()
            .and_then(|t| t.refresh_token.as_deref())
            .or(self.refresh_token.as_deref())
    }

    pub fn get_id_token(&self) -> Option<&str> {
        self.tokens
            .as_ref()
            .and_then(|t| t.id_token.as_deref())
            .or(self.id_token.as_deref())
    }

    pub fn get_account_id(&self) -> Option<&str> {
        self.tokens
            .as_ref()
            .and_then(|t| t.account_id.as_deref())
            .or(self.account_id.as_deref())
    }
}

#[derive(Debug, Clone)]
pub struct CodexResolvedAuth {
    pub access_token: String,
    pub account_id: Option<String>,
    pub is_fedramp: bool,
}

pub struct CodexAuthManager {
    refresh_lock: Mutex<()>,
    managed_paths: RwLock<Option<ManagedCodexAuthPaths>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManagedCodexAuthPaths {
    auth_file: PathBuf,
    import_staging_root: PathBuf,
}

static AUTH_MANAGER: LazyLock<Arc<CodexAuthManager>> =
    LazyLock::new(|| Arc::new(CodexAuthManager::default()));

pub(crate) fn codex_auth_manager() -> &'static CodexAuthManager {
    AUTH_MANAGER.as_ref()
}

pub async fn configure_codex_auth_repository(
    auth_file: PathBuf,
    import_staging_root: PathBuf,
) -> Arc<dyn CodexAuthRepository> {
    AUTH_MANAGER
        .configure_managed_storage(auth_file, import_staging_root)
        .await;
    AUTH_MANAGER.clone()
}

impl Default for CodexAuthManager {
    fn default() -> Self {
        Self {
            refresh_lock: Mutex::new(()),
            managed_paths: RwLock::new(None),
        }
    }
}

impl CodexAuthManager {
    async fn configure_managed_storage(&self, auth_file: PathBuf, import_staging_root: PathBuf) {
        *self.managed_paths.write().await = Some(ManagedCodexAuthPaths {
            auth_file,
            import_staging_root,
        });
    }

    #[cfg(test)]
    fn with_managed_storage(auth_file: PathBuf, import_staging_root: PathBuf) -> Self {
        Self {
            refresh_lock: Mutex::new(()),
            managed_paths: RwLock::new(Some(ManagedCodexAuthPaths {
                auth_file,
                import_staging_root,
            })),
        }
    }

    pub async fn load_auth(
        &self,
        client: &reqwest::Client,
    ) -> Result<CodexResolvedAuth, DomainError> {
        let _guard = self.refresh_lock.lock().await;
        let auth_path = self.resolve_auth_file_path().await?;

        let content = tokio::fs::read_to_string(&auth_path)
            .await
            .map_err(|error| {
                DomainError::InvalidData(format!(
                    "Could not read Codex authentication at {}: {error}. Import auth.json in TauriTavern Settings or run \"codex login\" on desktop.",
                    auth_path.display()
                ))
            })?;

        let mut auth: CodexAuth = serde_json::from_str(&content).map_err(|error| {
            DomainError::InvalidData(format!(
                "Invalid Codex login auth JSON at {}: {error}",
                auth_path.display()
            ))
        })?;

        if should_refresh(&auth) {
            refresh_auth(client, &mut auth, &auth_path, &content).await?;
        }

        let access_token = auth.get_access_token().map(str::to_string).ok_or_else(|| {
            DomainError::InvalidData(format!(
                "No ChatGPT OAuth token found in {}. Import a valid Codex auth.json in TauriTavern Settings or run \"codex login\" on desktop.",
                auth_path.display()
            ))
        })?;

        let id_claims = auth
            .get_id_token()
            .and_then(decode_jwt_claims)
            .unwrap_or(Value::Null);

        let access_claims = decode_jwt_claims(&access_token).unwrap_or(Value::Null);

        let account_id = auth
            .get_account_id()
            .map(str::to_string)
            .or_else(|| extract_claim_string(&id_claims, "chatgpt_account_id"))
            .or_else(|| extract_claim_string(&access_claims, "chatgpt_account_id"));

        let is_fedramp = extract_claim_bool(&id_claims, "chatgpt_account_is_fedramp")
            || extract_claim_bool(&access_claims, "chatgpt_account_is_fedramp");

        Ok(CodexResolvedAuth {
            access_token,
            account_id,
            is_fedramp,
        })
    }

    async fn resolve_auth_file_path(&self) -> Result<PathBuf, DomainError> {
        let managed = self.managed_paths.read().await.clone();

        if cfg!(any(target_os = "android", target_os = "ios")) {
            return managed.map(|paths| paths.auth_file).ok_or_else(|| {
                DomainError::InternalError(
                    "TauriTavern managed Codex authentication storage is not configured"
                        .to_string(),
                )
            });
        }

        if let Some(paths) = managed.as_ref() {
            match tokio::fs::try_exists(&paths.auth_file).await {
                Ok(true) => return Ok(paths.auth_file.clone()),
                Ok(false) => {}
                Err(error) => {
                    return Err(DomainError::InternalError(format!(
                        "Failed to inspect managed Codex authentication at {}: {error}",
                        paths.auth_file.display()
                    )));
                }
            }
        }

        resolve_cli_auth_file_path()
            .or_else(|error| managed.map(|paths| paths.auth_file).ok_or(error))
    }
}

#[async_trait::async_trait]
impl CodexAuthRepository for CodexAuthManager {
    async fn prepare_import_staging_path(&self) -> Result<PathBuf, DomainError> {
        let paths = self.require_managed_paths().await?;
        tokio::fs::create_dir_all(&paths.import_staging_root)
            .await
            .map_err(|error| {
                DomainError::InternalError(format!(
                    "Failed to create Codex auth import staging directory {}: {error}",
                    paths.import_staging_root.display()
                ))
            })?;

        Ok(paths.import_staging_root.join(format!(
            "{IMPORT_STAGING_FILE_PREFIX}{}.json",
            Uuid::new_v4()
        )))
    }

    async fn import_auth_file(
        &self,
        source_path: &Path,
        expected_existing: Option<&str>,
    ) -> Result<CodexAuthImportOutcome, DomainError> {
        let paths = self.require_managed_paths().await?;
        if cfg!(any(target_os = "android", target_os = "ios")) {
            require_import_staging_path(source_path, &paths.import_staging_root)?;
        }

        let incoming = read_auth_file_bounded(source_path, "selected Codex auth file").await?;
        validate_imported_auth(&incoming, source_path)?;

        let _guard = self.refresh_lock.lock().await;
        let existing = match read_optional_auth_file(&paths.auth_file).await? {
            Some(existing) if existing == incoming => {
                return Ok(CodexAuthImportOutcome::AlreadyCurrent);
            }
            existing => existing,
        };

        match (existing.as_deref(), expected_existing) {
            (Some(current), Some(expected)) => {
                let actual = auth_fingerprint(current);
                if actual != expected {
                    return Err(DomainError::Conflict(
                        "Imported Codex authentication changed after confirmation; review it again before replacing"
                            .to_string(),
                    ));
                }
                persist_auth_file(&paths.auth_file, &incoming, Some(current)).await?;
                Ok(CodexAuthImportOutcome::Imported {
                    replaced_existing: true,
                })
            }
            (None, Some(_)) => Err(DomainError::Conflict(
                "Imported Codex authentication changed after confirmation; review it again before replacing"
                    .to_string(),
            )),
            (Some(current), None) if contains_usable_authentication(current) => {
                Ok(CodexAuthImportOutcome::RequiresConfirmation {
                    confirmation: auth_fingerprint(current),
                })
            }
            (Some(current), None) => {
                persist_auth_file(&paths.auth_file, &incoming, Some(current)).await?;
                Ok(CodexAuthImportOutcome::Imported {
                    replaced_existing: true,
                })
            }
            (None, None) => {
                persist_auth_file(&paths.auth_file, &incoming, None).await?;
                Ok(CodexAuthImportOutcome::Imported {
                    replaced_existing: false,
                })
            }
        }
    }

    async fn discard_import_staging_path(&self, path: &Path) -> Result<(), DomainError> {
        let paths = self.require_managed_paths().await?;
        require_import_staging_path(path, &paths.import_staging_root)?;

        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(DomainError::InternalError(format!(
                "Failed to discard staged Codex auth import {}: {error}",
                path.display()
            ))),
        }
    }
}

impl CodexAuthManager {
    async fn require_managed_paths(&self) -> Result<ManagedCodexAuthPaths, DomainError> {
        self.managed_paths.read().await.clone().ok_or_else(|| {
            DomainError::InternalError(
                "TauriTavern managed Codex authentication storage is not configured".to_string(),
            )
        })
    }
}

fn require_import_staging_path(path: &Path, staging_root: &Path) -> Result<(), DomainError> {
    let has_expected_parent = path.parent() == Some(staging_root);
    let has_expected_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name.starts_with(IMPORT_STAGING_FILE_PREFIX) && name.ends_with(".json")
        });
    if !has_expected_parent || !has_expected_name {
        return Err(DomainError::InvalidData(format!(
            "Codex auth import path is outside the managed staging directory: {}",
            path.display()
        )));
    }

    Ok(())
}

async fn read_optional_auth_file(path: &Path) -> Result<Option<Vec<u8>>, DomainError> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) if !metadata.is_file() => Err(DomainError::InvalidData(format!(
            "Codex auth path is not a file: {}",
            path.display()
        ))),
        Ok(metadata) if metadata.len() > MAX_AUTH_FILE_BYTES => {
            Err(DomainError::InvalidData(format!(
                "Codex auth file is too large (maximum {MAX_AUTH_FILE_BYTES} bytes): {}",
                path.display()
            )))
        }
        Ok(_) => tokio::fs::read(path).await.map(Some).map_err(|error| {
            DomainError::InternalError(format!(
                "Failed to read Codex auth file {}: {error}",
                path.display()
            ))
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(DomainError::InternalError(format!(
            "Failed to inspect Codex auth file {}: {error}",
            path.display()
        ))),
    }
}

async fn read_auth_file_bounded(path: &Path, label: &str) -> Result<Vec<u8>, DomainError> {
    let Some(bytes) = read_optional_auth_file(path).await? else {
        return Err(DomainError::NotFound(format!(
            "{label} does not exist: {}",
            path.display()
        )));
    };
    if bytes.len() as u64 > MAX_AUTH_FILE_BYTES {
        return Err(DomainError::InvalidData(format!(
            "{label} is too large (maximum {MAX_AUTH_FILE_BYTES} bytes): {}",
            path.display()
        )));
    }
    Ok(bytes)
}

fn validate_imported_auth(bytes: &[u8], path: &Path) -> Result<CodexAuth, DomainError> {
    let auth: CodexAuth = serde_json::from_slice(bytes).map_err(|error| {
        DomainError::InvalidData(format!(
            "Selected Codex auth file is not valid JSON at {}: {error}",
            path.display()
        ))
    })?;

    let has_access_token = auth
        .get_access_token()
        .is_some_and(|value| !value.trim().is_empty());
    let has_refresh_token = auth
        .get_refresh_token()
        .is_some_and(|value| !value.trim().is_empty());
    if !has_access_token || !has_refresh_token {
        return Err(DomainError::InvalidData(format!(
            "Selected Codex auth file at {} must contain non-empty ChatGPT OAuth access_token and refresh_token values",
            path.display()
        )));
    }

    Ok(auth)
}

fn contains_usable_authentication(bytes: &[u8]) -> bool {
    let Ok(auth) = serde_json::from_slice::<CodexAuth>(bytes) else {
        return false;
    };

    auth.get_access_token()
        .is_some_and(|value| !value.trim().is_empty())
        || auth
            .get_refresh_token()
            .is_some_and(|value| !value.trim().is_empty())
}

fn auth_fingerprint(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(bytes))
}

pub fn client_version() -> String {
    if let Ok(version) = std::env::var("CODEX_CLIENT_VERSION") {
        let trimmed = version.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    DEFAULT_CLIENT_VERSION.to_string()
}

fn resolve_cli_auth_file_path() -> Result<PathBuf, DomainError> {
    if let Ok(codex_home) = std::env::var("CODEX_HOME") {
        let trimmed = codex_home.trim();
        if !trimmed.is_empty() {
            return Ok(PathBuf::from(trimmed).join("auth.json"));
        }
    }

    if let Ok(user_profile) = std::env::var("USERPROFILE") {
        let trimmed = user_profile.trim();
        if !trimmed.is_empty() {
            return Ok(PathBuf::from(trimmed).join(".codex").join("auth.json"));
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        let trimmed = home.trim();
        if !trimmed.is_empty() {
            return Ok(PathBuf::from(trimmed).join(".codex").join("auth.json"));
        }
    }

    Err(DomainError::InvalidData(
        "Could not determine home directory for Codex login. Set CODEX_HOME environment variable."
            .to_string(),
    ))
}

pub fn decode_jwt_claims(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    if payload.is_empty() {
        return None;
    }

    let normalized = payload.replace('-', "+").replace('_', "/");
    let padded = match normalized.len() % 4 {
        2 => format!("{normalized}=="),
        3 => format!("{normalized}="),
        _ => normalized,
    };

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(padded.as_bytes())
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn extract_claim_string(claims: &Value, field: &str) -> Option<String> {
    if let Some(auth_obj) = claims.get("https://api.openai.com/auth")
        && let Some(val) = auth_obj.get(field).and_then(Value::as_str)
    {
        return Some(val.to_string());
    }

    claims
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn extract_claim_bool(claims: &Value, field: &str) -> bool {
    if let Some(auth_obj) = claims.get("https://api.openai.com/auth")
        && let Some(val) = auth_obj.get(field).and_then(Value::as_bool)
    {
        return val;
    }

    claims.get(field).and_then(Value::as_bool).unwrap_or(false)
}

fn should_refresh(auth: &CodexAuth) -> bool {
    let Some(access_token) = auth.get_access_token() else {
        return true;
    };

    if let Some(claims) = decode_jwt_claims(access_token)
        && let Some(exp) = claims.get("exp").and_then(Value::as_i64)
    {
        let now_secs = Utc::now().timestamp();
        if exp <= now_secs + 60 {
            return true;
        }
    }

    if let Some(last_refresh_str) = auth.last_refresh.as_deref()
        && let Ok(last_refresh) = DateTime::parse_from_rfc3339(last_refresh_str)
    {
        let now = Utc::now();
        if (now - last_refresh.with_timezone(&Utc)).num_seconds() > REFRESH_AFTER_SECS {
            return true;
        }
    }

    false
}

#[derive(Serialize)]
struct RefreshTokenRequest<'a> {
    client_id: &'a str,
    grant_type: &'a str,
    refresh_token: &'a str,
}

#[derive(Deserialize)]
struct RefreshTokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    id_token: Option<String>,
}

async fn refresh_auth(
    client: &reqwest::Client,
    auth: &mut CodexAuth,
    auth_file: &Path,
    expected_content: &str,
) -> Result<(), DomainError> {
    let refresh_token = auth.get_refresh_token().ok_or_else(|| {
        DomainError::InvalidData(
            "Codex refresh token is missing. Run \"codex login\" again.".to_string(),
        )
    })?;

    let request_body = RefreshTokenRequest {
        client_id: CODEX_CLIENT_ID,
        grant_type: "refresh_token",
        refresh_token,
    };

    let response = client
        .post(CODEX_REFRESH_URL)
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "application/json")
        .json(&request_body)
        .send()
        .await
        .map_err(|error| {
            DomainError::InternalError(format!("Codex token refresh request failed: {error}"))
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let error_body = response.text().await.unwrap_or_default();
        let snippet = error_body.chars().take(500).collect::<String>();
        return Err(DomainError::InternalError(format!(
            "Codex token refresh failed: {status} {snippet}"
        )));
    }

    let refreshed: RefreshTokenResponse = response.json().await.map_err(|error| {
        DomainError::InternalError(format!(
            "Failed to parse Codex token refresh response JSON: {error}"
        ))
    })?;

    let mut tokens = auth.tokens.clone().unwrap_or_default();
    tokens.access_token = Some(refreshed.access_token.clone());
    if let Some(new_refresh) = refreshed.refresh_token.clone() {
        tokens.refresh_token = Some(new_refresh.clone());
        auth.refresh_token = Some(new_refresh);
    }
    if let Some(new_id) = refreshed.id_token.clone() {
        tokens.id_token = Some(new_id.clone());
        auth.id_token = Some(new_id);
    }
    auth.access_token = Some(refreshed.access_token);
    auth.tokens = Some(tokens);
    auth.last_refresh = Some(Utc::now().to_rfc3339());

    let serialized = serde_json::to_string_pretty(auth).map_err(|error| {
        DomainError::InternalError(format!("Failed to serialize Codex auth JSON: {error}"))
    })?;

    persist_auth_file(
        auth_file,
        format!("{serialized}\n").as_bytes(),
        Some(expected_content.as_bytes()),
    )
    .await
}

async fn persist_auth_file(
    auth_file: &Path,
    serialized: &[u8],
    expected_content: Option<&[u8]>,
) -> Result<(), DomainError> {
    let parent = auth_file.parent().ok_or_else(|| {
        DomainError::InvalidData(format!(
            "Codex auth path has no parent directory: {}",
            auth_file.display()
        ))
    })?;
    tokio::fs::create_dir_all(parent).await.map_err(|error| {
        DomainError::InternalError(format!(
            "Failed to create Codex auth directory {}: {error}",
            parent.display()
        ))
    })?;

    let temp_path = crate::file_replace::unique_temp_path(auth_file);
    let write_result = async {
        let mut temp = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .await
            .map_err(|error| {
                DomainError::InternalError(format!(
                    "Failed to create temporary Codex auth file {}: {error}",
                    temp_path.display()
                ))
            })?;
        temp.write_all(serialized).await.map_err(|error| {
            DomainError::InternalError(format!(
                "Failed to write temporary Codex auth file {}: {error}",
                temp_path.display()
            ))
        })?;
        temp.sync_all().await.map_err(|error| {
            DomainError::InternalError(format!(
                "Failed to sync temporary Codex auth file {}: {error}",
                temp_path.display()
            ))
        })?;

        #[cfg(unix)]
        tokio::fs::set_permissions(&temp_path, {
            use std::os::unix::fs::PermissionsExt;
            std::fs::Permissions::from_mode(0o600)
        })
        .await
        .map_err(|error| {
            DomainError::InternalError(format!(
                "Failed to secure temporary Codex auth file {}: {error}",
                temp_path.display()
            ))
        })?;

        match expected_content {
            Some(expected) => {
                let current = tokio::fs::read(auth_file).await.map_err(|error| {
                    DomainError::InternalError(format!(
                        "Failed to re-read Codex auth file {} before commit: {error}",
                        auth_file.display()
                    ))
                })?;
                if current != expected {
                    return Err(DomainError::Conflict(format!(
                        "Codex auth file changed before commit: {}",
                        auth_file.display()
                    )));
                }
            }
            None => match tokio::fs::metadata(auth_file).await {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Ok(_) => {
                    return Err(DomainError::Conflict(format!(
                        "Codex auth file was created before commit: {}",
                        auth_file.display()
                    )));
                }
                Err(error) => {
                    return Err(DomainError::InternalError(format!(
                        "Failed to inspect Codex auth file {} before commit: {error}",
                        auth_file.display()
                    )));
                }
            },
        }

        tokio::fs::rename(&temp_path, auth_file)
            .await
            .map_err(|error| {
                DomainError::InternalError(format!(
                    "Failed to atomically replace Codex auth file {}: {error}",
                    auth_file.display()
                ))
            })
    }
    .await;

    if write_result.is_err() {
        let _ = tokio::fs::remove_file(&temp_path).await;
    }
    write_result
}

pub fn build_codex_headers(
    auth: &CodexResolvedAuth,
    client_version_override: Option<&str>,
    include_content_type: bool,
) -> Result<HeaderMap, DomainError> {
    let mut headers = HeaderMap::new();
    let version = client_version_override.unwrap_or(DEFAULT_CLIENT_VERSION);

    let auth_value = format!("Bearer {}", auth.access_token);
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&auth_value).map_err(|_| {
            DomainError::InvalidData("Invalid authorization header value".to_string())
        })?,
    );

    headers.insert(
        HeaderName::from_static("version"),
        HeaderValue::from_str(version)
            .map_err(|_| DomainError::InvalidData("Invalid version header value".to_string()))?,
    );

    headers.insert(
        HeaderName::from_static("originator"),
        HeaderValue::from_static("SillyTavern"),
    );

    let user_agent = format!("SillyTavern-Codex-RP/{version}");
    headers.insert(
        USER_AGENT,
        HeaderValue::from_str(&user_agent)
            .map_err(|_| DomainError::InvalidData("Invalid user-agent header value".to_string()))?,
    );

    if include_content_type {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }

    if let Some(account_id) = &auth.account_id {
        headers.insert(
            HeaderName::from_static("chatgpt-account-id"),
            HeaderValue::from_str(account_id).map_err(|_| {
                DomainError::InvalidData("Invalid ChatGPT-Account-ID header value".to_string())
            })?,
        );
    }

    if auth.is_fedramp {
        headers.insert(
            HeaderName::from_static("x-openai-fedramp"),
            HeaderValue::from_static("true"),
        );
    }

    Ok(headers)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn valid_auth_json(access_token: &str, refresh_token: &str) -> Vec<u8> {
        serde_json::to_vec_pretty(&json!({
            "tokens": {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "future_token_field": "preserved"
            },
            "future_top_level": { "enabled": true }
        }))
        .expect("serialize test auth")
    }

    #[test]
    fn test_jwt_claims_decoding() {
        // JWT header: {"alg":"none"}, payload: {"sub":"user_123","exp":1893456000,"https://api.openai.com/auth":{"chatgpt_account_id":"acc_456","chatgpt_account_is_fedramp":true}}
        let payload = r#"{"sub":"user_123","exp":1893456000,"https://api.openai.com/auth":{"chatgpt_account_id":"acc_456","chatgpt_account_is_fedramp":true}}"#;
        let b64_payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
        let jwt = format!("eyJhbGciOiJub25lIn0.{b64_payload}.sig");

        let claims = decode_jwt_claims(&jwt).expect("claims should be decoded");
        assert_eq!(claims.get("sub").and_then(Value::as_str), Some("user_123"));
        assert_eq!(claims.get("exp").and_then(Value::as_i64), Some(1893456000));
        assert_eq!(
            extract_claim_string(&claims, "chatgpt_account_id"),
            Some("acc_456".to_string())
        );
        assert!(extract_claim_bool(&claims, "chatgpt_account_is_fedramp"));
    }

    #[test]
    fn test_build_codex_headers() {
        let auth = CodexResolvedAuth {
            access_token: "test-token-123".to_string(),
            account_id: Some("acc_xyz".to_string()),
            is_fedramp: true,
        };

        let headers = build_codex_headers(&auth, Some("0.145.0"), true).expect("headers build");
        assert_eq!(
            headers.get(AUTHORIZATION).unwrap().to_str().unwrap(),
            "Bearer test-token-123"
        );
        assert_eq!(headers.get("version").unwrap().to_str().unwrap(), "0.145.0");
        assert_eq!(
            headers.get("originator").unwrap().to_str().unwrap(),
            "SillyTavern"
        );
        assert_eq!(
            headers.get(USER_AGENT).unwrap().to_str().unwrap(),
            "SillyTavern-Codex-RP/0.145.0"
        );
        assert_eq!(
            headers.get("chatgpt-account-id").unwrap().to_str().unwrap(),
            "acc_xyz"
        );
        assert_eq!(
            headers.get("x-openai-fedramp").unwrap().to_str().unwrap(),
            "true"
        );
        assert_eq!(
            headers.get(CONTENT_TYPE).unwrap().to_str().unwrap(),
            "application/json"
        );
    }

    #[test]
    fn auth_round_trip_preserves_unknown_cli_fields() {
        let source = json!({
            "tokens": {
                "access_token": "access",
                "refresh_token": "refresh",
                "future_token_field": "preserved"
            },
            "last_refresh": "2026-08-23T00:00:00Z",
            "OPENAI_API_KEY": null,
            "future_top_level": { "enabled": true }
        });

        let auth: CodexAuth = serde_json::from_value(source.clone()).expect("parse auth");
        let encoded = serde_json::to_value(auth).expect("encode auth");

        assert_eq!(encoded["tokens"]["future_token_field"], "preserved");
        assert_eq!(encoded["future_top_level"], source["future_top_level"]);
        assert!(encoded.get("OPENAI_API_KEY").is_some());
    }

    #[tokio::test]
    async fn auth_persistence_rejects_concurrent_cli_changes_without_overwriting() {
        let root = std::env::temp_dir().join(format!(
            "tauritavern-codex-auth-test-{}",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::create_dir_all(&root).await.expect("temp root");
        let auth_file = root.join("auth.json");
        tokio::fs::write(&auth_file, "cli-new\n")
            .await
            .expect("write current auth");

        let result = persist_auth_file(&auth_file, b"app-refresh\n", Some(b"stale-old\n")).await;

        assert!(matches!(result, Err(DomainError::Conflict(_))));
        assert_eq!(
            tokio::fs::read_to_string(&auth_file)
                .await
                .expect("read retained auth"),
            "cli-new\n"
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn auth_persistence_replaces_the_complete_file() {
        let root = std::env::temp_dir().join(format!(
            "tauritavern-codex-auth-replace-test-{}",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::create_dir_all(&root).await.expect("temp root");
        let auth_file = root.join("auth.json");
        tokio::fs::write(&auth_file, "old\n")
            .await
            .expect("write old auth");

        persist_auth_file(&auth_file, b"new-complete-json\n", Some(b"old\n"))
            .await
            .expect("atomic auth replacement");

        assert_eq!(
            tokio::fs::read_to_string(&auth_file)
                .await
                .expect("read replaced auth"),
            "new-complete-json\n"
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn imported_auth_requires_matching_confirmation_before_replacing_valid_credentials() {
        let root = std::env::temp_dir().join(format!(
            "tauritavern-codex-auth-import-test-{}",
            uuid::Uuid::new_v4()
        ));
        let auth_file = root.join("security").join("codex").join("auth.json");
        let staging_root = root.join("imports");
        let first_source = root.join("first-auth.json");
        let second_source = root.join("second-auth.json");
        tokio::fs::create_dir_all(&root).await.expect("temp root");
        let first = valid_auth_json("first-access", "first-refresh");
        let second = valid_auth_json("second-access", "second-refresh");
        tokio::fs::write(&first_source, &first)
            .await
            .expect("write first auth");
        tokio::fs::write(&second_source, &second)
            .await
            .expect("write second auth");

        let manager = CodexAuthManager::with_managed_storage(auth_file.clone(), staging_root);
        assert_eq!(
            manager
                .import_auth_file(&first_source, None)
                .await
                .expect("initial import"),
            CodexAuthImportOutcome::Imported {
                replaced_existing: false
            }
        );
        assert_eq!(
            manager
                .import_auth_file(&first_source, None)
                .await
                .expect("same import"),
            CodexAuthImportOutcome::AlreadyCurrent
        );

        let confirmation = match manager
            .import_auth_file(&second_source, None)
            .await
            .expect("replacement preview")
        {
            CodexAuthImportOutcome::RequiresConfirmation { confirmation } => confirmation,
            other => panic!("unexpected replacement preview: {other:?}"),
        };
        assert_eq!(
            tokio::fs::read(&auth_file)
                .await
                .expect("retained first auth"),
            first
        );

        let stale = manager
            .import_auth_file(&second_source, Some("stale-confirmation"))
            .await;
        assert!(matches!(stale, Err(DomainError::Conflict(_))));
        assert_eq!(
            manager
                .import_auth_file(&second_source, Some(&confirmation))
                .await
                .expect("confirmed replacement"),
            CodexAuthImportOutcome::Imported {
                replaced_existing: true
            }
        );
        assert_eq!(
            tokio::fs::read(&auth_file)
                .await
                .expect("read replaced auth"),
            second
        );

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn imported_auth_rejects_json_without_complete_oauth_tokens() {
        let root = std::env::temp_dir().join(format!(
            "tauritavern-codex-auth-invalid-import-test-{}",
            uuid::Uuid::new_v4()
        ));
        let auth_file = root.join("security").join("codex").join("auth.json");
        let source = root.join("auth.json");
        tokio::fs::create_dir_all(&root).await.expect("temp root");
        tokio::fs::write(&source, br#"{"tokens":{"access_token":"access"}}"#)
            .await
            .expect("write incomplete auth");
        let manager =
            CodexAuthManager::with_managed_storage(auth_file.clone(), root.join("imports"));

        let result = manager.import_auth_file(&source, None).await;

        assert!(
            matches!(result, Err(DomainError::InvalidData(message)) if message.contains("refresh_token"))
        );
        assert!(!auth_file.exists(), "invalid auth must not be imported");
        let _ = tokio::fs::remove_dir_all(root).await;
    }
}

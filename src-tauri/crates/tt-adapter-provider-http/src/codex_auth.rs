use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use base64::Engine;
use chrono::{DateTime, Utc};
use reqwest::header::{
    ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tt_domain::errors::DomainError;

pub const CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
pub const CODEX_REFRESH_URL: &str = "https://auth.openai.com/oauth/token";
pub const CODEX_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const DEFAULT_CLIENT_VERSION: &str = "0.145.0";
const REFRESH_AFTER_SECS: i64 = 8 * 24 * 60 * 60; // 8 days

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
}

static AUTH_MANAGER: LazyLock<CodexAuthManager> = LazyLock::new(CodexAuthManager::default);

pub(crate) fn codex_auth_manager() -> &'static CodexAuthManager {
    &AUTH_MANAGER
}

impl Default for CodexAuthManager {
    fn default() -> Self {
        Self {
            refresh_lock: Mutex::new(()),
        }
    }
}

impl CodexAuthManager {
    pub async fn load_auth(
        &self,
        client: &reqwest::Client,
    ) -> Result<CodexResolvedAuth, DomainError> {
        let auth_path = get_auth_file_path()?;
        let _guard = self.refresh_lock.lock().await;

        let content = tokio::fs::read_to_string(&auth_path)
            .await
            .map_err(|error| {
                DomainError::InvalidData(format!(
                    "Could not read Codex login at {}: {error}. Run \"codex login\" first.",
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
                "No ChatGPT OAuth token found in {}. Run \"codex login\".",
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

pub fn get_auth_file_path() -> Result<PathBuf, DomainError> {
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
        expected_content,
    )
    .await
}

async fn persist_auth_file(
    auth_file: &Path,
    serialized: &[u8],
    expected_content: &str,
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

        let current = tokio::fs::read_to_string(auth_file)
            .await
            .map_err(|error| {
                DomainError::InternalError(format!(
                    "Failed to re-read Codex auth file {} before refresh commit: {error}",
                    auth_file.display()
                ))
            })?;
        if current != expected_content {
            return Err(DomainError::Conflict(format!(
                "Codex auth file changed while tokens were refreshing: {}",
                auth_file.display()
            )));
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

        let result = persist_auth_file(&auth_file, b"app-refresh\n", "stale-old\n").await;

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

        persist_auth_file(&auth_file, b"new-complete-json\n", "old\n")
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
}

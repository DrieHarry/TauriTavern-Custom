use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{RwLock, watch};

use crate::dto::stable_diffusion_dto::{SdRouteResponseDto, SdRouteResponseKindDto};
use crate::errors::ApplicationError;
use tt_domain::models::endpoint_url::parse_user_http_endpoint;
use tt_domain::models::secret::SecretKeys;
use tt_ports::repositories::secret_repository::SecretRepository;
use tt_ports::repositories::stable_diffusion_repository::{
    SdRouteCredentials, SdRouteRequest, SdRouteResponseKind, StableDiffusionRepository,
};

pub struct StableDiffusionService {
    repository: Arc<dyn StableDiffusionRepository>,
    secret_repository: Arc<dyn SecretRepository>,
    active_requests: CancellationRegistry,
}

impl StableDiffusionService {
    pub fn new(
        repository: Arc<dyn StableDiffusionRepository>,
        secret_repository: Arc<dyn SecretRepository>,
    ) -> Self {
        Self {
            repository,
            secret_repository,
            active_requests: CancellationRegistry::default(),
        }
    }

    pub async fn handle_request(
        &self,
        request_id: &str,
        path: String,
        body: Value,
    ) -> Result<SdRouteResponseDto, ApplicationError> {
        let path = path.trim().trim_matches('/').to_ascii_lowercase();
        let credentials = if matches!(path.as_str(), "nanogpt/models" | "nanogpt/generate") {
            let Some(api_key) = self
                .secret_repository
                .read_secret(SecretKeys::NANOGPT, None)
                .await?
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
            else {
                return Ok(text_response(400, "NanoGPT API key is required"));
            };
            SdRouteCredentials::NanoGpt { api_key }
        } else if matches!(path.as_str(), "openrouter/models" | "openrouter/generate") {
            let Some(api_key) = self
                .secret_repository
                .read_secret(SecretKeys::OPENROUTER, None)
                .await?
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
            else {
                return Ok(text_response(400, "OpenRouter API key is required"));
            };
            SdRouteCredentials::OpenRouter { api_key }
        } else if matches!(path.as_str(), "workersai/models" | "workersai/generate") {
            let Some(api_key) = self
                .secret_repository
                .read_secret(SecretKeys::WORKERS_AI, None)
                .await?
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
            else {
                return Ok(text_response(
                    400,
                    "Cloudflare Workers AI API key is required",
                ));
            };
            SdRouteCredentials::WorkersAi { api_key }
        } else if matches!(
            path.as_str(),
            "custom-openai/models" | "custom-openai/generate"
        ) {
            let api_key = self
                .secret_repository
                .read_secret(SecretKeys::CUSTOM_OPENAI_SD, None)
                .await?
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            SdRouteCredentials::CustomOpenAi { api_key }
        } else {
            SdRouteCredentials::None
        };

        let cancel = self.active_requests.register(request_id).await;
        let result = self
            .repository
            .handle(
                SdRouteRequest {
                    path,
                    body,
                    credentials,
                },
                cancel,
            )
            .await;
        self.active_requests.complete(request_id).await;

        let response = result.map_err(ApplicationError::from)?;

        Ok(SdRouteResponseDto {
            status: response.status,
            kind: match response.kind {
                SdRouteResponseKind::Json => SdRouteResponseKindDto::Json,
                SdRouteResponseKind::Text => SdRouteResponseKindDto::Text,
                SdRouteResponseKind::Empty => SdRouteResponseKindDto::Empty,
            },
            body: response.body,
        })
    }

    pub fn resolve_user_endpoint(
        &self,
        path: &str,
        body: &Value,
    ) -> Result<Option<String>, ApplicationError> {
        resolve_custom_openai_user_endpoint(path, body).map_err(ApplicationError::from)
    }

    pub async fn cancel_request(&self, request_id: &str) -> bool {
        self.active_requests.cancel(request_id).await
    }
}

fn resolve_custom_openai_user_endpoint(
    path: &str,
    body: &Value,
) -> Result<Option<String>, tt_domain::errors::DomainError> {
    let path = path.trim().trim_matches('/').to_ascii_lowercase();
    if !matches!(
        path.as_str(),
        "custom-openai/models" | "custom-openai/generate"
    ) {
        return Ok(None);
    }

    let Some(url) = body.get("url") else {
        return Ok(None);
    };
    let url = url.as_str().ok_or_else(|| {
        tt_domain::errors::DomainError::InvalidData(
            "Custom OpenAI-compatible URL must be a string".to_string(),
        )
    })?;
    if url.trim().is_empty() || tt_domain::models::endpoint_url::is_codex_endpoint(url) {
        return Ok(None);
    }

    Ok(Some(parse_user_http_endpoint(url)?.to_string()))
}

fn text_response(status: u16, message: impl Into<String>) -> SdRouteResponseDto {
    SdRouteResponseDto {
        status,
        kind: SdRouteResponseKindDto::Text,
        body: Value::String(message.into()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::resolve_custom_openai_user_endpoint;

    #[test]
    fn custom_image_routes_require_a_canonical_user_endpoint_grant() {
        assert_eq!(
            resolve_custom_openai_user_endpoint(
                "custom-openai/generate",
                &json!({ "url": " HTTPS://EXAMPLE.COM:443/v1/ " }),
            )
            .expect("valid endpoint"),
            Some("https://example.com/v1".to_string())
        );
        assert_eq!(
            resolve_custom_openai_user_endpoint(
                "custom-openai/models",
                &json!({ "url": "http://codex.local/v1" }),
            )
            .expect("Codex is built in"),
            None
        );
        assert!(
            resolve_custom_openai_user_endpoint(
                "custom-openai/generate",
                &json!({ "url": "file:///etc/passwd" }),
            )
            .is_err()
        );
    }
}

#[derive(Default)]
struct CancellationRegistry {
    active: RwLock<HashMap<String, watch::Sender<bool>>>,
}

impl CancellationRegistry {
    async fn register(&self, request_id: &str) -> watch::Receiver<bool> {
        let (sender, receiver) = watch::channel(false);
        let mut active = self.active.write().await;

        if let Some(previous_sender) = active.insert(request_id.to_string(), sender) {
            let _ = previous_sender.send(true);
        }

        receiver
    }

    async fn cancel(&self, request_id: &str) -> bool {
        let mut active = self.active.write().await;
        let Some(sender) = active.remove(request_id) else {
            return false;
        };

        let _ = sender.send(true);
        true
    }

    async fn complete(&self, request_id: &str) {
        let mut active = self.active.write().await;
        active.remove(request_id);
    }
}

use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use reqwest::header::ACCEPT;
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tt_domain::errors::DomainError;
use tt_ports::repositories::chat_completion_repository::{
    ChatCompletionApiConfig, ChatCompletionCancelReceiver,
    ChatCompletionRepositoryGenerateResponse, ChatCompletionStreamSender,
};

use super::HttpChatCompletionRepository;
use super::openai_responses::{self, ResponsesStreamOptions};
use super::response_body::read_upstream_json_body;
use crate::codex_auth::{CODEX_BASE_URL, build_codex_headers, client_version, codex_auth_manager};

const MODEL_CACHE_TTL: Duration = Duration::from_secs(5 * 60);

struct ModelCache {
    expires_at: Instant,
    models: Value,
}

static MODEL_CACHE: LazyLock<RwLock<Option<ModelCache>>> = LazyLock::new(|| RwLock::new(None));

pub(super) async fn list_models(
    repository: &HttpChatCompletionRepository,
    config: &ChatCompletionApiConfig,
    provider_name: &str,
) -> Result<Value, DomainError> {
    {
        let cache_read = MODEL_CACHE.read().await;
        if let Some(cache) = cache_read.as_ref()
            && Instant::now() < cache.expires_at
        {
            return Ok(cache.models.clone());
        }
    }

    let client = repository.metadata_client(config)?;
    let auth = codex_auth_manager().load_auth(&client).await?;
    let version = client_version();
    let headers = build_codex_headers(&auth, Some(&version), false)?;

    let url = format!("{CODEX_BASE_URL}/models?client_version={version}");
    let response = client
        .get(&url)
        .headers(headers)
        .header(ACCEPT, "application/json")
        .send()
        .await
        .map_err(|error| {
            HttpChatCompletionRepository::map_transport_error("Codex model lookup failed", error)
        })?;

    if !response.status().is_success() {
        return Err(HttpChatCompletionRepository::map_error_response(
            provider_name,
            response,
            "Failed to list Codex models",
        )
        .await);
    }

    let raw_json: Value = response.json().await.map_err(|error| {
        DomainError::InternalError(format!("Failed to parse Codex models JSON: {error}"))
    })?;
    let result = json!({
        "object": "list",
        "data": parse_codex_models_json(&raw_json),
    });

    let mut cache_write = MODEL_CACHE.write().await;
    *cache_write = Some(ModelCache {
        expires_at: Instant::now() + MODEL_CACHE_TTL,
        models: result.clone(),
    });

    Ok(result)
}

pub(crate) fn parse_codex_models_json(raw_json: &Value) -> Vec<Value> {
    let mut model_items = Vec::new();
    let mut seen_ids = HashSet::new();

    let raw_models = if let Some(items) = raw_json.as_array() {
        items.as_slice()
    } else if let Some(items) = raw_json.get("models").and_then(Value::as_array) {
        items.as_slice()
    } else if let Some(items) = raw_json.get("data").and_then(Value::as_array) {
        items.as_slice()
    } else {
        &[]
    };

    for item in raw_models {
        let model_id = item
            .get("slug")
            .or_else(|| item.get("id"))
            .or_else(|| item.get("model"))
            .and_then(Value::as_str);
        let Some(id) = model_id else {
            continue;
        };

        if seen_ids.contains(id)
            || item
                .get("visibility")
                .and_then(Value::as_str)
                .is_some_and(|visibility| visibility == "hidden" || visibility == "hide")
            || item.get("enabled").and_then(Value::as_bool) == Some(false)
        {
            continue;
        }

        seen_ids.insert(id.to_string());
        let display_name = item
            .get("display_name")
            .or_else(|| item.get("name"))
            .and_then(Value::as_str)
            .unwrap_or(id);
        model_items.push(json!({
            "id": id,
            "name": display_name,
            "object": "model",
        }));
    }

    model_items.sort_by(|a, b| {
        let a_id = a.get("id").and_then(Value::as_str).unwrap_or("");
        let b_id = b.get("id").and_then(Value::as_str).unwrap_or("");
        a_id.cmp(b_id)
    });
    model_items
}

pub(super) async fn generate(
    repository: &HttpChatCompletionRepository,
    config: &ChatCompletionApiConfig,
    _endpoint_path: &str,
    payload: &Value,
    provider_name: &str,
) -> Result<ChatCompletionRepositoryGenerateResponse, DomainError> {
    let client = repository.client(config)?;
    let auth = codex_auth_manager().load_auth(&client).await?;
    let version = client_version();
    let headers = build_codex_headers(&auth, Some(&version), true)?;
    let request_body = codex_request_payload(payload, false)?;

    let response = client
        .post(format!("{CODEX_BASE_URL}/responses"))
        .headers(headers)
        .header(ACCEPT, "application/json")
        .json(&request_body)
        .send()
        .await
        .map_err(|error| {
            HttpChatCompletionRepository::map_transport_error(
                "Codex chat completion request failed",
                error,
            )
        })?;

    if !response.status().is_success() {
        return Err(HttpChatCompletionRepository::map_error_response(
            provider_name,
            response,
            "Codex chat completion request failed",
        )
        .await);
    }

    let response = read_upstream_json_body(provider_name, "generate", response).await?;
    normalize_codex_response(response, should_emit_reasoning(payload))
}

pub(super) async fn generate_stream(
    repository: &HttpChatCompletionRepository,
    config: &ChatCompletionApiConfig,
    _endpoint_path: &str,
    payload: &Value,
    provider_name: &str,
    sender: ChatCompletionStreamSender,
    mut cancel: ChatCompletionCancelReceiver,
) -> Result<(), DomainError> {
    let client = repository.stream_client(config)?;
    let auth = codex_auth_manager().load_auth(&client).await?;
    let version = client_version();
    let headers = build_codex_headers(&auth, Some(&version), true)?;
    let request_body = codex_request_payload(payload, true)?;
    let model = request_body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    let send = client
        .post(format!("{CODEX_BASE_URL}/responses"))
        .headers(headers)
        .header(ACCEPT, "text/event-stream")
        .json(&request_body)
        .send();
    let response = tokio::select! {
        response = send => response.map_err(|error| {
            HttpChatCompletionRepository::map_transport_error(
                "Codex chat completion request failed",
                error,
            )
        })?,
        changed = cancel.changed() => {
            let _ = changed;
            return Ok(());
        }
    };

    if !response.status().is_success() {
        return Err(HttpChatCompletionRepository::map_error_response(
            provider_name,
            response,
            "Codex stream request failed",
        )
        .await);
    }

    let emit_reasoning = should_emit_reasoning(payload);
    openai_responses::stream_http_response(
        provider_name,
        response,
        model,
        sender,
        cancel,
        ResponsesStreamOptions {
            emit_reasoning,
            include_reasoning_alias: emit_reasoning,
            prefer_reasoning_text: true,
        },
    )
    .await
}

fn codex_request_payload(payload: &Value, stream: bool) -> Result<Value, DomainError> {
    let mut request = openai_responses::upstream_payload(payload)?;
    let object = request.as_object_mut().ok_or_else(|| {
        DomainError::InvalidData("Codex Responses payload must be an object".to_string())
    })?;
    object.insert("stream".to_string(), Value::Bool(stream));
    object.insert("store".to_string(), Value::Bool(false));
    Ok(request)
}

fn should_emit_reasoning(payload: &Value) -> bool {
    payload
        .pointer("/reasoning/summary")
        .and_then(Value::as_str)
        == Some("detailed")
}

fn normalize_codex_response(
    response: Value,
    emit_reasoning: bool,
) -> Result<ChatCompletionRepositoryGenerateResponse, DomainError> {
    let mut normalized = openai_responses::normalize_completed_response(response)?;
    if let Some(message) = normalized
        .body
        .pointer_mut("/choices/0/message")
        .and_then(Value::as_object_mut)
    {
        // Codex remains an OpenAI-compatible custom provider; Responses replay metadata is not
        // exposed as its canonical provider representation.
        message.remove("native");
        if !emit_reasoning {
            message.remove("reasoning_content");
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_non_stream_response_uses_completed_responses_output_and_usage() {
        let response = json!({
            "id": "resp_123",
            "status": "completed",
            "model": "gpt-5.6",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{ "type": "output_text", "text": "Hello" }]
            }],
            "usage": {
                "input_tokens": 10,
                "output_tokens": 4,
                "total_tokens": 14
            }
        });

        let normalized = normalize_codex_response(response, false)
            .expect("completed response should normalize")
            .body;

        assert_eq!(normalized["choices"][0]["message"]["content"], "Hello");
        assert_eq!(normalized["usage"]["prompt_tokens"], 10);
        assert_eq!(normalized["usage"]["completion_tokens"], 4);
        assert!(normalized["choices"][0]["message"].get("native").is_none());
    }

    #[test]
    fn codex_request_payload_forces_transport_stream_mode() {
        let payload = json!({
            "model": "gpt-5.6",
            "input": [{ "role": "user", "content": "Hello" }],
            "stream": false,
            "_tauritavern_provider_state": { "transport": "internal" }
        });

        let request = codex_request_payload(&payload, true).expect("Codex payload");
        assert_eq!(request["stream"], true);
        assert_eq!(request["store"], false);
        assert!(request.get("_tauritavern_provider_state").is_none());
    }

    #[test]
    fn parse_codex_models_filters_hidden_disabled_and_duplicate_items() {
        let raw = json!({
            "models": [
                { "slug": "gpt-5.6", "display_name": "GPT-5.6", "visibility": "visible", "enabled": true },
                { "slug": "gpt-5.1", "display_name": "GPT-5.1", "visibility": "visible" },
                { "slug": "gpt-5.1", "display_name": "Duplicate" },
                { "slug": "hidden-model", "visibility": "hidden" },
                { "slug": "disabled-model", "enabled": false }
            ]
        });

        let parsed = parse_codex_models_json(&raw);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["id"], "gpt-5.1");
        assert_eq!(parsed[1]["id"], "gpt-5.6");
    }
}

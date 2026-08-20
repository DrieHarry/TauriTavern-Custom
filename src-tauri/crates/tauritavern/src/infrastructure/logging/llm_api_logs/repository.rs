use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde_json::Value;

use super::files::{request_raw_path, response_raw_sse_path};
use super::readable::{
    StreamReadableCollector, extract_model, format_endpoint, format_request_readable,
    format_response_readable, pretty_json, stream_readable_source, wire_log_payload,
};
use super::store::LlmApiLogStore;
use super::types::{LlmApiLogMeta, LlmApiRawKind};
use tt_domain::errors::DomainError;
use tt_ports::repositories::chat_completion_repository::{
    ChatCompletionApiConfig, ChatCompletionCancelReceiver, ChatCompletionRepository,
    ChatCompletionRepositoryGenerateResponse, ChatCompletionSource, ChatCompletionStreamSender,
};
use tt_ports::repositories::stable_diffusion_repository::{
    SdRouteRequest, SdRouteResponse, StableDiffusionRepository,
};

pub struct LoggingChatCompletionRepository {
    inner: Arc<dyn ChatCompletionRepository>,
    store: Arc<LlmApiLogStore>,
}

impl LoggingChatCompletionRepository {
    pub fn new(inner: Arc<dyn ChatCompletionRepository>, store: Arc<LlmApiLogStore>) -> Self {
        Self { inner, store }
    }
}

#[async_trait]
impl ChatCompletionRepository for LoggingChatCompletionRepository {
    async fn list_models(
        &self,
        source: ChatCompletionSource,
        config: &ChatCompletionApiConfig,
    ) -> Result<Value, DomainError> {
        self.inner.list_models(source, config).await
    }

    async fn generate(
        &self,
        source: ChatCompletionSource,
        config: &ChatCompletionApiConfig,
        endpoint_path: &str,
        payload: &Value,
    ) -> Result<ChatCompletionRepositoryGenerateResponse, DomainError> {
        let started = Instant::now();
        let started_at_ms = chrono::Utc::now().timestamp_millis();

        let result = self
            .inner
            .generate(source, config, endpoint_path, payload)
            .await;

        let id = self.store.allocate_id();
        let duration_ms = started.elapsed().as_millis().min(u128::from(u32::MAX)) as u32;

        let (ok, level, error_message, response_value) = match &result {
            Ok(response) => (true, "INFO".to_string(), None, Some(&response.body)),
            Err(error) => {
                let level = if matches!(error, DomainError::Cancelled(_)) {
                    "WARN"
                } else {
                    "ERROR"
                };
                (false, level.to_string(), Some(error.to_string()), None)
            }
        };

        let endpoint = format_endpoint(&config.base_url, endpoint_path);
        let log_payload = wire_log_payload(payload);
        let model = extract_model(&log_payload);

        let request_raw = pretty_json(&log_payload);
        let request_readable = format_request_readable(source, &log_payload);
        let response_log_payload = response_value.map(wire_log_payload);
        let (response_readable, response_raw_inline, response_raw_kind) =
            match response_log_payload.as_deref() {
                Some(value) => (
                    format_response_readable(value),
                    Some(pretty_json(value)),
                    Some(LlmApiRawKind::Json),
                ),
                None => (error_message.clone().unwrap_or_default(), None, None),
            };

        let meta = LlmApiLogMeta {
            id,
            timestamp_ms: started_at_ms,
            level,
            ok,
            source: source.key().to_string(),
            model,
            endpoint,
            duration_ms,
            stream: false,
            error_message,
            request_readable,
            response_readable,
            request_raw_kind: LlmApiRawKind::Json,
            response_raw_kind,
        };

        self.store
            .record_entry(meta, Some(request_raw), response_raw_inline)
            .await;
        result
    }

    async fn generate_stream(
        &self,
        source: ChatCompletionSource,
        config: &ChatCompletionApiConfig,
        endpoint_path: &str,
        payload: &Value,
        sender: ChatCompletionStreamSender,
        cancel: ChatCompletionCancelReceiver,
    ) -> Result<(), DomainError> {
        let started = Instant::now();
        let started_at_ms = chrono::Utc::now().timestamp_millis();

        let id = self.store.allocate_id();
        let endpoint = format_endpoint(&config.base_url, endpoint_path);
        let log_payload = wire_log_payload(payload);
        let model = extract_model(&log_payload);

        let request_raw = pretty_json(&log_payload);
        let request_readable = format_request_readable(source, &log_payload);
        let request_path = request_raw_path(self.store.log_root(), id);
        tauri::async_runtime::spawn(async move {
            if let Err(error) = tokio::fs::write(&request_path, request_raw).await {
                tracing::error!(
                    "Failed to write LLM API request log file {}: {}",
                    request_path.display(),
                    error
                );
            }
        });

        let response_path = response_raw_sse_path(self.store.log_root(), id);
        let response_writer = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&response_path)
            .await;

        let response_writer = match response_writer {
            Ok(file) => Some(tokio::io::BufWriter::new(file)),
            Err(error) => {
                tracing::error!(
                    "Failed to open LLM API SSE log file {}: {}",
                    response_path.display(),
                    error
                );
                None
            }
        };
        let response_raw_kind = response_writer.as_ref().map(|_| LlmApiRawKind::Sse);

        let readable_source = stream_readable_source(source, endpoint_path);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let forward_task = tauri::async_runtime::spawn(async move {
            let mut writer = response_writer;
            let mut readable = StreamReadableCollector::new(readable_source);

            while let Some(chunk) = rx.recv().await {
                let _ = sender.send(chunk.clone());
                readable.push(&chunk);

                if let Some(writer_ref) = writer.as_mut()
                    && (tokio::io::AsyncWriteExt::write_all(writer_ref, chunk.as_bytes())
                        .await
                        .is_err()
                        || tokio::io::AsyncWriteExt::write_all(writer_ref, b"\n")
                            .await
                            .is_err())
                {
                    writer = None;
                }
            }

            if let Some(mut writer) = writer {
                let _ = tokio::io::AsyncWriteExt::flush(&mut writer).await;
            }

            readable.into_string()
        });

        let result = self
            .inner
            .generate_stream(source, config, endpoint_path, payload, tx, cancel)
            .await;

        let response_readable = match forward_task.await {
            Ok(text) => text,
            Err(error) => format!("Stream forward task join failed: {error}"),
        };

        let duration_ms = started.elapsed().as_millis().min(u128::from(u32::MAX)) as u32;
        let ok = result.is_ok();
        let (level, error_message) = match &result {
            Ok(()) => ("INFO".to_string(), None),
            Err(error) => {
                let level = if matches!(error, DomainError::Cancelled(_)) {
                    "WARN"
                } else {
                    "ERROR"
                };
                (level.to_string(), Some(error.to_string()))
            }
        };

        let meta = LlmApiLogMeta {
            id,
            timestamp_ms: started_at_ms,
            level,
            ok,
            source: source.key().to_string(),
            model,
            endpoint,
            duration_ms,
            stream: true,
            error_message,
            request_readable,
            response_readable,
            request_raw_kind: LlmApiRawKind::Json,
            response_raw_kind,
        };

        self.store.record_entry(meta, None, None).await;
        result
    }

    async fn close_provider_session(&self, session_id: &str) {
        self.inner.close_provider_session(session_id).await;
    }
}

pub struct LoggingStableDiffusionRepository {
    inner: Arc<dyn StableDiffusionRepository>,
    store: Arc<LlmApiLogStore>,
}

impl LoggingStableDiffusionRepository {
    pub fn new(inner: Arc<dyn StableDiffusionRepository>, store: Arc<LlmApiLogStore>) -> Self {
        Self { inner, store }
    }
}

#[async_trait]
impl StableDiffusionRepository for LoggingStableDiffusionRepository {
    async fn handle(
        &self,
        request: SdRouteRequest,
        cancel: tokio::sync::watch::Receiver<bool>,
    ) -> Result<SdRouteResponse, DomainError> {
        let is_generate = request.path.ends_with("/generate") || request.path == "generate";
        if !is_generate {
            return self.inner.handle(request, cancel).await;
        }

        let started = Instant::now();
        let started_at_ms = chrono::Utc::now().timestamp_millis();

        let id = self.store.allocate_id();

        let log_payload = wire_log_payload(&request.body);
        let model = log_payload
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_string);
        let endpoint = log_payload
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or(&request.path)
            .to_string();

        let request_raw = pretty_json(&log_payload);
        let request_readable = format_image_request_readable(&request.path, &log_payload);

        let result = self.inner.handle(request.clone(), cancel).await;
        let duration_ms = started.elapsed().as_millis().min(u128::from(u32::MAX)) as u32;

        let (ok, level, error_message, response_readable, response_raw_inline, response_raw_kind) =
            match &result {
                Ok(response) => {
                    let is_success = (200..300).contains(&response.status);
                    let (level, error_msg) = if is_success {
                        ("INFO".to_string(), None)
                    } else {
                        let err_str = response
                            .body
                            .get("error")
                            .and_then(|e| e.get("message").or(Some(e)))
                            .and_then(Value::as_str)
                            .unwrap_or("Image generation failed");
                        ("ERROR".to_string(), Some(err_str.to_string()))
                    };

                    let response_log_payload = wire_log_payload(&response.body);
                    let readable = format_image_response_readable(response.status, &response_log_payload);
                    let raw = pretty_json(&response_log_payload);
                    (
                        is_success,
                        level,
                        error_msg,
                        readable,
                        Some(raw),
                        Some(LlmApiRawKind::Json),
                    )
                }
                Err(error) => {
                    let level = if matches!(error, DomainError::Cancelled(_)) {
                        "WARN"
                    } else {
                        "ERROR"
                    };
                    (
                        false,
                        level.to_string(),
                        Some(error.to_string()),
                        format!("Error: {error}"),
                        None,
                        None,
                    )
                }
            };

        let meta = LlmApiLogMeta {
            id,
            timestamp_ms: started_at_ms,
            level,
            ok,
            source: format!("Image ({})", request.path),
            model,
            endpoint,
            duration_ms,
            stream: false,
            error_message,
            request_readable,
            response_readable,
            request_raw_kind: LlmApiRawKind::Json,
            response_raw_kind,
        };

        self.store
            .record_entry(meta, Some(request_raw), response_raw_inline)
            .await;

        result
    }
}

fn format_image_request_readable(path: &str, body: &Value) -> String {
    let mut out = String::new();
    out.push_str(&format!("[Image Generation: {path}]\n\n"));

    if let Some(prompt) = body.get("prompt").and_then(Value::as_str) {
        out.push_str(&format!("Prompt:\n{prompt}\n\n"));
    }

    if let Some(negative) = body.get("negative_prompt").and_then(Value::as_str)
        && !negative.trim().is_empty()
    {
        out.push_str(&format!("Negative Prompt:\n{negative}\n\n"));
    }

    let mut params = Vec::new();
    if let Some(model) = body.get("model").and_then(Value::as_str) {
        params.push(format!("model={model}"));
    }
    if let Some(size) = body.get("size").and_then(Value::as_str) {
        params.push(format!("size={size}"));
    } else if let (Some(w), Some(h)) = (
        body.get("width").and_then(Value::as_u64),
        body.get("height").and_then(Value::as_u64),
    ) {
        params.push(format!("size={w}x{h}"));
    }
    if let Some(sampler) = body.get("sampler_name").or_else(|| body.get("sampler")).and_then(Value::as_str) {
        params.push(format!("sampler={sampler}"));
    }
    if let Some(steps) = body.get("steps").and_then(Value::as_u64) {
        params.push(format!("steps={steps}"));
    }
    if let Some(cfg) = body.get("cfg_scale").or_else(|| body.get("scale")).and_then(Value::as_f64) {
        params.push(format!("cfg_scale={cfg}"));
    }
    if let Some(seed) = body.get("seed").and_then(Value::as_i64) {
        params.push(format!("seed={seed}"));
    }

    if !params.is_empty() {
        out.push_str(&format!("[Params: {}]\n", params.join(", ")));
    }

    out.trim_end().to_string()
}

fn format_image_response_readable(status: u16, body: &Value) -> String {
    let mut out = String::new();
    if (200..300).contains(&status) {
        let format = body.get("format").and_then(Value::as_str).unwrap_or("image");
        out.push_str(&format!("[Image generated: format={format}]\n"));

        if let Some(data) = body.get("data").and_then(Value::as_str) {
            out.push_str(&format!("Image data size: {} chars\n", data.len()));
        }
        if let Some(revised) = body.get("revised_prompt").and_then(Value::as_str) {
            out.push_str(&format!("\nRevised Prompt:\n{revised}\n"));
        }
    } else {
        out.push_str(&format!("[Error status={status}]\n"));
        if let Some(err) = body.get("error").and_then(|e| e.get("message").or(Some(e))).and_then(Value::as_str) {
            out.push_str(err);
        } else {
            out.push_str(&body.to_string());
        }
    }
    out.trim_end().to_string()
}

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
    ChatCompletionToolCallDelta,
};
use tt_ports::repositories::stable_diffusion_repository::{
    SdRouteRequest, SdRouteResponse, StableDiffusionRepository,
};

pub struct LoggingChatCompletionRepository {
    inner: Arc<dyn ChatCompletionRepository>,
    store: Arc<LlmApiLogStore>,
}

struct JsonGenerationLog<'a> {
    source: ChatCompletionSource,
    config: &'a ChatCompletionApiConfig,
    endpoint_path: &'a str,
    payload: &'a Value,
    started: Instant,
    started_at_ms: i64,
    stream: bool,
}

impl<'a> JsonGenerationLog<'a> {
    fn start(
        source: ChatCompletionSource,
        config: &'a ChatCompletionApiConfig,
        endpoint_path: &'a str,
        payload: &'a Value,
        stream: bool,
    ) -> Self {
        Self {
            source,
            config,
            endpoint_path,
            payload,
            started: Instant::now(),
            started_at_ms: chrono::Utc::now().timestamp_millis(),
            stream,
        }
    }
}

impl LoggingChatCompletionRepository {
    pub fn new(inner: Arc<dyn ChatCompletionRepository>, store: Arc<LlmApiLogStore>) -> Self {
        Self { inner, store }
    }

    async fn record_json_generation(
        &self,
        log: JsonGenerationLog<'_>,
        result: &Result<ChatCompletionRepositoryGenerateResponse, DomainError>,
    ) {
        let id = self.store.allocate_id();
        let duration_ms = log.started.elapsed().as_millis().min(u128::from(u32::MAX)) as u32;

        let (ok, level, error_message, response_value) = match result {
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

        let endpoint = format_endpoint(&log.config.base_url, log.endpoint_path);
        let log_payload = wire_log_payload(log.payload);
        let model = extract_model(&log_payload);
        let request_raw = pretty_json(&log_payload);
        let request_readable = format_request_readable(log.source, &log_payload);
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
            timestamp_ms: log.started_at_ms,
            level,
            ok,
            source: log.source.key().to_string(),
            model,
            endpoint,
            duration_ms,
            stream: log.stream,
            error_message,
            request_readable,
            response_readable,
            request_raw_kind: LlmApiRawKind::Json,
            response_raw_kind,
        };

        self.store
            .record_entry(meta, Some(request_raw), response_raw_inline)
            .await;
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
        let log = JsonGenerationLog::start(source, config, endpoint_path, payload, false);
        let result = self
            .inner
            .generate(source, config, endpoint_path, payload)
            .await;
        self.record_json_generation(log, &result).await;
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
        let log_root = self.store.log_root().to_path_buf();
        let _ = tokio::fs::create_dir_all(&log_root).await;

        let request_path = request_raw_path(&log_root, id);
        let request_raw_for_file = request_raw.clone();
        tauri::async_runtime::spawn(async move {
            if let Err(error) = tokio::fs::write(&request_path, request_raw_for_file).await {
                tracing::error!(
                    "Failed to write LLM API request log file {}: {}",
                    request_path.display(),
                    error
                );
            }
        });

        let response_path = response_raw_sse_path(&log_root, id);
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

    async fn generate_with_tool_call_deltas(
        &self,
        source: ChatCompletionSource,
        config: &ChatCompletionApiConfig,
        endpoint_path: &str,
        payload: &Value,
        on_tool_call_delta: &mut (dyn FnMut(ChatCompletionToolCallDelta) + Send),
    ) -> Result<ChatCompletionRepositoryGenerateResponse, DomainError> {
        let log = JsonGenerationLog::start(source, config, endpoint_path, payload, true);
        let result = self
            .inner
            .generate_with_tool_call_deltas(
                source,
                config,
                endpoint_path,
                payload,
                on_tool_call_delta,
            )
            .await;
        self.record_json_generation(log, &result).await;
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

        let log_payload = image_request_log_payload(&request.body);
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

                    let response_log_payload = image_response_log_payload(&response.body);
                    let readable =
                        format_image_response_readable(response.status, &response_log_payload);
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

fn image_request_log_payload(payload: &Value) -> Value {
    let mut sanitized = wire_log_payload(payload).into_owned();
    let format = sanitized
        .get("format")
        .and_then(Value::as_str)
        .map(str::to_string);
    redact_image_request_content(&mut sanitized, None, format.as_deref());
    sanitized
}

fn redact_image_request_content(value: &mut Value, field: Option<&str>, format: Option<&str>) {
    match value {
        Value::String(_) if field.is_some_and(is_image_request_content_field) => {
            replace_image_content(value, format);
        }
        Value::Array(values) => {
            for value in values {
                redact_image_request_content(value, field, format);
            }
        }
        Value::Object(object) => {
            for (key, value) in object {
                redact_image_request_content(value, Some(key), format);
            }
        }
        _ => {}
    }
}

fn is_image_request_content_field(field: &str) -> bool {
    matches!(
        field,
        "init_images"
            | "mask"
            | "image"
            | "image_base64"
            | "input_image"
            | "source_image"
            | "control_image"
            | "reference_image"
            | "b64_json"
    )
}

fn image_response_log_payload(payload: &Value) -> Value {
    let mut sanitized = wire_log_payload(payload).into_owned();
    let format = sanitized
        .get("format")
        .and_then(Value::as_str)
        .map(str::to_string);

    if let Some(object) = sanitized.as_object_mut() {
        for field in ["data", "image"] {
            if let Some(value) = object.get_mut(field)
                && value.is_string()
            {
                replace_image_content(value, format.as_deref());
            }
        }

        if let Some(images) = object.get_mut("images").and_then(Value::as_array_mut) {
            for image in images.iter_mut().filter(|image| image.is_string()) {
                replace_image_content(image, format.as_deref());
            }
        }
    }

    redact_named_base64_fields(&mut sanitized, format.as_deref());
    sanitized
}

fn redact_named_base64_fields(value: &mut Value, format: Option<&str>) {
    match value {
        Value::Array(values) => {
            for value in values {
                redact_named_base64_fields(value, format);
            }
        }
        Value::Object(object) => {
            for (key, value) in object {
                if matches!(key.as_str(), "b64_json" | "base64" | "image_base64")
                    && value.is_string()
                {
                    replace_image_content(value, format);
                } else {
                    redact_named_base64_fields(value, format);
                }
            }
        }
        _ => {}
    }
}

fn replace_image_content(value: &mut Value, format: Option<&str>) {
    let Some(encoded) = value.as_str() else {
        return;
    };
    if encoded.starts_with("<inline media omitted;")
        || encoded.starts_with("<generated image omitted;")
    {
        return;
    }

    let (mime_type, base64_chars) = data_url_image(encoded)
        .map(|(mime_type, data)| (Some(mime_type), data.len()))
        .unwrap_or((None, encoded.len()));
    let media = mime_type
        .map(|mime_type| format!("mime={mime_type}"))
        .or_else(|| format.map(|format| format!("format={format}")))
        .unwrap_or_else(|| "format=unknown".to_string());
    *value = Value::String(format!(
        "<generated image omitted; {media}; base64_chars={base64_chars}>"
    ));
}

fn data_url_image(value: &str) -> Option<(&str, &str)> {
    let body = value.strip_prefix("data:")?;
    let (metadata, data) = body.split_once(',')?;
    let mime_type = metadata.strip_suffix(";base64")?.trim();
    (mime_type.starts_with("image/") && !data.is_empty()).then_some((mime_type, data))
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
    if let Some(sampler) = body
        .get("sampler_name")
        .or_else(|| body.get("sampler"))
        .and_then(Value::as_str)
    {
        params.push(format!("sampler={sampler}"));
    }
    if let Some(steps) = body.get("steps").and_then(Value::as_u64) {
        params.push(format!("steps={steps}"));
    }
    if let Some(cfg) = body
        .get("cfg_scale")
        .or_else(|| body.get("scale"))
        .and_then(Value::as_f64)
    {
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
        let format = body
            .get("format")
            .and_then(Value::as_str)
            .unwrap_or("image");
        out.push_str(&format!("[Image generated: format={format}]\n"));

        if let Some(data) = generated_image_content(body) {
            let encoded_chars = logged_image_content_chars(data);
            out.push_str(&format!("Image data size: {encoded_chars} chars\n"));
        }
        if let Some(revised) = revised_image_prompt(body) {
            out.push_str(&format!("\nRevised Prompt:\n{revised}\n"));
        }
    } else {
        out.push_str(&format!("[Error status={status}]\n"));
        if let Some(err) = body
            .get("error")
            .and_then(|e| e.get("message").or(Some(e)))
            .and_then(Value::as_str)
        {
            out.push_str(err);
        } else {
            out.push_str(&body.to_string());
        }
    }
    out.trim_end().to_string()
}

fn logged_image_content_chars(value: &str) -> usize {
    value
        .strip_prefix("<generated image omitted;")
        .or_else(|| value.strip_prefix("<inline media omitted;"))
        .and_then(|metadata| metadata.split("base64_chars=").nth(1))
        .and_then(|length| length.trim_end_matches('>').parse::<usize>().ok())
        .or_else(|| data_url_image(value).map(|(_, encoded)| encoded.len()))
        .unwrap_or(value.len())
}

fn generated_image_content(body: &Value) -> Option<&str> {
    body.get("data")
        .and_then(Value::as_str)
        .or_else(|| body.get("image").and_then(Value::as_str))
        .or_else(|| {
            body.get("images")
                .and_then(Value::as_array)
                .and_then(|images| images.first())
                .and_then(Value::as_str)
        })
        .or_else(|| {
            body.get("data")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(|item| item.get("b64_json"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            body.get("artifacts")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(|item| item.get("base64"))
                .and_then(Value::as_str)
        })
}

fn revised_image_prompt(body: &Value) -> Option<&str> {
    body.get("revised_prompt")
        .or_else(|| body.get("revisedPrompt"))
        .and_then(Value::as_str)
        .or_else(|| {
            body.get("data")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(|item| {
                    item.get("revised_prompt")
                        .or_else(|| item.get("revisedPrompt"))
                })
                .and_then(Value::as_str)
        })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        format_image_response_readable, image_request_log_payload, image_response_log_payload,
    };

    #[test]
    fn image_request_log_copy_redacts_inputs_without_losing_generation_metadata() {
        let request = json!({
            "prompt": "a lighthouse in a storm",
            "negative_prompt": "blurry",
            "model": "flux-test",
            "width": 1024,
            "height": 768,
            "steps": 28,
            "init_images": ["REQUEST_IMAGE_BASE64"],
            "mask": "data:image/png;base64,MASK_IMAGE_BASE64",
        });

        let logged = image_request_log_payload(&request);

        assert_eq!(logged["prompt"], request["prompt"]);
        assert_eq!(logged["negative_prompt"], request["negative_prompt"]);
        assert_eq!(logged["model"], request["model"]);
        assert_eq!(logged["width"], 1024);
        assert_eq!(logged["height"], 768);
        assert_eq!(logged["steps"], 28);
        assert!(!logged.to_string().contains("REQUEST_IMAGE_BASE64"));
        assert!(!logged.to_string().contains("MASK_IMAGE_BASE64"));
        assert_eq!(request["init_images"][0], "REQUEST_IMAGE_BASE64");
        assert_eq!(request["mask"], "data:image/png;base64,MASK_IMAGE_BASE64");
    }

    #[test]
    fn image_response_log_copy_redacts_supported_provider_shapes_and_keeps_metadata() {
        let responses = [
            json!({
                "format": "png",
                "data": "NORMALIZED_DATA_BASE64",
                "revised_prompt": "a brighter lighthouse",
                "seed": 42,
            }),
            json!({
                "format": "jpeg",
                "image": "NORMALIZED_IMAGE_BASE64",
                "provider": "workers-ai",
            }),
            json!({
                "images": ["WEBUI_IMAGE_BASE64"],
                "parameters": { "steps": 30, "cfg_scale": 7.5 },
                "info": "{\"seed\":1234}",
            }),
            json!({
                "data": [{
                    "b64_json": "OPENAI_IMAGE_BASE64",
                    "revised_prompt": "a revised provider prompt",
                }],
                "created": 123456,
            }),
            json!({
                "artifacts": [{ "base64": "STABILITY_IMAGE_BASE64", "finishReason": "SUCCESS" }],
            }),
        ];

        for response in &responses {
            let logged = image_response_log_payload(response);
            let logged_text = logged.to_string();
            for raw_image in [
                "NORMALIZED_DATA_BASE64",
                "NORMALIZED_IMAGE_BASE64",
                "WEBUI_IMAGE_BASE64",
                "OPENAI_IMAGE_BASE64",
                "STABILITY_IMAGE_BASE64",
            ] {
                assert!(!logged_text.contains(raw_image));
            }
            assert!(logged_text.contains("image omitted"));

            // Sanitization operates on the log copy, never on the response returned to the app.
            assert!(!response.to_string().contains("image omitted"));
        }

        let logged = image_response_log_payload(&responses[0]);
        assert_eq!(logged["revised_prompt"], "a brighter lighthouse");
        assert_eq!(logged["seed"], 42);
        let webui_logged = image_response_log_payload(&responses[2]);
        assert_eq!(webui_logged["parameters"]["steps"], 30);
        assert_eq!(webui_logged["info"], "{\"seed\":1234}");
    }

    #[test]
    fn image_response_readable_keeps_size_and_nested_revised_prompt() {
        let response = json!({
            "data": [{
                "b64_json": "12345678",
                "revised_prompt": "a calmer sea",
            }],
        });

        let readable = format_image_response_readable(200, &response);

        assert!(readable.contains("Image data size: 8 chars"));
        assert!(readable.contains("Revised Prompt:\na calmer sea"));
        assert!(!readable.contains("12345678"));
    }
}

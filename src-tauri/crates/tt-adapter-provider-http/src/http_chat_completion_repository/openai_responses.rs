use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderName, HeaderValue};
use reqwest::{Client, StatusCode};
use serde_json::{Map, Value, json};
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;

use tt_domain::errors::DomainError;
use tt_ports::repositories::chat_completion_repository::{
    CHAT_COMPLETION_PROVIDER_STATE_FIELD, ChatCompletionApiConfig, ChatCompletionCancelReceiver,
    ChatCompletionRepositoryGenerateResponse, ChatCompletionStreamSender,
    OPENAI_RESPONSES_WEBSOCKET_TRANSPORT,
};

use super::HttpChatCompletionRepository;
use super::normalizers;
use super::response_body::{log_upstream_body_parse_failure, read_upstream_json_body};

type ResponsesWsStream = tokio_tungstenite::WebSocketStream<reqwest::Upgraded>;

const OPERATION_GENERATE_STREAM_HTTP: &str = "generate_stream_http";
const OPERATION_GENERATE_PERSISTENT_WS: &str = "generate_persistent_ws";

#[derive(Default)]
pub(super) struct ResponsesWsSessionPool {
    sessions: Mutex<HashMap<String, Arc<Mutex<ResponsesWsSession>>>>,
}

struct ResponsesWsSession {
    connection_key: String,
    socket: ResponsesWsStream,
}

impl ResponsesWsSessionPool {
    async fn session(
        &self,
        repository: &HttpChatCompletionRepository,
        config: &ChatCompletionApiConfig,
        endpoint_path: &str,
        session_id: &str,
    ) -> Result<Arc<Mutex<ResponsesWsSession>>, DomainError> {
        let (client, transport_revision) = repository.websocket_client(config)?;
        let connection_key = ws_connection_key(config, endpoint_path, transport_revision)?;
        if let Some(session) = self.sessions.lock().await.get(session_id).cloned()
            && session.lock().await.connection_key == connection_key
        {
            return Ok(session);
        }

        let socket = connect_responses_ws(client, config, endpoint_path).await?;
        let session = Arc::new(Mutex::new(ResponsesWsSession {
            connection_key,
            socket,
        }));
        self.sessions
            .lock()
            .await
            .insert(session_id.to_string(), session.clone());
        Ok(session)
    }

    pub(super) async fn close(&self, session_id: &str) {
        let session = self.sessions.lock().await.remove(session_id);
        if let Some(session) = session {
            let close_result = session.lock().await.close().await;
            if let Err(error) = close_result {
                tracing::warn!(
                    session_id,
                    error = %error,
                    "Failed to close OpenAI Responses WebSocket session"
                );
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ResponsesStreamOptions {
    pub emit_reasoning: bool,
    pub include_reasoning_alias: bool,
    pub prefer_reasoning_text: bool,
}

impl Default for ResponsesStreamOptions {
    fn default() -> Self {
        Self {
            emit_reasoning: true,
            include_reasoning_alias: false,
            prefer_reasoning_text: false,
        }
    }
}

#[derive(Default)]
struct ResponsesToolCallState {
    index: usize,
    name: String,
    announced: bool,
    arguments_streamed: bool,
}

struct ResponsesStreamState {
    created: u64,
    model: String,
    response_id: Option<String>,
    sent_role: bool,
    saw_tool_call: bool,
    done_sent: bool,
    saw_reasoning_text: bool,
    options: ResponsesStreamOptions,
    tool_calls: HashMap<String, ResponsesToolCallState>,
    tool_item_to_call_id: HashMap<String, String>,
}

impl ResponsesStreamState {
    #[cfg(test)]
    fn new(model: String) -> Self {
        Self::with_options(model, ResponsesStreamOptions::default())
    }

    fn with_options(model: String, options: ResponsesStreamOptions) -> Self {
        Self {
            created: current_unix_timestamp(),
            model,
            response_id: None,
            sent_role: false,
            saw_tool_call: false,
            done_sent: false,
            saw_reasoning_text: false,
            options,
            tool_calls: HashMap::new(),
            tool_item_to_call_id: HashMap::new(),
        }
    }

    fn handle_event(
        &mut self,
        sender: &ChatCompletionStreamSender,
        event: &Value,
    ) -> Result<(), DomainError> {
        if self.done_sent {
            return Ok(());
        }

        if terminal_response_from_event(event)?.is_some() {
            let finish_reason = if self.saw_tool_call {
                "tool_calls"
            } else {
                "stop"
            };

            self.send_delta(sender, json!({}), Some(finish_reason));
            let _ = sender.send("[DONE]".to_string());
            self.done_sent = true;
            return Ok(());
        }

        if let Some(response_id) = event
            .get("response_id")
            .and_then(Value::as_str)
            .or_else(|| event.pointer("/response/id").and_then(Value::as_str))
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            self.response_id = Some(response_id.to_string());
        }

        if let Some(event_type) = event.get("type").and_then(Value::as_str) {
            match event_type {
                "response.output_text.delta" | "response.text.delta" | "response.refusal.delta" => {
                    if let Some(delta) = event.get("delta").and_then(Value::as_str)
                        && !delta.is_empty()
                    {
                        self.send_delta(sender, json!({ "content": delta }), None);
                    }
                }
                "response.reasoning_text.delta" | "response.reasoning.delta" => {
                    if let Some(delta) = event.get("delta").and_then(Value::as_str)
                        && !delta.is_empty()
                        && self.options.emit_reasoning
                    {
                        self.saw_reasoning_text = true;
                        self.send_reasoning_delta(sender, delta);
                    }
                }
                "response.reasoning_summary_text.delta" => {
                    if let Some(delta) = event.get("delta").and_then(Value::as_str)
                        && !delta.is_empty()
                        && self.options.emit_reasoning
                        && (!self.options.prefer_reasoning_text || !self.saw_reasoning_text)
                    {
                        self.send_reasoning_delta(sender, delta);
                    }
                }
                "response.output_item.added" => {
                    if let Some(item) = event.get("item") {
                        self.handle_tool_call_added(sender, event, item);
                    }
                }
                "response.function_call_arguments.delta" => {
                    self.handle_tool_call_arguments(sender, event);
                }
                "response.output_item.done" => {
                    if let Some(item) = event.get("item") {
                        self.handle_tool_call_done(sender, event, item);
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn send_reasoning_delta(&mut self, sender: &ChatCompletionStreamSender, delta: &str) {
        let mut reasoning_delta = json!({ "reasoning_content": delta });
        if self.options.include_reasoning_alias {
            reasoning_delta["reasoning"] = Value::String(delta.to_string());
        }
        self.send_delta(sender, reasoning_delta, None);
    }

    fn handle_tool_call_added(
        &mut self,
        sender: &ChatCompletionStreamSender,
        event: &Value,
        item: &Value,
    ) {
        let Some((call_id, index, name, announce)) = self.register_tool_call(event, item) else {
            return;
        };
        if announce {
            self.send_tool_call_delta(sender, index, Some(&call_id), Some(&name), Some(""));
        }
    }

    fn handle_tool_call_arguments(&mut self, sender: &ChatCompletionStreamSender, event: &Value) {
        let item_id = event
            .get("item_id")
            .or_else(|| event.get("call_id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let Some(item_id) = item_id else {
            return;
        };
        let call_id = self
            .tool_item_to_call_id
            .get(item_id)
            .cloned()
            .unwrap_or_else(|| item_id.to_string());
        let name = event
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let arguments = event
            .get("delta")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if arguments.is_empty() {
            return;
        }

        let next_index = self.tool_calls.len();
        let state =
            self.tool_calls
                .entry(call_id.clone())
                .or_insert_with(|| ResponsesToolCallState {
                    index: next_index,
                    name: name.clone(),
                    ..Default::default()
                });
        if state.name.is_empty() && !name.is_empty() {
            state.name = name;
        }
        let index = state.index;
        let announce = !state.announced;
        state.announced = true;
        state.arguments_streamed = true;
        let name = state.name.clone();
        self.saw_tool_call = true;

        if announce {
            self.send_tool_call_delta(sender, index, Some(&call_id), Some(&name), Some(arguments));
        } else {
            self.send_tool_call_delta(sender, index, None, None, Some(arguments));
        }
    }

    fn handle_tool_call_done(
        &mut self,
        sender: &ChatCompletionStreamSender,
        event: &Value,
        item: &Value,
    ) {
        let Some((call_id, index, name, announce)) = self.register_tool_call(event, item) else {
            return;
        };
        let arguments = item
            .get("arguments")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let arguments_streamed = self
            .tool_calls
            .get(&call_id)
            .is_some_and(|state| state.arguments_streamed);

        if announce {
            self.send_tool_call_delta(sender, index, Some(&call_id), Some(&name), Some(arguments));
        } else if !arguments_streamed && !arguments.is_empty() {
            self.send_tool_call_delta(sender, index, None, None, Some(arguments));
        }
    }

    fn register_tool_call(
        &mut self,
        event: &Value,
        item: &Value,
    ) -> Option<(String, usize, String, bool)> {
        if item.get("type").and_then(Value::as_str) != Some("function_call") {
            return None;
        }

        let call_id = item
            .get("call_id")
            .or_else(|| item.get("id"))
            .or_else(|| event.get("item_id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())?
            .to_string();
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        if let Some(item_id) = item
            .get("id")
            .or_else(|| event.get("item_id"))
            .and_then(Value::as_str)
        {
            self.tool_item_to_call_id
                .insert(item_id.to_string(), call_id.clone());
        }

        let next_index = event
            .get("output_index")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(self.tool_calls.len());
        let state =
            self.tool_calls
                .entry(call_id.clone())
                .or_insert_with(|| ResponsesToolCallState {
                    index: next_index,
                    name: name.clone(),
                    ..Default::default()
                });
        if state.name.is_empty() && !name.is_empty() {
            state.name = name;
        }
        let announce = !state.announced;
        state.announced = true;
        self.saw_tool_call = true;

        Some((call_id, state.index, state.name.clone(), announce))
    }

    fn send_tool_call_delta(
        &mut self,
        sender: &ChatCompletionStreamSender,
        index: usize,
        call_id: Option<&str>,
        name: Option<&str>,
        arguments: Option<&str>,
    ) {
        let mut tool_call = Map::new();
        tool_call.insert("index".to_string(), json!(index));
        if let Some(call_id) = call_id {
            tool_call.insert("id".to_string(), Value::String(call_id.to_string()));
            tool_call.insert("type".to_string(), Value::String("function".to_string()));
        }
        let mut function = Map::new();
        if let Some(name) = name {
            function.insert("name".to_string(), Value::String(name.to_string()));
        }
        if let Some(arguments) = arguments {
            function.insert(
                "arguments".to_string(),
                Value::String(arguments.to_string()),
            );
        }
        if !function.is_empty() {
            tool_call.insert("function".to_string(), Value::Object(function));
        }
        self.send_delta(
            sender,
            json!({ "tool_calls": [Value::Object(tool_call)] }),
            None,
        );
    }

    fn ensure_completed(&self, cancelled: bool) -> Result<(), DomainError> {
        if self.done_sent || cancelled {
            return Ok(());
        }

        Err(DomainError::transient(
            "OpenAI Responses stream closed before response.completed".to_string(),
        ))
    }

    fn send_delta(
        &mut self,
        sender: &ChatCompletionStreamSender,
        delta: Value,
        finish_reason: Option<&str>,
    ) {
        if !self.sent_role {
            self.sent_role = true;
            let role_chunk = self.build_chunk(json!({ "role": "assistant" }), None);
            if let Ok(payload) = serde_json::to_string(&role_chunk) {
                let _ = sender.send(payload);
            }
        }

        let chunk = self.build_chunk(delta, finish_reason);
        if let Ok(payload) = serde_json::to_string(&chunk) {
            let _ = sender.send(payload);
        }
    }

    fn build_chunk(&self, delta: Value, finish_reason: Option<&str>) -> Value {
        let id = self
            .response_id
            .clone()
            .unwrap_or_else(|| "openai-responses-stream".to_string());

        json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model,
            "choices": [{
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason
            }]
        })
    }
}

pub(super) async fn generate(
    repository: &HttpChatCompletionRepository,
    config: &ChatCompletionApiConfig,
    endpoint_path: &str,
    payload: &Value,
    provider_name: &str,
) -> Result<ChatCompletionRepositoryGenerateResponse, DomainError> {
    if let Some(session_id) = provider_session_id(payload)? {
        return generate_persistent_ws(
            &repository.openai_responses_ws_sessions,
            repository,
            config,
            endpoint_path,
            payload,
            &session_id,
        )
        .await;
    }

    generate_http(repository, config, endpoint_path, payload, provider_name).await
}

async fn generate_http(
    repository: &HttpChatCompletionRepository,
    config: &ChatCompletionApiConfig,
    endpoint_path: &str,
    payload: &Value,
    provider_name: &str,
) -> Result<ChatCompletionRepositoryGenerateResponse, DomainError> {
    let url = HttpChatCompletionRepository::build_url(&config.base_url, endpoint_path)?;

    let client = repository.client(config)?;
    let http_payload = upstream_payload(payload)?;
    let request = client
        .post(url)
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "application/json")
        .json(&http_payload);

    let request = HttpChatCompletionRepository::apply_openai_auth(request, config);
    let request = HttpChatCompletionRepository::apply_extra_headers(request, &config.extra_headers);
    let request = HttpChatCompletionRepository::apply_additional_headers(request, config);

    let response = request.send().await.map_err(|error| {
        HttpChatCompletionRepository::map_transport_error("Generation request failed", error)
    })?;

    if !response.status().is_success() {
        return Err(HttpChatCompletionRepository::map_error_response(
            provider_name,
            response,
            "Generation request failed",
        )
        .await);
    }

    let body = read_upstream_json_body(provider_name, "generate", response).await?;
    normalize_completed_response(body)
}

pub(crate) fn normalize_completed_response(
    response: Value,
) -> Result<ChatCompletionRepositoryGenerateResponse, DomainError> {
    validate_terminal_response(&response)?;
    Ok(normalizers::normalize_openai_responses_response(response))
}

pub(super) async fn generate_stream(
    repository: &HttpChatCompletionRepository,
    config: &ChatCompletionApiConfig,
    endpoint_path: &str,
    payload: &Value,
    provider_name: &str,
    sender: ChatCompletionStreamSender,
    cancel: ChatCompletionCancelReceiver,
) -> Result<(), DomainError> {
    generate_stream_http(
        repository,
        config,
        endpoint_path,
        payload,
        provider_name,
        sender,
        cancel,
    )
    .await
}

async fn generate_stream_http(
    repository: &HttpChatCompletionRepository,
    config: &ChatCompletionApiConfig,
    endpoint_path: &str,
    payload: &Value,
    provider_name: &str,
    sender: ChatCompletionStreamSender,
    cancel: ChatCompletionCancelReceiver,
) -> Result<(), DomainError> {
    let url = HttpChatCompletionRepository::build_url(&config.base_url, endpoint_path)?;

    let client = repository.stream_client(config)?;
    let http_payload = upstream_payload(payload)?;
    let request = client
        .post(url)
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "text/event-stream")
        .json(&http_payload);

    let request = HttpChatCompletionRepository::apply_openai_auth(request, config);
    let request = HttpChatCompletionRepository::apply_extra_headers(request, &config.extra_headers);
    let request = HttpChatCompletionRepository::apply_additional_headers(request, config);

    let response = request.send().await.map_err(|error| {
        HttpChatCompletionRepository::map_transport_error("Generation request failed", error)
    })?;

    if !response.status().is_success() {
        return Err(HttpChatCompletionRepository::map_error_response(
            provider_name,
            response,
            "Generation request failed",
        )
        .await);
    }

    let model = payload
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    stream_http_response(
        provider_name,
        response,
        model,
        sender,
        cancel,
        ResponsesStreamOptions::default(),
    )
    .await
}

pub(crate) async fn stream_http_response(
    provider_name: &str,
    response: reqwest::Response,
    model: String,
    sender: ChatCompletionStreamSender,
    cancel: ChatCompletionCancelReceiver,
    options: ResponsesStreamOptions,
) -> Result<(), DomainError> {
    let mut state = ResponsesStreamState::with_options(model, options);
    let cancelled = cancel.clone();

    let (dummy_sender, dummy_receiver) = mpsc::unbounded_channel::<String>();
    drop(dummy_receiver);

    HttpChatCompletionRepository::stream_sse_response_internal(
        provider_name,
        response,
        dummy_sender,
        cancel,
        |payload| {
            if payload == b"[DONE]" {
                return Ok(());
            }
            let event = parse_sse_event(payload, OPERATION_GENERATE_STREAM_HTTP)?;
            state.handle_event(&sender, &event)
        },
    )
    .await?;

    let was_cancelled = *cancelled.borrow();
    state.ensure_completed(was_cancelled)
}

async fn generate_persistent_ws(
    pool: &ResponsesWsSessionPool,
    repository: &HttpChatCompletionRepository,
    config: &ChatCompletionApiConfig,
    endpoint_path: &str,
    payload: &Value,
    session_id: &str,
) -> Result<ChatCompletionRepositoryGenerateResponse, DomainError> {
    let event = response_create_event(payload)?;
    let session = pool
        .session(repository, config, endpoint_path, session_id)
        .await?;
    let result = {
        let mut session = session.lock().await;
        session.generate(event).await
    };

    match result {
        Ok(response) => Ok(normalizers::normalize_openai_responses_response(response)),
        Err(error) => {
            pool.close(session_id).await;
            Err(error)
        }
    }
}

impl ResponsesWsSession {
    async fn close(&mut self) -> Result<(), DomainError> {
        self.socket.close(None).await.map_err(|error| {
            DomainError::InternalError(format!("OpenAI Responses WebSocket close failed: {error}"))
        })
    }

    async fn generate(&mut self, event: Value) -> Result<Value, DomainError> {
        self.socket
            .send(Message::Text(event.to_string().into()))
            .await
            .map_err(|error| {
                DomainError::transient(format!("OpenAI Responses WebSocket send failed: {error}"))
            })?;

        loop {
            let Some(message) = self.socket.next().await else {
                return Err(DomainError::transient(
                    "OpenAI Responses WebSocket closed before response.completed".to_string(),
                ));
            };
            let message = message.map_err(|error| {
                DomainError::transient(format!("OpenAI Responses WebSocket read failed: {error}"))
            })?;

            match message {
                Message::Text(text) => {
                    if let Some(response) = response_from_ws_payload(
                        text.as_str().as_bytes(),
                        OPERATION_GENERATE_PERSISTENT_WS,
                    )? {
                        return Ok(response);
                    }
                }
                Message::Binary(bytes) => {
                    if let Some(response) =
                        response_from_ws_payload(bytes.as_ref(), OPERATION_GENERATE_PERSISTENT_WS)?
                    {
                        return Ok(response);
                    }
                }
                Message::Ping(bytes) => {
                    self.socket
                        .send(Message::Pong(bytes))
                        .await
                        .map_err(|error| {
                            DomainError::transient(format!(
                                "OpenAI Responses WebSocket pong failed: {error}"
                            ))
                        })?;
                }
                Message::Close(frame) => {
                    return Err(DomainError::transient(format!(
                        "OpenAI Responses WebSocket closed before response.completed: {frame:?}"
                    )));
                }
                Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }
}

async fn connect_responses_ws(
    client: Client,
    config: &ChatCompletionApiConfig,
    endpoint_path: &str,
) -> Result<ResponsesWsStream, DomainError> {
    let key = generate_key();
    let request = build_ws_upgrade_request(&client, config, endpoint_path, &key)?;
    let response = client.execute(request).await.map_err(|error| {
        HttpChatCompletionRepository::map_transport_error(
            "OpenAI Responses WebSocket upgrade request failed",
            error,
        )
    })?;

    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        return Err(HttpChatCompletionRepository::map_error_response(
            "OpenAI Responses WebSocket",
            response,
            "OpenAI Responses WebSocket upgrade failed",
        )
        .await);
    }
    verify_ws_upgrade_response(&response, &key)?;

    let upgraded = response.upgrade().await.map_err(|error| {
        DomainError::transient(format!(
            "OpenAI Responses WebSocket upgrade failed: {error}"
        ))
    })?;
    Ok(tokio_tungstenite::WebSocketStream::from_raw_socket(upgraded, Role::Client, None).await)
}

fn build_ws_upgrade_request(
    client: &Client,
    config: &ChatCompletionApiConfig,
    endpoint_path: &str,
    key: &str,
) -> Result<reqwest::Request, DomainError> {
    let url = responses_ws_upgrade_url(&config.base_url, endpoint_path)?;
    let request = client.get(url);
    let request = HttpChatCompletionRepository::apply_openai_auth(request, config);
    let request = HttpChatCompletionRepository::apply_extra_headers(request, &config.extra_headers);
    let request = HttpChatCompletionRepository::apply_additional_headers(request, config);
    let mut request = request.build().map_err(|error| {
        DomainError::InvalidData(format!(
            "Invalid OpenAI Responses WebSocket upgrade request: {error}"
        ))
    })?;

    let key = HeaderValue::from_str(key).map_err(|error| {
        DomainError::InvalidData(format!(
            "Invalid OpenAI Responses WebSocket key header: {error}"
        ))
    })?;
    let headers = request.headers_mut();
    headers.insert(
        HeaderName::from_static("connection"),
        HeaderValue::from_static("Upgrade"),
    );
    headers.insert(
        HeaderName::from_static("upgrade"),
        HeaderValue::from_static("websocket"),
    );
    headers.insert(
        HeaderName::from_static("sec-websocket-version"),
        HeaderValue::from_static("13"),
    );
    headers.insert(HeaderName::from_static("sec-websocket-key"), key);

    Ok(request)
}

fn verify_ws_upgrade_response(response: &reqwest::Response, key: &str) -> Result<(), DomainError> {
    let expected = derive_accept_key(key.as_bytes());
    let accept = response
        .headers()
        .get(HeaderName::from_static("sec-websocket-accept"))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .ok_or_else(|| {
            DomainError::InternalError(
                "OpenAI Responses WebSocket upgrade missing Sec-WebSocket-Accept".to_string(),
            )
        })?;

    if accept != expected {
        return Err(DomainError::InternalError(
            "OpenAI Responses WebSocket upgrade returned invalid Sec-WebSocket-Accept".to_string(),
        ));
    }

    Ok(())
}

fn ws_connection_key(
    config: &ChatCompletionApiConfig,
    endpoint_path: &str,
    transport_revision: u64,
) -> Result<String, DomainError> {
    let mut headers = config
        .extra_headers
        .iter()
        .chain(config.additional_headers.iter())
        .map(|(key, value)| format!("{}={}", key.trim().to_ascii_lowercase(), value.trim()))
        .collect::<Vec<_>>();
    headers.sort_unstable();

    Ok(format!(
        "{}\n{}\n{}\n{}",
        responses_ws_url(&config.base_url, endpoint_path)?,
        transport_revision,
        websocket_authorization_header(config).unwrap_or_default(),
        headers.join("\n")
    ))
}

fn websocket_authorization_header(config: &ChatCompletionApiConfig) -> Option<String> {
    config
        .authorization_header
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            let api_key = config.api_key.trim();
            (!api_key.is_empty()).then(|| format!("Bearer {api_key}"))
        })
}

fn responses_ws_upgrade_url(base_url: &str, endpoint_path: &str) -> Result<String, DomainError> {
    let mut url = HttpChatCompletionRepository::build_url(base_url, endpoint_path)?;
    let scheme = match url.scheme() {
        "https" | "http" => return Ok(url.to_string()),
        "wss" => "https",
        "ws" => "http",
        other => {
            return Err(DomainError::InvalidData(format!(
                "OpenAI Responses WebSocket URL must use http, https, ws, or wss scheme: {other}"
            )));
        }
    };
    url.set_scheme(scheme).map_err(|_| {
        DomainError::InvalidData(format!("Invalid OpenAI Responses WebSocket URL {url}"))
    })?;
    Ok(url.to_string())
}

fn responses_ws_url(base_url: &str, endpoint_path: &str) -> Result<String, DomainError> {
    let mut url = HttpChatCompletionRepository::build_url(base_url, endpoint_path)?;
    let scheme = match url.scheme() {
        "https" => "wss",
        "http" => "ws",
        "ws" | "wss" => return Ok(url.to_string()),
        other => {
            return Err(DomainError::InvalidData(format!(
                "OpenAI Responses WebSocket URL must use http, https, ws, or wss scheme: {other}"
            )));
        }
    };
    url.set_scheme(scheme).map_err(|_| {
        DomainError::InvalidData(format!("Invalid OpenAI Responses WebSocket URL {url}"))
    })?;
    Ok(url.to_string())
}

fn response_create_event(payload: &Value) -> Result<Value, DomainError> {
    let mut event = websocket_response_payload(payload)?;
    event.insert(
        "type".to_string(),
        Value::String("response.create".to_string()),
    );
    Ok(Value::Object(event))
}

fn websocket_response_payload(
    payload: &Value,
) -> Result<serde_json::Map<String, Value>, DomainError> {
    let object = payload.as_object().ok_or_else(|| {
        DomainError::InvalidData("OpenAI Responses payload must be an object".to_string())
    })?;
    let mut response = object.clone();
    response.remove("stream");
    response.remove("background");
    response.remove(CHAT_COMPLETION_PROVIDER_STATE_FIELD);
    Ok(response)
}

pub(crate) fn upstream_payload(payload: &Value) -> Result<Value, DomainError> {
    let mut object = payload.as_object().cloned().ok_or_else(|| {
        DomainError::InvalidData("OpenAI Responses payload must be an object".to_string())
    })?;
    object.remove(CHAT_COMPLETION_PROVIDER_STATE_FIELD);
    Ok(Value::Object(object))
}

fn provider_session_id(payload: &Value) -> Result<Option<String>, DomainError> {
    let Some(provider_state) = payload.get(CHAT_COMPLETION_PROVIDER_STATE_FIELD) else {
        return Ok(None);
    };
    match provider_state.get("transport").and_then(Value::as_str) {
        None => return Ok(None),
        Some(OPENAI_RESPONSES_WEBSOCKET_TRANSPORT) => {}
        Some(transport) => {
            return Err(DomainError::InvalidData(format!(
                "Unsupported OpenAI Responses provider transport: {transport}"
            )));
        }
    }
    let session_id = provider_state
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            DomainError::InvalidData(
                "OpenAI Responses provider state is missing sessionId".to_string(),
            )
        })?;
    Ok(Some(session_id.to_string()))
}

fn response_from_ws_payload(payload: &[u8], operation: &str) -> Result<Option<Value>, DomainError> {
    let event = parse_ws_event(payload, operation)?;
    terminal_response_from_event(&event).map(|response| response.cloned())
}

pub(crate) fn terminal_response_from_event(event: &Value) -> Result<Option<&Value>, DomainError> {
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();

    match event_type {
        "response.completed" | "response.done" => {
            let response = event.get("response").ok_or_else(|| {
                DomainError::InternalError(
                    "OpenAI Responses completion event is missing response".to_string(),
                )
            })?;
            validate_terminal_response(response)?;
            Ok(Some(response))
        }
        "response.incomplete" => Err(response_incomplete_error(
            event.get("response").unwrap_or(event),
        )),
        "response.failed" | "error" => Err(response_failure_error(
            event.get("response").unwrap_or(event),
        )),
        _ => Ok(None),
    }
}

fn validate_terminal_response(response: &Value) -> Result<(), DomainError> {
    match response.get("status").and_then(Value::as_str) {
        None | Some("completed") => Ok(()),
        Some("incomplete") => Err(response_incomplete_error(response)),
        Some("failed") => Err(response_failure_error(response)),
        Some(status) => Err(DomainError::InternalError(format!(
            "OpenAI Responses response did not complete (status: {status})"
        ))),
    }
}

fn response_incomplete_error(response: &Value) -> DomainError {
    let reason = response
        .pointer("/incomplete_details/reason")
        .and_then(Value::as_str)
        .unwrap_or("unspecified reason");

    DomainError::InternalError(format!("OpenAI Responses response incomplete: {reason}"))
}

fn response_failure_error(response: &Value) -> DomainError {
    let message = response
        .get("error")
        .and_then(|error| error.get("message"))
        .or_else(|| response.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("OpenAI Responses response failed");

    DomainError::InternalError(message.to_string())
}

fn parse_ws_event(payload: &[u8], operation: &str) -> Result<Value, DomainError> {
    serde_json::from_slice(payload).map_err(|error| {
        log_upstream_body_parse_failure(
            "OpenAI Responses",
            operation,
            StatusCode::SWITCHING_PROTOCOLS,
            "application/json",
            payload,
            &error,
        );
        DomainError::transient(format!(
            "model.upstream_invalid_response: OpenAI Responses WebSocket event is not valid JSON ({operation}): {error}"
        ))
    })
}

pub(crate) fn parse_sse_event(payload: &[u8], operation: &str) -> Result<Value, DomainError> {
    serde_json::from_slice(payload).map_err(|error| {
        log_upstream_body_parse_failure(
            "OpenAI Responses",
            operation,
            StatusCode::OK,
            "text/event-stream",
            payload,
            &error,
        );
        DomainError::InternalError(format!(
            "model.upstream_invalid_response: OpenAI Responses stream event is not valid JSON ({operation}): {error}"
        ))
    })
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;
    use tt_ports::repositories::chat_completion_repository::AnthropicBetaHeaderMode;

    #[test]
    fn responses_ws_url_maps_http_schemes() {
        assert_eq!(
            responses_ws_url("https://api.openai.com/v1", "/responses").unwrap(),
            "wss://api.openai.com/v1/responses"
        );
        assert_eq!(
            responses_ws_url("http://localhost:8080/v1", "/responses").unwrap(),
            "ws://localhost:8080/v1/responses"
        );
    }

    #[test]
    fn response_create_event_removes_http_only_fields() {
        let mut payload = json!({
            "model": "gpt-test",
            "input": [],
            "stream": true,
            "background": false,
            "include": ["reasoning.encrypted_content"]
        });
        payload.as_object_mut().unwrap().insert(
            CHAT_COMPLETION_PROVIDER_STATE_FIELD.to_string(),
            json!({ "sessionId": "run_1" }),
        );
        let event = response_create_event(&payload).unwrap();

        assert_eq!(event["type"], json!("response.create"));
        assert!(event.get("stream").is_none());
        assert!(event.get("background").is_none());
        assert!(event.get(CHAT_COMPLETION_PROVIDER_STATE_FIELD).is_none());
        assert_eq!(event["model"], json!("gpt-test"));
        assert_eq!(event["input"], json!([]));
    }

    #[test]
    fn provider_state_selects_websocket_only_for_explicit_transport() {
        let portable = json!({
            CHAT_COMPLETION_PROVIDER_STATE_FIELD: { "sessionId": "run_1" }
        });
        assert_eq!(provider_session_id(&portable).unwrap(), None);

        let websocket = json!({
            CHAT_COMPLETION_PROVIDER_STATE_FIELD: {
                "sessionId": "run_1",
                "transport": "responses_websocket"
            }
        });
        assert_eq!(
            provider_session_id(&websocket).unwrap().as_deref(),
            Some("run_1")
        );

        let unknown = json!({
            CHAT_COMPLETION_PROVIDER_STATE_FIELD: {
                "sessionId": "run_1",
                "transport": "unknown"
            }
        });
        assert!(provider_session_id(&unknown).is_err());
    }

    #[test]
    fn websocket_request_prefers_explicit_authorization_header() {
        let config = ChatCompletionApiConfig {
            base_url: "https://api.openai.com/v1".to_string(),
            user_configured_endpoint: false,
            api_key: "secret".to_string(),
            authorization_header: Some("Bearer override".to_string()),
            vertexai_service_account_json: None,
            extra_headers: HashMap::new(),
            additional_headers: HashMap::new(),
            anthropic_beta_header_mode: AnthropicBetaHeaderMode::None,
            aws_bedrock_custom_response_path: None,
            aws_bedrock_custom_stream_path: None,
        };

        let client = Client::new();
        let request = build_ws_upgrade_request(&client, &config, "/responses", "test-key").unwrap();

        assert_eq!(
            request
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer override")
        );
        assert_eq!(
            request
                .headers()
                .get("sec-websocket-key")
                .and_then(|value| value.to_str().ok()),
            Some("test-key")
        );
    }

    #[test]
    fn non_completed_responses_fail() {
        let incomplete = json!({
            "status": "incomplete",
            "incomplete_details": { "reason": "max_output_tokens" }
        });
        let error = validate_terminal_response(&incomplete).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Internal error: OpenAI Responses response incomplete: max_output_tokens"
        );

        let failed = json!({
            "status": "failed",
            "error": { "message": "provider rejected the response" }
        });
        assert_eq!(
            validate_terminal_response(&failed).unwrap_err().to_string(),
            "Internal error: provider rejected the response"
        );
    }

    #[test]
    fn responses_stream_emits_each_completed_tool_call_once() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let mut state = ResponsesStreamState::new("gpt-5.6".to_string());

        state
            .handle_event(
                &sender,
                &json!({
                    "type": "response.created",
                    "response": { "id": "resp_1" }
                }),
            )
            .unwrap();
        state
            .handle_event(
                &sender,
                &json!({
                    "type": "response.output_item.done",
                    "output_index": 0,
                    "item": {
                        "type": "function_call",
                        "call_id": "call_weather",
                        "name": "weather",
                        "arguments": "{\"city\":\"Paris\"}"
                    }
                }),
            )
            .unwrap();
        state
            .handle_event(
                &sender,
                &json!({
                    "type": "response.completed",
                    "response": { "id": "resp_1", "status": "completed" }
                }),
            )
            .unwrap();

        let mut tool_calls = Vec::new();
        let mut saw_done = false;
        while let Ok(payload) = receiver.try_recv() {
            if payload == "[DONE]" {
                saw_done = true;
                continue;
            }
            let chunk: Value = serde_json::from_str(&payload).unwrap();
            if let Some(tool_call) = chunk.pointer("/choices/0/delta/tool_calls/0") {
                tool_calls.push(tool_call.clone());
            }
        }

        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0]["id"], json!("call_weather"));
        assert_eq!(tool_calls[0]["function"]["name"], json!("weather"));
        assert_eq!(
            tool_calls[0]["function"]["arguments"],
            json!("{\"city\":\"Paris\"}")
        );
        assert!(saw_done);
        state.ensure_completed(false).unwrap();
    }

    #[test]
    fn responses_stream_preserves_refusal_text() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let mut state = ResponsesStreamState::new("gpt-5.6".to_string());

        state
            .handle_event(
                &sender,
                &json!({ "type": "response.refusal.delta", "delta": "Cannot comply." }),
            )
            .unwrap();

        let mut content = None;
        while let Ok(payload) = receiver.try_recv() {
            let chunk: Value = serde_json::from_str(&payload).unwrap();
            content = content.or_else(|| {
                chunk
                    .pointer("/choices/0/delta/content")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });
        }
        assert_eq!(content.as_deref(), Some("Cannot comply."));
    }

    #[test]
    fn responses_stream_requires_a_terminal_event_unless_cancelled() {
        let state = ResponsesStreamState::new("gpt-5.6".to_string());

        assert!(state.ensure_completed(false).is_err());
        state.ensure_completed(true).unwrap();
    }
}

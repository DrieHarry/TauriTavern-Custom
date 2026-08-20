use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use chrono::Utc;
use futures_util::StreamExt;
use reqwest::header::ACCEPT;
use serde_json::{Map, Value, json};
use tokio::sync::RwLock;
use tt_domain::errors::DomainError;
use tt_ports::repositories::chat_completion_repository::{
    ChatCompletionApiConfig, ChatCompletionCancelReceiver,
    ChatCompletionRepositoryGenerateResponse, ChatCompletionStreamSender,
};

use super::HttpChatCompletionRepository;
use crate::codex_auth::{
    CODEX_BASE_URL, CodexAuthManager, build_codex_headers, client_version,
};

const MODEL_CACHE_TTL: Duration = Duration::from_secs(5 * 60);

static AUTH_MANAGER: LazyLock<CodexAuthManager> = LazyLock::new(CodexAuthManager::default);

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
    let auth = AUTH_MANAGER.load_auth(&client).await?;
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

    let model_items = parse_codex_models_json(&raw_json);

    let result = json!({
        "object": "list",
        "data": model_items,
    });

    {
        let mut cache_write = MODEL_CACHE.write().await;
        *cache_write = Some(ModelCache {
            expires_at: Instant::now() + MODEL_CACHE_TTL,
            models: result.clone(),
        });
    }

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

        if seen_ids.contains(id) {
            continue;
        }

        if let Some(visibility) = item.get("visibility").and_then(Value::as_str)
            && (visibility == "hidden" || visibility == "hide")
        {
            continue;
        }

        if item.get("enabled").and_then(Value::as_bool) == Some(false) {
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
    _provider_name: &str,
) -> Result<ChatCompletionRepositoryGenerateResponse, DomainError> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

    let stream_fut = generate_stream(
        repository,
        config,
        "/responses",
        payload,
        "Codex",
        sender,
        cancel_rx,
    );

    let receive_fut = async {
        let mut full_text = String::new();
        let mut full_reasoning = String::new();
        let mut tool_calls_map: HashMap<usize, (String, String, String)> = HashMap::new(); // index -> (id, name, args)

        while let Some(chunk_text) = receiver.recv().await {
            for line in chunk_text.lines() {
                let line = line.trim();
                if !line.starts_with("data:") {
                    continue;
                }
                let data_str = line.trim_start_matches("data:").trim();
                if data_str.is_empty() || data_str == "[DONE]" {
                    continue;
                }

                if let Ok(chunk_json) = serde_json::from_str::<Value>(data_str)
                    && let Some(choices) = chunk_json.get("choices").and_then(Value::as_array)
                    && let Some(first_choice) = choices.first()
                    && let Some(delta) = first_choice.get("delta")
                {
                    if let Some(content) = delta.get("content").and_then(Value::as_str) {
                        full_text.push_str(content);
                    }
                    if let Some(reasoning) = delta.get("reasoning_content").and_then(Value::as_str) {
                        full_reasoning.push_str(reasoning);
                    }
                    if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                        for tc in tool_calls {
                            let idx = tc.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
                            let entry = tool_calls_map.entry(idx).or_insert_with(|| {
                                (
                                    tc.get("id").and_then(Value::as_str).unwrap_or("").to_string(),
                                    tc.get("function")
                                        .and_then(|f| f.get("name"))
                                        .and_then(Value::as_str)
                                        .unwrap_or("")
                                        .to_string(),
                                    String::new(),
                                )
                            });
                            if let Some(id) = tc.get("id").and_then(Value::as_str)
                                && !id.is_empty()
                            {
                                entry.0 = id.to_string();
                            }
                            if let Some(fn_obj) = tc.get("function") {
                                if let Some(name) = fn_obj.get("name").and_then(Value::as_str)
                                    && !name.is_empty()
                                {
                                    entry.1 = name.to_string();
                                }
                                if let Some(args) = fn_obj.get("arguments").and_then(Value::as_str) {
                                    entry.2.push_str(args);
                                }
                            }
                        }
                    }
                }
            }
        }

        (full_text, full_reasoning, tool_calls_map)
    };

    let (stream_result, (full_text, full_reasoning, mut tool_calls_map)) =
        tokio::join!(stream_fut, receive_fut);
    stream_result?;

    let model = payload
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("gpt-5.1");
    let completion_id = format!("chatcmpl-codex-{}", Utc::now().timestamp_millis());

    let mut tool_calls_vec = Vec::new();
    let mut sorted_indices: Vec<_> = tool_calls_map.keys().copied().collect();
    sorted_indices.sort_unstable();
    for idx in sorted_indices {
        if let Some((id, name, args)) = tool_calls_map.remove(&idx) {
            tool_calls_vec.push(json!({
                "id": id,
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": args,
                }
            }));
        }
    }

    let finish_reason = if !tool_calls_vec.is_empty() {
        "tool_calls"
    } else {
        "stop"
    };

    let mut message_obj = json!({
        "role": "assistant",
        "content": full_text,
    });

    if !full_reasoning.is_empty() {
        message_obj["reasoning_content"] = Value::String(full_reasoning);
    }
    if !tool_calls_vec.is_empty() {
        message_obj["tool_calls"] = Value::Array(tool_calls_vec);
    }

    let response_body = json!({
        "id": completion_id,
        "object": "chat.completion",
        "created": Utc::now().timestamp(),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": message_obj,
                "finish_reason": finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }
    });

    Ok(ChatCompletionRepositoryGenerateResponse::from_body(response_body))
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
    let auth = AUTH_MANAGER.load_auth(&client).await?;
    let version = client_version();
    let headers = build_codex_headers(&auth, Some(&version), true)?;

    let model = payload
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            DomainError::InvalidData("A Codex/ChatGPT model name is required.".to_string())
        })?;

    let include_reasoning = payload
        .get("include_reasoning")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let converted_messages = convert_messages(payload.get("messages").and_then(Value::as_array));
    let request_body = build_codex_request_body(payload, model, converted_messages);

    let url = format!("{CODEX_BASE_URL}/responses");
    let response = client
        .post(&url)
        .headers(headers)
        .header(ACCEPT, "text/event-stream")
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
            "Codex stream request failed",
        )
        .await);
    }

    let completion_id = format!("chatcmpl-codex-{}", Utc::now().timestamp_millis());

    // Send initial assistant chunk
    let initial_chunk = json!({
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": Utc::now().timestamp(),
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": {
                    "role": "assistant",
                }
            }
        ]
    });
    let _ = sender.send(serde_json::to_string(&initial_chunk).unwrap_or_default());

    let mut stream = response.bytes_stream();
    let mut buffer = String::new();
    let mut current_event_type = String::new();

    let mut saw_reasoning_text = false;
    let mut tool_calls: HashMap<String, ToolCallState> = HashMap::new(); // call_id -> state
    let mut tool_item_to_id: HashMap<String, String> = HashMap::new(); // item_id -> call_id

    loop {
        if *cancel.borrow() {
            return Ok(());
        }

        let chunk = tokio::select! {
            _ = cancel.changed() => {
                if *cancel.borrow() {
                    return Ok(());
                }
                continue;
            }
            chunk = stream.next() => chunk,
        };

        let Some(chunk_result) = chunk else {
            break;
        };

        let chunk = chunk_result.map_err(|error| {
            DomainError::InternalError(format!("Error reading Codex response stream: {error}"))
        })?;

        let text = String::from_utf8_lossy(&chunk);
        buffer.push_str(&text);

        while let Some(newline_pos) = buffer.find('\n') {
            let line = buffer[..newline_pos].trim_end_matches('\r').to_string();
            buffer.drain(..=newline_pos);

            let line = line.trim();
            if line.is_empty() {
                current_event_type.clear();
                continue;
            }

            if let Some(event_name) = line.strip_prefix("event:") {
                current_event_type = event_name.trim().to_string();
                continue;
            }

            if let Some(data_str) = line.strip_prefix("data:") {
                let data_str = data_str.trim();
                if data_str.is_empty() {
                    continue;
                }

                if data_str == "[DONE]" {
                    break;
                }

                if let Ok(event_json) = serde_json::from_str::<Value>(data_str) {
                    let event_type = event_json
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or(&current_event_type);

                    process_codex_event(
                        event_type,
                        &event_json,
                        &completion_id,
                        model,
                        include_reasoning,
                        &mut saw_reasoning_text,
                        &mut tool_calls,
                        &mut tool_item_to_id,
                        &sender,
                    );
                }
            }
        }
    }

    // Send final finish chunk
    let finish_reason = if tool_calls.is_empty() {
        "stop"
    } else {
        "tool_calls"
    };

    let final_chunk = json!({
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": Utc::now().timestamp(),
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": {},
                "finish_reason": finish_reason,
            }
        ]
    });
    let _ = sender.send(serde_json::to_string(&final_chunk).unwrap_or_default());
    let _ = sender.send("[DONE]".to_string());

    Ok(())
}

#[derive(Default)]
struct ToolCallState {
    index: usize,
    call_id: String,
    name: String,
    announced: bool,
}

#[allow(clippy::too_many_arguments)]
fn process_codex_event(
    event_type: &str,
    event: &Value,
    completion_id: &str,
    model: &str,
    include_reasoning: bool,
    saw_reasoning_text: &mut bool,
    tool_calls: &mut HashMap<String, ToolCallState>,
    tool_item_to_id: &mut HashMap<String, String>,
    sender: &ChatCompletionStreamSender,
) {
    match event_type {
        "response.output_text.delta"
        | "response.text.delta"
        | "response.refusal.delta" => {
            if let Some(delta) = event.get("delta").and_then(Value::as_str)
                && !delta.is_empty()
            {
                let chunk = json!({
                    "id": completion_id,
                    "object": "chat.completion.chunk",
                    "created": Utc::now().timestamp(),
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "content": delta,
                            }
                        }
                    ]
                });
                let _ = sender.send(serde_json::to_string(&chunk).unwrap_or_default());
            }
        }
        "response.reasoning_text.delta" => {
            if let Some(delta) = event.get("delta").and_then(Value::as_str)
                && !delta.is_empty()
            {
                *saw_reasoning_text = true;
                if include_reasoning {
                    let chunk = json!({
                        "id": completion_id,
                        "object": "chat.completion.chunk",
                        "created": Utc::now().timestamp(),
                        "model": model,
                        "choices": [
                            {
                                "index": 0,
                                "delta": {
                                    "reasoning_content": delta,
                                    "reasoning": delta,
                                }
                            }
                        ]
                    });
                    let _ = sender.send(serde_json::to_string(&chunk).unwrap_or_default());
                }
            }
        }
        "response.reasoning_summary_text.delta" => {
            if let Some(delta) = event.get("delta").and_then(Value::as_str)
                && !delta.is_empty()
                && !*saw_reasoning_text
                && include_reasoning
            {
                let chunk = json!({
                    "id": completion_id,
                    "object": "chat.completion.chunk",
                    "created": Utc::now().timestamp(),
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "reasoning_content": delta,
                                "reasoning": delta,
                            }
                        }
                    ]
                });
                let _ = sender.send(serde_json::to_string(&chunk).unwrap_or_default());
            }
        }
        "response.output_item.added" => {
            if let Some(item) = event.get("item")
                && item.get("type").and_then(Value::as_str) == Some("function_call")
            {
                let call_id = item
                    .get("call_id")
                    .or_else(|| item.get("id"))
                    .or_else(|| event.get("item_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("call_0")
                    .to_string();

                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();

                let index = tool_calls.len();
                let state = tool_calls.entry(call_id.clone()).or_insert_with(|| ToolCallState {
                    index,
                    call_id: call_id.clone(),
                    name: name.clone(),
                    announced: false,
                });

                if let Some(item_id) = item.get("id").and_then(Value::as_str) {
                    tool_item_to_id.insert(item_id.to_string(), call_id.clone());
                }
                if let Some(item_id) = event.get("item_id").and_then(Value::as_str) {
                    tool_item_to_id.insert(item_id.to_string(), call_id.clone());
                }

                if !state.announced {
                    state.announced = true;
                    let chunk = json!({
                        "id": completion_id,
                        "object": "chat.completion.chunk",
                        "created": Utc::now().timestamp(),
                        "model": model,
                        "choices": [
                            {
                                "index": 0,
                                "delta": {
                                    "tool_calls": [
                                        {
                                            "index": state.index,
                                            "id": state.call_id,
                                            "type": "function",
                                            "function": {
                                                "name": state.name,
                                                "arguments": "",
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    });
                    let _ = sender.send(serde_json::to_string(&chunk).unwrap_or_default());
                }
            }
        }
        "response.function_call_arguments.delta" => {
            let item_key = event
                .get("item_id")
                .or_else(|| event.get("call_id"))
                .and_then(Value::as_str)
                .unwrap_or("");

            let call_id = tool_item_to_id
                .get(item_key)
                .cloned()
                .unwrap_or_else(|| item_key.to_string());

            let name = event
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();

            let index = tool_calls.len();
            let state = tool_calls.entry(call_id.clone()).or_insert_with(|| ToolCallState {
                index,
                call_id: call_id.clone(),
                name,
                announced: false,
            });

            let delta = event
                .get("delta")
                .and_then(Value::as_str)
                .unwrap_or("");

            if !delta.is_empty() {
                let chunk = json!({
                    "id": completion_id,
                    "object": "chat.completion.chunk",
                    "created": Utc::now().timestamp(),
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "tool_calls": [
                                    {
                                        "index": state.index,
                                        "function": {
                                            "arguments": delta,
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                });
                let _ = sender.send(serde_json::to_string(&chunk).unwrap_or_default());
            }
        }
        _ => {}
    }
}

struct ConvertedMessages {
    instructions: Option<String>,
    input: Vec<Value>,
}

fn convert_messages(messages: Option<&Vec<Value>>) -> ConvertedMessages {
    let mut instructions = Vec::new();
    let mut encountered_conversation = false;
    let mut input = Vec::new();

    for msg in messages.into_iter().flatten() {
        let role = msg.get("role").and_then(Value::as_str).unwrap_or("");

        if role == "system" || role == "developer" {
            let text = get_text_content(msg.get("content"));
            if !text.is_empty() {
                if !encountered_conversation {
                    instructions.push(text);
                } else {
                    input.push(json!({
                        "role": role,
                        "content": text,
                    }));
                }
            }
            continue;
        }

        encountered_conversation = true;

        if role == "tool" {
            let call_id = msg
                .get("tool_call_id")
                .or_else(|| msg.get("call_id"))
                .or_else(|| msg.get("id"))
                .and_then(Value::as_str)
                .unwrap_or("");

            if !call_id.is_empty() {
                input.push(json!({
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": stringify_content(msg.get("content")),
                }));
            }
            continue;
        }

        if role == "assistant" {
            let content = convert_message_content(msg.get("content"));
            if !content_is_empty(&content) {
                input.push(json!({
                    "role": "assistant",
                    "content": content,
                }));
            }

            if let Some(tool_calls) = msg.get("tool_calls").and_then(Value::as_array) {
                for tc in tool_calls {
                    if tc.get("type").and_then(Value::as_str) == Some("function")
                        && let Some(fn_obj) = tc.get("function")
                        && let Some(name) = fn_obj.get("name").and_then(Value::as_str)
                    {
                        let call_id = tc
                            .get("id")
                            .or_else(|| tc.get("call_id"))
                            .and_then(Value::as_str)
                            .unwrap_or("call_0");
                        let args = stringify_content(fn_obj.get("arguments"));
                        input.push(json!({
                            "type": "function_call",
                            "call_id": call_id,
                            "name": name,
                            "arguments": args,
                            "status": "completed",
                        }));
                    }
                }
            }
            continue;
        }

        if role == "user" {
            let content = convert_message_content(msg.get("content"));
            if !content_is_empty(&content) {
                input.push(json!({
                    "role": "user",
                    "content": content,
                }));
            }
        }
    }

    if input.is_empty() {
        input.push(json!({
            "role": "user",
            "content": "Continue.",
        }));
    }

    let instructions_str = if !instructions.is_empty() {
        Some(instructions.join("\n\n"))
    } else {
        None
    };

    ConvertedMessages {
        instructions: instructions_str,
        input,
    }
}

fn content_is_empty(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(s) => s.trim().is_empty(),
        Value::Array(arr) => arr.is_empty(),
        _ => false,
    }
}

fn get_text_content(content: Option<&Value>) -> String {
    let Some(content) = content else {
        return String::new();
    };

    match content {
        Value::String(s) => s.to_string(),
        Value::Array(arr) => {
            let mut parts = Vec::new();
            for item in arr {
                if let Some(text) = item.get("text").and_then(Value::as_str) {
                    parts.push(text);
                } else if let Some(s) = item.as_str() {
                    parts.push(s);
                }
            }
            parts.join("\n")
        }
        _ => stringify_content(Some(content)),
    }
}

fn stringify_content(content: Option<&Value>) -> String {
    let Some(content) = content else {
        return String::new();
    };
    match content {
        Value::String(s) => s.clone(),
        _ => serde_json::to_string(content).unwrap_or_default(),
    }
}

fn convert_message_content(content: Option<&Value>) -> Value {
    let Some(content) = content else {
        return Value::String(String::new());
    };

    match content {
        Value::String(s) => Value::String(s.clone()),
        Value::Array(arr) => {
            let mut converted_parts = Vec::new();
            let mut all_text = true;

            for part in arr {
                if let Some(s) = part.as_str() {
                    converted_parts.push(json!({
                        "type": "input_text",
                        "text": s,
                    }));
                } else if let Some(obj) = part.as_object() {
                    let part_type = obj.get("type").and_then(Value::as_str).unwrap_or("");
                    if part_type == "text" || part_type == "input_text" || part_type == "output_text" {
                        if let Some(text) = obj.get("text").and_then(Value::as_str) {
                            converted_parts.push(json!({
                                "type": "input_text",
                                "text": text,
                            }));
                        }
                    } else if part_type == "image_url" || part_type == "input_image" {
                        all_text = false;
                        let image_url = obj
                            .get("image_url")
                            .and_then(|u| u.as_str().or_else(|| u.get("url").and_then(Value::as_str)))
                            .or_else(|| obj.get("url").and_then(Value::as_str));
                        let file_id = obj
                            .get("file_id")
                            .or_else(|| obj.get("image_url").and_then(|u| u.get("file_id")))
                            .and_then(Value::as_str);

                        if image_url.is_some() || file_id.is_some() {
                            let mut item = json!({ "type": "input_image" });
                            if let Some(url) = image_url {
                                item["image_url"] = Value::String(url.to_string());
                            }
                            if let Some(id) = file_id {
                                item["file_id"] = Value::String(id.to_string());
                            }
                            let detail = obj
                                .get("detail")
                                .or_else(|| obj.get("image_url").and_then(|u| u.get("detail")))
                                .and_then(Value::as_str)
                                .unwrap_or("auto");
                            item["detail"] = Value::String(detail.to_string());
                            converted_parts.push(item);
                        }
                    } else if part_type == "file" || part_type == "input_file" {
                        all_text = false;
                        let mut item = json!({ "type": "input_file" });
                        if let Some(data) = obj.get("file_data").or_else(|| obj.get("data")).and_then(Value::as_str) {
                            item["file_data"] = Value::String(data.to_string());
                        }
                        if let Some(id) = obj.get("file_id").or_else(|| obj.get("id")).and_then(Value::as_str) {
                            item["file_id"] = Value::String(id.to_string());
                        }
                        if let Some(url) = obj.get("file_url").or_else(|| obj.get("url")).and_then(Value::as_str) {
                            item["file_url"] = Value::String(url.to_string());
                        }
                        if let Some(name) = obj.get("filename").or_else(|| obj.get("name")).and_then(Value::as_str) {
                            item["filename"] = Value::String(name.to_string());
                        }
                        if item.as_object().is_some_and(|o| o.len() > 1) {
                            converted_parts.push(item);
                        }
                    }
                }
            }

            if all_text && !converted_parts.is_empty() {
                let joined = converted_parts
                    .iter()
                    .filter_map(|p| p.get("text").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("\n");
                Value::String(joined)
            } else {
                Value::Array(converted_parts)
            }
        }
        _ => Value::String(stringify_content(Some(content))),
    }
}

fn codex_model_supports_maximum_reasoning(model: &str) -> bool {
    let lower = model.to_ascii_lowercase();
    lower.starts_with("gpt-5.6")
}

fn normalize_reasoning_effort(raw: Option<&str>, model: &str) -> Option<String> {
    let raw = raw?.trim().to_ascii_lowercase();
    if raw.is_empty() || raw == "auto" {
        return None;
    }

    let normalized = match raw.as_str() {
        "min" | "minimum" => "none",
        "maximum" => "max",
        "extra-high" => "xhigh",
        other => other,
    };

    if normalized == "max" {
        return Some(if codex_model_supports_maximum_reasoning(model) {
            "max".to_string()
        } else {
            "xhigh".to_string()
        });
    }

    let allowed: HashSet<&str> = ["none", "low", "medium", "high", "xhigh"].into_iter().collect();
    if allowed.contains(normalized) {
        Some(normalized.to_string())
    } else {
        None
    }
}

fn build_codex_request_body(
    payload: &Value,
    model: &str,
    converted: ConvertedMessages,
) -> Value {
    let mut request_body = json!({
        "input": converted.input,
        "model": model,
        "store": false,
        "stream": true,
    });

    if let Some(instructions) = converted.instructions {
        request_body["instructions"] = Value::String(instructions);
    }

    let raw_effort = payload.get("reasoning_effort").and_then(Value::as_str);
    let reasoning_effort = normalize_reasoning_effort(raw_effort, model);
    let include_reasoning = payload.get("include_reasoning").and_then(Value::as_bool) == Some(true);

    if reasoning_effort.is_some() || include_reasoning {
        let mut reasoning_obj = Map::new();
        if let Some(effort) = reasoning_effort {
            reasoning_obj.insert("effort".to_string(), Value::String(effort));
        }
        if include_reasoning {
            reasoning_obj.insert("summary".to_string(), Value::String("detailed".to_string()));
        }
        request_body["reasoning"] = Value::Object(reasoning_obj);
    }

    let mut tools_vec = Vec::new();
    if let Some(tools_arr) = payload.get("tools").and_then(Value::as_array) {
        for t in tools_arr {
            if let Some(obj) = t.as_object() {
                let tool_type = obj.get("type").and_then(Value::as_str).unwrap_or("");
                if tool_type == "function"
                    && let Some(fn_obj) = obj.get("function")
                    && let Some(name) = fn_obj.get("name").and_then(Value::as_str)
                {
                    let mut converted_tool = json!({
                        "type": "function",
                        "name": name,
                        "description": fn_obj.get("description").and_then(Value::as_str).unwrap_or(""),
                        "parameters": fn_obj.get("parameters").cloned().unwrap_or_else(|| json!({"type": "object", "properties": {}})),
                    });
                    if let Some(strict) = fn_obj.get("strict").and_then(Value::as_bool) {
                        converted_tool["strict"] = Value::Bool(strict);
                    }
                    tools_vec.push(converted_tool);
                } else if ["web_search", "web_search_preview", "image_generation", "file_search", "code_interpreter", "computer_use_preview", "mcp", "custom"].contains(&tool_type) {
                    tools_vec.push(t.clone());
                }
            }
        }
    }

    if payload.get("enable_web_search").and_then(Value::as_bool) == Some(true) {
        let has_web_search = tools_vec.iter().any(|t| {
            t.get("type").and_then(Value::as_str).is_some_and(|tp| tp == "web_search" || tp == "web_search_preview")
        });
        if !has_web_search {
            tools_vec.insert(0, json!({ "type": "web_search" }));
        }
    }

    if payload.get("request_images").and_then(Value::as_bool) == Some(true) {
        let has_image_gen = tools_vec.iter().any(|t| {
            t.get("type").and_then(Value::as_str) == Some("image_generation")
        });
        if !has_image_gen {
            let mut image_tool = json!({ "type": "image_generation" });
            if let Some(res) = payload.get("request_image_resolution").and_then(Value::as_str) {
                image_tool["size"] = Value::String(res.to_string());
            }
            if let Some(aspect) = payload.get("request_image_aspect_ratio").and_then(Value::as_str) {
                image_tool["aspect_ratio"] = Value::String(aspect.to_string());
            }
            tools_vec.push(image_tool);
        }
    }

    if !tools_vec.is_empty() {
        if tools_vec.iter().any(|t| {
            t.get("type").and_then(Value::as_str).is_some_and(|tp| tp == "web_search" || tp == "web_search_preview")
        }) {
            request_body["include"] = json!(["web_search_call.action.sources"]);
        }
        request_body["tools"] = Value::Array(tools_vec);

        if let Some(tool_choice) = payload.get("tool_choice") {
            if let Some(s) = tool_choice.as_str() {
                if ["auto", "none", "required"].contains(&s) {
                    request_body["tool_choice"] = Value::String(s.to_string());
                }
            } else if let Some(fn_choice) = tool_choice.get("function").and_then(|f| f.get("name")).and_then(Value::as_str) {
                request_body["tool_choice"] = json!({
                    "type": "function",
                    "name": fn_choice,
                });
            }
        }
    }

    if let Some(parallel) = payload.get("parallel_tool_calls").and_then(Value::as_bool) {
        request_body["parallel_tool_calls"] = Value::Bool(parallel);
    }

    let mut text_obj = Map::new();
    if let Some(verbosity) = payload.get("verbosity").and_then(Value::as_str) {
        let v = verbosity.trim().to_ascii_lowercase();
        if ["low", "medium", "high"].contains(&v.as_str()) {
            text_obj.insert("verbosity".to_string(), Value::String(v));
        }
    }

    if let Some(schema_val) = payload.get("json_schema").and_then(|j| j.get("value")) {
        let name = payload.get("json_schema").and_then(|j| j.get("name")).and_then(Value::as_str).unwrap_or("response");
        let strict = payload.get("json_schema").and_then(|j| j.get("strict")).and_then(Value::as_bool).unwrap_or(true);
        text_obj.insert("format".to_string(), json!({
            "type": "json_schema",
            "name": name,
            "schema": schema_val,
            "strict": strict,
        }));
    } else if let Some(response_format) = payload.get("response_format") {
        if response_format.get("type").and_then(Value::as_str) == Some("json_schema")
            && let Some(schema_obj) = response_format.get("json_schema")
        {
            let name = schema_obj.get("name").and_then(Value::as_str).unwrap_or("response");
            let schema = schema_obj.get("schema").or_else(|| schema_obj.get("value")).unwrap_or(&Value::Null);
            let strict = schema_obj.get("strict").and_then(Value::as_bool).unwrap_or(true);
            text_obj.insert("format".to_string(), json!({
                "type": "json_schema",
                "name": name,
                "schema": schema,
                "strict": strict,
            }));
        } else if response_format.get("type").and_then(Value::as_str) == Some("json_object") {
            text_obj.insert("format".to_string(), json!({
                "type": "json_object"
            }));
        }
    }

    if !text_obj.is_empty() {
        request_body["text"] = Value::Object(text_obj);
    }

    request_body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_messages_instructions_and_dialogue() {
        let messages = vec![
            json!({
                "role": "system",
                "content": "You are a helpful assistant."
            }),
            json!({
                "role": "user",
                "content": "Hello!"
            }),
            json!({
                "role": "assistant",
                "content": "Hi there!"
            }),
            json!({
                "role": "user",
                "content": "What's the weather?"
            }),
        ];

        let converted = convert_messages(Some(&messages));
        assert_eq!(
            converted.instructions.as_deref(),
            Some("You are a helpful assistant.")
        );
        assert_eq!(converted.input.len(), 3);
        assert_eq!(converted.input[0]["role"], "user");
        assert_eq!(converted.input[0]["content"], "Hello!");
        assert_eq!(converted.input[1]["role"], "assistant");
        assert_eq!(converted.input[1]["content"], "Hi there!");
        assert_eq!(converted.input[2]["role"], "user");
        assert_eq!(converted.input[2]["content"], "What's the weather?");
    }

    #[test]
    fn test_convert_messages_tools_and_tool_outputs() {
        let messages = vec![
            json!({
                "role": "assistant",
                "content": "Calling tool",
                "tool_calls": [
                    {
                        "id": "call_123",
                        "type": "function",
                        "function": {
                            "name": "get_weather",
                            "arguments": "{\"location\":\"Tokyo\"}"
                        }
                    }
                ]
            }),
            json!({
                "role": "tool",
                "tool_call_id": "call_123",
                "content": "{\"temp\": 25}"
            }),
        ];

        let converted = convert_messages(Some(&messages));
        assert_eq!(converted.input.len(), 3); // assistant message, function_call, function_call_output
        assert_eq!(converted.input[0]["role"], "assistant");
        assert_eq!(converted.input[0]["content"], "Calling tool");
        assert_eq!(converted.input[1]["type"], "function_call");
        assert_eq!(converted.input[1]["call_id"], "call_123");
        assert_eq!(converted.input[1]["name"], "get_weather");
        assert_eq!(converted.input[2]["type"], "function_call_output");
        assert_eq!(converted.input[2]["call_id"], "call_123");
        assert_eq!(converted.input[2]["output"], "{\"temp\": 25}");
    }

    #[test]
    fn test_normalize_reasoning_effort() {
        assert_eq!(normalize_reasoning_effort(Some("auto"), "gpt-5.1"), None);
        assert_eq!(
            normalize_reasoning_effort(Some("min"), "gpt-5.1").as_deref(),
            Some("none")
        );
        assert_eq!(
            normalize_reasoning_effort(Some("minimum"), "gpt-5.1").as_deref(),
            Some("none")
        );
        assert_eq!(
            normalize_reasoning_effort(Some("low"), "gpt-5.1").as_deref(),
            Some("low")
        );
        assert_eq!(
            normalize_reasoning_effort(Some("high"), "gpt-5.1").as_deref(),
            Some("high")
        );
        assert_eq!(
            normalize_reasoning_effort(Some("xhigh"), "gpt-5.1").as_deref(),
            Some("xhigh")
        );
        // max on gpt-5.1 -> xhigh
        assert_eq!(
            normalize_reasoning_effort(Some("max"), "gpt-5.1").as_deref(),
            Some("xhigh")
        );
        // max on gpt-5.6 -> max
        assert_eq!(
            normalize_reasoning_effort(Some("max"), "gpt-5.6-luna").as_deref(),
            Some("max")
        );
    }

    #[test]
    fn test_build_codex_request_body() {
        let payload = json!({
            "model": "gpt-5.1",
            "messages": [
                { "role": "system", "content": "Be concise." },
                { "role": "user", "content": "Ping" }
            ],
            "reasoning_effort": "high",
            "include_reasoning": true,
            "enable_web_search": true,
        });

        let converted = convert_messages(payload.get("messages").and_then(Value::as_array));
        let body = build_codex_request_body(&payload, "gpt-5.1", converted);

        assert_eq!(body["model"], "gpt-5.1");
        assert_eq!(body["instructions"], "Be concise.");
        assert_eq!(body["reasoning"]["effort"], "high");
        assert_eq!(body["reasoning"]["summary"], "detailed");
        assert_eq!(body["tools"][0]["type"], "web_search");
        assert_eq!(body["include"], json!(["web_search_call.action.sources"]));
    }

    #[test]
    fn test_parse_codex_models_json() {
        let raw = json!({
            "models": [
                {
                    "slug": "gpt-5.6",
                    "display_name": "GPT-5.6",
                    "visibility": "visible",
                    "enabled": true
                },
                {
                    "slug": "gpt-5.1",
                    "display_name": "GPT-5.1",
                    "visibility": "visible"
                },
                {
                    "slug": "hidden-model",
                    "visibility": "hidden"
                },
                {
                    "slug": "disabled-model",
                    "enabled": false
                }
            ]
        });

        let parsed = parse_codex_models_json(&raw);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["id"], "gpt-5.1");
        assert_eq!(parsed[0]["name"], "GPT-5.1");
        assert_eq!(parsed[1]["id"], "gpt-5.6");
        assert_eq!(parsed[1]["name"], "GPT-5.6");
    }
}
